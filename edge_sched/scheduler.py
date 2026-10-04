"""调度器核心：事件循环派发 + 工作线程执行 + 背压 + 可选优先级。

结构:

- submit / submit_nowait 在调用方线程通过校验后，任务进入有界的入站堆
  （按 priority 降序，同优先级按接受先后 FCFS）；submit 阻塞等待结果，
  submit_nowait 立即返回 TaskHandle。
- 一个事件循环线程先取得空闲工作线程许可，再从入站堆中选取当前最高优先级
  的未取消任务派发到就绪队列——选择发生在取得许可之后，因此先入队的低
  优先级任务不会挡住后到的高优先级任务。
- 工作线程执行无参数 callable，记录排队/总延迟，唤醒等待方。
- 未完成（已接受但未结束）任务数达到 ``max_pending`` 时触发背压拒绝。
- 优先级只影响“已接受但尚未开始执行”的任务进入工作线程的顺序；执行中、
  已完成、已取消、已拒绝任务一律不重排，任务只执行一次。
- 已接受但尚未开始执行的任务可通过 submit_nowait 返回的 TaskHandle.cancel
  取消；取消与开始执行在同一把锁上决出唯一结果。
- close 先令新 submit 失败，再等待全部已接受任务结束，最后停止线程；
  已取消任务不阻塞关闭，已完成/已取消任务的结果在关闭后仍可读取。
"""

from __future__ import annotations

import heapq
import queue
import threading
import time
from typing import Any, Callable, Optional, TypeVar

from .errors import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    SchedulerClosedError,
    TaskCancelledError,
)
from .stats import Stats, StatsSnapshot, to_ms

T = TypeVar("T")

# 就绪队列中的停止哨兵；工作线程取到即退出。
_SENTINEL = object()

# 事件循环在空闲时轮询停止信号的间隔（秒）。
_DISPATCH_POLL = 0.05


class _TaskEntry:
    """一个任务的全部可变状态。

    取消与执行的唯一裁决依赖两个受 Scheduler._cond 保护的标志：

    - ``started``：工作线程已在锁内认领任务、即将执行 callable。
    - ``cancelled``：任务已在锁内被取消，进入取消终态。

    二者在同一把锁上以先到者为准，故每个任务只可能有一种终态。
    """

    __slots__ = (
        "task_id",
        "fn",
        "priority",
        "seq",
        "submit_time",
        "done",
        "success",
        "value",
        "exception",
        "started",
        "cancelled",
    )

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 submit_time: float, priority: int, seq: int) -> None:
        self.task_id = task_id
        self.fn = fn
        self.priority = priority
        # 接受序号：同优先级时严格按接受先后（FCFS）派发。
        self.seq = seq
        self.submit_time = submit_time
        self.done = threading.Event()
        self.success = False
        self.value: Any = None
        self.exception: Optional[BaseException] = None
        self.started = False
        self.cancelled = False


class TaskHandle:
    """非阻塞提交（:meth:`Scheduler.submit_nowait`）返回的任务句柄。

    句柄绑定到提交时创建的任务条目：即使 task_id 之后被复用，
    通过该句柄读到的仍是本次提交的结果。
    """

    __slots__ = ("_scheduler", "_entry")

    def __init__(self, scheduler: "Scheduler", entry: "_TaskEntry") -> None:
        self._scheduler = scheduler
        self._entry = entry

    def done(self) -> bool:
        """任务已结束（成功、失败或被取消）返回 True，否则 False。"""
        return self._entry.done.is_set()

    def cancel(self) -> bool:
        """尝试取消尚未开始执行的任务。

        - 任务尚未开始：取消成功，callable 完全不执行，返回 True；
          此后 :meth:`done` 为 True，:meth:`result` 抛
          :class:`TaskCancelledError`。
        - 任务已经开始执行或已有终态（含已被取消）：返回 False，
          原终态与结果不受影响。

        与“开始执行”竞争时，由调度器在同一原子边界决定唯一结果。
        """
        return self._scheduler._cancel(self._entry)

    def result(self, timeout: Optional[float] = None) -> Any:
        """等待并读取任务结果。

        - 成功：返回 callable 的返回值；失败：抛出 callable 抛出的原始异常。
        - 被取消：抛出 :class:`TaskCancelledError`。
        - ``timeout`` 为 None 时一直等待；有限超时内未结束则抛
          :class:`TimeoutError`，任务本身继续执行，之后仍可再次读取结果。
        """
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )
        entry = self._entry
        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds"
                % (entry.task_id, timeout)
            )
        if entry.success:
            return entry.value
        assert entry.exception is not None
        raise entry.exception


class Scheduler:
    """高并发任务调度器。

    派发顺序：任务按 ``priority`` 降序派发（数值大者先进入工作线程），
    相同优先级按接受先后派发；缺省优先级均为 0，即整体退化为 FCFS。
    优先级只影响已接受但尚未开始执行的任务，不重排执行中或已终态任务。

    :param workers: 工作线程数，必须为 >= 1 的整数。
    :param max_pending: 未完成任务（含排队中与执行中）上限，必须为 >= 1 的整数。
    """

    def __init__(self, workers: int, max_pending: int) -> None:
        if not self._is_positive_int(workers):
            raise InputValidationError(
                "workers must be an integer >= 1, got %r" % (workers,)
            )
        if not self._is_positive_int(max_pending):
            raise InputValidationError(
                "max_pending must be an integer >= 1, got %r" % (max_pending,)
            )
        self._workers = workers
        self._max_pending = max_pending

        # _cond 同时承担状态锁：_closing/_closed/_pending/_unfinished 的读写。
        self._cond = threading.Condition()
        self._closing = False
        self._closed = False
        self._pending = 0
        self._unfinished: set[str] = set()
        # 全部已接受任务的条目长期保留，关闭后结果仍可读取。
        self._tasks: dict[str, _TaskEntry] = {}

        self._stats = Stats()
        # 入站堆：项为 (-priority, 接受序号, 条目)——priority 数值大者先派发，
        # 同优先级按接受序号小者（先接受）先派发。
        self._inbound_cond = threading.Condition()
        self._inbound: list[tuple[int, int, "_TaskEntry"]] = []
        self._accept_seq = 0
        self._ready: "queue.Queue[Any]" = queue.Queue()
        # 空闲工作线程许可：事件循环派发前必须先取得一个许可。
        self._worker_slots = threading.BoundedSemaphore(workers)
        self._stop_dispatcher = threading.Event()

        self._dispatcher = threading.Thread(
            target=self._run_dispatcher, name="edge-sched-loop", daemon=True
        )
        self._worker_threads = [
            threading.Thread(
                target=self._run_worker,
                name="edge-sched-worker-%d" % i,
                daemon=True,
            )
            for i in range(workers)
        ]
        self._dispatcher.start()
        for t in self._worker_threads:
            t.start()

    @staticmethod
    def _is_positive_int(value: Any) -> bool:
        # bool 是 int 的子类，但并发参数不接受布尔值。
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1

    @staticmethod
    def _is_priority(value: Any) -> bool:
        # priority 接受任意整数（含负数），但 bool 不是合法优先级。
        return isinstance(value, int) and not isinstance(value, bool)

    # ------------------------------------------------------------------ public

    def submit(self, task_id: str, fn: Callable[[], T],
               timeout: Optional[float] = None,
               priority: int = 0) -> T:
        """提交任务并阻塞等待结果。

        结果语义（每个任务只可能有一种）:

        - 成功：返回 callable 的返回值。
        - 失败：抛出 callable 抛出的**原始异常**；任务计入 failed。
        - 未完成任务数已达上限：抛 :class:`BackpressureError`，无结果无统计。
        - task_id 与未完成任务重复：抛 :class:`DuplicateTaskError`。
        - 调度器已关闭：抛 :class:`SchedulerClosedError`。
        - 参数非法（含 priority 非整数或为布尔值）：抛
          :class:`InputValidationError`。
        - 等待超过 ``timeout`` 秒：抛 :class:`TimeoutError`，任务仍继续执行，
          其最终结果不受影响。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param timeout: 可选等待超时（秒），位置与语义保持不变，
            None 表示一直等待。
        :param priority: 可选整数优先级，缺省 0；数值越大越早被派发给
            工作线程，相同数值按接受先后派发。优先级只影响尚未开始执行
            的任务的派发顺序，不抢占执行中的任务。
        """
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )
        entry = self._admit(task_id, fn, priority)

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def submit_nowait(self, task_id: str, fn: Callable[[], T],
                      priority: int = 0) -> TaskHandle:
        """提交任务并立即返回句柄，不等待 callable 执行结束。

        准入校验与 :meth:`submit` 完全一致（参数校验、priority 校验、关闭、
        重复 task_id、背压），通过后任务进入同一事件循环，由工作线程按
        “priority 降序、同优先级按接受先后”派发；返回前 accepted 计数已更新。
        任务进度与结果通过返回的 :class:`TaskHandle` 查询。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param priority: 可选整数优先级，缺省 0，语义同 :meth:`submit`。
        """
        entry = self._admit(task_id, fn, priority)
        return TaskHandle(self, entry)

    def result(self, task_id: str) -> Any:
        """读取一个已结束任务的结果（非阻塞）。

        - 成功：返回 callable 的返回值；失败：重新抛出其原始异常。
        - 任务在开始前被取消：抛 :class:`TaskCancelledError`。
        - 任务尚未结束：抛 :class:`RuntimeError`。
        - 从未接受过该 task_id：抛 :class:`KeyError`。

        task_id 曾被复用时读取最近一次提交的结果；调度器关闭后
        已结束任务的结果仍可通过本方法读取。
        """
        with self._cond:
            entry = self._tasks.get(task_id)
            if entry is None:
                raise KeyError(task_id)
            if not entry.done.is_set():
                raise RuntimeError("task %r is not finished" % (task_id,))
        if entry.success:
            return entry.value
        assert entry.exception is not None
        raise entry.exception

    def snapshot(self) -> StatsSnapshot:
        """返回累计统计快照；快照为值拷贝，不随后续任务变化。"""
        return self._stats.snapshot()

    def close(self) -> None:
        """关闭调度器。

        先令此后的 submit 立即抛 :class:`SchedulerClosedError`，
        再等待全部已接受任务执行结束，最后停止事件循环与工作线程。
        可重复调用，且并发调用安全。
        """
        with self._cond:
            if self._closed:
                return
            if self._closing:
                # 另一个线程正在执行关闭，等待其完成即可。
                teardown_by_other = True
            else:
                teardown_by_other = False
                self._closing = True
            if teardown_by_other:
                while not self._closed:
                    self._cond.wait()
                return

        # 等待全部已接受任务结束（关闭期间事件循环与工作线程照常运转）。
        with self._cond:
            while self._pending > 0:
                self._cond.wait()

        # pending 归零意味着没有未结束任务：入站堆中至多残留已取消任务
        # 的惰性令牌（派发选取时会按 done 丢弃），停止事件循环是安全的。
        self._stop_dispatcher.set()
        # 事件循环可能正在 _inbound_cond 上等待，唤醒它以尽快观察停止信号。
        with self._inbound_cond:
            self._inbound_cond.notify_all()
        self._dispatcher.join()

        # 每个工作线程一个停止哨兵。
        for _ in range(self._workers):
            self._ready.put(_SENTINEL)
        for t in self._worker_threads:
            t.join()

        with self._cond:
            self._closed = True
            self._cond.notify_all()

    # ------------------------------------------------------------- internals

    def _admit(self, task_id: str, fn: Callable[[], Any],
               priority: int = 0) -> "_TaskEntry":
        """校验参数并完成准入：登记任务、更新计数、放入入站堆。

        submit 与 submit_nowait 共用；任何失败路径都不产生任务条目，
        除背压拒绝计入 rejected 外不改变统计。
        """
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task_id must be a non-empty str, got %r" % (task_id,)
            )
        if not callable(fn):
            raise InputValidationError("fn must be callable, got %r" % (fn,))
        if not self._is_priority(priority):
            raise InputValidationError(
                "priority must be an integer (bool not allowed), got %r"
                % (priority,)
            )

        with self._cond:
            if self._closing:
                raise SchedulerClosedError("scheduler is closed")
            if task_id in self._unfinished:
                raise DuplicateTaskError(
                    "task_id %r is already pending" % (task_id,)
                )
            if self._pending >= self._max_pending:
                self._stats.record_rejected()
                raise BackpressureError(
                    "pending task limit %d reached" % self._max_pending
                )

            seq = self._accept_seq
            self._accept_seq += 1
            entry = _TaskEntry(
                task_id, fn, time.monotonic(), priority, seq
            )
            self._tasks[task_id] = entry
            self._unfinished.add(task_id)
            self._pending += 1
            self._stats.record_accepted()

        # 入堆在 _cond 锁外：堆由 _inbound_cond 单独保护，入堆顺序由
        # 单调递增的接受序号承载。堆携带条目本身而非 task_id：任务取消后
        # task_id 立即可被同名新任务复用，旧令牌绝不能因此误取到新条目。
        with self._inbound_cond:
            heapq.heappush(
                self._inbound, (-priority, seq, entry)
            )
            self._inbound_cond.notify()
        return entry

    def _cancel(self, entry: "_TaskEntry") -> bool:
        """取消一个已接受任务；仅在任务尚未开始执行时成功。

        与工作线程的“认领”操作共用 _cond：认领置 started、取消置
        cancelled，先到者在锁内决定唯一终态。取消成功即释放 pending
        额度与 task_id 占用、计入 cancelled 并唤醒等待方。
        """
        with self._cond:
            if entry.started or entry.cancelled or entry.done.is_set():
                return False
            entry.cancelled = True
            entry.exception = TaskCancelledError(
                "task %r was cancelled before it started" % (entry.task_id,)
            )
            self._pending -= 1
            self._unfinished.discard(entry.task_id)
            self._stats.record_cancelled()
            self._cond.notify_all()
        # done 在锁外置位：结果字段与计数在锁内已全部落定，
        # 被唤醒的等待方只会读到一致的取消终态。
        entry.done.set()
        return True

    def _prune_cancelled_locked(self) -> None:
        """丢弃堆顶连续的已取消惰性令牌；调用时须持有 ``_inbound_cond``。

        已取消条目可能埋在未取消条目之下——那种情况下它不影响堆顶选择，
        留待将来弹到堆顶时再丢弃即可，故只需从堆顶清理。
        """
        while self._inbound and self._inbound[0][2].done.is_set():
            heapq.heappop(self._inbound)

    def _pop_next_locked(self) -> "Optional[_TaskEntry]":
        """弹出并返回当前堆中最高优先级的未结束任务，调用时须持有
        ``_inbound_cond``。

        等待派发期间被取消任务的惰性令牌会被依次丢弃（堆顶剪枝之外，
        弹出的条目也再确认一次 ``done``，以覆盖与取消线程的最后窗口）；
        堆中没有可派发任务时返回 None。
        """
        while self._inbound:
            _, _, entry = heapq.heappop(self._inbound)
            if not entry.done.is_set():
                return entry
        return None

    def _run_dispatcher(self) -> None:
        """事件循环：取得空闲工作线程许可后，按优先级把任务派发到就绪队列。

        顺序为 priority 降序、同优先级按接受先后（FCFS）。关键次序是
        “先取得许可、再在堆顶选取”：若先选定任务再等许可，先入队的低
        优先级任务会占住派发位置，挡住后到的高优先级任务。
        """
        while not self._stop_dispatcher.is_set():
            with self._inbound_cond:
                self._prune_cancelled_locked()
                while (not self._stop_dispatcher.is_set()
                       and not self._inbound):
                    self._inbound_cond.wait(_DISPATCH_POLL)
                    self._prune_cancelled_locked()
                if self._stop_dispatcher.is_set():
                    return

            # 取得一个空闲工作线程许可后再选取任务，保证选出的任务立即
            # 有线程可以执行；就绪队列中至多有 workers 个待执行任务。
            self._worker_slots.acquire()
            if self._stop_dispatcher.is_set():
                self._worker_slots.release()
                return
            with self._inbound_cond:
                entry = self._pop_next_locked()
            if entry is None:
                # 等待许可期间堆中的任务已全部被取消：归还许可继续循环。
                self._worker_slots.release()
                continue
            self._ready.put(entry)

    def _run_worker(self) -> None:
        """工作线程：认领 -> 执行 callable -> 记录统计 -> 唤醒等待方。"""
        while True:
            item = self._ready.get()
            if item is _SENTINEL:
                return
            entry: "_TaskEntry" = item

            # 原子认领：与 cancel 在同一把锁上决出唯一结果。
            # 认领成功（置 started）后 callable 必执行到底，cancel 必失败；
            # 已取消则 callable 绝不执行，立即归还许可。
            with self._cond:
                if entry.cancelled:
                    self._worker_slots.release()
                    continue
                entry.started = True

            start_time = time.monotonic()
            try:
                value = entry.fn()
            except Exception as exc:  # 调度器吞掉异常以隔离任务，继续工作
                end_time = time.monotonic()
                entry.success = False
                entry.exception = exc
                success = False
            else:
                end_time = time.monotonic()
                entry.success = True
                entry.value = value
                success = True

            queue_wait_ms = to_ms(start_time - entry.submit_time)
            total_latency_ms = to_ms(end_time - entry.submit_time)

            # 先记账再唤醒：被唤醒的 submit 调用方返回后即可读到一致统计。
            self._stats.record_finished(
                queue_wait_ms, total_latency_ms, success
            )
            with self._cond:
                self._pending -= 1
                self._unfinished.discard(entry.task_id)
                self._cond.notify_all()
            entry.done.set()

            # 执行结束才释放许可，许可数即并发执行数。
            self._worker_slots.release()

    # ------------------------------------------------------------- context mgr

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
