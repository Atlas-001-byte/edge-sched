"""调度器核心：事件循环派发 + 工作线程执行 + 背压。

结构:

- submit / submit_nowait 在调用方线程通过校验后，任务进入有界的入站队列
  （先到先服务）；submit 阻塞等待结果，submit_nowait 立即返回 TaskHandle。
- 一个事件循环线程按入队顺序取出任务，在有空闲工作线程时派发到就绪队列。
- 工作线程执行无参数 callable，记录排队/总延迟，唤醒等待方。
- 未完成（已接受但未结束）任务数达到 ``max_pending`` 时触发背压拒绝。
- submit_nowait 返回的句柄可在任务开始执行前 cancel：取消在与“开始执行”
  相同的锁上原子裁决，成功后 callable 不执行、立即释放未完成额度与
  task_id 占用，计入 cancelled 而不贡献延迟样本。
- close 先令新 submit 失败，再等待全部已接受任务结束，最后停止线程；
  已结束（含已取消）任务的结果在关闭后仍可读取。
"""

from __future__ import annotations

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

    ``started`` 与 ``cancelled`` 的读写都在调度器状态锁（``Scheduler._cond``）
    保护下完成，二者共同构成“开始执行 vs 取消”的原子裁决边界：
    一旦 ``started`` 置位，取消必然失败；一旦 ``cancelled`` 置位，
    callable 必然不会执行。
    """

    __slots__ = (
        "task_id",
        "fn",
        "submit_time",
        "done",
        "started",
        "cancelled",
        "success",
        "value",
        "exception",
    )

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 submit_time: float) -> None:
        self.task_id = task_id
        self.fn = fn
        self.submit_time = submit_time
        self.done = threading.Event()
        self.started = False
        self.cancelled = False
        self.success = False
        self.value: Any = None
        self.exception: Optional[BaseException] = None


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
        """任务已结束（成功、失败或取消）返回 True，否则 False。"""
        return self._entry.done.is_set()

    def cancel(self) -> bool:
        """尝试取消尚未开始执行的任务。

        - 任务仍排队、未开始执行：取消成功，返回 True，任务进入取消终态，
          其 callable 完全不会执行，句柄 :meth:`done` 随即为 True。
        - 任务已开始执行，或已处于任何终态（成功/失败/已取消）：返回 False，
          原有终态保持不变。

        取消与“开始执行”在调度器内以同一把锁原子裁决，结果唯一。
        """
        return self._scheduler._cancel(self._entry)

    def result(self, timeout: Optional[float] = None) -> Any:
        """等待并读取任务结果。

        - 成功：返回 callable 的返回值；失败：抛出 callable 抛出的原始异常。
        - 任务被取消：抛出 :class:`TaskCancelledError`。
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
        if entry.cancelled:
            raise TaskCancelledError(
                "task %r was cancelled before it started" % (entry.task_id,)
            )
        if entry.success:
            return entry.value
        assert entry.exception is not None
        raise entry.exception


class Scheduler:
    """高并发任务调度器。

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
        self._inbound: "queue.Queue[Any]" = queue.Queue()
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

    # ------------------------------------------------------------------ public

    def submit(self, task_id: str, fn: Callable[[], T],
               timeout: Optional[float] = None) -> T:
        """提交任务并阻塞等待结果。

        结果语义（每个任务只可能有一种）:

        - 成功：返回 callable 的返回值。
        - 失败：抛出 callable 抛出的**原始异常**；任务计入 failed。
        - 任务在开始前被（其它途径）取消：抛 :class:`TaskCancelledError`。
        - 未完成任务数已达上限：抛 :class:`BackpressureError`，无结果无统计。
        - task_id 与未完成任务重复：抛 :class:`DuplicateTaskError`。
        - 调度器已关闭：抛 :class:`SchedulerClosedError`。
        - 参数非法：抛 :class:`InputValidationError`。
        - 等待超过 ``timeout`` 秒：抛 :class:`TimeoutError`，任务仍继续执行，
          其最终结果不受影响。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param timeout: 可选等待超时（秒），None 表示一直等待。
        """
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )
        entry = self._admit(task_id, fn)

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.cancelled:
            raise TaskCancelledError(
                "task %r was cancelled before it started" % (task_id,)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def submit_nowait(self, task_id: str, fn: Callable[[], T]) -> TaskHandle:
        """提交任务并立即返回句柄，不等待 callable 执行结束。

        准入校验与 :meth:`submit` 完全一致（参数校验、关闭、重复 task_id、
        背压），通过后任务进入同一事件循环由工作线程按顺序派发；
        返回前 accepted 计数已更新。任务进度与结果通过返回的
        :class:`TaskHandle` 查询。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        """
        entry = self._admit(task_id, fn)
        return TaskHandle(self, entry)

    def result(self, task_id: str) -> Any:
        """读取一个已结束任务的结果（非阻塞）。

        - 成功：返回 callable 的返回值；失败：重新抛出其原始异常。
        - 任务在开始前被取消：抛 :class:`TaskCancelledError`。
        - 任务尚未结束：抛 :class:`RuntimeError`。
        - 从未接受过该 task_id：抛 :class:`KeyError`。

        若同名 task_id 被复用，读取的是最近一次接受的任务；
        调度器关闭后已结束任务的结果仍可通过本方法读取。
        """
        with self._cond:
            entry = self._tasks.get(task_id)
            if entry is None:
                raise KeyError(task_id)
            if not entry.done.is_set():
                raise RuntimeError("task %r is not finished" % (task_id,))
            cancelled = entry.cancelled
        if cancelled:
            raise TaskCancelledError(
                "task %r was cancelled before it started" % (task_id,)
            )
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

        # 此时入站队列必为空，停止事件循环。
        self._stop_dispatcher.set()
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

    def _admit(self, task_id: str, fn: Callable[[], Any]) -> "_TaskEntry":
        """校验参数并完成准入：登记任务、更新计数、放入入站队列。

        submit 与 submit_nowait 共用；任何失败路径都不产生任务条目，
        除背压拒绝计入 rejected 外不改变统计。
        """
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task_id must be a non-empty str, got %r" % (task_id,)
            )
        if not callable(fn):
            raise InputValidationError("fn must be callable, got %r" % (fn,))

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

            entry = _TaskEntry(task_id, fn, time.monotonic())
            self._tasks[task_id] = entry
            self._unfinished.add(task_id)
            self._pending += 1
            self._stats.record_accepted()

        # 入队在锁外：Queue 本身线程安全，入队顺序即接受顺序（FCFS）。
        self._inbound.put(entry)
        return entry

    def _cancel(self, entry: "_TaskEntry") -> bool:
        """取消的原子裁决，供 :meth:`TaskHandle.cancel` 调用。

        与工作线程“开始执行”的裁决共用 ``_cond``：只有在任务既未开始、
        也未取消（即仍处于排队等待）时才可能取消成功。成功后立即把任务
        推进取消终态——释放未完成额度与 task_id 占用、计入 cancelled、
        唤醒所有等待方；不贡献任何延迟样本。
        """
        with self._cond:
            if entry.started or entry.cancelled:
                return False
            entry.cancelled = True
            self._pending -= 1
            self._unfinished.discard(entry.task_id)
            self._stats.record_cancelled()
            self._cond.notify_all()
        # 先完成记账与额度释放，再置位 done：被唤醒的等待方读到的状态一致。
        entry.done.set()
        return True

    def _run_dispatcher(self) -> None:
        """事件循环：按 FCFS 顺序把任务派发给空闲工作线程。"""
        while not self._stop_dispatcher.is_set():
            try:
                entry = self._inbound.get(timeout=_DISPATCH_POLL)
            except queue.Empty:
                continue
            # 取得一个空闲工作线程许可后再派发，保证就绪队列中至多有
            # workers 个待执行任务，且任务不会在就绪队列里无限堆积。
            # 任务可能在此刻已被取消；仍照常派发，由工作线程在原子边界跳过，
            # 不执行 callable，并立即归还许可。
            self._worker_slots.acquire()
            self._ready.put(entry)

    def _run_worker(self) -> None:
        """工作线程：取任务 -> 原子裁决 -> 执行 callable -> 记账 -> 唤醒。"""
        while True:
            item = self._ready.get()
            if item is _SENTINEL:
                return
            entry: "_TaskEntry" = item

            # “开始执行 vs 取消”的唯一原子边界：与 _cancel 争抢同一把锁。
            # 先看到 cancelled 则 callable 绝不执行；先置位 started 则
            # 取消必败，任务按既有语义运行到底。二者只有一方获胜。
            with self._cond:
                if entry.cancelled:
                    # 取消已完成全部记账与额度释放，这里只需归还派发许可。
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
