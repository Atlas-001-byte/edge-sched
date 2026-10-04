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
- 提交时可指定 max_queue_wait_ms 排队时限：按单调时钟从任务被接受计到
  工作线程原子认领，到期未认领的任务进入 expired 终态，callable 不执行；
  到期与认领、取消在同一把锁上裁决，每个任务只可能有一种终态。
- submit_with_wait 在容量满时不立即拒绝，而是按发起先后排队等待名额：
  到达 max_pending 上限后，等待中的调用严格按 FIFO 获得准入（每次释放只
  唤醒并接纳队首），接纳后再按现有优先级与同级 FCFS 派发；等待期间同样
  登记 task_id（同名提交抛 DuplicateTaskError）。admission_timeout_ms
  为 None 时无限等待，0 时仅在有空位时接纳，其余为非负整数毫秒时限。
  等待至时限仍无名额抛 BackpressureError（计入 rejected），close 开始时
  尚未接纳者抛 SchedulerClosedError；两种拒绝都不创建任务、不执行
  callable、不改变其余计数或延迟样本，并立即释放 task_id 占用。被接纳
  任务的 max_queue_wait_ms 从实际接纳时刻起算。
- close 先令新 submit 失败，再等待全部已接受任务结束，最后停止线程；
  已取消/已到期任务不阻塞关闭，已完成/已取消/已到期任务的结果在关闭后
  仍可读取。close 开始时尚在准入队列中等待的调用立即得到
  SchedulerClosedError。
- stats_checkpoint 在与统计事件相同的锁内创建不可变区间边界
  StatsCheckpoint；snapshot_since 返回边界后事件构成的 StatsSnapshot
  （字段、分位口径与 to_dict 形态同 snapshot）。跨边界任务按接纳时刻计入
  接纳区间的 accepted，按结束时刻计入结束区间的 completed/failed 并贡献
  延迟样本。关闭后仍可创建边界、查询历史区间。
"""

from __future__ import annotations

import heapq
import queue
import threading
import time
from collections import deque
from typing import Any, Callable, Optional, TypeVar

from .errors import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    QueueTimeoutError,
    SchedulerClosedError,
    TaskCancelledError,
)
from .stats import Stats, StatsCheckpoint, StatsSnapshot, to_ms

T = TypeVar("T")

# 就绪队列中的停止哨兵；工作线程取到即退出。
_SENTINEL = object()

# 事件循环在空闲时轮询停止信号的间隔（秒）。
_DISPATCH_POLL = 0.05


class _TaskEntry:
    """一个任务的全部可变状态。

    取消、到期与执行的唯一裁决依赖三个受 Scheduler._cond 保护的标志：

    - ``started``：工作线程已在锁内认领任务、即将执行 callable。
    - ``cancelled``：任务已在锁内被取消，进入取消终态。
    - ``expired``：任务已在锁内超过排队时限，进入到期终态。

    三者在同一把锁上以先到者为准，故每个任务只可能有一种终态。
    """

    __slots__ = (
        "task_id",
        "fn",
        "priority",
        "seq",
        "submit_time",
        "deadline",
        "done",
        "success",
        "value",
        "exception",
        "started",
        "cancelled",
        "expired",
    )

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 submit_time: float, priority: int, seq: int,
                 max_queue_wait_ms: Optional[int] = None) -> None:
        self.task_id = task_id
        self.fn = fn
        self.priority = priority
        # 接受序号：同优先级时严格按接受先后（FCFS）派发。
        self.seq = seq
        self.submit_time = submit_time
        # 认领截止时刻（单调时钟）：None 表示不限排队时间。
        self.deadline: Optional[float] = (
            None if max_queue_wait_ms is None
            else submit_time + max_queue_wait_ms / 1000.0
        )
        self.done = threading.Event()
        self.success = False
        self.value: Any = None
        self.exception: Optional[BaseException] = None
        self.started = False
        self.cancelled = False
        self.expired = False


class _AdmissionWaiter:
    """submit_with_wait 在容量满时的一个等待者。

    等待者在进入准入队列时即登记 task_id（与已接受任务共用同一套同名
    校验），但此时尚未创建任务、未占用 pending 额度。所有字段都在
    ``Scheduler._cond`` 锁内访问；``admitted`` 与 ``closed`` 是互斥的
    唤醒原因，先到者在锁内决定该等待者的唯一结局。

    被提升（获名额）的那一刻在锁内按 FIFO 顺序一次性预留接受序号
    ``seq``、接纳时刻 ``admit_time``、pending 名额、accepted 计数并建好
    任务条目 ``entry``：这样入站堆的同级 FCFS 顺序与排队时限的起算点都
    锚定在真实接纳时刻，不随等待线程被调度唤醒的先后而改变。
    """

    __slots__ = (
        "task_id", "fn", "priority", "max_queue_wait_ms",
        "admitted", "closed", "entry",
    )

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 priority: int,
                 max_queue_wait_ms: Optional[int]) -> None:
        self.task_id = task_id
        self.fn = fn
        self.priority = priority
        self.max_queue_wait_ms = max_queue_wait_ms
        self.admitted = False
        self.closed = False
        self.entry: "Optional[_TaskEntry]" = None


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

        - 任务尚未开始且未到期：取消成功，callable 完全不执行，返回 True；
          此后 :meth:`done` 为 True，:meth:`result` 抛
          :class:`TaskCancelledError`。
        - 任务已经开始执行或已有终态（含已被取消、已到期）：返回 False，
          原终态与结果不受影响。

        与“开始执行”“排队到期”竞争时，由调度器在同一原子边界决定唯一结果。
        """
        return self._scheduler._cancel(self._entry)

    def result(self, timeout: Optional[float] = None) -> Any:
        """等待并读取任务结果。

        - 成功：返回 callable 的返回值；失败：抛出 callable 抛出的原始异常。
        - 被取消：抛出 :class:`TaskCancelledError`。
        - 排队到期：抛出 :class:`QueueTimeoutError`。
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
        # submit_with_wait 的阻塞准入队列：队首是等待最久的调用。
        # 等待者在入队时即占用 task_id，但不占 pending 额度、不产生任务。
        self._admission_queue: "deque[_AdmissionWaiter]" = deque()
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

    @staticmethod
    def _is_nonneg_int(value: Any) -> bool:
        # admission_timeout_ms 接受 0 与正整数，但 bool 非法。
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    # ------------------------------------------------------------------ public

    def submit(self, task_id: str, fn: Callable[[], T],
               timeout: Optional[float] = None,
               priority: int = 0,
               max_queue_wait_ms: Optional[int] = None) -> T:
        """提交任务并阻塞等待结果。

        结果语义（每个任务只可能有一种）:

        - 成功：返回 callable 的返回值。
        - 失败：抛出 callable 抛出的**原始异常**；任务计入 failed。
        - 排队超过 ``max_queue_wait_ms`` 仍未被工作线程认领：抛
          :class:`QueueTimeoutError`，callable 不执行，任务计入 expired。
        - 未完成任务数已达上限：抛 :class:`BackpressureError`，无结果无统计。
        - task_id 与未完成任务重复：抛 :class:`DuplicateTaskError`。
        - 调度器已关闭：抛 :class:`SchedulerClosedError`。
        - 参数非法（含 priority 非整数或为布尔值、max_queue_wait_ms 非法）：
          抛 :class:`InputValidationError`。
        - 等待超过 ``timeout`` 秒：抛 :class:`TimeoutError`，任务仍继续执行，
          其最终结果不受影响。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param timeout: 可选等待超时（秒），位置与语义保持不变，
            None 表示一直等待。
        :param priority: 可选整数优先级，缺省 0；数值越大越早被派发给
            工作线程，相同数值按接受先后派发。优先级只影响尚未开始执行
            的任务的派发顺序，不抢占执行中的任务。
        :param max_queue_wait_ms: 可选排队时限（毫秒），缺省 None 表示
            不限排队时间；只能为 None 或 >= 1 的整数（bool 非法）。
            按单调时钟从任务被接受计到工作线程原子认领；认领先发生
            则任务执行到底，不受时限中断。
        """
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )
        entry = self._admit(task_id, fn, priority, max_queue_wait_ms)

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def submit_nowait(self, task_id: str, fn: Callable[[], T],
                      priority: int = 0,
                      max_queue_wait_ms: Optional[int] = None) -> TaskHandle:
        """提交任务并立即返回句柄，不等待 callable 执行结束。

        准入校验与 :meth:`submit` 完全一致（参数校验、priority 与
        max_queue_wait_ms 校验、关闭、重复 task_id、背压），通过后任务进入
        同一事件循环，由工作线程按“priority 降序、同优先级按接受先后”派发；
        返回前 accepted 计数已更新。任务进度与结果通过返回的
        :class:`TaskHandle` 查询。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param priority: 可选整数优先级，缺省 0，语义同 :meth:`submit`。
        :param max_queue_wait_ms: 可选排队时限（毫秒），缺省 None 表示
            不限排队时间，语义同 :meth:`submit`。
        """
        entry = self._admit(task_id, fn, priority, max_queue_wait_ms)
        return TaskHandle(self, entry)

    def submit_with_wait(self, task_id: str, fn: Callable[[], T],
                         timeout: Optional[float] = None,
                         priority: int = 0,
                         max_queue_wait_ms: Optional[int] = None,
                         admission_timeout_ms: Optional[int] = None) -> T:
        """提交任务；容量满时按发起先后排队等待名额，再阻塞等待结果。

        与 :meth:`submit` 参数语义一致并同样返回 callable 的结果，只额外
        增加准入等待参数 ``admission_timeout_ms``:

        - ``None``（缺省）：无空位时无限等待，直到获得名额。
        - ``0``：仅在调用瞬间有空位时接纳，否则立即抛
          :class:`BackpressureError`（语义同 :meth:`submit` 的满员拒绝）。
        - 正整数：最多等待该毫秒数；到达时限仍未获名额抛
          :class:`BackpressureError`。
        只接受 None 或非负整数，布尔值与负数抛 :class:`InputValidationError`。

        准入规则:

        - 达到 ``max_pending`` 后，调用严格按发起先后（FIFO）排队；任何
          成功、失败、取消或到期每释放一个名额，只唤醒并接纳队首一人，
          随后该任务按现有“priority 降序、同优先级接受先后”派发，不抢占
          执行中的任务。
        - 等待期间即登记 task_id：与其他等待者或未完成任务同名的提交抛
          :class:`DuplicateTaskError`，不入队、不占名额。
        - 等待至准入时限：抛 :class:`BackpressureError`，该次拒绝计入
          rejected；释放其 task_id 占用，不创建任务、不执行 callable、
          不改变其余计数或延迟样本。
        - :meth:`close` 开始时尚未被接纳的等待者抛
          :class:`SchedulerClosedError`，同样不建任务、不执行 callable、
          不计入 rejected。
        - 一旦被接纳，``max_queue_wait_ms`` 从**实际接纳时刻**起算；其余
          结果语义（成功原值、失败原异常、认领前到期
          :class:`QueueTimeoutError`、等待结果 ``timeout``）与
          :meth:`submit` 完全一致，callable 只执行一次。

        :param admission_timeout_ms: 准入等待上限（毫秒），None 无限等待，
            0 仅在有空位时接纳；只能为 None 或非负整数（bool 非法）。
        """
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )
        if (admission_timeout_ms is not None
                and not self._is_nonneg_int(admission_timeout_ms)):
            raise InputValidationError(
                "admission_timeout_ms must be a non-negative integer or None "
                "(bool not allowed), got %r" % (admission_timeout_ms,)
            )
        self._validate_submit_args(task_id, fn, priority, max_queue_wait_ms)

        with self._cond:
            entry = self._admit_with_wait_locked(
                task_id, fn, priority, max_queue_wait_ms,
                admission_timeout_ms,
            )

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def result(self, task_id: str) -> Any:
        """读取一个已结束任务的结果（非阻塞）。

        - 成功：返回 callable 的返回值；失败：重新抛出其原始异常。
        - 任务在开始前被取消：抛 :class:`TaskCancelledError`。
        - 任务排队到期：抛 :class:`QueueTimeoutError`。
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

    def stats_checkpoint(self) -> StatsCheckpoint:
        """创建一个区间统计观测边界。

        边界与所有统计事件在同一原子顺序上落定：accepted/rejected/
        cancelled/expired 按各自接纳、拒绝、取消、到期时刻归属，
        completed/failed 及两类延迟样本按结束时刻归属。跨边界的任务因此
        在接纳所在区间计入 accepted，在结束所在区间计入 completed 或
        failed 并贡献延迟样本；边界两侧都不统计的事件不存在。

        同一 checkpoint 可反复用于 :meth:`snapshot_since`，不改变累计统计
        或后续区间；调度器关闭后仍可创建边界。
        """
        return self._stats.checkpoint()

    def snapshot_since(self, checkpoint: StatsCheckpoint) -> StatsSnapshot:
        """返回自 ``checkpoint`` 边界之后事件构成的区间统计快照。

        字段、分位口径与 :meth:`snapshot` 完全一致：六项计数只含边界后
        事件，``queue_wait_ms`` / ``total_latency_ms`` 只收集边界后成功或
        失败结束的任务样本；边界后无事件时计数全为 0，两个分布的
        p50/p95/p99/max 均为 0.0。同一 checkpoint 可反复查询，不改变累计
        统计、延迟样本或任务状态。

        - 调度器关闭后仍可查询历史区间。
        - ``checkpoint`` 不是 :class:`StatsCheckpoint`、由其他调度器创建
          或字段缺失/被篡改（损坏对象）时，抛 :class:`InputValidationError`，
          且不改变计数、延迟样本或任务状态。
        """
        try:
            return self._stats.snapshot_since(checkpoint)
        except ValueError as exc:
            raise InputValidationError(str(exc)) from None

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
            # 关闭开始：准入队列中所有尚未获名额的等待者立即得到
            # SchedulerClosedError。它们不占 pending 额度，释放其 task_id
            # 占用；不计入 rejected，也不创建任何任务。
            while self._admission_queue:
                waiter = self._admission_queue.popleft()
                waiter.closed = True
                self._unfinished.discard(waiter.task_id)
            self._cond.notify_all()

        # 等待全部已接受任务结束（关闭期间事件循环与工作线程照常运转）。
        with self._cond:
            while self._pending > 0:
                self._cond.wait()

        # pending 归零意味着没有未结束任务：入站堆中至多残留已结束任务
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
               priority: int = 0,
               max_queue_wait_ms: Optional[int] = None) -> "_TaskEntry":
        """校验参数并完成准入：登记任务、更新计数、放入入站堆。

        submit 与 submit_nowait 共用；任何失败路径都不产生任务条目，
        除背压拒绝计入 rejected 外不改变统计。
        """
        self._validate_submit_args(task_id, fn, priority, max_queue_wait_ms)

        with self._cond:
            # 登记与入堆在同一把锁内原子完成（锁序 _cond -> _inbound_cond），
            # 使并发提交的同级 FCFS 顺序即接受顺序，不存在“已接受但尚未入堆”
            # 被后来者越过的窗口。
            entry = self._commit_admit_locked(
                task_id, fn, priority, max_queue_wait_ms
            )
        return entry

    @staticmethod
    def _validate_submit_args(task_id: Any, fn: Any, priority: Any,
                              max_queue_wait_ms: Any) -> None:
        """submit / submit_nowait / submit_with_wait 共用的参数校验。"""
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task_id must be a non-empty str, got %r" % (task_id,)
            )
        if not callable(fn):
            raise InputValidationError("fn must be callable, got %r" % (fn,))
        if not Scheduler._is_priority(priority):
            raise InputValidationError(
                "priority must be an integer (bool not allowed), got %r"
                % (priority,)
            )
        if (max_queue_wait_ms is not None
                and not Scheduler._is_positive_int(max_queue_wait_ms)):
            raise InputValidationError(
                "max_queue_wait_ms must be an integer >= 1 or None "
                "(bool not allowed), got %r" % (max_queue_wait_ms,)
            )

    def _commit_admit_locked(self, task_id: str, fn: Callable[[], Any],
                             priority: int,
                             max_queue_wait_ms: Optional[int]) -> "_TaskEntry":
        """在容量可用时登记任务并入堆；调用时须持有 ``_cond``。

        满员时立即计入 rejected 并抛 :class:`BackpressureError`（submit /
        submit_nowait 的即时拒绝语义）。任何失败路径都不产生任务条目。

        登记、计数与入入站堆在同一原子边界完成（锁序
        _cond -> _inbound_cond）：并发提交一旦被接受，其令牌必已按接受
        序号入堆，后来者无法在“已接受但未入堆”的窗口里越过它。
        """
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
            task_id, fn, time.monotonic(), priority, seq,
            max_queue_wait_ms,
        )
        self._tasks[task_id] = entry
        self._unfinished.add(task_id)
        self._pending += 1
        self._stats.record_accepted()
        self._enqueue_inbound_locked(entry, priority)
        return entry

    def _enqueue_inbound_locked(self, entry: "_TaskEntry",
                                priority: int) -> None:
        """把条目推入入站堆并唤醒事件循环；调用时须持有 ``_cond``。

        本方法在持 ``_cond`` 时嵌套获取 ``_inbound_cond``（锁序
        _cond -> _inbound_cond），使“接纳/提升”与“令牌入堆”对事件循环
        原子可见。
        """
        with self._inbound_cond:
            heapq.heappush(
                self._inbound, (-priority, entry.seq, entry)
            )
            self._inbound_cond.notify()

    def _admit_with_wait_locked(
        self, task_id: str, fn: Callable[[], Any], priority: int,
        max_queue_wait_ms: Optional[int],
        admission_timeout_ms: Optional[int],
    ) -> "_TaskEntry":
        """submit_with_wait 的持锁准入：满员则在 FIFO 队列中等待名额。

        调用时须持有 ``_cond``。等待者入队即登记 task_id（同名抛
        DuplicateTaskError）；获名额后直接在锁内完成与 _admit 相同的
        登记。等待至准入时限抛 BackpressureError（计入 rejected），
        close 开始则抛 SchedulerClosedError；两种拒绝都移除等待者并
        释放 task_id，不创建任务、不执行 callable。
        """
        if self._closing:
            raise SchedulerClosedError("scheduler is closed")
        if task_id in self._unfinished:
            raise DuplicateTaskError(
                "task_id %r is already pending" % (task_id,)
            )

        # 严格 FIFO：已有等待者时，即便有空位也先让队首获名额，新调用
        # 不得插队。正常不变量下“有等待者即满员”，此处兜底处理空位
        # 瞬态，避免后来者绕过队首或令队首无人唤醒。
        if self._admission_queue:
            self._promote_waiter_locked()

        if (not self._admission_queue
                and self._pending < self._max_pending):
            # 调用瞬间有空位且无人排队：立即接纳（None 与 0 路径一致）。
            return self._commit_admit_locked(
                task_id, fn, priority, max_queue_wait_ms
            )

        # 0 = 只在有空位时接纳：不排队。
        if admission_timeout_ms == 0:
            self._stats.record_rejected()
            raise BackpressureError(
                "pending task limit %d reached" % self._max_pending
            )

        waiter = _AdmissionWaiter(task_id, fn, priority, max_queue_wait_ms)
        self._admission_queue.append(waiter)
        self._unfinished.add(task_id)

        if admission_timeout_ms is None:
            while not waiter.admitted and not waiter.closed:
                self._cond.wait()
        else:
            deadline = time.monotonic() + admission_timeout_ms / 1000.0
            while not waiter.admitted and not waiter.closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    break
                self._cond.wait(remaining)

        if waiter.admitted:
            # 任务条目（含接受序号、接纳时刻、pending 名额与 accepted 计数）
            # 已在提升那一刻由 _promote_waiter_locked 在锁内按 FIFO 建好。
            assert waiter.entry is not None
            return waiter.entry

        # 被唤醒的原因是关闭（close 已整队清出），或等待自身超时：
        # 仅当仍在队列中时移除自身。
        if waiter in self._admission_queue:
            self._admission_queue.remove(waiter)
        self._unfinished.discard(task_id)
        if waiter.closed:
            raise SchedulerClosedError("scheduler is closed")
        # 超时拒绝：若此刻恰有空位，把名额交给队首，再拒绝本次调用。
        self._promote_waiter_locked()
        self._stats.record_rejected()
        raise BackpressureError(
            "admission to scheduler timed out after %d ms"
            % admission_timeout_ms
        )

    def _promote_waiter_locked(self) -> bool:
        """若有空位且存在等待者，把队首提升为已接纳并在锁内建好任务。

        调用时须持有 ``_cond``。提升是真正的接纳时刻：此处一次性按 FIFO
        预留接受序号、记录接纳时刻（max_queue_wait_ms 起算点）、占用
        pending 名额、登记 task_id/任务条目、计入 accepted，并把令牌原子
        推入入站堆（锁序 _cond -> _inbound_cond）。因此这些结果既不依赖
        等待线程之后被调度唤醒的先后，也不存在“已接纳但未入堆”被后来者
        越过的窗口。返回是否提升了一个等待者；一次释放只提升队首一人。
        """
        while self._admission_queue:
            waiter = self._admission_queue.popleft()
            if waiter.closed:
                # close 唤醒途中残留的等待者不会出现（关闭时整队列出队），
                # 这里仅作防御性跳过。
                continue
            if self._pending >= self._max_pending:
                self._admission_queue.appendleft(waiter)
                return False
            seq = self._accept_seq
            self._accept_seq += 1
            entry = _TaskEntry(
                waiter.task_id, waiter.fn, time.monotonic(),
                waiter.priority, seq, waiter.max_queue_wait_ms,
            )
            waiter.entry = entry
            self._tasks[waiter.task_id] = entry
            self._pending += 1
            self._stats.record_accepted()
            self._enqueue_inbound_locked(entry, waiter.priority)
            waiter.admitted = True
            self._cond.notify_all()
            return True
        return False

    def _cancel(self, entry: "_TaskEntry") -> bool:
        """取消一个已接受任务；仅在任务尚未开始执行且未到期时成功。

        与工作线程的“认领”、到期裁决共用 _cond：认领置 started、取消置
        cancelled、到期置 expired，先到者在锁内决定唯一终态。取消成功即
        释放 pending 额度与 task_id 占用、计入 cancelled 并唤醒等待方。
        """
        with self._cond:
            if (entry.started or entry.cancelled or entry.expired
                    or entry.done.is_set()):
                return False
            entry.cancelled = True
            entry.exception = TaskCancelledError(
                "task %r was cancelled before it started" % (entry.task_id,)
            )
            self._pending -= 1
            self._unfinished.discard(entry.task_id)
            self._stats.record_cancelled()
            # 名额释放：若有 submit_with_wait 等待者，在锁内直接把队首
            # 提升为已接纳（预占新空出的名额），一次释放只接纳一人。
            self._promote_waiter_locked()
            self._cond.notify_all()
        # done 在锁外置位：结果字段与计数在锁内已全部落定，
        # 被唤醒的等待方只会读到一致的取消终态。
        entry.done.set()
        return True

    def _expire_locked(self, entry: "_TaskEntry") -> bool:
        """把任务置入到期终态；调用时须持有 ``_cond``，返回是否成功。

        与认领、取消在同一把锁上以先到者为准。到期成功即释放 pending
        额度与 task_id 占用、计入 expired 并唤醒等待方；``done`` 由
        调用方在锁外置位。
        """
        if (entry.started or entry.cancelled or entry.expired
                or entry.done.is_set()):
            return False
        entry.expired = True
        entry.exception = QueueTimeoutError(
            "task %r exceeded max_queue_wait_ms before being claimed"
            % (entry.task_id,)
        )
        self._pending -= 1
        self._unfinished.discard(entry.task_id)
        self._stats.record_expired()
        # 到期同样释放名额：一次释放只把队首等待者提升为已接纳。
        self._promote_waiter_locked()
        self._cond.notify_all()
        return True

    def _expire(self, entry: "_TaskEntry") -> bool:
        """尝试令一个排队中的任务到期；仅在尚未认领、未取消、未到期时成功。"""
        with self._cond:
            if not self._expire_locked(entry):
                return False
        entry.done.set()
        return True

    def _prune_cancelled_locked(self) -> None:
        """丢弃堆顶连续的已结束（取消/到期）惰性令牌；调用时须持有
        ``_inbound_cond``。

        已结束条目可能埋在未结束条目之下——那种情况下它不影响堆顶选择，
        留待将来弹到堆顶时再丢弃即可，故只需从堆顶清理。
        """
        while self._inbound and self._inbound[0][2].done.is_set():
            heapq.heappop(self._inbound)

    def _collect_due_locked(self) -> "list[_TaskEntry]":
        """收集堆中已逾认领截止时刻、尚未结束的任务；调用时须持有
        ``_inbound_cond``。

        只收集、不改状态：到期裁决（_expire）会获取 ``_cond``，而全局锁序
        是 _cond -> _inbound_cond，故不能在持有 ``_inbound_cond`` 时反取
        ``_cond``。调用方在释放 ``_inbound_cond`` 后逐个调用 :meth:`_expire`。
        """
        now = time.monotonic()
        return [
            entry
            for _, _, entry in self._inbound
            if entry.deadline is not None and now >= entry.deadline
            and not entry.done.is_set()
        ]

    def _expire_entries(self, entries: "list[_TaskEntry]") -> None:
        """在不持有 ``_inbound_cond`` 的情况下，对收集到的到期任务逐个裁决。

        去重后调用 :meth:`_expire`（其内部获取 ``_cond``），符合全局锁序
        _cond -> _inbound_cond；重复条目的第二次裁决幂等失败。
        """
        for entry in set(entries):
            self._expire(entry)

    def _deadline_wait_locked(self) -> float:
        """返回到最近一次认领截止时刻的等待秒数（上限 _DISPATCH_POLL）。

        调用时须持有 ``_inbound_cond``；堆中没有带时限的未结束任务时
        返回 _DISPATCH_POLL。用于许可等待与空闲轮询的超时，保证到期
        任务被及时处理。
        """
        now = time.monotonic()
        waits = [
            entry.deadline - now  # type: ignore[operator]
            for _, _, entry in self._inbound
            if entry.deadline is not None and not entry.done.is_set()
        ]
        if not waits:
            return _DISPATCH_POLL
        return min(_DISPATCH_POLL, max(0.0, min(waits)))

    def _pop_next_locked(self) -> "Optional[_TaskEntry]":
        """弹出并返回当前堆中最高优先级的未结束任务，调用时须持有
        ``_inbound_cond``。

        等待派发期间已结束（取消/到期）任务的惰性令牌会被依次丢弃
        （堆顶剪枝之外，弹出的条目也再确认一次 ``done``，以覆盖与
        取消/到期线程的最后窗口）；堆中没有可派发任务时返回 None。
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

        许可等待带超时（最近一次认领截止时刻与 _DISPATCH_POLL 的较小者），
        超时回到循环顶部处理到期任务与停止信号：带排队时限的任务即使
        没有新提交、工作线程也全忙，仍会在到期时被及时置入到期终态。
        """
        while not self._stop_dispatcher.is_set():
            with self._inbound_cond:
                self._prune_cancelled_locked()
                due = self._collect_due_locked()
                while (not self._stop_dispatcher.is_set()
                       and not self._inbound):
                    self._inbound_cond.wait(_DISPATCH_POLL)
                    self._prune_cancelled_locked()
                    due = self._collect_due_locked()
                if self._stop_dispatcher.is_set():
                    stopping = True
                else:
                    stopping = False
                    wait = self._deadline_wait_locked()

            # 到期裁决需要 _cond，放到 _inbound_cond 之外进行（锁序
            # _cond -> _inbound_cond），避免持 _inbound_cond 反取 _cond。
            self._expire_entries(due)
            if stopping:
                return

            # 取得一个空闲工作线程许可后再选取任务，保证选出的任务立即
            # 有线程可以执行；就绪队列中至多有 workers 个待执行任务。
            if not self._worker_slots.acquire(timeout=wait):
                # 许可等待超时：回循环顶部处理到期任务/停止信号。
                continue
            if self._stop_dispatcher.is_set():
                self._worker_slots.release()
                return
            with self._inbound_cond:
                due = self._collect_due_locked()
            self._expire_entries(due)
            with self._inbound_cond:
                entry = self._pop_next_locked()
            if entry is None:
                # 等待许可期间堆中的任务已全部被取消/到期：归还许可继续循环。
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

            # 原子认领：与 cancel、到期在同一把锁上决出唯一终态。
            # 认领成功（置 started）后 callable 必执行到底，cancel 必失败、
            # 时限不再生效；已取消/已到期则 callable 绝不执行，立即归还许可。
            # 认领时刻已逾截止时刻视为到期先发生：到期优先，callable 不执行。
            with self._cond:
                if entry.cancelled or entry.expired:
                    self._worker_slots.release()
                    continue
                if (entry.deadline is not None
                        and time.monotonic() >= entry.deadline):
                    self._expire_locked(entry)
                    expired = True
                else:
                    entry.started = True
                    expired = False
            if expired:
                entry.done.set()
                self._worker_slots.release()
                continue

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
                # 成功/失败释放名额：一次释放只接纳准入队列队首一人。
                self._promote_waiter_locked()
                self._cond.notify_all()
            entry.done.set()

            # 执行结束才释放许可，许可数即并发执行数。
            self._worker_slots.release()

    # ------------------------------------------------------------- context mgr

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
