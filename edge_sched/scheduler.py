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
- 构造时可选启用排队优先级老化（aging_interval_ms，缺省 None 关闭）：
  启用后，已接纳但尚未被工作线程认领的任务，每越过一个老化边界
  （自实际接纳时刻起按 aging_interval_ms 划分），其派发比较用的有效
  优先级在原 priority 上加 1。派发依次按有效优先级降序、原 priority
  降序、接受先后升序；老化只改变未开始任务的派发先后，不抢占、不重排
  执行中任务，工作线程原子认领后有效优先级即冻结，也不改变
  max_queue_wait_ms 的起算与到期口径。未启用老化时派发语义与之前完全
  一致（原 priority 降序、同级接受先后）。
- submit_with_wait 在容量满时不立即拒绝，而是按发起先后排队等待名额：
  到达 max_pending 上限后，等待中的调用严格按 FIFO 获得准入（每次释放只
  唤醒并考察队首），接纳后再按现有优先级与同级 FCFS 派发；等待期间同样
  登记 task_id（同名提交抛 DuplicateTaskError）。admission_timeout_ms
  为 None 时无限等待，0 时仅在有空位时接纳，其余为非负整数毫秒时限。
  等待至时限仍无名额抛 BackpressureError（计入 rejected），close 开始时
  尚未接纳者抛 SchedulerClosedError；两种拒绝都不创建任务、不执行
  callable、不改变其余计数或延迟样本，并立即释放 task_id 占用。被接纳
  任务的 max_queue_wait_ms 从实际接纳时刻起算。
- submit_batch_with_wait 把一次调用中的多个任务作为**整组**原子准入：它
  与 submit_with_wait 共用同一条 FIFO 准入队列（同为队首的一个等待者），
  只有当余量足以容纳整组时才一次性接纳，余量不足即继续等待，后续的单个
  任务或更小的组都不得绕过它；接纳瞬间组内任务共享同一接纳时刻、按输入
  顺序预留连续接受序号，accepted 一次增加组内任务数，max_queue_wait_ms
  自此同一刻起算，随后组内按 priority 降序、同级按输入顺序派发。等待
  期间组内全部 task_id 即被占用；超时整组被拒（rejected 加 1），close 时
  未被整组接纳则整组得到 SchedulerClosedError（rejected 不变）——任何
  失败路径都不创建任务、不执行 callable、不改变延迟样本。
- close 先令新 submit 失败，再等待全部已接受任务结束，最后停止线程；
  已取消/已到期任务不阻塞关闭，已完成/已取消/已到期任务的结果在关闭后
  仍可读取。close 开始时尚在准入队列中等待的调用立即得到
  SchedulerClosedError。
- stats_checkpoint 在与统计事件相同的锁内创建不可变区间边界
  StatsCheckpoint；snapshot_since 返回边界后事件构成的 StatsSnapshot
  （字段、分位口径与 to_dict 形态同 snapshot）。跨边界任务按接纳时刻计入
  接纳区间的 accepted，按结束时刻计入结束区间的 completed/failed 并贡献
  queue_wait_ms / total_latency_ms / execution_ms 延迟样本。关闭后仍可
  创建边界、查询历史区间。
- 统计在两类既有延迟之外另计 execution_ms：从工作线程原子认领任务后
  开始计时，到 callable 正常返回或抛出 Exception 为止（不含认领前排队
  等待），成功与失败任务都恰好贡献一个执行耗时样本；未开始执行就被取消、
  排队到期、提交被拒或校验失败的任务不贡献该样本。
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
        "aging_interval_ms",
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
                 max_queue_wait_ms: Optional[int] = None,
                 aging_interval_ms: Optional[int] = None) -> None:
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
        # 老化周期（毫秒）：None 表示该任务不参与优先级老化。
        self.aging_interval_ms = aging_interval_ms
        self.done = threading.Event()
        self.success = False
        self.value: Any = None
        self.exception: Optional[BaseException] = None
        self.started = False
        self.cancelled = False
        self.expired = False


def _heap_key_at(entry: "_TaskEntry", now: float) -> tuple:
    """构造条目在时刻 ``now`` 的入站堆键。

    键为 ``(-有效优先级, -原优先级, 接受序号, 条目)``：先按有效优先级
    （原 priority 加自接纳时刻起已完成的老化周期数）降序，再按原
    priority 降序，最后按接受序号升序（先接受先派发）。

    末位的条目引用只用于在键相等时仍可入堆且能取回对象：前三项已构成
    全序（同优先级同刻接纳时接受序号仍连续互异），条目本身从不真正参与
    比较（``_TaskEntry`` 也刻意不定义顺序）。
    """
    if entry.aging_interval_ms is None:
        effective = entry.priority
    else:
        elapsed_ms = (now - entry.submit_time) * 1000.0
        cycles = int(elapsed_ms // entry.aging_interval_ms)
        effective = entry.priority + cycles
    return (-effective, -entry.priority, entry.seq, entry)


def _aging_boundary(entry: "_TaskEntry", now: float) -> Optional[float]:
    """返回到下一个老化边界的秒数；不参与老化或已越界时返回 None。"""
    if entry.aging_interval_ms is None:
        return None
    elapsed = now - entry.submit_time
    if elapsed < 0:
        return None
    interval = entry.aging_interval_ms / 1000.0
    cycles = int(elapsed // interval)
    return entry.submit_time + (cycles + 1) * interval - now


class _AdmissionItem:
    """一次准入调用中的单个任务规格（submit_with_wait 为单项一组）。"""

    __slots__ = ("task_id", "fn", "priority", "max_queue_wait_ms")

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 priority: int,
                 max_queue_wait_ms: Optional[int]) -> None:
        self.task_id = task_id
        self.fn = fn
        self.priority = priority
        self.max_queue_wait_ms = max_queue_wait_ms


class _AdmissionWaiter:
    """准入队列中的一个等待者：一次 submit_with_wait 或
    submit_batch_with_wait 调用。

    单个任务的等待者只有一个 :class:`_AdmissionItem`；成组调用持有多个
    item，并以**整组**为最小准入单位：要么全部获得名额，要么继续等待，
    组内任务绝不会被部分接纳。

    等待者在进入准入队列时即登记其全部 task_id（与已接受任务共用同一套
    同名校验），但此时尚未创建任务、未占用 pending 额度。所有字段都在
    ``Scheduler._cond`` 锁内访问；``admitted`` 与 ``closed`` 是互斥的
    唤醒原因，先到者在锁内决定该等待者的唯一结局。

    被提升（整组获名额）的那一刻在锁内按 FIFO 顺序一次性预留连续的接受
    序号 ``seq``、同一接纳时刻 ``admit_time``、全部 pending 名额、
    accepted 计数并建好全部任务条目 ``entries``：这样入站堆的同级 FCFS
    顺序与排队时限的起算点都锚定在真实接纳时刻（组内各任务完全相同），
    不随等待线程被调度唤醒的先后而改变。
    """

    __slots__ = ("items", "admitted", "closed", "entries")

    def __init__(self, items: "list[_AdmissionItem]") -> None:
        self.items = items
        self.admitted = False
        self.closed = False
        # 与 items 等长、同序：提升后每项对应一个已建好的任务条目。
        self.entries: "Optional[list[_TaskEntry]]" = None

    @property
    def size(self) -> int:
        """本组需要（也是接纳时一次性占用）的 pending 名额数。"""
        return len(self.items)

    @property
    def task_ids(self) -> "list[str]":
        """本组占用的全部 task_id（输入顺序）。"""
        return [item.task_id for item in self.items]


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

    启用 ``aging_interval_ms`` 后，排队中（已接纳、尚未被工作线程认领）
    的任务自实际接纳时刻起每越过一个老化周期，派发比较用的有效优先级
    增加 1；派发依次按有效优先级降序、原 priority 降序、接受先后升序。
    老化不抢占、不重排执行中任务，原子认领后有效优先级冻结。

    :param workers: 工作线程数，必须为 >= 1 的整数。
    :param max_pending: 未完成任务（含排队中与执行中）上限，必须为 >= 1 的整数。
    :param aging_interval_ms: 可选排队优先级老化周期（毫秒），缺省 None
        表示关闭老化；启用时只接受 >= 1 的整数，布尔值、零、负数、
        浮点数或其他类型均抛 :class:`InputValidationError`。
    """

    def __init__(self, workers: int, max_pending: int,
                 aging_interval_ms: Optional[int] = None) -> None:
        if not self._is_positive_int(workers):
            raise InputValidationError(
                "workers must be an integer >= 1, got %r" % (workers,)
            )
        if not self._is_positive_int(max_pending):
            raise InputValidationError(
                "max_pending must be an integer >= 1, got %r" % (max_pending,)
            )
        if (aging_interval_ms is not None
                and not self._is_positive_int(aging_interval_ms)):
            raise InputValidationError(
                "aging_interval_ms must be an integer >= 1 or None "
                "(bool not allowed), got %r" % (aging_interval_ms,)
            )
        self._workers = workers
        self._max_pending = max_pending
        self._aging_interval_ms = aging_interval_ms

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
        # 入站堆：项为
        # (-有效优先级, -原优先级, 接受序号, 条目)——有效优先级高者先派发
        # （含老化加成），其次原优先级高者，最后接受序号小者（先接受）先
        # 派发。启用老化时键随时间变化，故每次派发前在当前时刻重排堆。
        self._inbound_cond = threading.Condition()
        self._inbound: list[tuple] = []
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
          成功、失败、取消或到期每释放一个名额，只唤醒并考察队首（可能是
          成组调用）：余量足以容纳队首整组时才一次性接纳，否则继续等待，
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

        item = _AdmissionItem(task_id, fn, priority, max_queue_wait_ms)
        with self._cond:
            entries = self._admit_items_locked(
                [item], admission_timeout_ms
            )
        entry = entries[0]

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def submit_batch_with_wait(
        self,
        tasks: "list[dict[str, Any]]",
        admission_timeout_ms: Optional[int] = None,
    ) -> "tuple[TaskHandle, ...]":
        """成组提交多个任务；整组原子地等待名额，立即返回句柄元组。

        ``tasks`` 必须是非空列表，每个元素是只含以下键的字典:

        - ``task_id``（必填）：非空字符串。
        - ``fn``（必填）：无参数 callable。
        - ``priority``（可选，缺省 0）：整数，语义同 :meth:`submit`。
        - ``max_queue_wait_ms``（可选，缺省 None）：>= 1 的整数毫秒或 None。

        返回与输入等长、同序的 :class:`TaskHandle` 元组；整组要么全部被
        接纳，要么全部不被接纳，不存在部分接纳。被接纳后各任务的执行、
        取消、到期与结果语义与 :meth:`submit_nowait` 完全一致：不抢占
        执行中的任务，认领前取消抛 :class:`TaskCancelledError`，排队到期
        抛 :class:`QueueTimeoutError` 且 callable 不执行，成功返回原值，
        失败传播原始异常；一个任务的终态不影响组内其他任务。

        ``admission_timeout_ms`` 的语义与 :meth:`submit_with_wait` 相同:

        - ``None``（缺省）：无足够余量时无限等待，直到整组获得名额。
        - ``0``：仅在调用瞬间余量足以容纳**整组**时接纳，否则立即抛
          :class:`BackpressureError`。
        - 正整数：最多等待该毫秒数；到达时限仍不足以容纳整组抛
          :class:`BackpressureError`。
        只接受 None 或非负整数，布尔值与负数抛 :class:`InputValidationError`。

        成组准入规则:

        - 本组与 :meth:`submit_with_wait` 调用按发起先后共用同一条 FIFO
          准入队列；每次名额释放只考察队首，余量足以容纳队首整组时才一次
          性接纳，否则继续等待。后续的单个任务或更小的组即使放得下也不
          得绕过尚未满足的队首大组。
        - 等待期间组内全部 task_id 即被占用：与已接受任务或其他等待者
          同名的提交抛 :class:`DuplicateTaskError`；组内 task_id 重复同样
          抛 :class:`InputValidationError`。
        - 任务数超过 ``max_pending`` 抛 :class:`InputValidationError`
          （整组永远不可能被容量容纳，不进入准入队列）。
        - 等待至准入时限：整组被拒并抛 :class:`BackpressureError`，
          rejected 只增加 1（一次调用算一次拒绝）；释放全部 task_id
          占用，不创建任何任务、不执行任何 callable、不改变其余计数或
          延迟样本。
        - :meth:`close` 开始时尚未被整组接纳：抛
          :class:`SchedulerClosedError`，rejected 不变，同样不建任务、
          不执行 callable。
        - 接纳瞬间组内任务共享同一接纳时刻，``max_queue_wait_ms`` 自该刻
          起算；accepted 一次增加组内任务数。派发仍按全局规则进行：
          priority 降序、同级按接受先后——组内任务的接受序号按输入顺序
          连续预留，故同优先级组内即按输入顺序派发。
        """
        items = self._validate_batch_args(tasks)
        if (admission_timeout_ms is not None
                and not self._is_nonneg_int(admission_timeout_ms)):
            raise InputValidationError(
                "admission_timeout_ms must be a non-negative integer or None "
                "(bool not allowed), got %r" % (admission_timeout_ms,)
            )

        with self._cond:
            entries = self._admit_items_locked(
                items, admission_timeout_ms
            )
        return tuple(TaskHandle(self, entry) for entry in entries)

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
        completed/failed 及三类延迟样本（含 execution_ms）按结束时刻归属。
        跨边界的任务因此在接纳所在区间计入 accepted，在结束所在区间计入
        completed 或 failed 并贡献延迟样本；边界两侧都不统计的事件不存在。

        同一 checkpoint 可反复用于 :meth:`snapshot_since`，不改变累计统计
        或后续区间；调度器关闭后仍可创建边界。
        """
        return self._stats.checkpoint()

    def snapshot_since(self, checkpoint: StatsCheckpoint) -> StatsSnapshot:
        """返回自 ``checkpoint`` 边界之后事件构成的区间统计快照。

        字段、分位口径与 :meth:`snapshot` 完全一致：六项计数只含边界后
        事件，``queue_wait_ms`` / ``total_latency_ms`` / ``execution_ms``
        只收集边界后成功或失败结束的任务样本；边界后无事件时计数全为 0，
        三个分布的 p50/p95/p99/max 均为 0.0。同一 checkpoint 可反复查询，
        不改变累计统计、延迟样本或任务状态。

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
                for tid in waiter.task_ids:
                    self._unfinished.discard(tid)
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

        满员（或准入队列仍有等待者）时立即计入 rejected 并抛
        :class:`BackpressureError`（submit / submit_nowait 的即时拒绝
        语义）。任何失败路径都不产生任务条目。

        成组等待者可能在仍有余量时排队（余量不足以容纳队首整组）：这些
        余量不能被即时提交插队使用，否则持续到达的小任务会让大组永远无法
        凑齐名额。故入队前先兜底放行一次队首，之后只要队列非空即按满员
        拒绝本次即时提交——保证 FIFO 不被绕过。

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
        # 严格 FIFO：准入队列中已有等待者（可能是余量不足以容纳的成组
        # 等待者）时，即时提交（submit / submit_nowait）不得利用队首暂时
        # 用不掉的余量插队。先兜底放行一次队首；只要仍有等待者，本次即时
        # 提交就按满员拒绝（语义与容量达上限完全一致），而不是绕过队首。
        if self._admission_queue:
            self._promote_waiter_locked()
        if self._admission_queue or self._pending >= self._max_pending:
            self._stats.record_rejected()
            raise BackpressureError(
                "pending task limit %d reached" % self._max_pending
            )

        seq = self._accept_seq
        self._accept_seq += 1
        admit_time = time.monotonic()
        entry = _TaskEntry(
            task_id, fn, admit_time, priority, seq,
            max_queue_wait_ms, self._aging_interval_ms,
        )
        self._tasks[task_id] = entry
        self._unfinished.add(task_id)
        self._pending += 1
        self._stats.record_accepted()
        self._enqueue_inbound_locked(entry, admit_time)
        return entry

    def _enqueue_inbound_locked(self, entry: "_TaskEntry",
                                now: Optional[float] = None) -> None:
        """把条目推入入站堆并唤醒事件循环；调用时须持有 ``_cond``。

        本方法在持 ``_cond`` 时嵌套获取 ``_inbound_cond``（锁序
        _cond -> _inbound_cond），使“接纳/提升”与“令牌入堆”对事件循环
        原子可见。键按入堆时刻计算：未启用老化时即静态键；启用老化时
        老化周期自接纳时刻起累计，事件循环在每次越过老化边界后会以
        当前时刻重排整个堆，再选取最高有效优先级任务。
        """
        if now is None:
            now = time.monotonic()
        with self._inbound_cond:
            heapq.heappush(self._inbound, _heap_key_at(entry, now))
            self._inbound_cond.notify()

    def _validate_batch_args(
        self, tasks: Any
    ) -> "list[_AdmissionItem]":
        """校验 submit_batch_with_wait 的任务列表并返回 item 列表。

        任务列表必须是非空 list；每项必须是 dict，且只含允许的键：
        task_id（非空 str，必填）、fn（callable，必填）、priority（整数，
        bool 非法，缺省 0）、max_queue_wait_ms（>= 1 的整数或 None，
        bool 非法，缺省 None）。组内 task_id 不允许重复；任务数超过
        max_pending 同样拒绝（整组永远无法被容量容纳）。

        纯参数校验：任何失败都在进入准入锁之前抛出，不占用 task_id、
        不改变任何计数。
        """
        if not isinstance(tasks, list) or len(tasks) == 0:
            raise InputValidationError(
                "tasks must be a non-empty list, got %r" % (tasks,)
            )
        if len(tasks) > self._max_pending:
            raise InputValidationError(
                "batch size %d exceeds max_pending %d"
                % (len(tasks), self._max_pending)
            )

        allowed = {"task_id", "fn", "priority", "max_queue_wait_ms"}
        items: "list[_AdmissionItem]" = []
        seen: set[str] = set()
        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                raise InputValidationError(
                    "task at index %d must be a dict, got %s"
                    % (index, type(task).__name__)
                )
            extra = set(task) - allowed
            if extra:
                raise InputValidationError(
                    "task at index %d has unexpected field(s): %s"
                    % (index, sorted(extra))
                )
            if "task_id" not in task:
                raise InputValidationError(
                    "task at index %d is missing field 'task_id'" % index
                )
            if "fn" not in task:
                raise InputValidationError(
                    "task at index %d is missing field 'fn'" % index
                )
            task_id = task["task_id"]
            fn = task["fn"]
            priority = task.get("priority", 0)
            max_queue_wait_ms = task.get("max_queue_wait_ms", None)
            # 单项字段值沿用与 submit 完全一致的校验口径。
            self._validate_submit_args(task_id, fn, priority, max_queue_wait_ms)
            if task_id in seen:
                raise InputValidationError(
                    "duplicate task_id within batch: %r" % (task_id,)
                )
            seen.add(task_id)
            items.append(
                _AdmissionItem(task_id, fn, priority, max_queue_wait_ms)
            )
        return items

    def _admit_items_locked(
        self, items: "list[_AdmissionItem]",
        admission_timeout_ms: Optional[int],
    ) -> "list[_TaskEntry]":
        """submit_with_wait / submit_batch_with_wait 共用的持锁准入。

        调用时须持有 ``_cond``。``items`` 为 1 项时即原单任务语义，多项
        时为整组原子准入。等待者入队即登记其全部 task_id（同名抛
        DuplicateTaskError）；获名额后在锁内一次性完成登记。整组等待至
        准入时限抛 BackpressureError（一次调用 rejected 只加 1），close
        开始则抛 SchedulerClosedError；两种拒绝都移除等待者并释放全部
        task_id，不创建任务、不执行 callable。
        """
        if self._closing:
            raise SchedulerClosedError("scheduler is closed")
        for item in items:
            if item.task_id in self._unfinished:
                raise DuplicateTaskError(
                    "task_id %r is already pending" % (item.task_id,)
                )

        # 严格 FIFO：已有等待者时，即便余量足够也先让队首获名额，新调用
        # 不得插队。正常不变量下“有等待者即没有可继续放行的余量”，此处
        # 兜底处理余量瞬态，避免后来者绕过队首或令队首无人唤醒。
        if self._admission_queue:
            self._promote_waiter_locked()

        if (not self._admission_queue
                and self._pending + len(items) <= self._max_pending):
            # 调用瞬间余量足以容纳整组且无人排队：立即接纳
            # （None 与 0 路径一致）。
            return self._register_admissions_locked(items)

        # 0 = 只在调用瞬间能容纳整组时接纳：不排队。
        if admission_timeout_ms == 0:
            self._stats.record_rejected()
            raise BackpressureError(
                "pending task limit %d reached" % self._max_pending
            )

        waiter = _AdmissionWaiter(items)
        self._admission_queue.append(waiter)
        for item in items:
            self._unfinished.add(item.task_id)

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
            # 全部任务条目（含连续接受序号、同一接纳时刻、pending 名额与
            # accepted 计数）已在提升那一刻由 _promote_waiter_locked 在
            # 锁内按 FIFO 建好。
            assert waiter.entries is not None
            return waiter.entries

        # 被唤醒的原因是关闭（close 已整队清出），或等待自身超时：
        # 仅当仍在队列中时移除自身，并释放本组占用的全部 task_id。
        if waiter in self._admission_queue:
            self._admission_queue.remove(waiter)
        for item in items:
            self._unfinished.discard(item.task_id)
        if waiter.closed:
            raise SchedulerClosedError("scheduler is closed")
        # 超时拒绝：整组等待期间可能已累积多个空闲名额（每次释放都因容不下
        # 队首整组而未放行）。本组退出后，按 FIFO 尽量放行后续放得下的
        # 等待者，再拒绝本次调用。
        while self._promote_waiter_locked():
            pass
        self._stats.record_rejected()
        raise BackpressureError(
            "admission to scheduler timed out after %d ms"
            % admission_timeout_ms
        )

    def _register_admissions_locked(
        self, items: "list[_AdmissionItem]"
    ) -> "list[_TaskEntry]":
        """在容量已确认可容纳整组时，原子登记全部任务并入堆。

        调用时须持有 ``_cond``，且调用方已确认
        ``self._pending + len(items) <= self._max_pending``、调度器未关闭、
        各 task_id 均未占用。接纳是一次性的：组内任务共享同一接纳时刻
        （max_queue_wait_ms 的同一锚点），按输入顺序预留连续的接受序号
        （同级组内即输入顺序派发），accepted 一次增加组内任务数，随后各
        令牌按各自 priority 与 seq 原子推入入站堆（锁序
        _cond -> _inbound_cond）。返回与 items 等长、同序的条目列表。
        """
        admit_time = time.monotonic()
        entries: "list[_TaskEntry]" = []
        for item in items:
            seq = self._accept_seq
            self._accept_seq += 1
            entry = _TaskEntry(
                item.task_id, item.fn, admit_time,
                item.priority, seq, item.max_queue_wait_ms,
                self._aging_interval_ms,
            )
            self._tasks[item.task_id] = entry
            self._unfinished.add(item.task_id)
            self._pending += 1
            self._stats.record_accepted()
            self._enqueue_inbound_locked(entry, admit_time)
            entries.append(entry)
        return entries

    def _promote_waiter_locked(self) -> bool:
        """若余量足以容纳队首整组，则把队首整组提升为已接纳。

        调用时须持有 ``_cond``。提升是真正的接纳时刻：此处一次性按 FIFO
        预留连续接受序号、记录同一接纳时刻（max_queue_wait_ms 起算点）、
        占用整组全部 pending 名额、登记 task_id/任务条目、按组内任务数
        计入 accepted，并把各令牌原子推入入站堆（锁序
        _cond -> _inbound_cond）。因此这些结果既不依赖等待线程之后被
        调度唤醒的先后，也不存在“已接纳但未入堆”被后来者越过的窗口。

        余量不足以容纳队首整组时不提升、不跳过：后续等待者（哪怕更小、
        放得下）也必须排在后面，严格 FIFO 不允许绕过。返回是否提升了一个
        等待者；一次释放最多提升队首一组。
        """
        while self._admission_queue:
            waiter = self._admission_queue.popleft()
            if waiter.closed:
                # close 唤醒途中残留的等待者不会出现（关闭时整队列出队），
                # 这里仅作防御性跳过。
                continue
            if self._pending + waiter.size > self._max_pending:
                self._admission_queue.appendleft(waiter)
                return False
            waiter.entries = self._register_admissions_locked(waiter.items)
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
            # 名额释放：若有准入等待者，在锁内考察队首——余量足以容纳其
            # 整组时一次性接纳，否则本次释放不接纳任何人。
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
        # 到期同样释放名额：一次释放只考察队首（可能是成组调用）。
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
        while self._inbound and self._inbound[0][3].done.is_set():
            heapq.heappop(self._inbound)

    def _reheapify_locked(self, now: float) -> None:
        """按 ``now`` 重算全部未结束任务的有效优先级并重建堆；调用时须持有
        ``_inbound_cond``。

        老化只改变尚未被工作线程认领任务的派发先后：有效优先级随已完成
        老化周期数增长，键是时间的函数，故每次越过老化边界后以当前时刻
        重建堆，使随后选取的是最新有效优先级最高的任务。已结束条目的
        惰性令牌原样保留，仍由堆顶剪枝/弹出复检丢弃。
        """
        self._inbound = [
            _heap_key_at(item[3], now) if not item[3].done.is_set()
            else item
            for item in self._inbound
        ]
        heapq.heapify(self._inbound)

    def _aging_wait_locked(self) -> float:
        """返回到最近一个老化边界的等待秒数（上限 _DISPATCH_POLL）；调用时
        须持有 ``_inbound_cond``。堆中没有参与老化的未结束任务时返回
        _DISPATCH_POLL。用于在两次派发之间及时醒来，按“越过边界即比较”
        重排堆。
        """
        now = time.monotonic()
        waits: list[float] = []
        for _, _, _, entry in self._inbound:
            if entry.done.is_set() or entry.aging_interval_ms is None:
                continue
            w = _aging_boundary(entry, now)
            if w is not None:
                waits.append(w)
        if not waits:
            return _DISPATCH_POLL
        return min(_DISPATCH_POLL, max(0.0, min(waits)))

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
            for _, _, _, entry in self._inbound
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
            for _, _, _, entry in self._inbound
            if entry.deadline is not None and not entry.done.is_set()
        ]
        if not waits:
            return _DISPATCH_POLL
        return min(_DISPATCH_POLL, max(0.0, min(waits)))

    def _pop_next_locked(self, now: Optional[float] = None
                         ) -> "Optional[_TaskEntry]":
        """弹出并返回当前堆中最高有效优先级的未结束任务，调用时须持有
        ``_inbound_cond``。

        启用老化时先以 ``now``（缺省为当前单调时刻）重建堆，使“越过老化
        边界后立即比较”落到选取上：后接受但有效优先级更高的任务可先派发，
        有效优先级与原 priority 都相同则仍由接受先后决定。等待派发期间
        已结束（取消/到期）任务的惰性令牌会被依次丢弃（堆顶剪枝之外，
        弹出的条目也再确认一次 ``done``，以覆盖与取消/到期线程的最后
        窗口）；堆中没有可派发任务时返回 None。
        """
        if self._aging_interval_ms is not None:
            self._reheapify_locked(
                time.monotonic() if now is None else now
            )
        while self._inbound:
            _, _, _, entry = heapq.heappop(self._inbound)
            if not entry.done.is_set():
                return entry
        return None

    def _run_dispatcher(self) -> None:
        """事件循环：取得空闲工作线程许可后，按优先级把任务派发到就绪队列。

        顺序为有效优先级（启用老化时含老化加成）降序、原 priority 降序、
        接受先后（FCFS）。关键次序是“先取得许可、再在堆顶选取”：若先选定
        任务再等许可，先入队的低优先级任务会占住派发位置，挡住后到的高
        优先级任务。

        许可等待带超时：取最近一次认领截止时刻与最近一个老化边界（启用
        老化时）、_DISPATCH_POLL 的较小者。超时回到循环顶部处理到期任务
        与停止信号；真正选取前还会按当前时刻重排堆，因此越过老化边界后
        立即按最新有效优先级比较——既不会漏掉到期任务，也不会用过时的
        优先级派发。
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
                    if self._aging_interval_ms is not None:
                        wait = min(wait, self._aging_wait_locked())

            # 到期裁决需要 _cond，放到 _inbound_cond 之外进行（锁序
            # _cond -> _inbound_cond），避免持 _inbound_cond 反取 _cond。
            self._expire_entries(due)
            if stopping:
                return

            # 取得一个空闲工作线程许可后再选取任务，保证选出的任务立即
            # 有线程可以执行；就绪队列中至多有 workers 个待执行任务。
            if not self._worker_slots.acquire(timeout=wait):
                # 许可等待超时：回循环顶部处理到期任务/停止信号；选取前
                # 总会按当前时刻重排堆，等待期间越过的老化边界不会丢失。
                continue
            if self._stop_dispatcher.is_set():
                self._worker_slots.release()
                return
            with self._inbound_cond:
                due = self._collect_due_locked()
            self._expire_entries(due)
            with self._inbound_cond:
                # 启用老化时此处按当前单调时刻重排堆后再选取，使越过老化
                # 边界的低优先级任务立即获得与新有效优先级相符的派发位置。
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
            start_time = 0.0
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
                    # 执行计时起点锚定在锁内的原子认领时刻：不含认领前的
                    # 排队等待，也不含释放锁到真正调用 callable 之间的调度空隙。
                    start_time = time.monotonic()
            if expired:
                entry.done.set()
                self._worker_slots.release()
                continue

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
            # 只统计真正执行 callable 的时间：认领后到正常返回或抛出 Exception。
            execution_ms = to_ms(end_time - start_time)

            # 先记账再唤醒：被唤醒的 submit 调用方返回后即可读到一致统计。
            self._stats.record_finished(
                queue_wait_ms, total_latency_ms, execution_ms, success
            )
            with self._cond:
                self._pending -= 1
                self._unfinished.discard(entry.task_id)
                # 成功/失败释放名额：只考察准入队列队首（可能是整组）。
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
