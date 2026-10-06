"""延迟分布统计。

统计三类延迟样本（单位毫秒，保留三位小数）:

- ``queue_wait_ms`` -- 任务从被接受到开始执行（工作线程原子认领）的排队等待时间。
- ``total_latency_ms`` -- 任务从被接受到执行结束（成功或失败）的总时延。
- ``execution_ms`` -- 任务从被工作线程原子认领后开始，到 callable 正常返回
  或抛出 Exception 为止的真正执行耗时；不含认领前的排队等待，也不含结束
  后的记账/唤醒开销。

每个完成的任务（无论成功或失败）都同时贡献三组样本；未开始执行就被取消或
排队到期的任务不贡献 execution_ms。
百分位取排序后第 ``ceil(n * q)`` 个样本（1-based），空样本的分位值与 max 均为 0。

除启动以来的累计快照外，还支持区间观测：:meth:`Stats.checkpoint` 在与
统计事件相同的锁内创建不可变的 :class:`StatsCheckpoint` 边界，
:meth:`Stats.snapshot_since` 返回该边界之后事件构成的 :class:`StatsSnapshot`。
每个事件在创建边界的原子顺序上只落入一侧，相邻区间既不重复也不遗漏。

构造时还可选启用滑动窗口延迟观测（rolling_window 为保留的最近结束任务
数，None 关闭）：窗口按任务结束顺序保留最近 N 个任务的
（queue_wait_ms、total_latency_ms、execution_ms）三元组并同步进出，
:meth:`Stats.rolling_snapshot` 返回不可变 :class:`RollingStatsSnapshot`。
窗口样本只在与累计/区间记账同一把锁、同一次 record_finished 内追加，
故只含已记账的成功/失败结束；取消、到期、拒绝、未接纳任务永不入窗。
窗口与累计计数、延迟样本、区间边界互不影响。
"""

from __future__ import annotations

import math
import threading
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

# 单调时钟差值（秒）-> 毫秒的换算因子。
_SEC_PER_MS = 1_000.0


def to_ms(seconds: float) -> float:
    """秒为单位的单调时钟差值转换为保留三位小数的毫秒值。"""
    return round(seconds * _SEC_PER_MS, 3)


def percentile(sorted_samples: List[float], q: float) -> float:
    """返回已升序排序样本的第 q 分位值。

    取排序后第 ``ceil(n * q)`` 个样本（1-based，即下标 ``ceil(n*q) - 1``）。
    空样本返回 0。``q`` 取值范围为 (0, 1]。
    """
    n = len(sorted_samples)
    if n == 0:
        return 0.0
    rank = math.ceil(n * q)
    # 浮点误差防护：rank 被钳制在 [1, n]。
    if rank < 1:
        rank = 1
    elif rank > n:
        rank = n
    return sorted_samples[rank - 1]


def _distribution(samples: List[float]) -> Dict[str, float]:
    """由样本构造包含 p50/p95/p99/max 的分布字典（值统一保留三位小数）。"""
    ordered = sorted(samples)
    return {
        "p50": round(percentile(ordered, 0.50), 3),
        "p95": round(percentile(ordered, 0.95), 3),
        "p99": round(percentile(ordered, 0.99), 3),
        "max": round(ordered[-1], 3) if ordered else 0.0,
    }


class Stats:
    """线程安全的累计计数器与延迟样本收集器。

    ``rolling_window`` 为 None（缺省）时关闭滑动窗口观测；否则必须是
    >= 1 的整数，窗口按任务结束顺序保留最近该数量个任务的三元延迟样本。
    """

    def __init__(self, rolling_window: Optional[int] = None) -> None:
        self._lock = threading.Lock()
        self._accepted = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._expired = 0
        self._rejected = 0
        self._queue_wait: List[float] = []
        self._total_latency: List[float] = []
        # 执行耗时样本：与上述两类样本一一对应，每个成功或失败结束的任务
        # 恰好贡献一个；取消/到期/被拒任务不追加。
        self._execution: List[float] = []
        # 滑动窗口：None 表示关闭；启用时是有界 deque，按任务结束顺序保留
        # 最近 _rolling_window 个任务的（queue_wait_ms, total_latency_ms,
        # execution_ms）三元组，超出容量时最旧的样本从同一端同步离开。
        self._rolling_window = rolling_window
        self._rolling: "Optional[Deque[Tuple[float, float, float]]]" = (
            deque(maxlen=rolling_window) if rolling_window is not None else None
        )
        # 区间边界：checkpoint_id -> 创建瞬间的（六项累计计数，三类样本长度）。
        # 边界数据保存在 Stats 内而不是 checkpoint 对象上：checkpoint 只是
        # 不可变令牌，任何被篡改/伪造的令牌都无法通过 id 校验。
        self._checkpoints: Dict[int, tuple] = {}
        self._checkpoint_seq = 0

    def record_accepted(self) -> None:
        with self._lock:
            self._accepted += 1

    def record_rejected(self) -> None:
        with self._lock:
            self._rejected += 1

    def record_cancelled(self) -> None:
        """记录一个在开始执行前被取消的任务：计入 cancelled，无延迟样本。"""
        with self._lock:
            self._cancelled += 1

    def record_expired(self) -> None:
        """记录一个认领前排队到期的任务：计入 expired，无延迟样本。"""
        with self._lock:
            self._expired += 1

    def record_finished(self, queue_wait_ms: float, total_latency_ms: float,
                        execution_ms: float, success: bool) -> None:
        """记录一个已结束任务：成功计入 completed，否则计入 failed。

        成功或失败都恰好追加一个 execution_ms 执行耗时样本；queue_wait_ms /
        total_latency_ms 样本与执行样本一一对应。启用滑动窗口时，三类样本
        组成的三元组在同一把锁、同一次记账内按结束顺序进入窗口（窗口已满
        时最旧三元组同步离开），与累计样本和区间归属处于同一原子顺序。
        """
        with self._lock:
            if success:
                self._completed += 1
            else:
                self._failed += 1
            self._queue_wait.append(queue_wait_ms)
            self._total_latency.append(total_latency_ms)
            self._execution.append(execution_ms)
            if self._rolling is not None:
                self._rolling.append(
                    (queue_wait_ms, total_latency_ms, execution_ms)
                )

    def snapshot(self) -> "StatsSnapshot":
        """返回当前累计值的不可变快照；不随后续任务变化。"""
        with self._lock:
            return StatsSnapshot(
                accepted=self._accepted,
                completed=self._completed,
                failed=self._failed,
                cancelled=self._cancelled,
                expired=self._expired,
                rejected=self._rejected,
                queue_wait_samples=list(self._queue_wait),
                total_latency_samples=list(self._total_latency),
                execution_samples=list(self._execution),
            )

    def rolling_snapshot(self) -> "RollingStatsSnapshot":
        """返回当前滑动窗口的不可变快照（值拷贝，创建后不再变化）。

        窗口内的三元组在与累计/区间统计相同的锁内复制，故与
        :meth:`snapshot` / :meth:`snapshot_since` 处于同一记账顺序：只含
        已经记账的成功/失败结束，且三类样本严格同步（同属窗口内那批结束
        任务）。未启用窗口时返回 window_size=0、sampled_finished=0 与三个
        空分布的快照。可反复调用，不改变累计统计、区间或窗口内容。
        """
        with self._lock:
            if self._rolling is None:
                return RollingStatsSnapshot(
                    window_size=0, samples=[]
                )
            return RollingStatsSnapshot(
                window_size=self._rolling_window,  # type: ignore[arg-type]
                samples=list(self._rolling),
            )

    def checkpoint(self) -> "StatsCheckpoint":
        """创建一个区间观测边界。

        边界在与所有 ``record_*`` 事件相同的锁内取得，因此与事件记录处于
        同一原子顺序：先于边界落锁的事件全部计入边界前，之后的全部计入
        边界后，不存在横跨两侧的事件。创建边界不改变任何计数或延迟样本。
        """
        with self._lock:
            checkpoint_id = self._checkpoint_seq
            self._checkpoint_seq += 1
            self._checkpoints[checkpoint_id] = (
                self._accepted,
                self._completed,
                self._failed,
                self._cancelled,
                self._expired,
                self._rejected,
                len(self._queue_wait),
                len(self._total_latency),
                len(self._execution),
            )
        return StatsCheckpoint(checkpoint_id, self)

    def snapshot_since(self, checkpoint: "StatsCheckpoint") -> "StatsSnapshot":
        """返回边界之后事件构成的差值快照（值拷贝，创建后不再变化）。

        只统计边界后的事件：六项计数为当前累计值减去边界处累计值；三类
        延迟样本只取边界后追加的部分（即边界后成功或失败结束的任务，
        execution_ms 与 queue_wait_ms / total_latency_ms 一一对应）。
        边界后无事件时计数全为 0，三个分布的 p50/p95/p99/max 均为 0.0。
        可反复查询，不改变累计统计，也不影响后续区间。

        入参不是本收集器创建的 :class:`StatsCheckpoint`（含其他收集器的
        边界或字段缺失/被篡改的损坏对象）时抛 :class:`ValueError`，且不
        改变任何状态；调度器层将其转译为 ``InputValidationError``。
        """
        if not isinstance(checkpoint, StatsCheckpoint):
            raise ValueError(
                "checkpoint must be a StatsCheckpoint created by the "
                "scheduler, got %s" % (type(checkpoint).__name__,)
            )
        try:
            owner = checkpoint._owner
            checkpoint_id = checkpoint._checkpoint_id
        except AttributeError:
            # 不能在此对 checkpoint 使用 %r：损坏对象的 __repr__ 同样可能
            # 因字段缺失而抛 AttributeError，反而掩盖本校验错误。
            raise ValueError(
                "corrupted StatsCheckpoint: missing boundary fields"
            ) from None
        # 显式归属校验：不同 Stats 的边界序号都从 0 开始，仅比对 id 会把
        # 别的调度器的边界误认作本调度器的边界；bool 是 int 子类，同样拒绝。
        if owner is not self or not isinstance(checkpoint_id, int) \
                or isinstance(checkpoint_id, bool):
            raise ValueError(
                "checkpoint does not belong to this scheduler (id=%r)"
                % (checkpoint_id,)
            )
        with self._lock:
            boundary = self._checkpoints.get(checkpoint_id)
            if boundary is None:
                raise ValueError(
                    "unknown StatsCheckpoint for this scheduler (id=%r)"
                    % (checkpoint_id,)
                )
            (base_accepted, base_completed, base_failed, base_cancelled,
             base_expired, base_rejected,
             base_queue_wait_len, base_total_latency_len,
             base_execution_len) = boundary
            # 样本只追加、不移除：边界长度之后的切片恰为边界后结束的任务。
            queue_wait_samples = self._queue_wait[base_queue_wait_len:]
            total_latency_samples = self._total_latency[base_total_latency_len:]
            execution_samples = self._execution[base_execution_len:]
            return StatsSnapshot(
                accepted=self._accepted - base_accepted,
                completed=self._completed - base_completed,
                failed=self._failed - base_failed,
                cancelled=self._cancelled - base_cancelled,
                expired=self._expired - base_expired,
                rejected=self._rejected - base_rejected,
                queue_wait_samples=queue_wait_samples,
                total_latency_samples=total_latency_samples,
                execution_samples=execution_samples,
            )


class StatsSnapshot:
    """某一时刻的累计统计快照（值拷贝，创建后不再变化）。"""

    __slots__ = (
        "accepted",
        "completed",
        "failed",
        "cancelled",
        "expired",
        "rejected",
        "_queue_wait_samples",
        "_total_latency_samples",
        "_execution_samples",
    )

    def __init__(self, *, accepted: int, completed: int, failed: int,
                 cancelled: int, expired: int, rejected: int,
                 queue_wait_samples: List[float],
                 total_latency_samples: List[float],
                 execution_samples: List[float]) -> None:
        self.accepted = accepted
        self.completed = completed
        self.failed = failed
        self.cancelled = cancelled
        self.expired = expired
        self.rejected = rejected
        self._queue_wait_samples = list(queue_wait_samples)
        self._total_latency_samples = list(total_latency_samples)
        self._execution_samples = list(execution_samples)

    @property
    def queue_wait_ms(self) -> Dict[str, float]:
        return _distribution(self._queue_wait_samples)

    @property
    def total_latency_ms(self) -> Dict[str, float]:
        return _distribution(self._total_latency_samples)

    @property
    def execution_ms(self) -> Dict[str, float]:
        return _distribution(self._execution_samples)

    def to_dict(self) -> Dict[str, object]:
        """转为可 JSON 序列化的字典。"""
        return {
            "accepted": self.accepted,
            "completed": self.completed,
            "failed": self.failed,
            "cancelled": self.cancelled,
            "expired": self.expired,
            "rejected": self.rejected,
            "queue_wait_ms": self.queue_wait_ms,
            "total_latency_ms": self.total_latency_ms,
            "execution_ms": self.execution_ms,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            "StatsSnapshot(accepted={a}, completed={c}, failed={f}, "
            "cancelled={cn}, expired={ex}, rejected={r}, queue_wait_ms={qw}, "
            "total_latency_ms={tl}, execution_ms={exd})".format(
                a=self.accepted, c=self.completed, f=self.failed,
                cn=self.cancelled, ex=self.expired, r=self.rejected,
                qw=self.queue_wait_ms, tl=self.total_latency_ms,
                exd=self.execution_ms,
            )
        )


class StatsCheckpoint:
    """区间统计的不可变观测边界令牌。

    只能由拥有其统计收集器（:class:`Stats`，通常由对应调度器）经
    :meth:`Stats.checkpoint` 创建：令牌上只保存边界序号与归属收集器的
    反向引用，边界处的累计值保存在收集器内部，因此令牌被篡改也无法伪造
    或改动任何边界。创建后字段不可再赋值或删除；可反复用于
    :meth:`Stats.snapshot_since`，反复查询不改变累计统计或后续区间。
    """

    __slots__ = ("_checkpoint_id", "_owner")

    def __init__(self, checkpoint_id: int, owner: "Stats") -> None:
        object.__setattr__(self, "_checkpoint_id", checkpoint_id)
        object.__setattr__(self, "_owner", owner)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("StatsCheckpoint is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("StatsCheckpoint is immutable")

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        # 对绕过 __init__ 构造的损坏对象保持可用，避免二次抛错。
        try:
            return "StatsCheckpoint(id=%d)" % (self._checkpoint_id,)
        except AttributeError:
            return "StatsCheckpoint(<corrupted>)"


class RollingStatsSnapshot:
    """滑动窗口延迟观测的不可变快照。

    只能由 :class:`Stats`（通常经 :meth:`Scheduler.rolling_snapshot`）创建：
    全部字段在统计锁的同一次持锁区间内由窗口当前内容拷贝得到，构成同一
    记账时刻的一致只读视图，创建后不再随任务结束而变化。

    固定字段:

    - ``window_size``：窗口容量 N（构造参数 latency_window_tasks）；
      未启用窗口时为 0。
    - ``sampled_finished``：窗口内当前保留的结束任务数（即三类分布共用的
      样本数）；窗口未满时等于启用后成功/失败结束的任务总数，窗口满后恒为
      ``window_size``。未启用窗口时为 0。
    - ``queue_wait_ms`` / ``total_latency_ms`` / ``execution_ms``：窗口内
      样本（按任务结束顺序）构造的 p50/p95/p99/max 分布，键、ceil(n*q)
      取位与三位小数口径与 :class:`StatsSnapshot` 完全一致；空窗口四个值
      均为 0.0。三类样本来自同一批结束任务的三元组，严格一一对应。

    字段创建后不可赋值或删除；:meth:`to_dict` 只返回上述同名且 JSON 可
    序列化的字典，重复读取结果稳定。
    """

    __slots__ = (
        "window_size",
        "sampled_finished",
        "_queue_wait_samples",
        "_total_latency_samples",
        "_execution_samples",
    )

    def __init__(self, *, window_size: int,
                 samples: "List[Tuple[float, float, float]]") -> None:
        triples = list(samples)
        object.__setattr__(self, "window_size", window_size)
        object.__setattr__(self, "sampled_finished", len(triples))
        object.__setattr__(
            self, "_queue_wait_samples", [t[0] for t in triples]
        )
        object.__setattr__(
            self, "_total_latency_samples", [t[1] for t in triples]
        )
        object.__setattr__(
            self, "_execution_samples", [t[2] for t in triples]
        )

    @property
    def queue_wait_ms(self) -> Dict[str, float]:
        return _distribution(self._queue_wait_samples)

    @property
    def total_latency_ms(self) -> Dict[str, float]:
        return _distribution(self._total_latency_samples)

    @property
    def execution_ms(self) -> Dict[str, float]:
        return _distribution(self._execution_samples)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("RollingStatsSnapshot is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("RollingStatsSnapshot is immutable")

    def to_dict(self) -> Dict[str, object]:
        """转为只含固定字段、可 JSON 序列化的字典。"""
        return {
            "window_size": self.window_size,
            "sampled_finished": self.sampled_finished,
            "queue_wait_ms": self.queue_wait_ms,
            "total_latency_ms": self.total_latency_ms,
            "execution_ms": self.execution_ms,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            "RollingStatsSnapshot(window_size={w}, sampled_finished={n}, "
            "queue_wait_ms={qw}, total_latency_ms={tl}, "
            "execution_ms={ex})".format(
                w=self.window_size, n=self.sampled_finished,
                qw=self.queue_wait_ms, tl=self.total_latency_ms,
                ex=self.execution_ms,
            )
        )
