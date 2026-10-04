"""延迟分布统计。

统计两类延迟样本（单位毫秒，保留三位小数）:

- ``queue_wait_ms`` -- 任务从被接受到开始执行的排队等待时间。
- ``total_latency_ms`` -- 任务从被接受到执行结束（成功或失败）的总时延。

每个完成的任务（无论成功或失败）都同时贡献两组样本。
百分位取排序后第 ``ceil(n * q)`` 个样本（1-based），空样本的分位值与 max 均为 0。
"""

from __future__ import annotations

import math
import threading
from typing import Dict, List

from .errors import InputValidationError

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
    """线程安全的累计计数器与延迟样本收集器。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._accepted = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        self._expired = 0
        self._rejected = 0
        self._queue_wait: List[float] = []
        self._total_latency: List[float] = []

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
                        success: bool) -> None:
        """记录一个已结束任务：成功计入 completed，否则计入 failed。"""
        with self._lock:
            if success:
                self._completed += 1
            else:
                self._failed += 1
            self._queue_wait.append(queue_wait_ms)
            self._total_latency.append(total_latency_ms)

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
            )

    def checkpoint(self) -> "StatsCheckpoint":
        """创建一个与事件记录同一原子顺序的统计边界。

        在与 record_* 相同的锁内捕获当前累计计数与样本长度：锁序上
        先于 checkpoint 的事件整体归入边界之前，后于 checkpoint 的事件
        整体归入边界之后，不存在跨边界被重复或遗漏统计的事件。
        """
        with self._lock:
            return StatsCheckpoint(
                owner=self,
                accepted=self._accepted,
                completed=self._completed,
                failed=self._failed,
                cancelled=self._cancelled,
                expired=self._expired,
                rejected=self._rejected,
                queue_wait_len=len(self._queue_wait),
                total_latency_len=len(self._total_latency),
            )

    def snapshot_since(self, checkpoint: "StatsCheckpoint") -> "StatsSnapshot":
        """返回自 ``checkpoint`` 边界以来的区间统计快照。

        计数为当前累计值与边界值之差；延迟样本为边界之后追加的样本
        （样本列表只增不减，故按边界处长度切片即得）。本方法只读，
        不改变累计统计，也不影响同一 checkpoint 的后续查询。
        """
        base = self._validate_checkpoint(checkpoint)
        with self._lock:
            return StatsSnapshot(
                accepted=self._accepted - base.accepted,
                completed=self._completed - base.completed,
                failed=self._failed - base.failed,
                cancelled=self._cancelled - base.cancelled,
                expired=self._expired - base.expired,
                rejected=self._rejected - base.rejected,
                queue_wait_samples=self._queue_wait[base.queue_wait_len:],
                total_latency_samples=(
                    self._total_latency[base.total_latency_len:]
                ),
            )

    def _validate_checkpoint(self, checkpoint: object) -> "_CheckpointState":
        """校验 checkpoint 属于本统计器且字段完好，返回其边界值。

        非 StatsCheckpoint、属于其他调度器的 checkpoint 或字段损坏的
        对象一律抛 :class:`InputValidationError`；校验在任何读取之前
        完成，失败路径不改变计数、延迟样本或任何任务状态。
        """
        if not isinstance(checkpoint, StatsCheckpoint):
            raise InputValidationError(
                "checkpoint must be a StatsCheckpoint created by "
                "Scheduler.stats_checkpoint, got %r" % (checkpoint,)
            )
        if getattr(checkpoint, "_owner", None) is not self:
            raise InputValidationError(
                "checkpoint belongs to a different scheduler"
            )
        fields = {}
        for name in (
            "_accepted", "_completed", "_failed", "_cancelled",
            "_expired", "_rejected", "_queue_wait_len", "_total_latency_len",
        ):
            value = getattr(checkpoint, name, None)
            if (not isinstance(value, int) or isinstance(value, bool)
                    or value < 0):
                raise InputValidationError(
                    "checkpoint is corrupted: field %s is %r" % (name, value)
                )
            fields[name] = value
        return _CheckpointState(
            accepted=fields["_accepted"],
            completed=fields["_completed"],
            failed=fields["_failed"],
            cancelled=fields["_cancelled"],
            expired=fields["_expired"],
            rejected=fields["_rejected"],
            queue_wait_len=fields["_queue_wait_len"],
            total_latency_len=fields["_total_latency_len"],
        )


class _CheckpointState:
    """校验通过后从 checkpoint 提取的边界值（仅供 snapshot_since 使用）。"""

    __slots__ = (
        "accepted", "completed", "failed", "cancelled", "expired",
        "rejected", "queue_wait_len", "total_latency_len",
    )

    def __init__(self, *, accepted: int, completed: int, failed: int,
                 cancelled: int, expired: int, rejected: int,
                 queue_wait_len: int, total_latency_len: int) -> None:
        self.accepted = accepted
        self.completed = completed
        self.failed = failed
        self.cancelled = cancelled
        self.expired = expired
        self.rejected = rejected
        self.queue_wait_len = queue_wait_len
        self.total_latency_len = total_latency_len


class StatsCheckpoint:
    """某一统计边界的不可变检查点，由 :meth:`Scheduler.stats_checkpoint` 创建。

    检查点在统计器的事件锁内捕获累计计数与样本长度，因此它与每一项
    统计事件（接纳、拒绝、取消、到期、结束）处于同一原子顺序：边界
    之前的事件只计入边界之前的区间，边界之后的事件只计入
    :meth:`Scheduler.snapshot_since` 的区间。检查点本身只读，可反复
    用于查询，且不影响累计统计。
    """

    __slots__ = (
        "_owner", "_accepted", "_completed", "_failed", "_cancelled",
        "_expired", "_rejected", "_queue_wait_len", "_total_latency_len",
        "_frozen",
    )

    def __init__(self, *, owner: "Stats", accepted: int, completed: int,
                 failed: int, cancelled: int, expired: int, rejected: int,
                 queue_wait_len: int, total_latency_len: int) -> None:
        self._owner = owner
        self._accepted = accepted
        self._completed = completed
        self._failed = failed
        self._cancelled = cancelled
        self._expired = expired
        self._rejected = rejected
        self._queue_wait_len = queue_wait_len
        self._total_latency_len = total_latency_len
        self._frozen = True

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError("StatsCheckpoint is immutable")
        object.__setattr__(self, name, value)

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            "StatsCheckpoint(accepted={a}, completed={c}, failed={f}, "
            "cancelled={cn}, expired={ex}, rejected={r})".format(
                a=self._accepted, c=self._completed, f=self._failed,
                cn=self._cancelled, ex=self._expired, r=self._rejected,
            )
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
    )

    def __init__(self, *, accepted: int, completed: int, failed: int,
                 cancelled: int, expired: int, rejected: int,
                 queue_wait_samples: List[float],
                 total_latency_samples: List[float]) -> None:
        self.accepted = accepted
        self.completed = completed
        self.failed = failed
        self.cancelled = cancelled
        self.expired = expired
        self.rejected = rejected
        self._queue_wait_samples = list(queue_wait_samples)
        self._total_latency_samples = list(total_latency_samples)

    @property
    def queue_wait_ms(self) -> Dict[str, float]:
        return _distribution(self._queue_wait_samples)

    @property
    def total_latency_ms(self) -> Dict[str, float]:
        return _distribution(self._total_latency_samples)

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
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            "StatsSnapshot(accepted={a}, completed={c}, failed={f}, "
            "cancelled={cn}, expired={ex}, rejected={r}, queue_wait_ms={qw}, "
            "total_latency_ms={tl})".format(
                a=self.accepted, c=self.completed, f=self.failed,
                cn=self.cancelled, ex=self.expired, r=self.rejected,
                qw=self.queue_wait_ms, tl=self.total_latency_ms,
            )
        )
