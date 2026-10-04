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
        """记录一个在开始执行前被取消的任务。

        取消任务计入 cancelled，但不贡献任何延迟样本。
        """
        with self._lock:
            self._cancelled += 1

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
                rejected=self._rejected,
                queue_wait_samples=list(self._queue_wait),
                total_latency_samples=list(self._total_latency),
            )


class StatsSnapshot:
    """某一时刻的累计统计快照（值拷贝，创建后不再变化）。"""

    __slots__ = (
        "accepted",
        "completed",
        "failed",
        "cancelled",
        "rejected",
        "_queue_wait_samples",
        "_total_latency_samples",
    )

    def __init__(self, *, accepted: int, completed: int, failed: int,
                 cancelled: int, rejected: int,
                 queue_wait_samples: List[float],
                 total_latency_samples: List[float]) -> None:
        self.accepted = accepted
        self.completed = completed
        self.failed = failed
        self.cancelled = cancelled
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
            "rejected": self.rejected,
            "queue_wait_ms": self.queue_wait_ms,
            "total_latency_ms": self.total_latency_ms,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            "StatsSnapshot(accepted={a}, completed={c}, failed={f}, "
            "cancelled={cn}, rejected={r}, queue_wait_ms={qw}, "
            "total_latency_ms={tl})".format(
                a=self.accepted, c=self.completed, f=self.failed,
                cn=self.cancelled, r=self.rejected, qw=self.queue_wait_ms,
                tl=self.total_latency_ms,
            )
        )
