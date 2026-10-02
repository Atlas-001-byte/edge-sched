"""Latency sample collection and immutable statistics snapshots."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

_QUANTILES: Tuple[Tuple[str, float], ...] = (
    ("p50", 0.50),
    ("p95", 0.95),
    ("p99", 0.99),
)


def percentile(sorted_samples: List[float], q: float) -> float:
    """Return the nearest-rank percentile ``q`` (0 < q <= 1).

    The value is the ``ceil(n * q)``-th sample (1-indexed) of the sorted
    input. An empty sample set yields ``0.0``.
    """
    n = len(sorted_samples)
    if n == 0:
        return 0.0
    rank = math.ceil(n * q)
    if rank < 1:
        rank = 1
    elif rank > n:
        rank = n
    return sorted_samples[rank - 1]


@dataclass(frozen=True)
class LatencyDistribution:
    """A sample set with its percentile summary.

    All durations are in milliseconds rounded to three decimal places.
    """

    samples: List[float]
    p50: float
    p95: float
    p99: float
    max: float

    def to_dict(self) -> Dict[str, object]:
        return {
            "samples": list(self.samples),
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
            "max": self.max,
        }


@dataclass(frozen=True)
class Snapshot:
    """Immutable cumulative statistics snapshot.

    Counts are lifetime totals; latency samples cover every accepted task
    that has finished, including failed ones. A snapshot never changes when
    later tasks complete.
    """

    accepted: int
    completed: int
    failed: int
    rejected: int
    queue_wait_ms: LatencyDistribution
    total_latency_ms: LatencyDistribution

    def to_dict(self) -> Dict[str, object]:
        return {
            "accepted": self.accepted,
            "completed": self.completed,
            "failed": self.failed,
            "rejected": self.rejected,
            "queue_wait_ms": self.queue_wait_ms.to_dict(),
            "total_latency_ms": self.total_latency_ms.to_dict(),
        }


def _distribution(samples: List[float]) -> LatencyDistribution:
    ordered = sorted(samples)
    summary = {name: round(percentile(ordered, q), 3) for name, q in _QUANTILES}
    return LatencyDistribution(
        # Samples are kept in completion order; only the percentile
        # calculation sorts them.
        samples=[round(value, 3) for value in samples],
        p50=summary["p50"],
        p95=summary["p95"],
        p99=summary["p99"],
        max=round(ordered[-1], 3) if ordered else 0.0,
    )


class StatsCollector:
    """Thread-safe accumulator of task counters and latency samples."""

    def __init__(self) -> None:
        import threading

        self._lock = threading.Lock()
        self._accepted = 0
        self._completed = 0
        self._failed = 0
        self._rejected = 0
        self._queue_wait: List[float] = []
        self._total: List[float] = []

    def record_accepted(self) -> None:
        with self._lock:
            self._accepted += 1

    def record_rejected(self) -> None:
        with self._lock:
            self._rejected += 1

    def record_completion(self, queue_wait_ms: float, total_latency_ms: float) -> None:
        with self._lock:
            self._completed += 1
            self._queue_wait.append(queue_wait_ms)
            self._total.append(total_latency_ms)

    def record_failure(self, queue_wait_ms: float, total_latency_ms: float) -> None:
        with self._lock:
            self._failed += 1
            self._queue_wait.append(queue_wait_ms)
            self._total.append(total_latency_ms)

    def snapshot(self) -> Snapshot:
        with self._lock:
            return Snapshot(
                accepted=self._accepted,
                completed=self._completed,
                failed=self._failed,
                rejected=self._rejected,
                queue_wait_ms=_distribution(self._queue_wait),
                total_latency_ms=_distribution(self._total),
            )
