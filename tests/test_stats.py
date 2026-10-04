"""Stats / percentile 的单元测试。"""

import unittest

from edge_sched.stats import Stats, percentile


class PercentileTest(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(percentile([], 0.5), 0.0)
        self.assertEqual(percentile([], 0.99), 0.0)

    def test_rank_is_ceil_n_times_q(self) -> None:
        samples = list(range(1, 21))  # 1..20
        self.assertEqual(percentile(samples, 0.50), 10)   # ceil(10) = 10
        self.assertEqual(percentile(samples, 0.95), 19)   # ceil(19) = 19
        self.assertEqual(percentile(samples, 0.99), 20)   # ceil(19.8) = 20
        self.assertEqual(percentile(samples, 1.0), 20)

    def test_single_sample(self) -> None:
        self.assertEqual(percentile([42.5], 0.5), 42.5)
        self.assertEqual(percentile([42.5], 0.99), 42.5)

    def test_does_not_mutate_order(self) -> None:
        # 传入未排序数据时 percentile 只负责按下标取，分布逻辑负责排序。
        ordered = sorted([3.0, 1.0, 2.0])
        self.assertEqual(percentile(ordered, 0.5), 2.0)


class StatsTest(unittest.TestCase):
    def test_empty_snapshot(self) -> None:
        snap = Stats().snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.failed,
             snap.cancelled, snap.rejected),
            (0, 0, 0, 0, 0),
        )
        self.assertEqual(
            snap.queue_wait_ms,
            {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0},
        )
        self.assertEqual(
            snap.total_latency_ms,
            {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0},
        )

    def test_cumulative_counts_and_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_rejected()
        stats.record_finished(1.5, 5.25, success=True)
        stats.record_accepted()
        stats.record_finished(2.5, 9.0, success=False)

        snap = stats.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 1)
        self.assertEqual(snap.cancelled, 0)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.queue_wait_ms["max"], 2.5)
        self.assertEqual(snap.total_latency_ms["max"], 9.0)
        self.assertEqual(snap.total_latency_ms["p50"], 5.25)

    def test_cancelled_count_no_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_cancelled()
        snap = stats.snapshot()
        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.completed, 0)
        self.assertEqual(snap.failed, 0)
        # 取消任务不贡献任何延迟样本。
        self.assertEqual(snap.queue_wait_ms["max"], 0.0)
        self.assertEqual(snap.total_latency_ms["max"], 0.0)

    def test_snapshot_is_fixed(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(1.0, 1.0, success=True)
        first = stats.snapshot()

        stats.record_accepted()
        stats.record_finished(2.0, 2.0, success=True)

        # 早先快照不随后续任务变化。
        self.assertEqual(first.accepted, 1)
        self.assertEqual(first.total_latency_ms["max"], 1.0)
        self.assertEqual(stats.snapshot().total_latency_ms["max"], 2.0)

    def test_to_dict_shape(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(0.1236, 0.9994, success=True)
        d = stats.snapshot().to_dict()
        self.assertEqual(set(d), {
            "accepted", "completed", "failed", "cancelled", "rejected",
            "queue_wait_ms", "total_latency_ms",
        })
        # 毫秒保留三位小数。
        self.assertEqual(d["total_latency_ms"]["max"], 0.999)
        self.assertEqual(d["queue_wait_ms"]["max"], 0.124)


if __name__ == "__main__":
    unittest.main()
