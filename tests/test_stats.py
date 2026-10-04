"""Stats / percentile 的单元测试。"""

import unittest

from edge_sched.stats import Stats, StatsCheckpoint, percentile


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
             snap.cancelled, snap.expired, snap.rejected),
            (0, 0, 0, 0, 0, 0),
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

    def test_expired_count_no_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_expired()
        snap = stats.snapshot()
        self.assertEqual(snap.expired, 1)
        self.assertEqual(snap.completed, 0)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.cancelled, 0)
        # 到期任务不贡献任何延迟样本。
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
            "accepted", "completed", "failed", "cancelled", "expired",
            "rejected", "queue_wait_ms", "total_latency_ms",
        })
        # 毫秒保留三位小数。
        self.assertEqual(d["total_latency_ms"]["max"], 0.999)
        self.assertEqual(d["queue_wait_ms"]["max"], 0.124)


_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


class StatsCheckpointTest(unittest.TestCase):
    def test_empty_interval(self) -> None:
        stats = Stats()
        cp = stats.checkpoint()
        interval = stats.snapshot_since(cp)
        self.assertEqual(
            (interval.accepted, interval.completed, interval.failed,
             interval.cancelled, interval.expired, interval.rejected),
            (0, 0, 0, 0, 0, 0),
        )
        self.assertEqual(interval.queue_wait_ms, _EMPTY_DIST)
        self.assertEqual(interval.total_latency_ms, _EMPTY_DIST)
        # to_dict 形态与累计 snapshot 完全一致。
        self.assertEqual(set(interval.to_dict()),
                         set(stats.snapshot().to_dict()))

    def test_interval_counts_and_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(1.0, 2.0, success=True)

        cp = stats.checkpoint()
        # 接受时刻在区间内、结束时刻也在区间内。
        stats.record_accepted()
        stats.record_finished(3.0, 6.0, success=False)
        stats.record_rejected()

        interval = stats.snapshot_since(cp)
        self.assertEqual(interval.accepted, 1)
        self.assertEqual(interval.failed, 1)
        self.assertEqual(interval.rejected, 1)
        self.assertEqual(interval.completed, 0)
        self.assertEqual(interval.cancelled, 0)
        self.assertEqual(interval.expired, 0)
        self.assertEqual(interval.queue_wait_ms["max"], 3.0)
        self.assertEqual(interval.total_latency_ms["max"], 6.0)

        # 累计统计不受区间查询影响。
        total = stats.snapshot()
        self.assertEqual(total.accepted, 2)
        self.assertEqual(total.completed, 1)
        self.assertEqual(total.failed, 1)
        self.assertEqual(total.rejected, 1)
        self.assertEqual(total.total_latency_ms["max"], 6.0)

    def test_cross_boundary_task_attribution(self) -> None:
        # 任务在边界前 accepted，边界后才 finished：accepted 归前一区间，
        # completed 与延迟样本归后一区间。
        stats = Stats()
        stats.record_accepted()
        cp = stats.checkpoint()
        stats.record_finished(4.0, 8.0, success=True)

        interval = stats.snapshot_since(cp)
        self.assertEqual(interval.accepted, 0)
        self.assertEqual(interval.completed, 1)
        self.assertEqual(interval.failed, 0)
        self.assertEqual(interval.queue_wait_ms["max"], 4.0)
        self.assertEqual(interval.total_latency_ms["max"], 8.0)

    def test_cancelled_and_expired_follow_their_moments(self) -> None:
        stats = Stats()
        stats.record_accepted()
        cp = stats.checkpoint()
        stats.record_accepted()
        stats.record_cancelled()
        stats.record_accepted()
        stats.record_expired()

        interval = stats.snapshot_since(cp)
        self.assertEqual(interval.accepted, 2)
        self.assertEqual(interval.cancelled, 1)
        self.assertEqual(interval.expired, 1)
        # 取消/到期不贡献任何延迟样本。
        self.assertEqual(interval.queue_wait_ms, _EMPTY_DIST)
        self.assertEqual(interval.total_latency_ms, _EMPTY_DIST)

    def test_repeated_query_is_stable_and_non_mutating(self) -> None:
        stats = Stats()
        cp = stats.checkpoint()
        stats.record_accepted()
        stats.record_finished(1.0, 1.0, success=True)
        first = stats.snapshot_since(cp)
        second = stats.snapshot_since(cp)
        self.assertEqual(first.accepted, 1)
        self.assertEqual(second.accepted, 1)
        self.assertEqual(first.total_latency_ms["max"], 1.0)
        # 反复查询后累计值不变，边界仍可继续使用。
        self.assertEqual(stats.snapshot().accepted, 1)
        self.assertEqual(stats.snapshot_since(cp).accepted, 1)

    def test_adjacent_intervals_partition_events(self) -> None:
        stats = Stats()
        cp0 = stats.checkpoint()
        stats.record_accepted()
        stats.record_finished(1.0, 1.0, success=True)
        cp1 = stats.checkpoint()
        # 第一区间的快照须在第二区间事件发生前取得（快照本身不可变）。
        first = stats.snapshot_since(cp0)
        stats.record_accepted()
        stats.record_finished(2.0, 2.0, success=False)
        cp2 = stats.checkpoint()
        second = stats.snapshot_since(cp1)
        tail = stats.snapshot_since(cp2)

        self.assertEqual((first.accepted, first.completed), (1, 1))
        self.assertEqual((second.accepted, second.failed), (1, 1))
        self.assertEqual((tail.accepted, tail.completed, tail.failed),
                         (0, 0, 0))
        # 相邻区间不重不漏：各项事件之和等于启动以来的累计值。
        total = stats.snapshot()
        self.assertEqual(first.accepted + second.accepted, total.accepted)
        self.assertEqual(
            first.completed + second.completed, total.completed
        )
        self.assertEqual(first.failed + second.failed, total.failed)
        self.assertEqual(first.total_latency_ms["max"], 1.0)
        self.assertEqual(second.total_latency_ms["max"], 2.0)
        # 区间快照的 to_dict 形态与累计快照一致。
        self.assertEqual(set(first.to_dict()), set(total.to_dict()))

    def test_checkpoint_is_immutable(self) -> None:
        cp = Stats().checkpoint()
        with self.assertRaises(AttributeError):
            cp._checkpoint_id = 99  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            cp.new_attr = 1  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            del cp._owner  # type: ignore[misc]

    def test_non_checkpoint_rejected_without_state_change(self) -> None:
        stats = Stats()
        stats.record_accepted()
        for bad in (None, 1, "cp", object(), (), []):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    stats.snapshot_since(bad)  # type: ignore[arg-type]
        # 拒绝校验不改变任何计数或样本。
        self.assertEqual(stats.snapshot().accepted, 1)

    def test_foreign_and_corrupted_checkpoint_rejected(self) -> None:
        owner = Stats()
        other = Stats()
        cp = owner.checkpoint()

        # 其他收集器的 checkpoint。
        with self.assertRaises(ValueError):
            other.snapshot_since(cp)
        # 归属一致但边界未知（伪造 id）。
        forged = StatsCheckpoint(123, owner)
        with self.assertRaises(ValueError):
            owner.snapshot_since(forged)
        # 损坏对象：绕过 __init__/不可变限制构造缺字段实例。
        broken = StatsCheckpoint.__new__(StatsCheckpoint)
        with self.assertRaises(ValueError):
            owner.snapshot_since(broken)
        # id 被篡改成非 int 的损坏对象。
        bad_id = StatsCheckpoint.__new__(StatsCheckpoint)
        object.__setattr__(bad_id, "_owner", owner)
        object.__setattr__(bad_id, "_checkpoint_id", "0")
        with self.assertRaises(ValueError):
            owner.snapshot_since(bad_id)
        # 失败校验不改变任何状态。
        self.assertEqual(owner.snapshot().accepted, 0)
        self.assertEqual(other.snapshot().accepted, 0)


if __name__ == "__main__":
    unittest.main()
