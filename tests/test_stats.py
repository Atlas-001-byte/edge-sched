"""Stats / percentile 的单元测试。"""

import json
import unittest

from edge_sched.stats import RollingStatsSnapshot, Stats, StatsCheckpoint, percentile


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
        self.assertEqual(
            snap.execution_ms,
            {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0},
        )
        self.assertEqual(
            snap.admission_wait_ms,
            {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0},
        )

    def test_cumulative_counts_and_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_rejected()
        stats.record_finished(1.5, 5.25, 3.75, success=True)
        stats.record_accepted()
        stats.record_finished(2.5, 9.0, 6.5, success=False)

        snap = stats.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 1)
        self.assertEqual(snap.cancelled, 0)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.queue_wait_ms["max"], 2.5)
        self.assertEqual(snap.total_latency_ms["max"], 9.0)
        self.assertEqual(snap.total_latency_ms["p50"], 5.25)
        # 成功与失败任务各贡献一个 execution_ms 样本。
        self.assertEqual(snap.execution_ms["max"], 6.5)
        self.assertEqual(snap.execution_ms["p50"], 3.75)

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
        self.assertEqual(snap.execution_ms["max"], 0.0)

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
        self.assertEqual(snap.execution_ms["max"], 0.0)

    def test_snapshot_is_fixed(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(1.0, 1.0, 1.0, success=True)
        first = stats.snapshot()

        stats.record_accepted()
        stats.record_finished(2.0, 2.0, 2.0, success=True)

        # 早先快照不随后续任务变化。
        self.assertEqual(first.accepted, 1)
        self.assertEqual(first.total_latency_ms["max"], 1.0)
        self.assertEqual(first.execution_ms["max"], 1.0)
        self.assertEqual(stats.snapshot().total_latency_ms["max"], 2.0)
        self.assertEqual(stats.snapshot().execution_ms["max"], 2.0)

    def test_to_dict_shape(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(0.1236, 0.9994, 0.8758, success=True)
        d = stats.snapshot().to_dict()
        self.assertEqual(set(d), {
            "accepted", "completed", "failed", "cancelled", "expired",
            "rejected", "admission_wait_ms", "queue_wait_ms",
            "total_latency_ms", "execution_ms",
        })
        # 毫秒保留三位小数。
        self.assertEqual(d["total_latency_ms"]["max"], 0.999)
        self.assertEqual(d["queue_wait_ms"]["max"], 0.124)
        self.assertEqual(d["execution_ms"]["max"], 0.876)

    def test_admission_wait_distribution(self) -> None:
        stats = Stats()
        # 空快照：准入等待为空分布。
        self.assertEqual(stats.snapshot().admission_wait_ms, _EMPTY_DIST)
        # 缺省 0.0：调用瞬间取得名额的提交。
        stats.record_accepted()
        stats.record_accepted(12.5)
        stats.record_accepted(0.2504)
        snap = stats.snapshot()
        self.assertEqual(snap.accepted, 3)
        self.assertEqual(
            snap.admission_wait_ms,
            {"p50": 0.25, "p95": 12.5, "p99": 12.5, "max": 12.5},
        )
        # 拒绝、取消、到期与结束记账都不追加准入等待样本。
        stats.record_rejected()
        stats.record_cancelled()
        stats.record_expired()
        stats.record_finished(1.0, 1.0, 1.0, success=True)
        self.assertEqual(stats.snapshot().admission_wait_ms["max"], 12.5)
        d = snap.to_dict()
        self.assertEqual(d["admission_wait_ms"], snap.admission_wait_ms)
        # 快照为值拷贝且可 JSON 序列化。
        self.assertEqual(json.loads(json.dumps(d)), d)


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
        self.assertEqual(interval.execution_ms, _EMPTY_DIST)
        self.assertEqual(interval.admission_wait_ms, _EMPTY_DIST)
        # to_dict 形态与累计 snapshot 完全一致。
        self.assertEqual(set(interval.to_dict()),
                         set(stats.snapshot().to_dict()))

    def test_interval_counts_and_samples(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(1.0, 2.0, 1.0, success=True)

        cp = stats.checkpoint()
        # 接受时刻在区间内、结束时刻也在区间内。
        stats.record_accepted()
        stats.record_finished(3.0, 6.0, 3.0, success=False)
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
        # 区间内失败任务同样贡献一个 execution_ms 样本。
        self.assertEqual(interval.execution_ms["max"], 3.0)

        # 累计统计不受区间查询影响。
        total = stats.snapshot()
        self.assertEqual(total.accepted, 2)
        self.assertEqual(total.completed, 1)
        self.assertEqual(total.failed, 1)
        self.assertEqual(total.rejected, 1)
        self.assertEqual(total.total_latency_ms["max"], 6.0)
        self.assertEqual(total.execution_ms["max"], 3.0)

    def test_cross_boundary_task_attribution(self) -> None:
        # 任务在边界前 accepted，边界后才 finished：accepted 归前一区间，
        # completed 与延迟样本（含 execution_ms）归后一区间。
        stats = Stats()
        stats.record_accepted()
        cp = stats.checkpoint()
        stats.record_finished(4.0, 8.0, 4.0, success=True)

        interval = stats.snapshot_since(cp)
        self.assertEqual(interval.accepted, 0)
        self.assertEqual(interval.completed, 1)
        self.assertEqual(interval.failed, 0)
        self.assertEqual(interval.queue_wait_ms["max"], 4.0)
        self.assertEqual(interval.total_latency_ms["max"], 8.0)
        self.assertEqual(interval.execution_ms["max"], 4.0)

    def test_admission_wait_attributed_to_admission_interval(self) -> None:
        # 准入等待样本按接纳时刻归属：边界前接纳的任务其样本计入前段区间，
        # 即使它在边界后才结束；边界后接纳的任务样本计入本区间。
        stats = Stats()
        stats.record_accepted(7.5)
        cp = stats.checkpoint()
        stats.record_finished(1.0, 2.0, 1.0, success=True)
        stats.record_accepted(0.0)
        stats.record_accepted(3.25)

        interval = stats.snapshot_since(cp)
        self.assertEqual(interval.accepted, 2)
        self.assertEqual(
            interval.admission_wait_ms,
            {"p50": 0.0, "p95": 3.25, "p99": 3.25, "max": 3.25},
        )
        # 累计快照含全部三个准入样本；结束区间仍收到结束任务的延迟样本。
        total = stats.snapshot()
        self.assertEqual(total.admission_wait_ms["max"], 7.5)
        self.assertEqual(total.admission_wait_ms["p50"], 3.25)
        self.assertEqual(interval.queue_wait_ms["max"], 1.0)
        self.assertEqual(interval.execution_ms["max"], 1.0)

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
        self.assertEqual(interval.execution_ms, _EMPTY_DIST)

    def test_repeated_query_is_stable_and_non_mutating(self) -> None:
        stats = Stats()
        cp = stats.checkpoint()
        stats.record_accepted()
        stats.record_finished(1.0, 1.0, 1.0, success=True)
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
        stats.record_finished(1.0, 1.0, 1.0, success=True)
        cp1 = stats.checkpoint()
        # 第一区间的快照须在第二区间事件发生前取得（快照本身不可变）。
        first = stats.snapshot_since(cp0)
        stats.record_accepted()
        stats.record_finished(2.0, 2.0, 2.0, success=False)
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
        self.assertEqual(first.execution_ms["max"], 1.0)
        self.assertEqual(second.execution_ms["max"], 2.0)
        self.assertEqual(tail.execution_ms, _EMPTY_DIST)
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


class RollingStatsTest(unittest.TestCase):
    def test_disabled_window_is_empty(self) -> None:
        stats = Stats()
        stats.record_accepted()
        stats.record_finished(1.0, 2.0, 1.0, success=True)
        snap = stats.rolling_snapshot()
        self.assertIsInstance(snap, RollingStatsSnapshot)
        self.assertEqual(snap.window_size, 0)
        self.assertEqual(snap.sampled_finished, 0)
        self.assertEqual(snap.queue_wait_ms, _EMPTY_DIST)
        self.assertEqual(snap.total_latency_ms, _EMPTY_DIST)
        self.assertEqual(snap.execution_ms, _EMPTY_DIST)
        # 未启用窗口不影响累计统计。
        self.assertEqual(stats.snapshot().completed, 1)

    def test_partial_then_full_window(self) -> None:
        stats = Stats(window_size=3)
        stats.record_finished(1.0, 10.0, 9.0, success=True)
        stats.record_finished(2.0, 20.0, 18.0, success=False)
        partial = stats.rolling_snapshot()
        # 窗口未满：sampled_finished 等于启用后结束任务总数。
        self.assertEqual(partial.window_size, 3)
        self.assertEqual(partial.sampled_finished, 2)
        self.assertEqual(partial.queue_wait_ms["max"], 2.0)
        self.assertEqual(partial.total_latency_ms["max"], 20.0)
        self.assertEqual(partial.execution_ms["max"], 18.0)

        stats.record_finished(3.0, 30.0, 27.0, success=True)
        full = stats.rolling_snapshot()
        self.assertEqual(full.sampled_finished, 3)
        self.assertEqual(full.queue_wait_ms["max"], 3.0)
        self.assertEqual(full.execution_ms["max"], 27.0)

    def test_window_evicts_oldest_and_keeps_finish_order(self) -> None:
        stats = Stats(window_size=2)
        stats.record_finished(1.0, 10.0, 1.0, success=True)
        stats.record_finished(2.0, 20.0, 2.0, success=True)
        stats.record_finished(3.0, 30.0, 3.0, success=True)
        stats.record_finished(4.0, 40.0, 4.0, success=False)
        snap = stats.rolling_snapshot()
        self.assertEqual(snap.window_size, 2)
        self.assertEqual(snap.sampled_finished, 2)
        # 仅保留最近两个结束任务（3、4）：三类样本同步进出。
        self.assertEqual(snap.queue_wait_ms,
                         {"p50": 3.0, "p95": 4.0, "p99": 4.0, "max": 4.0})
        self.assertEqual(snap.total_latency_ms,
                         {"p50": 30.0, "p95": 40.0, "p99": 40.0, "max": 40.0})
        self.assertEqual(snap.execution_ms,
                         {"p50": 3.0, "p95": 4.0, "p99": 4.0, "max": 4.0})
        # 累计统计不受窗口剔除影响。
        self.assertEqual(stats.snapshot().completed, 3)
        self.assertEqual(stats.snapshot().failed, 1)

    def test_cancelled_expired_rejected_never_enter_window(self) -> None:
        stats = Stats(window_size=5)
        stats.record_accepted()
        stats.record_cancelled()
        stats.record_accepted()
        stats.record_expired()
        stats.record_rejected()
        snap = stats.rolling_snapshot()
        self.assertEqual(snap.sampled_finished, 0)
        self.assertEqual(snap.execution_ms, _EMPTY_DIST)
        stats.record_finished(1.0, 1.0, 1.0, success=True)
        self.assertEqual(stats.rolling_snapshot().sampled_finished, 1)

    def test_distribution_uses_same_percentile_rule(self) -> None:
        stats = Stats(window_size=20)
        for i in range(1, 21):
            stats.record_finished(float(i), float(i), float(i), success=True)
        snap = stats.rolling_snapshot()
        self.assertEqual(snap.sampled_finished, 20)
        # ceil(n*q)：p50=10、p95=19、p99=20，与累计口径一致。
        self.assertEqual(snap.queue_wait_ms["p50"], 10.0)
        self.assertEqual(snap.queue_wait_ms["p95"], 19.0)
        self.assertEqual(snap.queue_wait_ms["p99"], 20.0)

    def test_three_decimals_and_empty_are_zero(self) -> None:
        stats = Stats(window_size=3)
        stats.record_finished(0.1236, 0.9994, 0.8758, success=True)
        snap = stats.rolling_snapshot()
        self.assertEqual(snap.queue_wait_ms["max"], 0.124)
        self.assertEqual(snap.total_latency_ms["max"], 0.999)
        self.assertEqual(snap.execution_ms["max"], 0.876)
        empty = Stats(window_size=2).rolling_snapshot()
        self.assertEqual(empty.queue_wait_ms, _EMPTY_DIST)

    def test_snapshot_is_value_copy_and_stable(self) -> None:
        stats = Stats(window_size=2)
        stats.record_finished(1.0, 1.0, 1.0, success=True)
        first = stats.rolling_snapshot()
        stats.record_finished(2.0, 2.0, 2.0, success=True)
        stats.record_finished(3.0, 3.0, 3.0, success=True)
        # 早先快照不随后续事件变化。
        self.assertEqual(first.sampled_finished, 1)
        self.assertEqual(first.queue_wait_ms["max"], 1.0)
        again = stats.rolling_snapshot()
        self.assertEqual(again.sampled_finished, 2)
        self.assertEqual(stats.rolling_snapshot().to_dict(), again.to_dict())

    def test_to_dict_shape_and_json(self) -> None:
        stats = Stats(window_size=2)
        stats.record_finished(1.0, 2.0, 1.0, success=True)
        d = stats.rolling_snapshot().to_dict()
        self.assertEqual(set(d), {
            "window_size", "sampled_finished",
            "queue_wait_ms", "total_latency_ms", "execution_ms",
        })
        self.assertEqual(d["window_size"], 2)
        self.assertEqual(d["sampled_finished"], 1)
        encoded = json.dumps(d)
        self.assertEqual(json.loads(encoded), d)
        # 返回的字典是拷贝。
        d["sampled_finished"] = 99
        self.assertEqual(stats.rolling_snapshot().sampled_finished, 1)

    def test_snapshot_is_immutable(self) -> None:
        snap = Stats(window_size=2).rolling_snapshot()
        for name in ("window_size", "sampled_finished",
                     "queue_wait_ms", "new_attr"):
            with self.subTest(name=name):
                with self.assertRaises(AttributeError):
                    setattr(snap, name, 1)
                with self.assertRaises(AttributeError):
                    delattr(snap, name)


if __name__ == "__main__":
    unittest.main()
