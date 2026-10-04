"""StatsCheckpoint / snapshot_since 区间统计测试。"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    InputValidationError,
    Scheduler,
    StatsCheckpoint,
)


def _zero_dist() -> dict:
    return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


class CheckpointBasicTest(unittest.TestCase):
    def test_empty_interval_is_all_zero(self) -> None:
        with Scheduler(2, 4) as s:
            s.submit("a", lambda: 1)
            cp = s.stats_checkpoint()
            snap = s.snapshot_since(cp)
            self.assertEqual(
                (snap.accepted, snap.completed, snap.failed,
                 snap.cancelled, snap.expired, snap.rejected),
                (0, 0, 0, 0, 0, 0),
            )
            self.assertEqual(snap.queue_wait_ms, _zero_dist())
            self.assertEqual(snap.total_latency_ms, _zero_dist())

    def test_interval_counts_only_events_after_boundary(self) -> None:
        with Scheduler(2, 4) as s:
            s.submit("before", lambda: 1)
            cp = s.stats_checkpoint()
            s.submit("after-1", lambda: 2)
            s.submit("after-2", lambda: 3)

            snap = s.snapshot_since(cp)
            self.assertEqual(snap.accepted, 2)
            self.assertEqual(snap.completed, 2)
            self.assertEqual(snap.failed, 0)
            # 累计快照仍包含边界前的任务。
            total = s.snapshot()
            self.assertEqual(total.accepted, 3)
            self.assertEqual(total.completed, 3)

    def test_to_dict_shape_matches_snapshot(self) -> None:
        with Scheduler(1, 2) as s:
            s.submit("x", lambda: None)
            cp = s.stats_checkpoint()
            s.submit("y", lambda: None)
            d = s.snapshot_since(cp).to_dict()
            self.assertEqual(set(d), set(s.snapshot().to_dict()))
            self.assertEqual(
                set(d["queue_wait_ms"]), {"p50", "p95", "p99", "max"}
            )
            self.assertEqual(
                set(d["total_latency_ms"]), {"p50", "p95", "p99", "max"}
            )

    def test_failed_task_attributed_to_finish_interval(self) -> None:
        with Scheduler(1, 2) as s:
            cp = s.stats_checkpoint()

            def boom() -> None:
                raise ValueError("nope")

            with self.assertRaises(ValueError):
                s.submit("bad", boom)
            snap = s.snapshot_since(cp)
            self.assertEqual(snap.accepted, 1)
            self.assertEqual(snap.failed, 1)
            self.assertEqual(snap.completed, 0)
            # 失败任务同样贡献延迟样本。
            self.assertGreaterEqual(snap.total_latency_ms["max"], 0.0)
            self.assertNotEqual(snap.queue_wait_ms, _zero_dist())


class CrossBoundaryTest(unittest.TestCase):
    def test_task_accepted_before_finishes_after(self) -> None:
        with Scheduler(1, 4) as s:
            gate = threading.Event()

            def slow() -> str:
                gate.wait()
                return "ok"

            handle = s.submit_nowait("slow", slow)
            # 接纳在 submit_nowait 返回前已记录，故边界前必有 accepted。
            cp = s.stats_checkpoint()
            gate.set()
            self.assertEqual(handle.result(), "ok")

            snap = s.snapshot_since(cp)
            # 接纳发生在边界前：区间内不计 accepted。
            self.assertEqual(snap.accepted, 0)
            # 结束发生在边界后：区间内计 completed，并贡献延迟样本。
            self.assertEqual(snap.completed, 1)
            self.assertGreater(snap.total_latency_ms["max"], 0.0)
            self.assertGreaterEqual(snap.queue_wait_ms["max"], 0.0)

            total = s.snapshot()
            self.assertEqual(total.accepted, 1)
            self.assertEqual(total.completed, 1)

    def test_cancel_and_reject_attributed_to_their_interval(self) -> None:
        with Scheduler(1, 2) as s:
            blocker = threading.Event()
            s.submit_nowait("blocker", lambda: blocker.wait())
            # 唯一工作线程被 blocker 占用，victim 留在队列中等待认领。
            victim = s.submit_nowait("victim", lambda: None)
            cp = s.stats_checkpoint()
            try:
                self.assertTrue(victim.cancel())
                # 取消释放的名额由 filler 补上，再提交触发背压拒绝。
                s.submit_nowait("filler", lambda: None)
                with self.assertRaises(BackpressureError):
                    s.submit_nowait("overflow", lambda: None)

                snap = s.snapshot_since(cp)
                self.assertEqual(snap.cancelled, 1)
                self.assertEqual(snap.rejected, 1)
                # victim 在边界前接纳，区间内只有 filler 计 accepted。
                self.assertEqual(snap.accepted, 1)
                # 取消与拒绝不贡献延迟样本。
                self.assertEqual(snap.queue_wait_ms, _zero_dist())
            finally:
                blocker.set()


class CheckpointReuseTest(unittest.TestCase):
    def test_repeated_queries_are_stable_and_read_only(self) -> None:
        with Scheduler(2, 4) as s:
            cp = s.stats_checkpoint()
            s.submit("t1", lambda: 1)
            first = s.snapshot_since(cp)
            second = s.snapshot_since(cp)
            self.assertEqual(first.to_dict(), second.to_dict())
            self.assertEqual(first.accepted, 1)

            s.submit("t2", lambda: 2)
            third = s.snapshot_since(cp)
            self.assertEqual(third.accepted, 2)
            # 早先返回的快照对象不随后续任务变化。
            self.assertEqual(first.accepted, 1)
            # 累计统计不受查询影响。
            self.assertEqual(s.snapshot().accepted, 2)

    def test_chained_checkpoints_telescope(self) -> None:
        with Scheduler(2, 8) as s:
            c0 = s.stats_checkpoint()
            s.submit("t1", lambda: 1)
            c1 = s.stats_checkpoint()
            s.submit("t2", lambda: 2)
            s.submit("t3", lambda: 3)
            c2 = s.stats_checkpoint()

            seg1 = s.snapshot_since(c0)
            seg2 = s.snapshot_since(c1)
            seg3 = s.snapshot_since(c2)
            self.assertEqual((seg1.accepted, seg1.completed), (3, 3))
            self.assertEqual((seg2.accepted, seg2.completed), (2, 2))
            self.assertEqual((seg3.accepted, seg3.completed), (0, 0))
            # 相邻区间不重复也不遗漏：c0 区间 = c1 区间 + 其间事件。
            self.assertEqual(seg1.accepted - seg2.accepted, 1)
            self.assertEqual(seg1.completed - seg2.completed, 1)


class CheckpointValidationTest(unittest.TestCase):
    def test_non_checkpoint_rejected(self) -> None:
        with Scheduler(1, 2) as s:
            for bad in (None, 0, "cp", object(), s.snapshot()):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.snapshot_since(bad)  # type: ignore[arg-type]

    def test_other_schedulers_checkpoint_rejected(self) -> None:
        s1 = Scheduler(1, 2)
        s2 = Scheduler(1, 2)
        try:
            cp = s2.stats_checkpoint()
            with self.assertRaises(InputValidationError):
                s1.snapshot_since(cp)
            # 原调度器使用自己的 checkpoint 不受影响。
            self.assertEqual(s2.snapshot_since(cp).accepted, 0)
        finally:
            s1.close()
            s2.close()

    def test_corrupted_checkpoint_rejected_without_side_effects(self) -> None:
        with Scheduler(1, 2) as s:
            s.submit("t", lambda: 1)
            cp = s.stats_checkpoint()
            # 绕过不可变保护构造损坏对象。
            object.__setattr__(cp, "_accepted", -5)
            before = s.snapshot().to_dict()
            with self.assertRaises(InputValidationError):
                s.snapshot_since(cp)
            self.assertEqual(s.snapshot().to_dict(), before)

    def test_failed_validation_does_not_change_stats(self) -> None:
        with Scheduler(1, 2) as s:
            s.submit("t", lambda: 1)
            before = s.snapshot().to_dict()
            for bad in (None, 123, "x"):
                with self.assertRaises(InputValidationError):
                    s.snapshot_since(bad)  # type: ignore[arg-type]
            self.assertEqual(s.snapshot().to_dict(), before)

    def test_checkpoint_is_immutable(self) -> None:
        with Scheduler(1, 2) as s:
            cp = s.stats_checkpoint()
            with self.assertRaises(AttributeError):
                cp._accepted = 10  # type: ignore[attr-defined]
            with self.assertRaises(AttributeError):
                cp.extra = 1  # type: ignore[attr-defined]
            # 未被封印的字段读取不受影响，查询仍正常。
            self.assertEqual(s.snapshot_since(cp).accepted, 0)


class CheckpointAfterCloseTest(unittest.TestCase):
    def test_checkpoint_and_query_after_close(self) -> None:
        s = Scheduler(2, 4)
        s.submit("a", lambda: 1)
        cp = s.stats_checkpoint()
        s.submit("b", lambda: 2)
        s.close()

        # 关闭后仍可查询既有边界。
        snap = s.snapshot_since(cp)
        self.assertEqual((snap.accepted, snap.completed), (1, 1))
        # 关闭后仍可创建新检查点（此后无新事件，区间为空）。
        cp2 = s.stats_checkpoint()
        empty = s.snapshot_since(cp2)
        self.assertEqual(empty.accepted, 0)
        self.assertEqual(empty.queue_wait_ms, _zero_dist())
        # 历史累计统计同样可读。
        self.assertEqual(s.snapshot().accepted, 2)


class CheckpointConcurrencyTest(unittest.TestCase):
    def test_concurrent_events_partitioned_across_adjacent_intervals(self) -> None:
        # 多线程并发结束任务、创建检查点与查询：相邻区间的事件
        # 不重复也不遗漏（链式区间望远镜式拼接回累计差值）。
        with Scheduler(4, 16) as s:
            start = s.stats_checkpoint()
            stop = threading.Event()
            checkpoints = []

            def make_checkpoints() -> None:
                while not stop.is_set():
                    checkpoints.append(s.stats_checkpoint())
                    time.sleep(0.001)

            def work(worker: int) -> None:
                for i in range(20):
                    s.submit("w%d-%d" % (worker, i), lambda: None)

            marker = threading.Thread(target=make_checkpoints)
            marker.start()
            workers = [
                threading.Thread(target=work, args=(w,)) for w in range(4)
            ]
            for t in workers:
                t.start()
            for t in workers:
                t.join()
            stop.set()
            marker.join()
            end = s.stats_checkpoint()

            # 链式区间拼接：start..end 的 accepted/completed 等于
            # 各相邻检查点区间之和（望远镜性质）。
            total = s.snapshot_since(end)
            self.assertEqual((total.accepted, total.completed), (0, 0))
            whole = s.snapshot_since(start)
            self.assertEqual((whole.accepted, whole.completed), (80, 80))

            acc = 0
            comp = 0
            prev = start
            for cp in checkpoints + [end]:
                seg = s.snapshot_since(prev)
                nxt = s.snapshot_since(cp)
                # 区间单调性：边界越晚，区间事件越少（无重复计数）。
                self.assertGreaterEqual(seg.accepted, nxt.accepted)
                self.assertGreaterEqual(seg.completed, nxt.completed)
                acc += seg.accepted - nxt.accepted
                comp += seg.completed - nxt.completed
                prev = cp
            self.assertEqual(acc, 80)
            self.assertEqual(comp, 80)


if __name__ == "__main__":
    unittest.main()
