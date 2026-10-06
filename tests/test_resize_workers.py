"""Scheduler.resize_workers 运行时工作线程容量调整的验收测试。

覆盖：参数校验（bool/0/负数/浮点/其他类型）、相同容量无操作、扩容后新增
容量立即可用、缩容后并发上限立即降为目标值且不打断执行中任务、未认领任务
继续按优先级派发、runtime_snapshot 的 workers 报告目标容量且旧快照不变、
closing/closed 抛 SchedulerClosedError、并发调整串行生效、统计口径不变、
close 在调整后安全停止全部线程。
"""

import threading
import time
import unittest

from edge_sched import (
    InputValidationError,
    Scheduler,
    SchedulerClosedError,
)


def _wait_runtime(s: Scheduler, **expected: object) -> bool:
    """轮询 runtime_snapshot 直到指定字段全部匹配。"""
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        snap = s.runtime_snapshot()
        if all(getattr(snap, k) == v for k, v in expected.items()):
            return True
        time.sleep(0.002)
    return False


class ValidationTest(unittest.TestCase):
    def test_invalid_values_raise_and_keep_capacity(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            for bad in (True, False, 0, -1, -100, 1.5, 2.0, "2", None, []):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.resize_workers(bad)  # type: ignore[arg-type]
            self.assertEqual(s.runtime_snapshot().workers, 2)

    def test_same_capacity_is_noop(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            s.resize_workers(2)  # 正常返回，无操作
            self.assertEqual(s.runtime_snapshot().workers, 2)
            self.assertEqual(len(s._worker_threads), 2)  # type: ignore[attr-defined]

    def test_closed_scheduler_raises(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(3)
        # 相同容量调用在关闭后同样抛 SchedulerClosedError。
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(1)
        self.assertEqual(s.runtime_snapshot().workers, 1)

    def test_closing_scheduler_raises(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        closing = threading.Thread(target=s.close)
        closing.start()
        self.assertTrue(_wait_runtime(s, closing=True, closed=False))
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(4)
        self.assertEqual(s.runtime_snapshot().workers, 1)
        release.set()
        closing.join()


class ScaleUpTest(unittest.TestCase):
    def test_scale_up_allows_more_concurrency_immediately(self) -> None:
        entered = [threading.Event() for _ in range(3)]
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            handles = [
                s.submit_nowait(
                    "t%d" % i,
                    lambda i=i: (entered[i].set(), release.wait(2.0)),
                )
                for i in range(3)
            ]
            self.assertTrue(entered[0].wait(2.0))
            # 单线程容量：其余任务排队。
            self.assertFalse(entered[1].wait(0.1))
            s.resize_workers(3)
            self.assertEqual(s.runtime_snapshot().workers, 3)
            # 扩容后新增容量立即可用：其余两个任务无需先释放即可开始。
            self.assertTrue(entered[1].wait(2.0))
            self.assertTrue(entered[2].wait(2.0))
            self.assertTrue(_wait_runtime(s, running=3))
            release.set()
            for h in handles:
                h.result(2.0)
            self.assertTrue(_wait_runtime(s, running=0, unfinished=0))

    def test_scale_up_down_up_reuses_capacity(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            s.resize_workers(3)
            s.resize_workers(1)
            s.resize_workers(2)
            self.assertEqual(s.runtime_snapshot().workers, 2)
            entered = [threading.Event() for _ in range(2)]
            release = threading.Event()
            handles = [
                s.submit_nowait(
                    "t%d" % i,
                    lambda i=i: (entered[i].set(), release.wait(2.0)),
                )
                for i in range(2)
            ]
            for ev in entered:
                self.assertTrue(ev.wait(2.0))
            release.set()
            for h in handles:
                h.result(2.0)


class ScaleDownTest(unittest.TestCase):
    def test_scale_down_does_not_interrupt_running_and_caps_new(self) -> None:
        entered = [threading.Event() for _ in range(3)]
        release = threading.Event()
        with Scheduler(workers=3, max_pending=6) as s:
            handles = [
                s.submit_nowait(
                    "r%d" % i,
                    lambda i=i: (entered[i].set(), release.wait(2.0)),
                )
                for i in range(3)
            ]
            for ev in entered:
                self.assertTrue(ev.wait(2.0))
            # 缩容到 1：执行中任务可暂时超过目标容量直到结束。
            s.resize_workers(1)
            self.assertEqual(s.runtime_snapshot().workers, 1)
            self.assertTrue(_wait_runtime(s, running=3))

            # 执行中任务未结束时，新提交的任务不得启动（旧上限不再生效）。
            late_started = threading.Event()
            late_release = threading.Event()
            late = s.submit_nowait(
                "late",
                lambda: (late_started.set(), late_release.wait(2.0)),
            )
            time.sleep(0.1)
            self.assertFalse(late_started.is_set())

            # 全部旧任务结束、并发降回目标值后，late 才被认领执行。
            release.set()
            for h in handles:
                h.result(2.0)
            self.assertTrue(late_started.wait(2.0))
            late_release.set()
            late.result(2.0)

    def test_queued_tasks_keep_priority_order_after_shrink(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=2, max_pending=6) as s:
            threading.Thread(
                target=s.submit,
                args=("h1", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            threading.Thread(
                target=s.submit,
                args=("h2", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            self.assertTrue(_wait_runtime(s, running=2))
            s.resize_workers(1)
            order: list[str] = []
            lock = threading.Lock()

            def make(tid: str) -> "object":
                def fn() -> None:
                    with lock:
                        order.append(tid)
                return fn

            s.submit_nowait("lo", make("lo"), priority=0)
            s.submit_nowait("hi", make("hi"), priority=5)
            release.set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and len(order) < 2:
                time.sleep(0.005)
            # 高优先级先派发，低优先级随后；每个任务恰好执行一次。
            self.assertEqual(order, ["hi", "lo"])
            self.assertTrue(_wait_runtime(s, unfinished=0))


class SnapshotAndStatsTest(unittest.TestCase):
    def test_snapshot_reports_target_and_old_snapshot_frozen(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            before = s.runtime_snapshot()
            self.assertEqual(before.workers, 2)
            s.resize_workers(5)
            after = s.runtime_snapshot()
            self.assertEqual(after.workers, 5)
            self.assertEqual(after.to_dict()["workers"], 5)
            # 已返回快照不随调整变化。
            self.assertEqual(before.workers, 2)
            self.assertEqual(before.to_dict()["workers"], 2)
            s.resize_workers(1)
            self.assertEqual(s.runtime_snapshot().workers, 1)
            self.assertEqual(after.workers, 5)

    def test_stats_unaffected_by_resize(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            self.assertEqual(s.submit("a", lambda: 1), 1)
            s.resize_workers(3)
            cp = s.stats_checkpoint()
            self.assertEqual(s.submit("b", lambda: 2), 2)
            s.resize_workers(1)
            stats = s.snapshot()
            self.assertEqual(
                (stats.accepted, stats.completed, stats.failed,
                 stats.cancelled, stats.expired, stats.rejected),
                (2, 2, 0, 0, 0, 0),
            )
            since = s.snapshot_since(cp)
            self.assertEqual((since.accepted, since.completed), (1, 1))


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_resizes_serialize_last_wins(self) -> None:
        with Scheduler(workers=2, max_pending=8) as s:
            targets = [1, 4, 2, 6, 3, 5]
            threads = [
                threading.Thread(target=s.resize_workers, args=(t,))
                for t in targets
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            # 无论交错顺序如何，最终容量必是某次调整的目标值。
            self.assertIn(s.runtime_snapshot().workers, targets)

    def test_resize_during_load_no_duplicate_or_lost_tasks(self) -> None:
        with Scheduler(workers=2, max_pending=64) as s:
            stop = threading.Event()

            def resize_loop() -> None:
                for w in (1, 4, 2, 5, 1, 3):
                    if stop.is_set():
                        return
                    s.resize_workers(w)
                    time.sleep(0.005)

            resizer = threading.Thread(target=resize_loop)
            resizer.start()
            ran: list[int] = []
            lock = threading.Lock()

            def make(i: int) -> "object":
                def fn() -> int:
                    with lock:
                        ran.append(i)
                    return i
                return fn

            handles = [
                s.submit_nowait("t%d" % i, make(i)) for i in range(40)
            ]
            for h in handles:
                h.result(5.0)
            stop.set()
            resizer.join()
            # 每个任务恰好执行一次：不重复认领、不漏派发。
            self.assertEqual(sorted(ran), list(range(40)))
            self.assertTrue(_wait_runtime(s, unfinished=0))

    def test_close_after_resize_stops_all_threads(self) -> None:
        s = Scheduler(workers=1, max_pending=8)
        s.resize_workers(4)
        self.assertEqual(s.submit("t", lambda: 7), 7)
        s.close()
        for t in s._worker_threads:  # type: ignore[attr-defined]
            self.assertFalse(t.is_alive())
        snap = s.runtime_snapshot()
        self.assertTrue(snap.closed)
        self.assertEqual(snap.workers, 4)


if __name__ == "__main__":
    unittest.main()
