"""Scheduler.resize_workers 运行时工作线程容量调整测试。

覆盖：参数校验（bool/0/负数/浮点数/其他类型）、相同容量无操作、扩容后
新线程立即接收任务、缩容后并发上限立即降到目标值且不打断执行中任务、
扩回容量补足线程后恢复并发、runtime_snapshot 的 workers 口径与既有快照
不变、max_pending/背压/取消/准入等待/优先级派发语义不受容量变化影响、
closing/closed 时抛 SchedulerClosedError，以及并发调整与提交的安全性。
"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    InputValidationError,
    Scheduler,
    SchedulerClosedError,
    TaskCancelledError,
)


def _wait_until(pred, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.002)
    return False


class _ConcurrencyTracker:
    """记录 callable 并发执行数峰值的工具。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.current = 0
        self.peak = 0

    def task(self, duration: float = 0.15):
        def fn() -> None:
            with self._lock:
                self.current += 1
                if self.current > self.peak:
                    self.peak = self.current
            try:
                time.sleep(duration)
            finally:
                with self._lock:
                    self.current -= 1

        return fn


def _blocking_task(started: threading.Event, gate: threading.Event,
                   value="done"):
    """先置 started，再阻塞到 gate 被置位，最后返回 value。"""

    def fn():
        started.set()
        gate.wait(2.0)
        return value

    return fn


class ValidationTest(unittest.TestCase):
    def test_invalid_values_raise_and_keep_capacity(self) -> None:
        with Scheduler(2, 4) as s:
            for bad in (True, False, 0, -1, -8, 1.5, 2.0, "2", None,
                        object(), [2]):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.resize_workers(bad)  # type: ignore[arg-type]
            # 容量与可用性不变。
            self.assertEqual(s.runtime_snapshot().workers, 2)
            self.assertEqual(s.submit("ok", lambda: 7), 7)

    def test_validation_precedes_closed_check(self) -> None:
        s = Scheduler(2, 4)
        s.close()
        with self.assertRaises(InputValidationError):
            s.resize_workers(0)
        with self.assertRaises(InputValidationError):
            s.resize_workers(True)


class SameCapacityTest(unittest.TestCase):
    def test_same_capacity_is_noop(self) -> None:
        with Scheduler(2, 4) as s:
            threads_before = list(s._worker_threads)  # type: ignore[attr-defined]
            self.assertIsNone(s.resize_workers(2))
            self.assertIsNone(s.resize_workers(2))
            self.assertEqual(
                s._worker_threads, threads_before  # type: ignore[attr-defined]
            )
            self.assertEqual(s.runtime_snapshot().workers, 2)
            self.assertEqual(s.submit("t", lambda: "v"), "v")


class GrowTest(unittest.TestCase):
    def test_grow_allows_more_concurrency(self) -> None:
        with Scheduler(1, 8) as s:
            s.resize_workers(3)
            self.assertEqual(s.runtime_snapshot().workers, 3)
            tracker = _ConcurrencyTracker()
            handles = [
                s.submit_nowait("t%d" % i, tracker.task())
                for i in range(3)
            ]
            for h in handles:
                h.result()
            self.assertEqual(tracker.peak, 3)

    def test_grow_dispatches_queued_tasks_immediately(self) -> None:
        # 唯一工作线程被占时扩容：排队任务不必等 blocker 结束即可并发执行。
        with Scheduler(1, 8) as s:
            gate = threading.Event()
            started = threading.Event()
            s.submit_nowait("blocker", _blocking_task(started, gate))
            self.assertTrue(started.wait(2.0))
            tracker = _ConcurrencyTracker()
            followers = [
                s.submit_nowait("f%d" % i, tracker.task())
                for i in range(2)
            ]
            s.resize_workers(3)
            gate.set()
            for h in followers:
                h.result()
            self.assertEqual(tracker.peak, 2)

    def test_grow_back_after_shrink_restores_concurrency(self) -> None:
        with Scheduler(3, 8) as s:
            s.resize_workers(1)
            s.resize_workers(3)
            self.assertEqual(s.runtime_snapshot().workers, 3)
            tracker = _ConcurrencyTracker()
            handles = [
                s.submit_nowait("t%d" % i, tracker.task())
                for i in range(3)
            ]
            for h in handles:
                h.result()
            self.assertEqual(tracker.peak, 3)


class ShrinkTest(unittest.TestCase):
    def test_shrink_does_not_interrupt_running_tasks(self) -> None:
        with Scheduler(2, 4) as s:
            gate = threading.Event()
            started = [threading.Event() for _ in range(2)]
            h0 = s.submit_nowait(
                "b0", _blocking_task(started[0], gate, "done-0")
            )
            h1 = s.submit_nowait(
                "b1", _blocking_task(started[1], gate, "done-1")
            )
            self.assertTrue(started[0].wait(2.0))
            self.assertTrue(started[1].wait(2.0))
            s.resize_workers(1)
            self.assertEqual(s.runtime_snapshot().workers, 1)
            queued = s.submit_nowait("q", lambda: "queued")
            gate.set()
            # 执行中任务按原值结束，不被打断；排队任务随后正常执行。
            self.assertEqual(h0.result(), "done-0")
            self.assertEqual(h1.result(), "done-1")
            self.assertEqual(queued.result(), "queued")

    def test_shrink_caps_new_work_immediately(self) -> None:
        with Scheduler(3, 8) as s:
            gate = threading.Event()
            started = [threading.Event() for _ in range(3)]
            blockers = [
                s.submit_nowait(
                    "b%d" % i, _blocking_task(started[i], gate, i)
                )
                for i in range(3)
            ]
            for e in started:
                self.assertTrue(e.wait(2.0))
            s.resize_workers(1)
            tracker = _ConcurrencyTracker()
            followers = [
                s.submit_nowait("f%d" % i, tracker.task(0.05))
                for i in range(3)
            ]
            gate.set()
            for i, h in enumerate(blockers):
                self.assertEqual(h.result(), i)
            for h in followers:
                h.result()
            # 旧任务结束后不再按旧上限补位：新任务严格串行。
            self.assertEqual(tracker.peak, 1)

    def test_shrink_to_one_and_serial_execution(self) -> None:
        with Scheduler(4, 8) as s:
            s.resize_workers(1)
            tracker = _ConcurrencyTracker()
            handles = [
                s.submit_nowait("t%d" % i, tracker.task(0.05))
                for i in range(4)
            ]
            for h in handles:
                h.result()
            self.assertEqual(tracker.peak, 1)

    def test_running_task_failure_unaffected_by_shrink(self) -> None:
        with Scheduler(2, 4) as s:
            gate = threading.Event()
            started = threading.Event()

            def boom():
                started.set()
                gate.wait(2.0)
                raise ValueError("x")

            h = s.submit_nowait("boom", boom)
            self.assertTrue(started.wait(2.0))
            s.resize_workers(1)
            gate.set()
            # 执行中任务仍按原异常结束。
            with self.assertRaises(ValueError) as cm:
                h.result()
            self.assertEqual(str(cm.exception), "x")
            self.assertEqual(s.snapshot().failed, 1)


class UnchangedSemanticsTest(unittest.TestCase):
    def test_max_pending_and_backpressure_unchanged(self) -> None:
        with Scheduler(1, 2) as s:
            s.resize_workers(4)
            gate = threading.Event()
            s.submit_nowait("b1", lambda: gate.wait(2.0))
            s.submit_nowait("b2", lambda: gate.wait(2.0))
            # 扩容不改变 max_pending：两个未完成任务即触发背压。
            with self.assertRaises(BackpressureError):
                s.submit_nowait("overflow", lambda: None)
            self.assertEqual(s.runtime_snapshot().max_pending, 2)
            self.assertEqual(s.snapshot().rejected, 1)
            gate.set()

    def test_cancel_still_works_after_shrink(self) -> None:
        with Scheduler(2, 4) as s:
            gate = threading.Event()
            started = [threading.Event() for _ in range(2)]
            s.submit_nowait("b0", _blocking_task(started[0], gate))
            s.submit_nowait("b1", _blocking_task(started[1], gate))
            for e in started:
                self.assertTrue(e.wait(2.0))
            s.resize_workers(1)
            victim = s.submit_nowait("victim", lambda: "never")
            self.assertTrue(victim.cancel())
            with self.assertRaises(TaskCancelledError):
                victim.result()
            gate.set()
            self.assertEqual(s.snapshot().cancelled, 1)

    def test_submit_with_wait_fifo_unchanged_by_resize(self) -> None:
        with Scheduler(1, 1) as s:
            gate = threading.Event()
            started = threading.Event()
            s.submit_nowait("b", _blocking_task(started, gate))
            self.assertTrue(started.wait(2.0))
            s.resize_workers(2)
            results: list = []
            waiter = threading.Thread(
                target=lambda: results.append(
                    s.submit_with_wait(
                        "w", lambda: "w", admission_timeout_ms=2000
                    )
                )
            )
            waiter.start()
            # max_pending 仍为 1：扩容不放行准入等待者。
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            gate.set()
            waiter.join(2.0)
            self.assertFalse(waiter.is_alive())
            self.assertEqual(results, ["w"])

    def test_priority_dispatch_after_grow(self) -> None:
        with Scheduler(1, 8) as s:
            gate = threading.Event()
            started = threading.Event()
            s.submit_nowait("blocker", _blocking_task(started, gate))
            self.assertTrue(started.wait(2.0))
            order: "list[str]" = []
            lock = threading.Lock()

            def rec(name: str):
                def fn() -> None:
                    with lock:
                        order.append(name)

                return fn

            s.submit_nowait("low", rec("low"), priority=0)
            s.submit_nowait("high", rec("high"), priority=5)
            s.resize_workers(2)
            gate.set()
            deadline = time.monotonic() + 2.0
            while len(order) < 2 and time.monotonic() < deadline:
                time.sleep(0.005)
            # 扩容后空出的容量仍按 priority 降序派发。
            self.assertEqual(order, ["high", "low"])

    def test_resize_does_not_touch_stats(self) -> None:
        with Scheduler(1, 4) as s:
            self.assertEqual(s.submit("a", lambda: 1), 1)
            before = s.snapshot().to_dict()
            s.resize_workers(3)
            s.resize_workers(1)
            s.resize_workers(1)
            self.assertEqual(s.snapshot().to_dict(), before)


class SnapshotTest(unittest.TestCase):
    def test_runtime_snapshot_reports_current_target(self) -> None:
        with Scheduler(2, 4) as s:
            snap1 = s.runtime_snapshot()
            s.resize_workers(5)
            snap2 = s.runtime_snapshot()
            s.resize_workers(1)
            snap3 = s.runtime_snapshot()
            # 各快照报告其创建时刻的有效目标容量，已返回快照不随调整变化。
            self.assertEqual(snap1.workers, 2)
            self.assertEqual(snap2.workers, 5)
            self.assertEqual(snap3.workers, 1)
            self.assertEqual(snap1.to_dict()["workers"], 2)
            self.assertEqual(snap2.to_dict()["workers"], 5)
            self.assertEqual(snap3.to_dict()["workers"], 1)

    def test_snapshot_fields_unchanged_by_resize(self) -> None:
        with Scheduler(2, 4) as s:
            before = s.runtime_snapshot().to_dict()
            s.resize_workers(3)
            after = s.runtime_snapshot().to_dict()
            before["workers"] = 3
            self.assertEqual(before, after)


class CloseTest(unittest.TestCase):
    def test_resize_after_close_raises(self) -> None:
        s = Scheduler(2, 4)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(3)
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(2)
        snap = s.runtime_snapshot()
        self.assertEqual(snap.workers, 2)
        self.assertTrue(snap.closed)

    def test_resize_during_closing_raises_and_close_completes(self) -> None:
        s = Scheduler(1, 2)
        gate = threading.Event()
        started = threading.Event()
        s.submit_nowait("b", _blocking_task(started, gate, "ok"))
        self.assertTrue(started.wait(2.0))
        closer = threading.Thread(target=s.close)
        closer.start()
        self.assertTrue(
            _wait_until(lambda: s._closing)  # type: ignore[attr-defined]
        )
        with self.assertRaises(SchedulerClosedError):
            s.resize_workers(3)
        gate.set()
        closer.join(2.0)
        self.assertFalse(closer.is_alive())
        # close 仍等待全部已接受任务结束，结果可读。
        self.assertEqual(s.result("b"), "ok")
        self.assertTrue(s.runtime_snapshot().closed)

    def test_resize_before_close_then_close_stops_all_threads(self) -> None:
        s = Scheduler(1, 4)
        s.resize_workers(3)
        self.assertEqual(s.submit("t", lambda: 1), 1)
        s.close()
        self.assertEqual(s.runtime_snapshot().workers, 3)
        for t in s._worker_threads:  # type: ignore[attr-defined]
            self.assertFalse(t.is_alive())

    def test_shrink_then_close_stops_all_threads(self) -> None:
        s = Scheduler(4, 4)
        s.resize_workers(1)
        s.close()
        for t in s._worker_threads:  # type: ignore[attr-defined]
            self.assertFalse(t.is_alive())


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_resizes_and_submits(self) -> None:
        with Scheduler(2, 32) as s:
            stop = threading.Event()

            def resizer() -> None:
                n = 1
                while not stop.is_set():
                    try:
                        s.resize_workers(n)
                    except SchedulerClosedError:
                        return
                    n = n % 4 + 1

            threads = [threading.Thread(target=resizer) for _ in range(3)]
            for t in threads:
                t.start()
            for i in range(20):
                self.assertIsNone(s.submit("t%d" % i, lambda: None))
            stop.set()
            for t in threads:
                t.join(2.0)
            s.resize_workers(2)
            self.assertEqual(s.runtime_snapshot().workers, 2)
            snap = s.snapshot()
            self.assertEqual(snap.accepted, 20)
            self.assertEqual(snap.completed, 20)


if __name__ == "__main__":
    unittest.main()
