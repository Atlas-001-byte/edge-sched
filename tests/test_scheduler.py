"""Scheduler 核心语义测试。"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    Scheduler,
    SchedulerClosedError,
    TaskHandle,
)


class ValidationTest(unittest.TestCase):
    def test_bad_workers(self) -> None:
        for bad in (0, -1, 1.5, "2", True, None):
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(workers=bad, max_pending=2)  # type: ignore[arg-type]

    def test_bad_max_pending(self) -> None:
        for bad in (0, -3, 2.0, False, None):
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(workers=2, max_pending=bad)  # type: ignore[arg-type]

    def test_bad_task_id(self) -> None:
        with Scheduler(1, 2) as s:
            for bad in ("", 1, None, b"x", 1.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit(bad, lambda: None)  # type: ignore[arg-type]

    def test_not_callable(self) -> None:
        with Scheduler(1, 2) as s:
            for bad in (None, 1, "abc", object()):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit("t", bad)  # type: ignore[arg-type]

    def test_validation_before_state_checks(self) -> None:
        # 参数非法时即使调度器已关闭也报 InputValidationError，且无统计影响。
        s = Scheduler(1, 1)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit("", lambda: None)
        self.assertEqual(s.snapshot().accepted, 0)


class BasicExecutionTest(unittest.TestCase):
    def test_return_value(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(s.submit("t1", lambda: 40 + 2), 42)
            self.assertIsNone(s.submit("t2", lambda: None))

    def test_failure_raises_original_exception(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            with self.assertRaises(ValueError) as cm:
                s.submit("boom", self._raise_value_error)
            self.assertEqual(str(cm.exception), "x")
            self.assertIs(type(cm.exception), ValueError)

            # 失败任务之后调度器照常工作。
            self.assertEqual(s.submit("ok", lambda: "fine"), "fine")

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 1)
        # 失败任务同样贡献延迟样本。
        self.assertGreater(snap.total_latency_ms["max"], 0.0)
        self.assertEqual(len(_samples(snap, "total")), 2)

    def test_result_readable_after_close(self) -> None:
        s = Scheduler(workers=2, max_pending=4)
        s.submit("ok", lambda: "v")
        with self.assertRaises(RuntimeError):
            s.submit("err", self._raise)  # type: ignore[arg-type]
        s.close()

        self.assertEqual(s.result("ok"), "v")
        with self.assertRaises(RuntimeError):
            s.result("err")
        with self.assertRaises(KeyError):
            s.result("missing")

    @staticmethod
    def _raise() -> None:
        raise RuntimeError("nope")

    @staticmethod
    def _raise_value_error() -> None:
        raise ValueError("x")


class ConcurrencyTest(unittest.TestCase):
    def test_tasks_run_in_parallel(self) -> None:
        # 2 个工作线程执行 2 个各 0.2 秒的任务，应显著快于串行的 0.4 秒。
        with Scheduler(workers=2, max_pending=4) as s:
            start = time.monotonic()
            t1 = threading.Thread(target=s.submit, args=("a", self._sleep02))
            t2 = threading.Thread(target=s.submit, args=("b", self._sleep02))
            t1.start(); t2.start()
            t1.join(); t2.join()
            elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.35)

    def test_fcfs_order_with_single_worker(self) -> None:
        order: list[str] = []
        order_lock = threading.Lock()

        def make(tid: str) -> "any":
            def fn() -> str:
                with order_lock:
                    order.append(tid)
                return tid
            return fn

        # 用信号量卡住工作线程：先占住唯一线程，再按 a->b->c 顺序提交，
        # 全部入队后放行，单工作线程必须严格按入队顺序执行。
        worker_free = threading.Event()

        def occupy() -> None:
            worker_free.wait(2.0)

        with Scheduler(workers=1, max_pending=4) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            threads = []
            for tid in ("a", "b", "c"):
                t = threading.Thread(
                    target=lambda tid=tid: s.submit(tid, make(tid))
                )
                t.start()
                threads.append(t)
                time.sleep(0.03)  # 固定提交顺序 a -> b -> c
            worker_free.set()
            holder.join()
            for t in threads:
                t.join()
        self.assertEqual(order, ["a", "b", "c"])

    @staticmethod
    def _sleep02() -> None:
        time.sleep(0.2)


class BackpressureTest(unittest.TestCase):
    def test_reject_at_limit(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocking() -> None:
            entered.set()
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            t = threading.Thread(target=s.submit, args=("x", blocking))
            t.start()
            self.assertTrue(entered.wait(2.0))
            # 第二个占满 pending 上限（在入站队列中等待派发）。
            t2 = threading.Thread(
                target=s.submit, args=("y", lambda: None)
            )
            t2.start()
            self._wait_pending(s, 2)

            with self.assertRaises(BackpressureError):
                s.submit("z", lambda: None)

            release.set()
            t.join(); t2.join()

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 2)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.failed, 0)

    def test_rejected_task_leaves_no_trace(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            t = threading.Thread(
                target=s.submit, args=("only", lambda: release.wait(2.0))
            )
            t.start()
            self._wait_pending(s, 1)
            with self.assertRaises(BackpressureError):
                s.submit("dup-space", lambda: 1)
            release.set()
            t.join()

            # 被拒任务不占用 task_id，也无法读取结果。
            with self.assertRaises(KeyError):
                s.result("dup-space")
            self.assertEqual(s.submit("dup-space", lambda: 7), 7)

    @staticmethod
    def _wait_pending(s: Scheduler, n: int) -> None:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if s.snapshot().accepted == n:
                return
            time.sleep(0.005)
        raise AssertionError("pending count never reached %d" % n)


class DuplicateTest(unittest.TestCase):
    def test_duplicate_while_unfinished(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            t = threading.Thread(
                target=s.submit, args=("d", lambda: release.wait(2.0))
            )
            t.start()
            self.assertTrue(_wait_accepted(s, 1))
            with self.assertRaises(DuplicateTaskError):
                s.submit("d", lambda: None)
            release.set()
            t.join()

    def test_task_id_reusable_after_finish(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            self.assertEqual(s.submit("r", lambda: 1), 1)
            self.assertEqual(s.submit("r", lambda: 2), 2)
            self.assertEqual(s.result("r"), 2)


class CloseTest(unittest.TestCase):
    def test_submit_after_close_rejected(self) -> None:
        s = Scheduler(workers=2, max_pending=4)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.submit("late", lambda: None)
        # 关闭路径拒绝不计入 rejected（背压口径只统计运行期上限拒绝）。
        self.assertEqual(s.snapshot().rejected, 0)
        s.close()  # 重复关闭安全

    def test_close_waits_for_accepted_tasks(self) -> None:
        ran: list[str] = []

        def slow(tid: str) -> "any":
            def fn() -> None:
                time.sleep(0.15)
                ran.append(tid)
            return fn

        s = Scheduler(workers=1, max_pending=4)
        for tid in ("a", "b", "c"):
            threading.Thread(target=s.submit, args=(tid, slow(tid))).start()
        self.assertTrue(_wait_accepted(s, 3))
        s.close()  # 必须排空全部已接受任务后才返回
        self.assertEqual(ran, ["a", "b", "c"])
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.completed), (3, 3))

    def test_concurrent_close_and_submit(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=2, max_pending=8)
        outcomes: list[str] = []

        def worker() -> None:
            try:
                s.submit("t%d" % threading.get_ident(), lambda: release.wait(2.0))
                outcomes.append("accepted")
            except SchedulerClosedError:
                outcomes.append("closed")

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        s.close()
        release.set()
        for t in threads:
            t.join()
        self.assertEqual(len(outcomes), 4)
        self.assertTrue(set(outcomes) <= {"accepted", "closed"})
        self.assertEqual(
            s.snapshot().accepted, outcomes.count("accepted")
        )


class TimeoutTest(unittest.TestCase):
    def test_timeout_leaves_task_running(self) -> None:
        release = threading.Event()

        def slow() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(TimeoutError):
                s.submit("slow", slow, timeout=0.05)
            # 任务仍在执行；结果最终可读，统计恰好一次。
            release.set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().completed == 1:
                    break
                time.sleep(0.01)
            self.assertIsNone(s.result("slow"))
            self.assertEqual(s.snapshot().accepted, 1)

    def test_duplicate_detected_during_unfinished_timeout(self) -> None:
        release = threading.Event()

        def slow() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(TimeoutError):
                s.submit("u", slow, timeout=0.05)
            with self.assertRaises(DuplicateTaskError):
                s.submit("u", lambda: None)
            release.set()


class SnapshotLatencyTest(unittest.TestCase):
    def test_samples_recorded_per_finished_task(self) -> None:
        with Scheduler(workers=4, max_pending=8) as s:
            for i in range(10):
                s.submit("t%d" % i, lambda i=i: i * 2)
            snap = s.snapshot()
        self.assertEqual(snap.completed, 10)
        self.assertEqual(len(_samples(snap, "wait")), 10)
        self.assertEqual(len(_samples(snap, "total")), 10)
        for dist in (snap.queue_wait_ms, snap.total_latency_ms):
            for key in ("p50", "p95", "p99", "max"):
                self.assertIn(key, dist)
        self.assertGreaterEqual(
            snap.total_latency_ms["max"], snap.queue_wait_ms["max"]
        )


class SubmitNowaitBasicTest(unittest.TestCase):
    def test_returns_handle_immediately(self) -> None:
        release = threading.Event()

        def block() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            start = time.monotonic()
            handle = s.submit_nowait("h", block)
            self.assertLess(time.monotonic() - start, 0.2)
            self.assertIsInstance(handle, TaskHandle)
            self.assertFalse(handle.done())
            release.set()
            self.assertIsNone(handle.result(2.0))
            self.assertTrue(handle.done())

    def test_success_value_and_failure_original_exception(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            ok = s.submit_nowait("ok", lambda: 42)
            self.assertEqual(ok.result(2.0), 42)

            def boom() -> None:
                raise ValueError("x")

            bad = s.submit_nowait("bad", boom)
            with self.assertRaises(ValueError) as cm:
                bad.result(2.0)
            self.assertEqual(str(cm.exception), "x")
            self.assertIs(type(cm.exception), ValueError)
            self.assertTrue(bad.done())

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 1)

    def test_result_blocks_until_done_without_timeout(self) -> None:
        release = threading.Event()

        def fn() -> str:
            release.wait(2.0)
            return "v"

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("h", fn)
            finished = threading.Event()

            def waiter() -> None:
                self.assertEqual(handle.result(), "v")
                finished.set()

            t = threading.Thread(target=waiter)
            t.start()
            self.assertFalse(finished.wait(0.1))
            release.set()
            self.assertTrue(finished.wait(2.0))
            t.join()

    def test_result_remaining_readable_after_close(self) -> None:
        s = Scheduler(workers=2, max_pending=4)
        handle = s.submit_nowait("h", lambda: "v")
        s.close()
        self.assertTrue(handle.done())
        self.assertEqual(handle.result(), "v")

    def test_counts_settled_before_result_returns(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            handle = s.submit_nowait("h", lambda: 1)
            handle.result(2.0)
            snap = s.snapshot()
            self.assertEqual(snap.accepted, 1)
            self.assertEqual(snap.completed, 1)
            self.assertEqual(snap.failed, 0)
            self.assertEqual(len(_samples(snap, "total")), 1)

    def test_negative_timeout_validation(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("h", lambda: 1)
            handle.result(2.0)
            with self.assertRaises(InputValidationError):
                handle.result(-1)


class SubmitNowaitTimeoutTest(unittest.TestCase):
    def test_timeout_raises_and_keeps_task_running(self) -> None:
        release = threading.Event()

        def slow() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("slow", slow)
            with self.assertRaises(TimeoutError):
                handle.result(0.05)
            self.assertFalse(handle.done())
            # 任务继续；之后同一 handle 仍取得唯一结果。
            release.set()
            self.assertIsNone(handle.result(2.0))
            self.assertTrue(handle.done())
            self.assertEqual(s.snapshot().accepted, 1)
            self.assertEqual(s.snapshot().completed, 1)

    def test_result_idempotent_after_completion(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("h", lambda: 7)
            with self.assertRaises(TimeoutError):
                handle.result(0)
            self.assertEqual(handle.result(2.0), 7)
            # 重复读取返回同一结果，不重复记账。
            self.assertEqual(handle.result(), 7)
            self.assertEqual(s.snapshot().completed, 1)


class SubmitNowaitValidationTest(unittest.TestCase):
    def test_bad_task_id(self) -> None:
        with Scheduler(1, 2) as s:
            for bad in ("", 1, None, b"x", 1.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_nowait(bad, lambda: None)  # type: ignore[arg-type]
        self.assertEqual(s.snapshot().accepted, 0)

    def test_not_callable(self) -> None:
        with Scheduler(1, 2) as s:
            for bad in (None, 1, "abc", object()):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_nowait("t", bad)  # type: ignore[arg-type]
        self.assertEqual(s.snapshot().accepted, 0)

    def test_validation_before_state_checks(self) -> None:
        s = Scheduler(1, 1)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit_nowait("", lambda: None)
        self.assertEqual(s.snapshot().accepted, 0)

    def test_after_closed(self) -> None:
        s = Scheduler(1, 2)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.submit_nowait("late", lambda: None)
        self.assertEqual(s.snapshot().rejected, 0)

    def test_duplicate_unfinished(self) -> None:
        release = threading.Event()

        def block() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=4) as s:
            handle = s.submit_nowait("d", block)
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("d", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit("d", lambda: None)
            release.set()
            self.assertIsNone(handle.result(2.0))
        self.assertEqual(s.snapshot().accepted, 1)


class SubmitNowaitBackpressureTest(unittest.TestCase):
    def test_backpressure_from_nowait(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocking() -> None:
            entered.set()
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            h = s.submit_nowait("x", blocking)
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("y", lambda: None)
            self._wait_pending(s, 2)

            with self.assertRaises(BackpressureError):
                s.submit_nowait("z", lambda: None)
            with self.assertRaises(BackpressureError):
                s.submit("z2", lambda: None)

            release.set()
            self.assertIsNone(h.result(2.0))

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 2)
        self.assertEqual(snap.rejected, 2)
        self.assertEqual(snap.failed, 0)

    def test_rejected_nowait_leaves_no_trace(self) -> None:
        release = threading.Event()

        def block() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=1) as s:
            s.submit_nowait("only", block)
            self._wait_pending(s, 1)
            with self.assertRaises(BackpressureError):
                s.submit_nowait("nope", lambda: 1)
            with self.assertRaises(KeyError):
                s.result("nope")
            release.set()
            # 等唯一在途任务结束、pending 回落，再验证被拒 id 可被重新接受。
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().completed == 1:
                    break
                time.sleep(0.005)
            self.assertEqual(s.submit("nope", lambda: 7), 7)

    @staticmethod
    def _wait_pending(s: Scheduler, n: int) -> None:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if s.snapshot().accepted == n:
                return
            time.sleep(0.005)
        raise AssertionError("pending count never reached %d" % n)


class HandleAndTaskIdTest(unittest.TestCase):
    def test_handle_and_result_by_task_id_agree(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            handle = s.submit_nowait("k", lambda: "v")
            self.assertEqual(handle.result(2.0), "v")
            self.assertEqual(s.result("k"), "v")
            self.assertTrue(handle.done())

    def test_unfinished_result_by_task_id_is_runtime_error(self) -> None:
        release = threading.Event()

        def block() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("k", block)
            with self.assertRaises(RuntimeError):
                s.result("k")
            self.assertFalse(handle.done())
            release.set()
            self.assertIsNone(handle.result(2.0))
            self.assertEqual(s.result("k"), None)

    def test_old_handle_pinned_after_task_id_reuse(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            first = s.submit_nowait("r", lambda: 1)
            self.assertEqual(first.result(2.0), 1)
            # task_id 已释放，可复用；旧句柄仍读第一次的结果。
            second = s.submit_nowait("r", lambda: 2)
            self.assertEqual(second.result(2.0), 2)
            self.assertEqual(first.result(), 1)
            self.assertEqual(s.result("r"), 2)

    def test_old_handle_to_failed_task_after_reuse(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            def boom() -> None:
                raise ValueError("old")

            old = s.submit_nowait("r", boom)
            with self.assertRaises(ValueError):
                old.result(2.0)
            new = s.submit_nowait("r", lambda: "new")
            self.assertEqual(new.result(2.0), "new")
            with self.assertRaises(ValueError) as cm:
                old.result()
            self.assertEqual(str(cm.exception), "old")

    def test_many_handles_share_single_outcome(self) -> None:
        release = threading.Event()

        def fn() -> str:
            release.wait(2.0)
            return "done"

        with Scheduler(workers=1, max_pending=4) as s:
            h1 = s.submit_nowait("m", fn)
            # 同一条目的多个等待方都被同一次完成唤醒。
            ready = threading.Event()

            def waiter() -> None:
                ready.set()
                h1.result()

            threads = [threading.Thread(target=waiter) for _ in range(3)]
            for t in threads:
                t.start()
            self.assertTrue(ready.wait(2.0))
            time.sleep(0.05)
            release.set()
            for t in threads:
                t.join(2.0)
                self.assertFalse(t.is_alive())
            self.assertTrue(h1.done())
            self.assertEqual(h1.result(), "done")


def _wait_accepted(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if s.snapshot().accepted == n:
            return True
        time.sleep(0.005)
    return False


def _samples(snap: "object", kind: str) -> list[float]:
    # 通过私有样本构造分布的长度间接验证；这里直接用快照属性重算。
    from edge_sched.stats import _distribution  # type: ignore[attr-defined]
    attr = (
        "_queue_wait_samples" if kind == "wait" else "_total_latency_samples"
    )
    return getattr(snap, attr)


if __name__ == "__main__":
    unittest.main()
