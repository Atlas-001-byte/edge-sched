"""Scheduler 核心语义测试。"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    DuplicateTaskError,
    EdgeSchedError,
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    SchedulerClosedError,
    TaskCancelledError,
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
        # 失败任务同样贡献延迟样本（含执行耗时）。
        self.assertGreater(snap.total_latency_ms["max"], 0.0)
        self.assertEqual(len(_samples(snap, "total")), 2)
        self.assertEqual(len(_samples(snap, "execution")), 2)
        self.assertGreater(snap.execution_ms["max"], 0.0)

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


class PriorityTest(unittest.TestCase):
    @staticmethod
    def _make_recorder(tid: str, order: list,
                       order_lock: threading.Lock) -> "any":
        def fn() -> str:
            with order_lock:
                order.append(tid)
            return tid
        return fn

    def test_higher_priority_dispatched_first(self) -> None:
        # 单工作线程被占住时，按 low -> mid -> high 顺序提交；
        # 放行后必须严格按 high -> mid -> low 执行，而非提交顺序。
        order: list[str] = []
        order_lock = threading.Lock()
        entered = threading.Event()
        worker_free = threading.Event()

        def occupy() -> None:
            entered.set()
            worker_free.wait(2.0)

        with Scheduler(workers=1, max_pending=8) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            threads = []
            for tid, prio in (("low", 0), ("mid", 5), ("high", 10)):
                t = threading.Thread(
                    target=lambda tid=tid, p=prio: s.submit(
                        tid, self._make_recorder(tid, order, order_lock),
                        priority=p,
                    )
                )
                t.start()
                threads.append(t)
                time.sleep(0.03)  # 固定提交顺序 low -> mid -> high
            self.assertTrue(_wait_accepted(s, 4))
            worker_free.set()
            holder.join()
            for t in threads:
                t.join()
        self.assertEqual(order, ["high", "mid", "low"])

    def test_same_priority_keeps_acceptance_order(self) -> None:
        # 相同优先级按接受先后派发；不同优先级之间高优先级整体提前。
        order: list[str] = []
        order_lock = threading.Lock()
        entered = threading.Event()
        worker_free = threading.Event()

        def occupy() -> None:
            entered.set()
            worker_free.wait(2.0)

        with Scheduler(workers=1, max_pending=8) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            threads = []
            for tid, prio in (("a", 1), ("b", 1), ("c", 2), ("d", 0)):
                t = threading.Thread(
                    target=lambda tid=tid, p=prio: s.submit(
                        tid, self._make_recorder(tid, order, order_lock),
                        priority=p,
                    )
                )
                t.start()
                threads.append(t)
                time.sleep(0.03)
            self.assertTrue(_wait_accepted(s, 5))
            worker_free.set()
            holder.join()
            for t in threads:
                t.join()
        self.assertEqual(order, ["c", "a", "b", "d"])

    def test_default_priority_is_zero_and_negative_accepted(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(s.submit("z", lambda: 1), 1)
            self.assertEqual(
                s.submit("n", lambda: -1, priority=-100), -1
            )

    def test_priority_does_not_preempt_running_task(self) -> None:
        # 低优先级任务一旦开始执行，后到的高优先级任务只能等它结束，
        # 优先级不重排执行中任务。
        started = threading.Event()
        release = threading.Event()
        order: list[str] = []
        lock = threading.Lock()

        def low() -> None:
            with lock:
                order.append("low-start")
            started.set()
            release.wait(2.0)
            with lock:
                order.append("low-end")

        def high() -> None:
            with lock:
                order.append("high")

        with Scheduler(workers=1, max_pending=4) as s:
            hl = s.submit_nowait("low", low, priority=-5)
            self.assertTrue(started.wait(2.0))
            hh = s.submit_nowait("high", high, priority=100)
            time.sleep(0.1)
            with lock:
                self.assertEqual(order, ["low-start"])
            release.set()
            self.assertIsNone(hh.result())
            self.assertIsNone(hl.result())
        self.assertEqual(order, ["low-start", "low-end", "high"])

    def test_cancelled_skipped_then_next_priority_runs(self) -> None:
        # 最高优先级任务在排队中被取消后，派发落到次高优先级任务。
        order: list[str] = []
        order_lock = threading.Lock()
        entered = threading.Event()
        worker_free = threading.Event()

        def occupy() -> None:
            entered.set()
            worker_free.wait(2.0)

        with Scheduler(workers=1, max_pending=8) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            h_low = s.submit_nowait(
                "low", self._make_recorder("low", order, order_lock),
                priority=0,
            )
            h_high = s.submit_nowait(
                "high", self._make_recorder("high", order, order_lock),
                priority=10,
            )
            self.assertTrue(h_high.cancel())
            worker_free.set()
            holder.join()
            self.assertEqual(h_low.result(), "low")
            with self.assertRaises(TaskCancelledError):
                h_high.result()
        self.assertEqual(order, ["low"])

    def test_invalid_priority_raises(self) -> None:
        # bool 虽是 int 子类，但语义上不是合法优先级；float/str/None 同样拒绝。
        with Scheduler(workers=1, max_pending=4) as s:
            for bad in (1.5, "1", True, False, None, 0.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit("t", lambda: None,
                                 priority=bad)  # type: ignore[arg-type]
                    with self.assertRaises(InputValidationError):
                        s.submit_nowait(
                            "u", lambda: None,
                            priority=bad,  # type: ignore[arg-type]
                        )
        # 校验失败不留统计痕迹。
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.failed, snap.rejected),
            (0, 0, 0, 0),
        )

    def test_priority_validated_before_close_check(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit("t", lambda: None, priority=True)
        with self.assertRaises(SchedulerClosedError):
            s.submit("t", lambda: None, priority=1)


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
        self.assertEqual(len(_samples(snap, "execution")), 10)
        for dist in (snap.queue_wait_ms, snap.total_latency_ms,
                     snap.execution_ms):
            for key in ("p50", "p95", "p99", "max"):
                self.assertIn(key, dist)
        self.assertGreaterEqual(
            snap.total_latency_ms["max"], snap.queue_wait_ms["max"]
        )
        # 执行耗时不超过总时延（总时延 = 排队等待 + 执行，同以三位小数计）。
        self.assertGreaterEqual(
            snap.total_latency_ms["max"], snap.execution_ms["max"]
        )


class ExecutionTimingTest(unittest.TestCase):
    def test_execution_excludes_pre_claim_queue_wait(self) -> None:
        # 单工作线程：首个任务占住线程，第二个任务排队约 150ms 后才被认领，
        # 而其 callable 立即返回。execution_ms 必须只反映认领后的执行时间
        # （约 0），不含认领前的排队等待。
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_accepted(s, 1))
            queued = s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))
            time.sleep(0.15)  # 让 queued 在认领前排队等待一段时间。
            release.set()
            holder.result(2.0)
            queued.result(2.0)
            snap = s.snapshot()

        # 样本按结束顺序追加：holder 先结束、queued 认领后立即结束。
        wait_samples = _samples(snap, "wait")
        total_samples = _samples(snap, "total")
        exec_samples = _samples(snap, "execution")
        self.assertEqual(len(exec_samples), 2)
        queued_wait, queued_total = wait_samples[1], total_samples[1]
        queued_exec = exec_samples[1]
        self.assertGreaterEqual(queued_wait, 100.0)
        self.assertLess(queued_exec, 50.0)
        # 总时延 ≈ 排队等待 + 执行耗时（三者各自保留三位小数）。
        self.assertAlmostEqual(
            queued_total, queued_wait + queued_exec, delta=1.0
        )

    def test_failure_contributes_one_sample_and_keeps_original_exception(self) -> None:
        def boom() -> None:
            raise ValueError("boom")

        with Scheduler(workers=2, max_pending=4) as s:
            with self.assertRaises(ValueError) as cm:
                s.submit("f1", boom)
            self.assertEqual(str(cm.exception), "boom")
            h = s.submit_nowait("f2", boom)
            # TaskHandle.result 与 Scheduler.result 都原样抛出 callable 异常。
            with self.assertRaises(ValueError):
                h.result(2.0)
            with self.assertRaises(ValueError):
                s.result("f2")
            snap = s.snapshot()

        self.assertEqual(snap.failed, 2)
        self.assertEqual(snap.completed, 0)
        # 每个失败任务恰好贡献一个执行耗时样本。
        self.assertEqual(len(_samples(snap, "execution")), 2)
        self.assertEqual(set(snap.execution_ms), {"p50", "p95", "p99", "max"})

    def test_cancelled_expired_invalid_leave_no_execution_sample(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            running = s.submit_nowait("run", lambda: release.wait(2.0))
            self.assertTrue(_wait_accepted(s, 1))

            canc = s.submit_nowait("c", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))
            self.assertTrue(canc.cancel())
            with self.assertRaises(TaskCancelledError):
                canc.result(2.0)

            exp = s.submit_nowait("e", lambda: None, max_queue_wait_ms=30)
            with self.assertRaises(QueueTimeoutError):
                exp.result(2.0)

            # 参数校验失败：不接纳、不执行、不贡献样本。
            with self.assertRaises(InputValidationError):
                s.submit("", lambda: None)

            # 此时唯一执行中的任务尚未结束，尚无任何执行样本。
            self.assertEqual(len(_samples(s.snapshot(), "execution")), 0)

            release.set()
            running.result(2.0)
            snap = s.snapshot()

        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.expired, 1)
        self.assertEqual(snap.completed, 1)
        # 只有真正执行的 running 贡献一个 execution_ms 样本。
        self.assertEqual(len(_samples(snap, "execution")), 1)
        self.assertEqual(snap.execution_ms["max"], _samples(snap, "execution")[0])


_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


class StatsCheckpointTest(unittest.TestCase):
    def test_empty_interval_right_after_checkpoint(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            cp = s.stats_checkpoint()
            interval = s.snapshot_since(cp)
            self.assertEqual(
                (interval.accepted, interval.completed, interval.failed,
                 interval.cancelled, interval.expired, interval.rejected),
                (0, 0, 0, 0, 0, 0),
            )
            self.assertEqual(interval.queue_wait_ms, _EMPTY_DIST)
            self.assertEqual(interval.total_latency_ms, _EMPTY_DIST)
            self.assertEqual(interval.execution_ms, _EMPTY_DIST)
            self.assertEqual(set(interval.to_dict()),
                             set(s.snapshot().to_dict()))

    def test_interval_collects_only_later_events(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(s.submit("before", lambda: 1), 1)
            cp = s.stats_checkpoint()
            self.assertEqual(s.submit("after", lambda: 2), 2)

            interval = s.snapshot_since(cp)
            self.assertEqual(interval.accepted, 1)
            self.assertEqual(interval.completed, 1)
            self.assertEqual(len(_samples(interval, "wait")), 1)
            self.assertEqual(len(_samples(interval, "total")), 1)
            self.assertEqual(len(_samples(interval, "execution")), 1)
            # 累计 snapshot 仍是两个任务。
            total = s.snapshot()
            self.assertEqual(total.accepted, 2)
            self.assertEqual(total.completed, 2)

    def test_cross_boundary_task_split_by_moments(self) -> None:
        # 边界前接纳、边界后结束：accepted 不计入区间，completed 与三类
        # 延迟样本（含 execution_ms）计入区间。
        release = threading.Event()

        def wait_then_done() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("span", wait_then_done)
            self.assertTrue(_wait_accepted(s, 1))
            cp = s.stats_checkpoint()
            release.set()
            self.assertIsNone(handle.result(2.0))

            interval = s.snapshot_since(cp)
            self.assertEqual(interval.accepted, 0)
            self.assertEqual(interval.completed, 1)
            self.assertEqual(interval.failed, 0)
            self.assertEqual(len(_samples(interval, "wait")), 1)
            self.assertEqual(len(_samples(interval, "total")), 1)
            self.assertEqual(len(_samples(interval, "execution")), 1)
        # 区间快照在调度器关闭后仍可复算，值不变。
        self.assertEqual(interval.completed, 1)

    def test_cross_boundary_failure_counts_failed_after(self) -> None:
        release = threading.Event()

        def boom() -> None:
            release.wait(2.0)
            raise ValueError("late")

        with Scheduler(workers=1, max_pending=2) as s:
            s.submit_nowait("span", boom)
            self.assertTrue(_wait_accepted(s, 1))
            cp = s.stats_checkpoint()
            release.set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().failed == 1:
                    break
                time.sleep(0.005)
            interval = s.snapshot_since(cp)
            self.assertEqual(interval.accepted, 0)
            self.assertEqual(interval.failed, 1)
            self.assertEqual(interval.completed, 0)
            self.assertEqual(len(_samples(interval, "total")), 1)
            # 失败任务的执行样本同样按结束时刻落入边界后区间。
            self.assertEqual(len(_samples(interval, "execution")), 1)

    def test_two_boundaries_split_accept_and_finish(self) -> None:
        # 接纳落在第一区间，结束落在第二区间：跨区间任务在接纳区间计
        # accepted，在结束区间计 completed。
        release = threading.Event()

        def wait_then_done() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            cp0 = s.stats_checkpoint()
            handle = s.submit_nowait("span", wait_then_done)
            self.assertTrue(_wait_accepted(s, 1))
            cp1 = s.stats_checkpoint()
            # 任务尚未结束时取第一区间快照：只有 accepted、没有完成样本。
            first = s.snapshot_since(cp0)
            self.assertEqual((first.accepted, first.completed), (1, 0))
            self.assertEqual(len(_samples(first, "total")), 0)
            release.set()
            handle.result(2.0)

            # 早先取得的快照对象固定不变；重新查询同一边界得到增长后的
            # 区间（边界->此刻），结束事件落在 cp1 之后。
            self.assertEqual((first.accepted, first.completed), (1, 0))
            first_grown = s.snapshot_since(cp0)
            second = s.snapshot_since(cp1)
            self.assertEqual(
                (first_grown.accepted, first_grown.completed), (1, 1)
            )
            self.assertEqual(
                (second.accepted, second.completed), (0, 1)
            )
            self.assertEqual(len(_samples(first, "total")), 0)
            self.assertEqual(len(_samples(second, "total")), 1)
            # 区间代数：(cp0, cp1] 与 (cp1, 此刻] 不重不漏覆盖 (cp0, 此刻]。
            self.assertEqual(
                first.accepted + second.accepted, first_grown.accepted
            )
            self.assertEqual(
                first.completed + second.completed, first_grown.completed
            )

    def test_cancel_after_boundary_counts_in_interval(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            running = s.submit_nowait("run", lambda: release.wait(2.0))
            self.assertTrue(_wait_accepted(s, 1))
            queued = s.submit_nowait("q", lambda: None)
            # 等 queued 进入入站堆但尚未被认领。
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().accepted == 2:
                    break
                time.sleep(0.005)
            cp = s.stats_checkpoint()
            self.assertTrue(queued.cancel())

            interval = s.snapshot_since(cp)
            self.assertEqual(interval.accepted, 0)
            self.assertEqual(interval.cancelled, 1)
            self.assertEqual(len(_samples(interval, "wait")), 0)
            # 认领前取消不贡献执行样本。
            self.assertEqual(len(_samples(interval, "execution")), 0)
            release.set()
            running.result(2.0)

    def test_expired_after_boundary_counts_in_interval(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            running = s.submit_nowait("run", lambda: release.wait(2.0))
            self.assertTrue(_wait_accepted(s, 1))
            s.submit_nowait("q", lambda: None, max_queue_wait_ms=30)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().accepted == 2:
                    break
                time.sleep(0.005)
            cp = s.stats_checkpoint()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot_since(cp).expired == 1:
                    break
                time.sleep(0.005)
            interval = s.snapshot_since(cp)
            self.assertEqual(interval.accepted, 0)
            self.assertEqual(interval.expired, 1)
            self.assertEqual(len(_samples(interval, "wait")), 0)
            # 认领前到期不贡献执行样本。
            self.assertEqual(len(_samples(interval, "execution")), 0)
            release.set()
            running.result(2.0)

    def test_rejected_after_boundary_counts_in_interval(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            handle = s.submit_nowait("only", lambda: release.wait(2.0))
            self.assertTrue(_wait_accepted(s, 1))
            cp = s.stats_checkpoint()
            with self.assertRaises(BackpressureError):
                s.submit("overflow", lambda: None)
            interval = s.snapshot_since(cp)
            self.assertEqual(interval.rejected, 1)
            self.assertEqual(interval.accepted, 0)
            release.set()
            handle.result(2.0)

    def test_repeated_query_is_stable(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            cp = s.stats_checkpoint()
            s.submit("a", lambda: None)
            first = s.snapshot_since(cp)
            s.submit("b", lambda: None)
            second = s.snapshot_since(cp)
            third = s.snapshot_since(cp)
            # 同一 checkpoint 反复查询：早先结果固定，后续查询也不改变累计。
            self.assertEqual((first.accepted, first.completed), (1, 1))
            self.assertEqual((second.accepted, second.completed), (2, 2))
            self.assertEqual((third.accepted, third.completed), (2, 2))
            self.assertEqual(s.snapshot().accepted, 2)

    def test_checkpoint_and_query_after_close(self) -> None:
        s = Scheduler(workers=2, max_pending=4)
        early_cp = s.stats_checkpoint()
        self.assertEqual(s.submit("a", lambda: 1), 1)
        s.close()

        # 关闭后仍可创建边界、查询历史统计。
        late_cp = s.stats_checkpoint()
        self.assertEqual(s.snapshot_since(early_cp).accepted, 1)
        self.assertEqual(s.snapshot_since(early_cp).completed, 1)
        tail = s.snapshot_since(late_cp)
        self.assertEqual(tail.accepted, 0)
        self.assertEqual(tail.completed, 0)
        self.assertEqual(tail.queue_wait_ms, _EMPTY_DIST)
        self.assertEqual(tail.execution_ms, _EMPTY_DIST)
        # 已结束任务结果在关闭后照常读取。
        self.assertEqual(s.result("a"), 1)

    def test_invalid_checkpoint_raises_input_validation(self) -> None:
        from edge_sched import StatsCheckpoint, StatsSnapshot
        from edge_sched.stats import Stats

        with Scheduler(workers=1, max_pending=2) as s:
            s.submit("a", lambda: None)
            for bad in (None, 1, "cp", object(), (), [],
                        StatsSnapshot(
                            accepted=0, completed=0, failed=0,
                            cancelled=0, expired=0, rejected=0,
                            admission_wait_samples=[],
                            queue_wait_samples=[],
                            total_latency_samples=[],
                            execution_samples=[],
                        )):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.snapshot_since(bad)  # type: ignore[arg-type]

            other = Scheduler(workers=1, max_pending=2)
            try:
                foreign = other.stats_checkpoint()
                with self.assertRaises(InputValidationError):
                    s.snapshot_since(foreign)
                # 底层 Stats 直接构造的边界同样不属于本调度器。
                stranger = Stats().checkpoint()
                with self.assertRaises(InputValidationError):
                    s.snapshot_since(stranger)
            finally:
                other.close()

            # 损坏对象：缺字段、字段类型被篡改。
            broken = StatsCheckpoint.__new__(StatsCheckpoint)
            with self.assertRaises(InputValidationError):
                s.snapshot_since(broken)
            bad_owner = StatsCheckpoint.__new__(StatsCheckpoint)
            object.__setattr__(bad_owner, "_checkpoint_id", 0)
            object.__setattr__(bad_owner, "_owner", object())
            with self.assertRaises(InputValidationError):
                s.snapshot_since(bad_owner)
            bad_id = StatsCheckpoint.__new__(StatsCheckpoint)
            object.__setattr__(bad_id, "_owner", s._stats)
            object.__setattr__(bad_id, "_checkpoint_id", "0")
            with self.assertRaises(InputValidationError):
                s.snapshot_since(bad_id)

            # 全部拒绝都不改变计数或任务状态。
            total = s.snapshot()
            self.assertEqual(total.accepted, 1)
            self.assertEqual(total.completed, 1)
            self.assertEqual(s.result("a"), None)

    def test_checkpoint_is_exported_and_immutable(self) -> None:
        from edge_sched import StatsCheckpoint as PublicCheckpoint

        with Scheduler(workers=1, max_pending=2) as s:
            cp = s.stats_checkpoint()
            self.assertIsInstance(cp, PublicCheckpoint)
            with self.assertRaises(AttributeError):
                cp._checkpoint_id = 99  # type: ignore[misc]
            with self.assertRaises(AttributeError):
                cp.new_field = 1  # type: ignore[misc]

    def test_concurrent_checkpoints_partition_without_gap_or_dup(self) -> None:
        workers = 4
        per_thread = 120
        submitters = 6
        with Scheduler(workers=workers,
                       max_pending=submitters * per_thread + 8) as s:
            cp0 = s.stats_checkpoint()
            stop = threading.Event()
            checkpoints: list = []

            def make_checkpoints() -> None:
                while not stop.is_set():
                    checkpoints.append(s.stats_checkpoint())
                    time.sleep(0.0001)

            cp_threads = [
                threading.Thread(target=make_checkpoints) for _ in range(3)
            ]
            for t in cp_threads:
                t.start()

            def submit_batch(seed: int) -> None:
                for i in range(per_thread):
                    idx = seed * per_thread + i

                    def fn(idx: int = idx) -> int:
                        if idx % 37 == 0:
                            raise RuntimeError("boom-%d" % idx)
                        return idx

                    # 失败任务由 submit 原样抛回调用方：吞掉预期的任务异常，
                    # 任务本身仍计入 accepted/failed，提交线程继续跑完批次。
                    try:
                        s.submit("t%d" % idx, fn)
                    except RuntimeError as exc:
                        self.assertTrue(str(exc).startswith("boom-"))

            submit_threads = [
                threading.Thread(target=submit_batch, args=(k,))
                for k in range(submitters)
            ]
            for t in submit_threads:
                t.start()
            for t in submit_threads:
                t.join()
            # submit 阻塞到任务结束：此刻全部任务已落终态。先停掉边界线程
            # 并汇合，再创建最后一个边界，保证 ordered 严格按创建时刻递增。
            stop.set()
            for t in cp_threads:
                t.join()
            cp_last = s.stats_checkpoint()

            total_tasks = submitters * per_thread
            total = s.snapshot()
            self.assertEqual(total.accepted, total_tasks)
            self.assertEqual(
                total.completed + total.failed, total_tasks
            )

            # 所有结束样本数与 completed+failed 一致。
            self.assertEqual(
                len(_samples(total, "total")), total_tasks
            )

            # 区间代数：记 c0 < c1 < ... < ck，任务全部结束后
            # snapshot_since(c_i) = 总量 - 边界 i 基值；相邻两次查询之差
            # 即区间 (c_i, c_{i+1}] 的事件数。并发创建下每个区间的计数必
            # 非负，且各区间 + 尾区间不重不漏地覆盖 c0 之后全部事件。
            # 追加 list 的线程调度顺序未必等于边界创建顺序，按边界 id 排序。
            ordered = sorted(
                [cp0] + checkpoints + [cp_last],
                key=lambda cp: cp._checkpoint_id,
            )
            seen_since_cp0 = s.snapshot_since(cp0)

            def counts(snap: object) -> tuple:
                return (
                    snap.accepted, snap.completed, snap.failed,
                    snap.cancelled, snap.expired, snap.rejected,
                )

            # 去重并发产生的“同值不同对象”边界无需特殊处理：相邻边界基值
            # 相同的空区间各项差值为 0，仍满足非负与求和恒等式。
            deltas = [0, 0, 0, 0, 0, 0]
            wait_delta_total = 0
            total_delta_total = 0
            prev_counts = counts(seen_since_cp0)
            prev_wait = len(_samples(seen_since_cp0, "wait"))
            prev_total = len(_samples(seen_since_cp0, "total"))
            for cp in ordered[1:]:
                cur = s.snapshot_since(cp)
                cur_counts = counts(cur)
                for j, (a, b) in enumerate(zip(prev_counts, cur_counts)):
                    delta = a - b
                    self.assertGreaterEqual(delta, 0)
                    deltas[j] += delta
                # 每跨过一个边界，尾部样本只可能变短，缩短量即区间样本数；
                # 两类样本同样满足相邻区间不重不漏。
                cur_wait = len(_samples(cur, "wait"))
                cur_total = len(_samples(cur, "total"))
                self.assertLessEqual(cur_wait, prev_wait)
                self.assertLessEqual(cur_total, prev_total)
                wait_delta_total += prev_wait - cur_wait
                total_delta_total += prev_total - cur_total
                prev_counts = cur_counts
                prev_wait = cur_wait
                prev_total = cur_total

            # 尾区间 = 最后一个边界之后（任务此刻已全部结束，应为 0）。
            tail = s.snapshot_since(cp_last)
            self.assertEqual(counts(tail), (0, 0, 0, 0, 0, 0))

            # 各区间之和恰好等于 c0 之后的全部事件，不重不漏。
            self.assertEqual(tuple(deltas), counts(seen_since_cp0))
            # 两类延迟样本各区间之和也恰好覆盖 c0 之后的全部结束样本。
            self.assertEqual(
                wait_delta_total, len(_samples(seen_since_cp0, "wait"))
            )
            self.assertEqual(
                total_delta_total, len(_samples(seen_since_cp0, "total"))
            )
            # 区间结束样本数之和等于区间 completed+failed。
            self.assertEqual(
                deltas[1] + deltas[2],
                seen_since_cp0.completed + seen_since_cp0.failed,
            )


class SubmitNowaitTest(unittest.TestCase):
    def test_returns_handle_immediately(self) -> None:
        release = threading.Event()

        def slow() -> str:
            release.wait(2.0)
            return "v"

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("h", slow)
            self.assertIsInstance(handle, TaskHandle)
            # 任务被阻塞在执行中，句柄此刻未结束。
            self.assertFalse(handle.done())
            release.set()
            self.assertEqual(handle.result(), "v")
            self.assertTrue(handle.done())

    def test_handle_result_success_and_failure(self) -> None:
        def boom() -> None:
            raise ValueError("bad")

        with Scheduler(workers=2, max_pending=4) as s:
            ok = s.submit_nowait("ok", lambda: 42)
            bad = s.submit_nowait("bad", boom)
            self.assertEqual(ok.result(), 42)
            with self.assertRaises(ValueError) as cm:
                bad.result()
            self.assertEqual(str(cm.exception), "bad")
            # 结果唯一且可重复读取。
            self.assertEqual(ok.result(), 42)

    def test_handle_result_timeout_task_continues(self) -> None:
        release = threading.Event()

        def slow() -> int:
            release.wait(2.0)
            return 7

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("slow", slow)
            with self.assertRaises(TimeoutError):
                handle.result(timeout=0.05)
            self.assertFalse(handle.done())
            # 超时后任务继续，句柄之后仍能取得唯一结果。
            release.set()
            self.assertEqual(handle.result(timeout=2.0), 7)
            self.assertTrue(handle.done())

    def test_handle_result_bad_timeout(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("t", lambda: 1)
            with self.assertRaises(InputValidationError):
                handle.result(timeout=-1)
            self.assertEqual(handle.result(), 1)

    def test_validation_errors(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad_id in ("", 1, None, b"x"):
                with self.subTest(bad_id=bad_id):
                    with self.assertRaises(InputValidationError):
                        s.submit_nowait(bad_id, lambda: None)  # type: ignore[arg-type]
            with self.assertRaises(InputValidationError):
                s.submit_nowait("t", "not-callable")  # type: ignore[arg-type]
        # 校验失败不改变统计。
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.failed, snap.rejected),
            (0, 0, 0, 0),
        )

    def test_submit_nowait_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.submit_nowait("late", lambda: None)
        self.assertEqual(s.snapshot().accepted, 0)
        self.assertEqual(s.snapshot().rejected, 0)

    def test_duplicate_while_unfinished(self) -> None:
        release = threading.Event()

        def slow() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=4) as s:
            handle = s.submit_nowait("d", slow)
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("d", lambda: None)
            release.set()
            self.assertIsNone(handle.result())

    def test_backpressure_rejects_without_side_effects(self) -> None:
        release = threading.Event()

        def slow() -> str:
            release.wait(2.0)
            return "x"

        with Scheduler(workers=1, max_pending=1) as s:
            handle = s.submit_nowait("only", slow)
            self.assertTrue(_wait_accepted(s, 1))
            with self.assertRaises(BackpressureError):
                s.submit_nowait("extra", lambda: None)
            # 计入 rejected 而不计 accepted，且不留任务痕迹。
            self.assertEqual(s.snapshot().accepted, 1)
            self.assertEqual(s.snapshot().rejected, 1)
            with self.assertRaises(KeyError):
                s.result("extra")
            release.set()
            # 已有任务不受影响。
            self.assertEqual(handle.result(), "x")
            # 被拒的 task_id 之后仍可正常使用。
            self.assertEqual(s.submit_nowait("extra", lambda: 3).result(), 3)

    def test_accepted_counted_before_return(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("a", lambda: release.wait(2.0))
            # submit_nowait 返回时 accepted 已更新（任务可能尚未执行）。
            self.assertEqual(s.snapshot().accepted, 1)
            release.set()
            handle.result()

    def test_stats_visible_when_result_returns(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("s", lambda: "v")
            self.assertEqual(handle.result(), "v")
            # result 返回时完成计数与延迟样本已入快照。
            snap = s.snapshot()
            self.assertEqual(snap.completed, 1)
            self.assertEqual(len(_samples(snap, "total")), 1)
            self.assertEqual(len(_samples(snap, "wait")), 1)

    def test_handle_and_result_by_task_id_coexist(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("mix", lambda: 5)
            self.assertEqual(handle.result(), 5)
            self.assertEqual(s.result("mix"), 5)

    def test_handle_survives_task_id_reuse(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            first = s.submit_nowait("r", lambda: "old")
            self.assertEqual(first.result(), "old")
            second = s.submit_nowait("r", lambda: "new")
            self.assertEqual(second.result(), "new")
            # task_id 复用后，先前句柄仍读取其对应的结果。
            self.assertEqual(first.result(), "old")
            self.assertEqual(s.result("r"), "new")

    def test_handle_result_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        handle = s.submit_nowait("c", lambda: "done")
        s.close()
        self.assertTrue(handle.done())
        self.assertEqual(handle.result(), "done")


class CancelTest(unittest.TestCase):
    def test_cancel_queued_task_never_runs(self) -> None:
        release = threading.Event()
        ran: list[str] = []
        lock = threading.Lock()

        def record(tid: str) -> "any":
            def fn() -> str:
                with lock:
                    ran.append(tid)
                return tid
            return fn

        # 单工作线程：a 占住线程，b 只能在入站队列中等待，此时取消必然先于执行。
        with Scheduler(workers=1, max_pending=4) as s:
            ha = s.submit_nowait("a", lambda: release.wait(2.0))
            hb = s.submit_nowait("b", record("b"))
            self.assertTrue(_wait_started(s, "a"))
            self.assertTrue(hb.cancel())
            self.assertTrue(hb.done())
            # callable 完全不执行。
            with self.assertRaises(TaskCancelledError):
                hb.result()
            # 重复取消返回 False，不改变终态，也不重复计数。
            self.assertFalse(hb.cancel())
            with self.assertRaises(TaskCancelledError):
                hb.result()

            release.set()
            self.assertTrue(ha.result())

        self.assertEqual(ran, [])
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.cancelled, 1)
        # 取消任务不贡献延迟样本。
        self.assertEqual(len(_samples(snap, "wait")), 1)
        self.assertEqual(len(_samples(snap, "total")), 1)

    def test_scheduler_result_raises_cancelled(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("c", lambda: "no")
            self.assertTrue(handle.cancel())
            with self.assertRaises(TaskCancelledError):
                s.result("c")
        # 关闭后历史取消结果仍可读取，且类型体系正确。
        with self.assertRaises(TaskCancelledError) as cm:
            s.result("c")
        self.assertIsInstance(cm.exception, EdgeSchedError)

    def test_cancel_after_start_returns_false(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def slow() -> str:
            entered.set()
            release.wait(2.0)
            return "done"

        with Scheduler(workers=1, max_pending=2) as s:
            handle = s.submit_nowait("a", slow)
            self.assertTrue(entered.wait(2.0))
            # 已开始执行：取消失败，任务运行到底，原终态保留。
            self.assertFalse(handle.cancel())
            self.assertFalse(handle.done())
            release.set()
            self.assertEqual(handle.result(), "done")
            self.assertFalse(handle.cancel())
        self.assertEqual(s.snapshot().cancelled, 0)
        self.assertEqual(s.snapshot().completed, 1)

    def test_cancel_after_finish_keeps_original_terminal_state(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            ok = s.submit_nowait("ok", lambda: 42)
            self.assertEqual(ok.result(), 42)
            self.assertFalse(ok.cancel())
            self.assertEqual(ok.result(), 42)

            def boom() -> None:
                raise ValueError("x")

            bad = s.submit_nowait("bad", boom)
            with self.assertRaises(ValueError):
                bad.result()
            self.assertFalse(bad.cancel())
            with self.assertRaises(ValueError):
                bad.result()

    def test_cancel_frees_pending_capacity(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            queued = s.submit_nowait("b", lambda: "b")
            self.assertTrue(_wait_pending(s, 2))
            # 上限已满，新提交被背压拒绝。
            with self.assertRaises(BackpressureError):
                s.submit_nowait("c", lambda: None)
            # 取消排队中的 b：额度立即释放，c 得以接受。
            self.assertTrue(queued.cancel())
            hc = s.submit_nowait("c", lambda: "c")
            release.set()
            self.assertEqual(hc.result(), "c")
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled, snap.completed),
                         (3, 1, 2))

    def test_task_id_reusable_immediately_after_cancel(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            old = s.submit_nowait("r", lambda: "old")
            self.assertTrue(old.cancel())

            # 同名任务立即可以再提交，不报 DuplicateTaskError。
            new = s.submit_nowait("r", lambda: "new")
            release.set()
            self.assertEqual(new.result(), "new")

            # 旧句柄固定读取自己的取消结果；新句柄与 Scheduler.result
            # 读取最新同名任务。
            with self.assertRaises(TaskCancelledError):
                old.result()
            self.assertEqual(new.result(), "new")
            self.assertEqual(s.result("r"), "new")

    def test_cancel_does_not_block_close(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=4)
        s.submit_nowait("a", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "a"))
        queued = s.submit_nowait("b", lambda: "b")

        closed = threading.Event()

        def do_close() -> None:
            s.close()
            closed.set()

        t = threading.Thread(target=do_close)
        t.start()
        # a 仍在执行，close 在等待；取消排队中的 b 不造成异常。
        self.assertTrue(queued.cancel())
        self.assertFalse(closed.wait(0.2))
        release.set()
        self.assertTrue(closed.wait(2.0))
        t.join()
        # 关闭后取消结果仍可读取。
        with self.assertRaises(TaskCancelledError):
            queued.result()

    def test_cancel_queued_behind_other_queued_task(self) -> None:
        release = threading.Event()
        ran: list[str] = []
        lock = threading.Lock()

        def rec(tid: str) -> "any":
            def fn() -> None:
                with lock:
                    ran.append(tid)
            return fn

        with Scheduler(workers=1, max_pending=8) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            hb = s.submit_nowait("b", rec("b"))
            hc = s.submit_nowait("c", rec("c"))
            # 取消排在 b 后面的 c：FCFS 不受影响，c 的 callable 不执行。
            self.assertTrue(hc.cancel())
            release.set()
            self.assertEqual(hb.result(), None)
            with self.assertRaises(TaskCancelledError):
                hc.result()
        self.assertEqual(ran, ["b"])

    def test_cancel_vs_start_race_has_single_outcome(self) -> None:
        # 大量任务在“即将开始”的窗口与 cancel 竞争：每个任务必须恰好
        # 落入“执行完成”或“被取消”之一，不得部分执行后取消或重复计数。
        n = 200
        s = Scheduler(workers=4, max_pending=n + 1)
        ran: set[str] = set()
        ran_lock = threading.Lock()
        handles: list[TaskHandle] = []

        def make(tid: str) -> "any":
            def fn() -> str:
                # 极短等待，扩大 cancel 与认领交错的窗口。
                time.sleep(0.001)
                with ran_lock:
                    ran.add(tid)
                return tid
            return fn

        for i in range(n):
            handles.append(s.submit_nowait("t%d" % i, make("t%d" % i)))

        def race_cancel(h: TaskHandle) -> None:
            h.cancel()

        cancelers = [threading.Thread(target=race_cancel, args=(h,))
                     for h in handles]
        for t in cancelers:
            t.start()
        for t in cancelers:
            t.join()
        s.close()

        outcomes: dict[str, str] = {}
        for h in handles:
            tid = h._entry.task_id  # type: ignore[attr-defined]
            try:
                h.result()
                outcomes[tid] = "ran"
            except TaskCancelledError:
                outcomes[tid] = "cancelled"

        # 每个任务唯一终态；ran 集合与终态完全一致（无“执行又取消”）。
        self.assertEqual(len(outcomes), n)
        for tid, outcome in outcomes.items():
            if outcome == "ran":
                self.assertIn(tid, ran)
            else:
                self.assertNotIn(tid, ran)
        self.assertEqual(len(ran), sum(o == "ran" for o in outcomes.values()))

        snap = s.snapshot()
        self.assertEqual(snap.accepted, n)
        self.assertEqual(
            snap.completed + snap.failed + snap.cancelled, n
        )
        finished = snap.completed + snap.failed
        self.assertEqual(len(_samples(snap, "wait")), finished)
        self.assertEqual(len(_samples(snap, "total")), finished)


class QueueTimeoutTest(unittest.TestCase):
    def test_invalid_max_queue_wait_ms(self) -> None:
        # bool/0/负数/浮点/字符串均非法；只有 None 或 >= 1 的整数合法。
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in (True, False, 0, -1, 1.5, "100", 0.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit("t", lambda: None,
                                 max_queue_wait_ms=bad)  # type: ignore[arg-type]
                    with self.assertRaises(InputValidationError):
                        s.submit_nowait(
                            "u", lambda: None,
                            max_queue_wait_ms=bad,  # type: ignore[arg-type]
                        )
        # 校验失败不留统计痕迹。
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.expired, snap.rejected),
            (0, 0, 0, 0),
        )

    def test_none_and_positive_int_accepted(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            self.assertEqual(
                s.submit("a", lambda: 1, max_queue_wait_ms=None), 1
            )
            self.assertEqual(
                s.submit("b", lambda: 2, max_queue_wait_ms=60_000), 2
            )
            self.assertEqual(
                s.submit_nowait(
                    "c", lambda: 3, max_queue_wait_ms=60_000
                ).result(),
                3,
            )

    def test_max_queue_wait_validated_before_close_check(self) -> None:
        s = Scheduler(workers=1, max_pending=1)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit("t", lambda: None, max_queue_wait_ms=0)
        with self.assertRaises(SchedulerClosedError):
            s.submit("t", lambda: None, max_queue_wait_ms=1)

    def test_queued_task_expires_without_running(self) -> None:
        release = threading.Event()
        ran: list[str] = []

        with Scheduler(workers=1, max_pending=4) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            # 唯一工作线程被占住，排队任务必在 30ms 后到期；
            # 无后续提交，事件循环仍须及时完成到期。
            with self.assertRaises(QueueTimeoutError) as cm:
                s.submit("x", lambda: ran.append("x"),
                         max_queue_wait_ms=30)
            self.assertIsInstance(cm.exception, EdgeSchedError)
            release.set()
            self.assertTrue(holder.result())

        self.assertEqual(ran, [])  # callable 完全不执行
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.expired, 1)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.cancelled, 0)
        # 到期任务不贡献延迟样本。
        self.assertEqual(len(_samples(snap, "wait")), 1)
        self.assertEqual(len(_samples(snap, "total")), 1)

    def test_expired_result_via_handle_and_scheduler(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("x", lambda: "no", max_queue_wait_ms=20)
            with self.assertRaises(QueueTimeoutError):
                h.result()
            self.assertTrue(h.done())
            with self.assertRaises(QueueTimeoutError):
                s.result("x")
            release.set()
            self.assertTrue(holder.result())
        # 关闭后到期终态仍可读取，且类型体系正确。
        with self.assertRaises(QueueTimeoutError) as cm:
            h.result()
        self.assertIsInstance(cm.exception, EdgeSchedError)
        with self.assertRaises(QueueTimeoutError):
            s.result("x")

    def test_cancel_after_expiry_returns_false(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("x", lambda: "x", max_queue_wait_ms=20)
            with self.assertRaises(QueueTimeoutError):
                h.result()
            # 到期后取消失败，终态不变。
            self.assertFalse(h.cancel())
            with self.assertRaises(QueueTimeoutError):
                h.result()
            release.set()
            self.assertTrue(holder.result())
        snap = s.snapshot()
        self.assertEqual(snap.cancelled, 0)
        self.assertEqual(snap.expired, 1)

    def test_cancel_before_expiry_still_cancelled(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("x", lambda: "x", max_queue_wait_ms=60_000)
            # 未到期时手工取消：终态为取消而非到期。
            self.assertTrue(h.cancel())
            with self.assertRaises(TaskCancelledError):
                h.result()
            release.set()
            self.assertTrue(holder.result())
        snap = s.snapshot()
        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.expired, 0)

    def test_expiry_frees_pending_and_task_id(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            holder = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            old = s.submit_nowait("r", lambda: "old", max_queue_wait_ms=20)
            with self.assertRaises(QueueTimeoutError):
                old.result()
            # 到期立即释放 task_id 与 pending 额度：同名可再提交，
            # 且上限按释放后的占用计算。
            new = s.submit_nowait("r", lambda: "new",
                                  max_queue_wait_ms=60_000)
            hz = s.submit_nowait("z", lambda: "z",
                                 max_queue_wait_ms=60_000)
            with self.assertRaises(BackpressureError):
                s.submit_nowait("w", lambda: None)
            release.set()
            self.assertEqual(new.result(), "new")
            self.assertEqual(hz.result(), "z")
        # 旧句柄仍读自己的到期结果；Scheduler.result 读最新同名任务。
        with self.assertRaises(QueueTimeoutError):
            old.result()
        self.assertEqual(s.result("r"), "new")

    def test_claimed_task_runs_to_completion(self) -> None:
        # 认领先于到期：执行中的任务不受排队时限中断。
        def slow() -> str:
            time.sleep(0.1)
            return "done"

        with Scheduler(workers=1, max_pending=2) as s:
            self.assertEqual(
                s.submit("s", slow, max_queue_wait_ms=50), "done"
            )
        snap = s.snapshot()
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.expired, 0)

    def test_close_not_blocked_by_expiring_tasks(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=8)
        holder = s.submit_nowait("hold", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "hold"))
        handles = [
            s.submit_nowait("x%d" % i, lambda: None, max_queue_wait_ms=20)
            for i in range(4)
        ]
        # 无后续提交，事件循环仍须及时把排队任务置入到期终态。
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if s.snapshot().expired == 4:
                break
            time.sleep(0.005)
        self.assertEqual(s.snapshot().expired, 4)
        release.set()
        start = time.monotonic()
        s.close()  # 到期落定后 close 必须及时返回
        self.assertLess(time.monotonic() - start, 2.0)
        self.assertTrue(holder.result())
        for h in handles:
            with self.assertRaises(QueueTimeoutError):
                h.result()
        self.assertEqual(s.snapshot().expired, 4)

    def test_cancel_expire_claim_race_single_outcome(self) -> None:
        # 极短时限下认领、到期、取消三者充分交错：每个任务必须恰好落入
        # “执行完成 / 被取消 / 已到期”之一，不得重复计数或部分执行。
        n = 200
        s = Scheduler(workers=4, max_pending=n + 1)
        ran: set[str] = set()
        ran_lock = threading.Lock()
        handles: list[TaskHandle] = []

        def make(tid: str) -> "any":
            def fn() -> str:
                time.sleep(0.001)
                with ran_lock:
                    ran.add(tid)
                return tid
            return fn

        for i in range(n):
            handles.append(s.submit_nowait(
                "t%d" % i, make("t%d" % i), max_queue_wait_ms=1
            ))

        cancelers = [threading.Thread(target=h.cancel) for h in handles]
        for t in cancelers:
            t.start()
        for t in cancelers:
            t.join()
        s.close()

        outcomes: dict[str, str] = {}
        for h in handles:
            tid = h._entry.task_id  # type: ignore[attr-defined]
            try:
                h.result()
                outcomes[tid] = "ran"
            except TaskCancelledError:
                outcomes[tid] = "cancelled"
            except QueueTimeoutError:
                outcomes[tid] = "expired"

        self.assertEqual(len(outcomes), n)
        for tid, outcome in outcomes.items():
            if outcome == "ran":
                self.assertIn(tid, ran)
            else:
                self.assertNotIn(tid, ran)
        self.assertEqual(len(ran), sum(o == "ran" for o in outcomes.values()))

        snap = s.snapshot()
        self.assertEqual(snap.accepted, n)
        self.assertEqual(
            snap.completed + snap.failed + snap.cancelled + snap.expired, n
        )
        finished = snap.completed + snap.failed
        self.assertEqual(len(_samples(snap, "wait")), finished)
        self.assertEqual(len(_samples(snap, "total")), finished)


def _wait_started(s: Scheduler, task_id: str) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        entry = s._tasks.get(task_id)  # type: ignore[attr-defined]
        if entry is not None and entry.started:
            return True
        time.sleep(0.002)
    return False


def _wait_pending(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with s._cond:  # type: ignore[attr-defined]
            if s._pending == n:  # type: ignore[attr-defined]
                return True
        time.sleep(0.002)
    return False


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
    attr = {
        "wait": "_queue_wait_samples",
        "total": "_total_latency_samples",
        "execution": "_execution_samples",
    }[kind]
    return getattr(snap, attr)


if __name__ == "__main__":
    unittest.main()
