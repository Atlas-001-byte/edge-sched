"""Scheduler.rolling_snapshot 滑动延迟窗口的验收测试。

覆盖：未启用时的空窗口形态与类型、构造参数校验（bool/0/负/浮点等抛
InputValidationError 且不创建调度器）、窗口未满/已满的 sampled_finished、
按任务结束顺序保留最近 N 个三元样本、失败任务入窗、取消/到期/拒绝/close
期间未接纳任务不入窗、与累计和区间统计同序且互不影响、快照不可变与
to_dict JSON 形态、重复读取稳定、close 后仍可读。
"""

import json
import threading
import time
import unittest

from edge_sched import (
    InputValidationError,
    RollingStatsSnapshot,
    Scheduler,
)

_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
_FIELDS = {
    "window_size", "sampled_finished",
    "queue_wait_ms", "total_latency_ms", "execution_ms",
}


def _wait_completed(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        snap = s.snapshot()
        if snap.completed + snap.failed == n:
            return True
        time.sleep(0.005)
    return False


class DisabledWindowTest(unittest.TestCase):
    def test_default_disabled_shape(self) -> None:
        with Scheduler(workers=2, max_pending=3) as s:
            snap = s.rolling_snapshot()
            self.assertIsInstance(snap, RollingStatsSnapshot)
            self.assertEqual(snap.window_size, 0)
            self.assertEqual(snap.sampled_finished, 0)
            self.assertEqual(snap.queue_wait_ms, _EMPTY_DIST)
            self.assertEqual(snap.total_latency_ms, _EMPTY_DIST)
            self.assertEqual(snap.execution_ms, _EMPTY_DIST)
            self.assertEqual(set(snap.to_dict()), _FIELDS)

    def test_disabled_stays_empty_after_finishes(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            for i in range(5):
                self.assertEqual(s.submit("t%d" % i, lambda i=i: i), i)
            snap = s.rolling_snapshot()
            self.assertEqual(snap.window_size, 0)
            self.assertEqual(snap.sampled_finished, 0)
            self.assertEqual(snap.execution_ms, _EMPTY_DIST)


class ValidationTest(unittest.TestCase):
    def test_invalid_values_raise_and_create_no_scheduler(self) -> None:
        for bad in (True, False, 0, -1, -10, 1.5, 2.0, "3", [], None):
            if bad is None:
                continue  # None 合法（关闭）
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(1, 1, latency_window_tasks=bad)  # type: ignore[arg-type]

    def test_none_is_accepted(self) -> None:
        with Scheduler(workers=1, max_pending=1,
                       latency_window_tasks=None) as s:
            self.assertEqual(s.rolling_snapshot().window_size, 0)

    def test_no_threads_started_on_validation_failure(self) -> None:
        before = threading.active_count()
        for bad in (0, -1, True, 1.5):
            with self.assertRaises(InputValidationError):
                Scheduler(1, 1, latency_window_tasks=bad)
        time.sleep(0.05)
        self.assertEqual(threading.active_count(), before)

    def test_invalid_aging_still_rejected_first(self) -> None:
        with self.assertRaises(InputValidationError):
            Scheduler(1, 1, aging_interval_ms=0, latency_window_tasks=0)


class WindowContentTest(unittest.TestCase):
    def test_partial_window_sampled_finished(self) -> None:
        with Scheduler(workers=4, max_pending=8,
                       latency_window_tasks=10) as s:
            for i in range(3):
                self.assertEqual(s.submit("t%d" % i, lambda i=i: i), i)
            snap = s.rolling_snapshot()
            self.assertEqual(snap.window_size, 10)
            self.assertEqual(snap.sampled_finished, 3)

    def test_window_keeps_last_n_in_finish_order(self) -> None:
        # 单线程串行执行，结束顺序即 t0..t5；窗口 3 只保留 t3/t4/t5。
        with Scheduler(workers=1, max_pending=8,
                       latency_window_tasks=3) as s:
            for i in range(6):
                s.submit("t%d" % i, lambda i=i: (time.sleep(0.005), i)[1])
            snap = s.rolling_snapshot()
            self.assertEqual(snap.window_size, 3)
            self.assertEqual(snap.sampled_finished, 3)
            # 三个任务的排队/执行都是正的小值；验证分布仅来自最后 3 个结束。
            self.assertGreaterEqual(snap.execution_ms["max"], 4.0)
            self.assertEqual(snap.queue_wait_ms["p99"],
                             snap.queue_wait_ms["max"])
            # 累计仍有 6 个完成，窗口不影响它。
            self.assertEqual(s.snapshot().completed, 6)

    def test_failed_task_enters_window(self) -> None:
        with Scheduler(workers=1, max_pending=4,
                       latency_window_tasks=5) as s:
            self.assertEqual(s.submit("ok", lambda: 1), 1)

            def boom() -> None:
                raise ValueError("boom")

            with self.assertRaises(ValueError):
                s.submit("bad", boom)
            self.assertTrue(_wait_completed(s, 2))
            snap = s.rolling_snapshot()
            self.assertEqual(snap.sampled_finished, 2)
            total = s.snapshot()
            self.assertEqual(total.completed, 1)
            self.assertEqual(total.failed, 1)

    def test_cancelled_and_expired_excluded(self) -> None:
        with Scheduler(workers=1, max_pending=4,
                       latency_window_tasks=5) as s:
            entered = threading.Event()
            release = threading.Event()
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            cancel_handle = s.submit_nowait("c", lambda: 0)
            self.assertTrue(cancel_handle.cancel())
            s.submit_nowait("e", lambda: 0, max_queue_wait_ms=20)
            # 等待 e 到期后再放行 h。
            deadline = time.monotonic() + 2.0
            while s.snapshot().expired < 1 and time.monotonic() < deadline:
                time.sleep(0.005)
            release.set()
            self.assertTrue(_wait_completed(s, 1))
            snap = s.rolling_snapshot()
            # 仅 h 正常结束入窗；c 被取消、e 到期均不入窗。
            self.assertEqual(snap.sampled_finished, 1, snap.to_dict())
            stats = s.snapshot()
            self.assertEqual(stats.cancelled, 1)
            self.assertEqual(stats.expired, 1)
            self.assertEqual(stats.completed, 1)

    def test_rejected_submit_excluded(self) -> None:
        with Scheduler(workers=1, max_pending=1,
                       latency_window_tasks=5) as s:
            entered = threading.Event()
            release = threading.Event()
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            # 容量满：立即拒绝，不创建任务，不入窗。
            from edge_sched import BackpressureError
            with self.assertRaises(BackpressureError):
                s.submit_nowait("rej", lambda: 0)
            release.set()
            self.assertTrue(_wait_completed(s, 1))
            snap = s.rolling_snapshot()
            self.assertEqual(snap.sampled_finished, 1)
            self.assertEqual(s.snapshot().rejected, 1)

    def test_unadmitted_on_close_excluded(self) -> None:
        from edge_sched import SchedulerClosedError
        s = Scheduler(workers=1, max_pending=1,
                      latency_window_tasks=5)
        entered = threading.Event()
        release = threading.Event()
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        def wait_call() -> None:
            try:
                s.submit_with_wait("w", lambda: 0,
                                   admission_timeout_ms=None)
            except SchedulerClosedError:
                pass

        t = threading.Thread(target=wait_call)
        t.start()
        time.sleep(0.03)
        closing = threading.Thread(target=s.close)
        closing.start()
        release.set()
        closing.join()
        t.join()
        snap = s.rolling_snapshot()
        # close 期间未接纳的等待任务不入窗。
        self.assertEqual(snap.sampled_finished, 1, snap.to_dict())


class SnapshotSemanticsTest(unittest.TestCase):
    def test_to_dict_json_and_immutability(self) -> None:
        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=2) as s:
            s.submit("a", lambda: 1)
            snap = s.rolling_snapshot()
            data = snap.to_dict()
            self.assertEqual(set(data), _FIELDS)
            self.assertEqual(json.loads(json.dumps(data)), data)
            for name in tuple(_FIELDS) + ("unknown",):
                with self.subTest(name=name):
                    with self.assertRaises(AttributeError):
                        setattr(snap, name, 1)
                    with self.assertRaises(AttributeError):
                        delattr(snap, name)
            data["sampled_finished"] = 99
            data["extra"] = 1
            self.assertEqual(s.rolling_snapshot().sampled_finished, 1)

    def test_repeated_reads_stable_and_frozen(self) -> None:
        with Scheduler(workers=1, max_pending=4,
                       latency_window_tasks=2) as s:
            s.submit("a", lambda: (time.sleep(0.005), 1)[1])
            first = s.rolling_snapshot()
            s.submit("b", lambda: (time.sleep(0.005), 2)[1])
            s.submit("c", lambda: (time.sleep(0.005), 3)[1])
            # 早先快照冻结在创建时刻。
            self.assertEqual(first.sampled_finished, 1)
            self.assertTrue(_wait_completed(s, 3))
            later = s.rolling_snapshot()
            self.assertEqual(later.sampled_finished, 2)
            again = s.rolling_snapshot()
            self.assertEqual(later.to_dict(), again.to_dict())

    def test_readable_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2,
                      latency_window_tasks=2)
        s.submit("a", lambda: 1)
        s.close()
        snap = s.rolling_snapshot()
        self.assertEqual(snap.window_size, 2)
        self.assertEqual(snap.sampled_finished, 1)
        self.assertEqual(s.rolling_snapshot().to_dict(), snap.to_dict())

    def test_window_aligned_with_interval_stats(self) -> None:
        # rolling_snapshot 与 snapshot/snapshot_since 同刻边界一致：
        # 窗口与区间都只含已记账结束，互不影响。
        with Scheduler(workers=1, max_pending=8,
                       latency_window_tasks=2) as s:
            s.submit("a", lambda: (time.sleep(0.005), 1)[1])
            cp = s.stats_checkpoint()
            s.submit("b", lambda: (time.sleep(0.005), 2)[1])
            s.submit("c", lambda: (time.sleep(0.005), 3)[1])
            self.assertTrue(_wait_completed(s, 3))
            interval = s.snapshot_since(cp)
            rolling = s.rolling_snapshot()
            self.assertEqual(interval.completed, 2)
            # 窗口容量 2：恰为边界后结束的 b、c。
            self.assertEqual(rolling.sampled_finished, 2)
            self.assertEqual(
                rolling.total_latency_ms["max"],
                interval.total_latency_ms["max"],
            )
            # 累计仍为 3。
            self.assertEqual(s.snapshot().completed, 3)


if __name__ == "__main__":
    unittest.main()
