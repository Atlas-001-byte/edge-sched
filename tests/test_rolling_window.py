"""滑动窗口延迟观测（latency_window_tasks / rolling_snapshot）的验收测试。

覆盖：构造参数校验（None 与 >=1 整数合法，布尔/零/负数/浮点等抛
InputValidationError 且不创建调度器）、未启用时的空窗口、窗口未满与满后
的保留口径、三元样本按结束顺序同步进出、取消/到期/拒绝/close 未接纳任务
不入窗而成功/失败结束入窗、与累计统计同序（sampled_finished 恒等于
min(window_size, completed+failed)）、快照不可变与 to_dict 形态、快照
冻结、重复读取稳定、close 后可读，以及窗口不影响累计与区间统计。
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
from edge_sched.stats import Stats

_FIELDS = (
    "window_size",
    "sampled_finished",
    "queue_wait_ms",
    "total_latency_ms",
    "execution_ms",
)
_DIST_KEYS = {"p50", "p95", "p99", "max"}
_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


def _wait_stats(s: Scheduler, **expected: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        snap = s.snapshot()
        if all(getattr(snap, k) == v for k, v in expected.items()):
            return True
        time.sleep(0.002)
    return False


class ConstructorValidationTest(unittest.TestCase):
    def test_none_and_positive_ints_allowed(self) -> None:
        for value in (None, 1, 2, 100):
            with self.subTest(value=value):
                with Scheduler(
                    workers=1, max_pending=1,
                    latency_window_tasks=value,
                ) as s:
                    snap = s.rolling_snapshot()
                    self.assertEqual(
                        snap.window_size, 0 if value is None else value
                    )

    def test_invalid_values_raise(self) -> None:
        for bad in (True, False, 0, -1, -10, 1.5, 1.0, "2", [], {}):
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(
                        workers=1, max_pending=1,
                        latency_window_tasks=bad,  # type: ignore[arg-type]
                    )

    def test_invalid_value_does_not_start_scheduler(self) -> None:
        # 校验失败时不创建调度器：没有任何 edge-sched 线程残留。
        before = {
            t.name for t in threading.enumerate()
            if t.name.startswith("edge-sched")
        }
        with self.assertRaises(InputValidationError):
            Scheduler(workers=1, max_pending=1, latency_window_tasks=0)
        time.sleep(0.05)
        after = {
            t.name for t in threading.enumerate()
            if t.name.startswith("edge-sched")
        }
        self.assertEqual(before, after)

    def test_invalid_aging_still_rejected_independently(self) -> None:
        with self.assertRaises(InputValidationError):
            Scheduler(
                workers=1, max_pending=1,
                aging_interval_ms=0, latency_window_tasks=2,
            )
        with self.assertRaises(InputValidationError):
            Scheduler(
                workers=1, max_pending=1,
                aging_interval_ms=10, latency_window_tasks=True,
            )


class DisabledWindowTest(unittest.TestCase):
    def test_disabled_snapshot_is_empty(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            for i in range(3):
                self.assertEqual(s.submit("t%d" % i, lambda i=i: i), i)
            snap = s.rolling_snapshot()
            self.assertIsInstance(snap, RollingStatsSnapshot)
            self.assertEqual(snap.window_size, 0)
            self.assertEqual(snap.sampled_finished, 0)
            self.assertEqual(snap.queue_wait_ms, _EMPTY_DIST)
            self.assertEqual(snap.total_latency_ms, _EMPTY_DIST)
            self.assertEqual(snap.execution_ms, _EMPTY_DIST)

    def test_disabled_snapshot_stable_and_readable_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        self.assertEqual(s.submit("t", lambda: 1), 1)
        s.close()
        first = s.rolling_snapshot().to_dict()
        self.assertEqual(first["window_size"], 0)
        self.assertEqual(first["sampled_finished"], 0)
        self.assertEqual(s.rolling_snapshot().to_dict(), first)


class StatsUnitWindowTest(unittest.TestCase):
    """直接在 Stats 层验证三元组同步进出与分布取位。"""

    def test_triples_enter_and_leave_together(self) -> None:
        stats = Stats(rolling_window=2)
        empty = stats.rolling_snapshot()
        self.assertEqual(empty.window_size, 2)
        self.assertEqual(empty.sampled_finished, 0)

        stats.record_finished(1.0, 10.0, 100.0, success=True)
        stats.record_finished(2.0, 20.0, 200.0, success=True)
        stats.record_finished(3.0, 30.0, 300.0, success=False)
        snap = stats.rolling_snapshot()
        # 窗口容量 2：最旧的 (1,10,100) 已整体离开，三个维度同步只保留后两个。
        self.assertEqual(snap.window_size, 2)
        self.assertEqual(snap.sampled_finished, 2)
        self.assertEqual(snap.queue_wait_ms["max"], 3.0)
        self.assertEqual(snap.queue_wait_ms["p50"], 2.0)
        self.assertEqual(snap.queue_wait_ms["p95"], 3.0)
        self.assertEqual(snap.total_latency_ms["max"], 30.0)
        self.assertEqual(snap.total_latency_ms["p50"], 20.0)
        self.assertEqual(snap.execution_ms["max"], 300.0)
        self.assertEqual(snap.execution_ms["p50"], 200.0)

    def test_non_finish_events_never_enter_window(self) -> None:
        stats = Stats(rolling_window=5)
        stats.record_accepted()
        stats.record_accepted()
        stats.record_rejected()
        stats.record_cancelled()
        stats.record_expired()
        snap = stats.rolling_snapshot()
        self.assertEqual(snap.sampled_finished, 0)
        self.assertEqual(snap.queue_wait_ms, _EMPTY_DIST)
        self.assertEqual(snap.total_latency_ms, _EMPTY_DIST)
        self.assertEqual(snap.execution_ms, _EMPTY_DIST)


class WindowSamplingTest(unittest.TestCase):
    def test_partial_window_counts_finished_tasks(self) -> None:
        with Scheduler(workers=2, max_pending=4,
                       latency_window_tasks=3) as s:
            s.submit("a", lambda: 1)
            snap = s.rolling_snapshot()
            self.assertEqual(snap.window_size, 3)
            self.assertEqual(snap.sampled_finished, 1)
            s.submit("b", lambda: 2)
            self.assertEqual(s.rolling_snapshot().sampled_finished, 2)
            # 仍未满：等于启用后结束任务总数。
            self.assertEqual(
                s.rolling_snapshot().sampled_finished,
                s.snapshot().completed + s.snapshot().failed,
            )

    def test_window_keeps_most_recent_when_full(self) -> None:
        with Scheduler(workers=4, max_pending=8,
                       latency_window_tasks=2) as s:
            for i in range(5):
                s.submit("t%d" % i, lambda i=i: i)
            snap = s.rolling_snapshot()
            self.assertEqual(snap.window_size, 2)
            self.assertEqual(snap.sampled_finished, 2)
            # 窗口满后 sampled_finished 恒为容量，不再随结束总数增长。
            total_finished = s.snapshot().completed + s.snapshot().failed
            self.assertEqual(total_finished, 5)

    def test_failed_tasks_enter_window(self) -> None:
        def boom() -> None:
            raise RuntimeError("boom")

        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=3) as s:
            with self.assertRaises(RuntimeError):
                s.submit("bad", boom)
            self.assertEqual(s.snapshot().failed, 1)
            snap = s.rolling_snapshot()
            self.assertEqual(snap.sampled_finished, 1)
            for name in ("queue_wait_ms", "total_latency_ms", "execution_ms"):
                self.assertEqual(set(getattr(snap, name)), _DIST_KEYS)

    def test_window_samples_reflect_execution_and_queue(self) -> None:
        # workers=1 串行：第二个任务的排队等待约等于第一个任务的执行时长，
        # 但其 execution_ms 只含自身执行；窗口 N=1 只留最后结束的任务。
        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=1) as s:
            # 必须 nowait：submit 会阻塞到 long 结束，那样 short 无需排队。
            s.submit_nowait("long", lambda: time.sleep(0.08))
            s.submit_nowait("short", lambda: time.sleep(0.01))
            self.assertTrue(_wait_stats(s, completed=2))
            snap = s.rolling_snapshot()
            self.assertEqual(snap.sampled_finished, 1)
            # 窗口里只有 short：几乎没有排队之外的长执行，execution_ms 很小。
            self.assertLess(snap.execution_ms["max"], 40.0)
            # short 必须先等 long 执行完才被认领。
            self.assertGreaterEqual(snap.queue_wait_ms["max"], 50.0)
            self.assertGreaterEqual(snap.total_latency_ms["max"],
                                    snap.queue_wait_ms["max"])


class ExcludedEventsTest(unittest.TestCase):
    def _occupy_worker(self, s: Scheduler) -> tuple:
        entered = threading.Event()
        release = threading.Event()

        def hold() -> None:
            entered.set()
            release.wait(2.0)

        threading.Thread(
            target=s.submit, args=("hold", hold)
        ).start()
        self.assertTrue(entered.wait(2.0))
        return entered, release

    def test_cancelled_task_not_sampled(self) -> None:
        with Scheduler(workers=1, max_pending=3,
                       latency_window_tasks=5) as s:
            _, release = self._occupy_worker(s)
            handle = s.submit_nowait("q", lambda: 1)
            self.assertTrue(_wait_stats(s, accepted=2))
            self.assertTrue(handle.cancel())
            self.assertTrue(_wait_stats(s, cancelled=1))
            # hold 仍在执行、q 被取消：窗口没有任何已记账结束。
            self.assertEqual(s.rolling_snapshot().sampled_finished, 0)
            release.set()
            self.assertTrue(_wait_stats(s, completed=1))
            self.assertEqual(s.rolling_snapshot().sampled_finished, 1)

    def test_expired_task_not_sampled(self) -> None:
        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=5) as s:
            _, release = self._occupy_worker(s)
            s.submit_nowait("q", lambda: 1, max_queue_wait_ms=20)
            self.assertTrue(_wait_stats(s, expired=1))
            self.assertEqual(s.rolling_snapshot().sampled_finished, 0)
            release.set()
            self.assertTrue(_wait_stats(s, completed=1))
            self.assertEqual(s.rolling_snapshot().sampled_finished, 1)

    def test_rejected_task_not_sampled(self) -> None:
        with Scheduler(workers=1, max_pending=1,
                       latency_window_tasks=5) as s:
            _, release = self._occupy_worker(s)
            with self.assertRaises(Exception):
                s.submit_nowait("x", lambda: 1)
            self.assertTrue(_wait_stats(s, rejected=1))
            self.assertEqual(s.rolling_snapshot().sampled_finished, 0)
            release.set()
            self.assertTrue(_wait_stats(s, completed=1))
            self.assertEqual(s.rolling_snapshot().sampled_finished, 1)

    def test_unadmitted_waiter_at_close_not_sampled(self) -> None:
        s = Scheduler(workers=1, max_pending=1,
                      latency_window_tasks=5)
        _, release = self._occupy_worker(s)

        def wait_call() -> None:
            try:
                s.submit_with_wait("w", lambda: 1,
                                   admission_timeout_ms=None)
            except Exception:
                pass

        t = threading.Thread(target=wait_call)
        t.start()
        with s._cond:  # type: ignore[attr-defined]
            while not s._admission_queue:  # type: ignore[attr-defined]
                s._cond.wait(0.002)  # type: ignore[attr-defined]

        closing = threading.Thread(target=s.close)
        closing.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not s._closing:  # type: ignore[attr-defined]
            time.sleep(0.002)
        release.set()
        t.join()
        closing.join()
        snap = s.rolling_snapshot()
        # hold 随 close 结束入窗；w 在 close 期间未被接纳，不入窗。
        self.assertEqual(snap.sampled_finished, 1)
        self.assertEqual(s.snapshot().completed, 1)


class ConsistencyTest(unittest.TestCase):
    def test_sampled_finished_tracks_finished_count(self) -> None:
        # 任意同一记账时刻窗口样本数恒为 min(window_size, completed+failed)：
        # 取样与 record_finished 在同一把锁内，只含已记账结束。这里直接在
        # Stats 的同一把锁内原子读取内部状态校验（两次独立的公开快照调用
        # 之间允许有任务结束，本就不要求跨调用逐值相等）。
        s = Scheduler(workers=3, max_pending=6,
                      latency_window_tasks=4)
        stop = threading.Event()
        violations: list[str] = []
        vlock = threading.Lock()
        stats = s._stats  # type: ignore[attr-defined]

        def observe() -> None:
            while not stop.is_set():
                with stats._lock:  # type: ignore[attr-defined]
                    finished = stats._completed + stats._failed  # type: ignore[attr-defined]
                    in_window = len(stats._rolling)  # type: ignore[attr-defined]
                expected = min(4, finished)
                if in_window != expected:
                    with vlock:
                        violations.append(
                            "sampled=%d expected=%d" % (in_window, expected)
                        )

        observers = [threading.Thread(target=observe) for _ in range(3)]
        for t in observers:
            t.start()

        callers = []
        for i in range(80):
            def caller(i: int = i) -> None:
                try:
                    s.submit_with_wait(
                        "t%d" % i, lambda: time.sleep(0.001),
                        admission_timeout_ms=5_000,
                    )
                except Exception:  # pragma: no cover - 压力测试
                    pass
            t = threading.Thread(target=caller)
            t.start()
            callers.append(t)
        for t in callers:
            t.join()
        s.close()
        stop.set()
        for t in observers:
            t.join()
        self.assertEqual(violations, [])

    def test_window_does_not_change_cumulative_or_interval_stats(self) -> None:
        with Scheduler(workers=2, max_pending=4,
                       latency_window_tasks=2) as s:
            checkpoint = s.stats_checkpoint()
            for i in range(4):
                self.assertEqual(s.submit("t%d" % i, lambda i=i: i), i)
            stats = s.snapshot()
            since = s.snapshot_since(checkpoint)
            self.assertEqual(stats.accepted, 4)
            self.assertEqual(stats.completed, 4)
            self.assertEqual(stats.failed, 0)
            self.assertEqual(since.completed, 4)
            self.assertEqual(set(stats.to_dict()), {
                "accepted", "completed", "failed", "cancelled",
                "expired", "rejected", "queue_wait_ms",
                "total_latency_ms", "execution_ms",
            })
            # 累计分布仍含全部 4 个样本（不受窗口容量 2 限制）。
            rolling = s.rolling_snapshot()
            self.assertEqual(rolling.sampled_finished, 2)
            self.assertEqual(stats.execution_ms["max"],
                             since.execution_ms["max"])


class SnapshotShapeTest(unittest.TestCase):
    def test_to_dict_shape_and_json(self) -> None:
        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=3) as s:
            s.submit("a", lambda: 1)
            snap = s.rolling_snapshot()
            data = snap.to_dict()
            self.assertEqual(set(data), set(_FIELDS))
            for name in _FIELDS:
                self.assertEqual(data[name], getattr(snap, name))
            for group in ("queue_wait_ms", "total_latency_ms",
                          "execution_ms"):
                self.assertEqual(set(data[group]), _DIST_KEYS)
            encoded = json.dumps(data)
            self.assertEqual(json.loads(encoded), data)
            self.assertIsInstance(data["window_size"], int)
            self.assertIsInstance(data["sampled_finished"], int)
            self.assertIsInstance(data["queue_wait_ms"]["p50"], float)

    def test_snapshot_is_immutable(self) -> None:
        with Scheduler(workers=1, max_pending=1,
                       latency_window_tasks=2) as s:
            snap = s.rolling_snapshot()
            for name in _FIELDS:
                with self.subTest(name=name):
                    with self.assertRaises(AttributeError):
                        setattr(snap, name, 1)
                    with self.assertRaises(AttributeError):
                        delattr(snap, name)
            with self.assertRaises(AttributeError):
                snap.unknown = 1  # type: ignore[attr-defined]

    def test_dict_is_a_copy(self) -> None:
        with Scheduler(workers=1, max_pending=2,
                       latency_window_tasks=3) as s:
            s.submit("a", lambda: 1)
            snap = s.rolling_snapshot()
            data = snap.to_dict()
            data["sampled_finished"] = 999
            data["new"] = 1
            self.assertEqual(snap.sampled_finished, 1)
            self.assertEqual(set(snap.to_dict()), set(_FIELDS))

    def test_snapshot_is_frozen_and_stable(self) -> None:
        with Scheduler(workers=2, max_pending=4,
                       latency_window_tasks=10) as s:
            s.submit("a", lambda: 1)
            earlier = s.rolling_snapshot()
            earlier_dict = earlier.to_dict()
            for i in range(5):
                s.submit("b%d" % i, lambda: 0)
        # 后续结束不影响早先快照；重复读取同一快照结果稳定。
        self.assertEqual(earlier.sampled_finished, 1)
        self.assertEqual(earlier.to_dict(), earlier_dict)

    def test_readable_after_close_with_final_window(self) -> None:
        s = Scheduler(workers=2, max_pending=8,
                      latency_window_tasks=3)
        for i in range(7):
            s.submit("t%d" % i, lambda: 0)
        s.close()
        snap = s.rolling_snapshot()
        self.assertTrue(s.runtime_snapshot().closed)
        self.assertEqual(snap.window_size, 3)
        self.assertEqual(snap.sampled_finished, 3)
        self.assertEqual(snap.to_dict(), s.rolling_snapshot().to_dict())


if __name__ == "__main__":
    unittest.main()
