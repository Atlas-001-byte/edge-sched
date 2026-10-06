"""Scheduler.runtime_snapshot 即时运行观测的验收测试。

覆盖：空快照字段、queued/running/unfinished 口径与不变量、available_capacity
公式、取消/到期/结束不计、准入等待者与其占用任务数（含成组）、等待任务不
重复计数、两类最老耗时的起算与增长、to_dict 形态与 JSON 可序列化、快照
不可变、closing/closed 状态、快照创建后不随事件变化、观测不改变任务与统计，
以及高并发下快照恒满足 queued + running == unfinished。
"""

import json
import threading
import time
import unittest

from edge_sched import RuntimeSnapshot, Scheduler

_FIELDS = (
    "workers",
    "max_pending",
    "queued",
    "running",
    "unfinished",
    "admission_waiters",
    "admission_waiting_tasks",
    "available_capacity",
    "oldest_queued_age_ms",
    "oldest_admission_wait_ms",
    "closing",
    "closed",
)


def _wait_started(s: Scheduler, task_id: str) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        entry = s._tasks.get(task_id)  # type: ignore[attr-defined]
        if entry is not None and entry.started:
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


def _wait_admission_queue(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with s._cond:  # type: ignore[attr-defined]
            if len(s._admission_queue) == n:  # type: ignore[attr-defined]
                return True
        time.sleep(0.002)
    return False


def _wait_runtime(s: Scheduler, **expected: object) -> bool:
    """轮询 runtime_snapshot 直到指定字段全部匹配。"""
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        snap = s.runtime_snapshot()
        if all(getattr(snap, k) == v for k, v in expected.items()):
            return True
        time.sleep(0.002)
    return False


class EmptySnapshotTest(unittest.TestCase):
    def test_fresh_scheduler_snapshot(self) -> None:
        with Scheduler(workers=2, max_pending=3) as s:
            snap = s.runtime_snapshot()
            self.assertIsInstance(snap, RuntimeSnapshot)
            self.assertEqual(snap.workers, 2)
            self.assertEqual(snap.max_pending, 3)
            self.assertEqual(
                (snap.queued, snap.running, snap.unfinished), (0, 0, 0)
            )
            self.assertEqual(
                (snap.admission_waiters, snap.admission_waiting_tasks),
                (0, 0),
            )
            self.assertEqual(snap.available_capacity, 3)
            self.assertEqual(snap.oldest_queued_age_ms, 0.0)
            self.assertEqual(snap.oldest_admission_wait_ms, 0.0)
            self.assertIsInstance(snap.oldest_queued_age_ms, float)
            self.assertIsInstance(snap.oldest_admission_wait_ms, float)
            self.assertFalse(snap.closing)
            self.assertFalse(snap.closed)

    def test_empty_snapshot_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        snap = s.runtime_snapshot()  # closed 后仍可调用
        self.assertTrue(snap.closed)
        self.assertTrue(snap.closing)
        self.assertEqual(
            (snap.queued, snap.running, snap.unfinished), (0, 0, 0)
        )
        self.assertEqual(
            (snap.admission_waiters, snap.admission_waiting_tasks),
            (0, 0),
        )
        self.assertEqual(snap.available_capacity, 2)
        self.assertEqual(snap.oldest_queued_age_ms, 0.0)
        self.assertEqual(snap.oldest_admission_wait_ms, 0.0)
        # 已关闭调度器上反复调用结果一致。
        self.assertEqual(snap.to_dict(), s.runtime_snapshot().to_dict())


class ToDictAndImmutabilityTest(unittest.TestCase):
    def test_to_dict_shape_and_json_serializable(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            snap = s.runtime_snapshot()
            data = snap.to_dict()
            self.assertEqual(set(data), set(_FIELDS))
            # 值与同名属性一一对应。
            for name in _FIELDS:
                self.assertEqual(data[name], getattr(snap, name))
            # 全部为 JSON 原生类型，可往返序列化。
            encoded = json.dumps(data)
            self.assertEqual(json.loads(encoded), data)
            self.assertIsInstance(data["workers"], int)
            self.assertIsInstance(data["queued"], int)
            self.assertIsInstance(data["oldest_queued_age_ms"], float)
            self.assertIsInstance(data["closing"], bool)

    def test_snapshot_is_immutable(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            snap = s.runtime_snapshot()
            for name in _FIELDS:
                with self.subTest(name=name):
                    with self.assertRaises(AttributeError):
                        setattr(snap, name, 1)
                    with self.assertRaises(AttributeError):
                        delattr(snap, name)
            # 不允许新增属性。
            with self.assertRaises(AttributeError):
                snap.unknown = 1  # type: ignore[attr-defined]

    def test_dict_is_a_copy(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            snap = s.runtime_snapshot()
            data = snap.to_dict()
            data["queued"] = 999
            data["new"] = 1
            self.assertEqual(snap.queued, 0)
            self.assertEqual(set(snap.to_dict()), set(_FIELDS))


class QueueRunningCountsTest(unittest.TestCase):
    def test_queued_running_and_capacity(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q1", lambda: 1)
            s.submit_nowait("q2", lambda: 2)
            self.assertTrue(_wait_accepted(s, 3))

            snap = s.runtime_snapshot()
            self.assertEqual(snap.running, 1)
            self.assertEqual(snap.queued, 2)
            self.assertEqual(snap.unfinished, 3)
            self.assertEqual(snap.queued + snap.running, snap.unfinished)
            self.assertEqual(snap.available_capacity, 0)

            release.set()
            self.assertTrue(
                _wait_runtime(
                    s, queued=0, running=0, unfinished=0,
                    available_capacity=3,
                )
            )

    def test_running_capped_by_workers_with_more_workers(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        barrier = 3

        def gate() -> None:
            entered.set()
            release.wait(2.0)

        with Scheduler(workers=3, max_pending=5) as s:
            handles = []
            for i in range(barrier):
                handles.append(s.submit_nowait("r%d" % i, gate))
            for i in range(barrier, 5):
                handles.append(s.submit_nowait("q%d" % i, lambda: 0))
            self.assertTrue(_wait_accepted(s, 5))
            self.assertTrue(
                _wait_runtime(s, running=3, queued=2, unfinished=5)
            )
            snap = s.runtime_snapshot()
            self.assertEqual(snap.available_capacity, 0)
            release.set()
            for h in handles:
                h.result(2.0)
            self.assertTrue(_wait_runtime(s, unfinished=0))

    def test_cancelled_queued_task_not_counted(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            q1 = s.submit_nowait("q1", lambda: 1)
            q2 = s.submit_nowait("q2", lambda: 2)
            self.assertTrue(_wait_accepted(s, 3))
            self.assertTrue(q1.cancel())
            snap = s.runtime_snapshot()
            self.assertEqual(snap.queued, 1)
            self.assertEqual(snap.running, 1)
            self.assertEqual(snap.unfinished, 2)
            self.assertEqual(snap.available_capacity, 1)
            # q2 仍排队，取消 q1 不影响它。
            release.set()
            self.assertEqual(q2.result(2.0), 2)

    def test_finished_tasks_leave_all_counts(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            for i in range(4):
                self.assertEqual(s.submit("t%d" % i, lambda i=i: i), i)
            snap = s.runtime_snapshot()
            self.assertEqual(snap.unfinished, 0)
            self.assertEqual(snap.queued, 0)
            self.assertEqual(snap.running, 0)
            self.assertEqual(snap.available_capacity, 4)

    def test_expired_queued_task_not_counted(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: 1, max_queue_wait_ms=30)
            self.assertTrue(_wait_accepted(s, 2))
            # q 在认领前到期：离开 queued/unfinished，容量恢复。
            self.assertTrue(
                _wait_runtime(
                    s, queued=0, running=1, unfinished=1,
                    available_capacity=1,
                )
            )
            self.assertEqual(s.snapshot().expired, 1)
            release.set()


class OldestQueuedAgeTest(unittest.TestCase):
    def test_age_anchored_at_admission_and_grows(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q1", lambda: 1)
            self.assertTrue(_wait_accepted(s, 2))
            time.sleep(0.08)
            s.submit_nowait("q2", lambda: 2)
            self.assertTrue(_wait_accepted(s, 3))

            snap = s.runtime_snapshot()
            # 最老排队任务是 q1：年龄至少覆盖 q1 之后的 80ms 睡眠。
            self.assertGreaterEqual(snap.oldest_queued_age_ms, 70.0)
            self.assertLess(snap.oldest_queued_age_ms, 2_000.0)
            before = snap.oldest_queued_age_ms
            time.sleep(0.03)
            after = s.runtime_snapshot().oldest_queued_age_ms
            self.assertGreaterEqual(after, before + 20.0)
            # 毫秒值保留三位小数。
            self.assertEqual(after, round(after, 3))
            release.set()

    def test_running_task_excluded_from_queued_age(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            time.sleep(0.05)  # 执行中任务再老也不计入排队年龄。
            self.assertEqual(s.runtime_snapshot().queued, 0)
            self.assertEqual(s.runtime_snapshot().oldest_queued_age_ms, 0.0)
            release.set()

    def test_age_zero_when_no_queued(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            self.assertEqual(s.submit("t", lambda: 1), 1)
            self.assertEqual(s.runtime_snapshot().oldest_queued_age_ms, 0.0)


class AdmissionWaiterSnapshotTest(unittest.TestCase):
    def test_single_waiter_counts_and_wait_age(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            done = threading.Event()

            def wait_call() -> None:
                s.submit_with_wait("w", lambda: "W",
                                   admission_timeout_ms=10_000)
                done.set()

            t = threading.Thread(target=wait_call)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            snap = s.runtime_snapshot()
            self.assertEqual(snap.admission_waiters, 1)
            self.assertEqual(snap.admission_waiting_tasks, 1)
            # 等待任务尚未接纳：不计入 unfinished/queued/running。
            self.assertEqual(snap.unfinished, 1)
            self.assertEqual(snap.running, 1)
            self.assertEqual(snap.queued, 0)
            self.assertEqual(snap.available_capacity, 0)
            self.assertGreaterEqual(snap.oldest_admission_wait_ms, 0.0)
            time.sleep(0.05)
            aged = s.runtime_snapshot()
            self.assertGreaterEqual(aged.oldest_admission_wait_ms, 40.0)
            self.assertEqual(aged.admission_waiters, 1)

            release.set()
            self.assertTrue(done.wait(2.0))
            t.join()
            self.assertTrue(
                _wait_runtime(
                    s, admission_waiters=0, admission_waiting_tasks=0,
                    unfinished=0,
                )
            )
            self.assertEqual(
                s.runtime_snapshot().oldest_admission_wait_ms, 0.0
            )

    def test_batch_waiter_counts_group_size(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            # 余 2：3 个任务的整组放得进容量但不能一次容纳，必须成组等待。
            handles_box: dict[str, object] = {}

            def batch_call() -> None:
                handles_box["v"] = s.submit_batch_with_wait(
                    [
                        {"task_id": "g0", "fn": lambda: 0},
                        {"task_id": "g1", "fn": lambda: 1},
                        {"task_id": "g2", "fn": lambda: 2},
                    ],
                    admission_timeout_ms=10_000,
                )

            t = threading.Thread(target=batch_call)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            snap = s.runtime_snapshot()
            # 整组是一个等待者，但占用 3 个 task_id。
            self.assertEqual(snap.admission_waiters, 1)
            self.assertEqual(snap.admission_waiting_tasks, 3)
            self.assertEqual(snap.unfinished, 1)  # 组未接纳，不占 pending
            self.assertEqual(snap.available_capacity, 2)

            release.set()
            t.join()
            handles = handles_box["v"]
            self.assertIsInstance(handles, tuple)
            self.assertEqual(len(handles), 3)
            self.assertTrue(
                _wait_runtime(
                    s, admission_waiters=0, admission_waiting_tasks=0,
                    unfinished=0,
                )
            )

    def test_waiters_cleared_on_close(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        def wait_call() -> None:
            try:
                s.submit_with_wait("w", lambda: None,
                                   admission_timeout_ms=None)
            except Exception:
                pass

        t = threading.Thread(target=wait_call)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        self.assertEqual(s.runtime_snapshot().admission_waiters, 1)

        closing = threading.Thread(target=s.close)
        closing.start()
        # close 一开始等待者即清空：closing=True、closed 尚为 False。
        self.assertTrue(
            _wait_runtime(
                s, admission_waiters=0, admission_waiting_tasks=0,
                closing=True, closed=False,
            )
        )
        release.set()
        closing.join()
        t.join()
        snap = s.runtime_snapshot()
        self.assertTrue(snap.closing and snap.closed)
        self.assertEqual(snap.admission_waiters, 0)
        self.assertEqual(snap.admission_waiting_tasks, 0)


class SnapshotConsistencyTest(unittest.TestCase):
    def test_snapshot_does_not_change_state_or_stats(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: 1)
            self.assertTrue(_wait_accepted(s, 2))
            for _ in range(50):
                s.runtime_snapshot()
            time.sleep(0.02)
            for _ in range(50):
                s.runtime_snapshot()
            stats = s.snapshot()
            self.assertEqual((stats.accepted, stats.completed), (2, 0))
            # 观测不改变运行计数。
            snap = s.runtime_snapshot()
            self.assertEqual((snap.running, snap.queued), (1, 1))
            release.set()

    def test_returned_snapshot_is_frozen(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: 1)
            self.assertTrue(_wait_accepted(s, 2))
            earlier = s.runtime_snapshot()
            release.set()
            self.assertTrue(_wait_runtime(s, unfinished=0))
        # 事件继续发生后，早先快照保持创建时的值。
        self.assertEqual(earlier.unfinished, 2)
        self.assertEqual(earlier.running, 1)
        self.assertEqual(earlier.queued, 1)
        self.assertEqual(earlier.closed, False)
        self.assertEqual(earlier.closing, False)

    def test_concurrent_invariant_queued_plus_running(self) -> None:
        # 高并发提交/等待/取消与观测交错：每张快照都必须满足
        # queued + running == unfinished、容量公式与 running <= workers。
        s = Scheduler(workers=3, max_pending=6)
        stop = threading.Event()
        violations: list[str] = []
        vlock = threading.Lock()

        def observe() -> None:
            while not stop.is_set():
                snap = s.runtime_snapshot()
                bad = (
                    snap.queued + snap.running != snap.unfinished
                    or snap.available_capacity
                       != max(0, snap.max_pending - snap.unfinished)
                    or snap.running > snap.workers
                    or snap.unfinished > snap.max_pending
                    or snap.admission_waiting_tasks < snap.admission_waiters
                )
                if bad:
                    with vlock:
                        violations.append(snap.to_dict())

        observers = [threading.Thread(target=observe) for _ in range(3)]
        for t in observers:
            t.start()

        def fn(i: int) -> int:
            time.sleep(0.001)
            return i

        callers = []
        for i in range(120):
            def caller(i: int = i) -> None:
                try:
                    s.submit_with_wait(
                        "t%d" % i, lambda i=i: fn(i),
                        admission_timeout_ms=5_000,
                    )
                except Exception:  # pragma: no cover - 观测压力测试
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


if __name__ == "__main__":
    unittest.main()
