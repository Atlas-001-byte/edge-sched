"""runtime_snapshot 即时运行观测的验收测试。

覆盖：空调度器基线、to_dict 形态与 JSON 可序列化、属性不可变、
queued/running/unfinished 口径（取消/到期/结束不计、等待任务不计）、
available_capacity、两个最老时长的锚点与三位小数、准入等待者与成组
计数、接纳即退出等待侧、事件交错下的不变量、closing/closed 状态、
既有快照不随后续事件变化，以及快照本身不改变统计与任务。
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


def _wait_until(predicate, timeout=2.0, interval=0.002):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _assert_invariants(testcase, snap: RuntimeSnapshot,
                       workers: int, max_pending: int) -> None:
    testcase.assertEqual(snap.queued + snap.running, snap.unfinished)
    testcase.assertEqual(
        snap.available_capacity, max(0, max_pending - snap.unfinished)
    )
    testcase.assertGreaterEqual(snap.unfinished, 0)
    testcase.assertLessEqual(snap.unfinished, max_pending)
    testcase.assertLessEqual(snap.running, workers)
    testcase.assertLessEqual(snap.queued, max_pending)
    testcase.assertGreaterEqual(snap.admission_waiters, 0)
    testcase.assertGreaterEqual(
        snap.admission_waiting_tasks, snap.admission_waiters
    )
    testcase.assertGreaterEqual(snap.oldest_queued_age_ms, 0.0)
    testcase.assertGreaterEqual(snap.oldest_admission_wait_ms, 0.0)
    testcase.assertEqual(
        round(snap.oldest_queued_age_ms, 3), snap.oldest_queued_age_ms
    )
    testcase.assertEqual(
        round(snap.oldest_admission_wait_ms, 3),
        snap.oldest_admission_wait_ms,
    )


class EmptySnapshotTest(unittest.TestCase):
    def test_empty_baseline(self) -> None:
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
            self.assertFalse(snap.closing)
            self.assertFalse(snap.closed)
            _assert_invariants(self, snap, 2, 3)

    def test_to_dict_shape_and_json(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            snap = s.runtime_snapshot()
            data = snap.to_dict()
            self.assertEqual(set(data.keys()), set(_FIELDS))
            for name in _FIELDS:
                self.assertEqual(getattr(snap, name), data[name])
            # 全部为 int/float/bool：json.dumps 必须直接可用。
            encoded = json.dumps(data)
            self.assertEqual(json.loads(encoded), data)
            # to_dict 返回独立字典，不与快照共享可变容器。
            data["queued"] = 999
            self.assertEqual(snap.queued, 0)

    def test_immutable_attributes(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            snap = s.runtime_snapshot()
            for name in _FIELDS:
                with self.subTest(name=name):
                    with self.assertRaises(AttributeError):
                        setattr(snap, name, None)
                    with self.assertRaises(AttributeError):
                        delattr(snap, name)
            with self.assertRaises(AttributeError):
                snap.unknown = 1  # type: ignore[attr-defined]

    def test_does_not_change_stats(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit("a", lambda: 1)
            before = s.snapshot().to_dict()
            for _ in range(5):
                s.runtime_snapshot()
            after = s.snapshot().to_dict()
            self.assertEqual(before, after)


class LiveCountsTest(unittest.TestCase):
    def test_queued_running_unfinished(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            # 运行中的任务先执行一段时间后再排队新任务：最老排队年龄必须
            # 锚定排队任务的接纳时刻，而不是更早的运行中任务。
            time.sleep(0.08)
            s.submit_nowait("q1", lambda: None)
            s.submit_nowait("q2", lambda: None)
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().queued == 2
                )
            )
            snap = s.runtime_snapshot()
            self.assertEqual((snap.queued, snap.running), (2, 1))
            self.assertEqual(snap.unfinished, 3)
            self.assertEqual(snap.available_capacity, 1)
            self.assertLess(snap.oldest_queued_age_ms, 50.0)
            _assert_invariants(self, snap, 1, 4)

            # 取消一个排队任务：queued 与 unfinished 同步下降，容量回升。
            handle = s.submit_nowait("q3", lambda: None)
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().queued == 3
                )
            )
            self.assertTrue(handle.cancel())
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().queued == 2
                )
            )
            snap = s.runtime_snapshot()
            self.assertEqual((snap.queued, snap.running), (2, 1))
            self.assertEqual(snap.unfinished, 3)
            self.assertEqual(snap.available_capacity, 1)

            gate.set()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().unfinished == 0
                )
            )
            snap = s.runtime_snapshot()
            self.assertEqual(
                (snap.queued, snap.running, snap.unfinished), (0, 0, 0)
            )
            self.assertEqual(snap.available_capacity, 4)
            self.assertEqual(snap.oldest_queued_age_ms, 0.0)

    def test_expired_task_excluded(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            s.submit_nowait("soon", lambda: None, max_queue_wait_ms=30)
            self.assertTrue(
                _wait_until(lambda: s.snapshot().expired == 1)
            )
            snap = s.runtime_snapshot()
            # 到期任务既不在 queued 也不在 running。
            self.assertEqual((snap.queued, snap.running), (0, 1))
            self.assertEqual(snap.unfinished, 1)
            self.assertEqual(snap.available_capacity, 2)
            gate.set()

    def test_oldest_queued_age_anchor(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            s.submit_nowait("q1", lambda: None)
            time.sleep(0.06)
            s.submit_nowait("q2", lambda: None)
            snap = s.runtime_snapshot()
            # 最老排队任务是 q1：年龄至少约 60ms（q2 更新，不得拉低）。
            self.assertGreaterEqual(snap.oldest_queued_age_ms, 50.0)
            gate.set()


class AdmissionWaitSnapshotTest(unittest.TestCase):
    def test_waiters_counted_by_calls_and_tasks(self) -> None:
        gate = threading.Event()
        errors = []

        def run_waiter(scheduler, task_id):
            try:
                scheduler.submit_with_wait(
                    task_id, lambda: None, admission_timeout_ms=None
                )
            except BaseException as exc:  # noqa: BLE001 - 记录到断言
                errors.append(exc)

        with Scheduler(workers=1, max_pending=2) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            s.submit_nowait("queued", lambda: None)
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().unfinished == 2
                )
            )

            t1 = threading.Thread(target=run_waiter, args=(s, "w1"))
            t1.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            time.sleep(0.06)
            t2 = threading.Thread(
                target=lambda: s.submit_batch_with_wait(
                    [
                        {"task_id": "w2", "fn": lambda: None},
                        {"task_id": "w3", "fn": lambda: None,
                         "priority": 1},
                    ],
                    admission_timeout_ms=None,
                )
            )
            t2.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 2
                )
            )

            snap = s.runtime_snapshot()
            # 容量被已接纳任务占满：等待调用不占 unfinished，成组按任务数计。
            self.assertEqual(snap.admission_waiters, 2)
            self.assertEqual(snap.admission_waiting_tasks, 3)
            self.assertEqual((snap.queued, snap.running), (1, 1))
            self.assertEqual(snap.unfinished, 2)
            self.assertEqual(snap.available_capacity, 0)
            # 最早等待者 t1 至快照时刻已等待约 60ms 以上。
            self.assertGreaterEqual(snap.oldest_admission_wait_ms, 50.0)
            _assert_invariants(self, snap, 1, 2)

            # 等待期间同名 task_id 已被占用（DuplicateTaskError 不入队）。
            from edge_sched import DuplicateTaskError
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("w1", lambda: None)

            gate.set()
            t1.join(5)
            t2.join(5)
            self.assertFalse(t1.is_alive())
            self.assertFalse(t2.is_alive())
            self.assertEqual(errors, [])

            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().unfinished == 0
                )
            )
            snap = s.runtime_snapshot()
            self.assertEqual(
                (
                    snap.admission_waiters,
                    snap.admission_waiting_tasks,
                    snap.oldest_admission_wait_ms,
                ),
                (0, 0, 0.0),
            )
            self.assertEqual(snap.available_capacity, 2)

    def test_admission_moves_waiting_to_accepted(self) -> None:
        # 一个等待者：被接纳的瞬间即退出等待侧、进入 queued 侧，
        # 绝不会同时计入 admission_waiting_tasks 与 accepted/queued。
        gate = threading.Event()
        done = threading.Event()

        def run_waiter(scheduler):
            scheduler.submit_with_wait(
                "w", lambda: gate.wait(5), admission_timeout_ms=None
            )
            done.set()

        with Scheduler(workers=1, max_pending=1) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            t = threading.Thread(target=run_waiter, args=(s,))
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            snap = s.runtime_snapshot()
            self.assertEqual(snap.admission_waiting_tasks, 1)
            self.assertEqual(snap.queued, 0)

            gate.set()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 0
                )
            )
            t.join(5)
            self.assertFalse(t.is_alive())
            self.assertTrue(done.is_set())
            snap = s.runtime_snapshot()
            self.assertEqual(snap.admission_waiting_tasks, 0)
            self.assertEqual(snap.unfinished, 0)


class CloseSnapshotTest(unittest.TestCase):
    def test_closing_then_closed(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=2, max_pending=4) as s:
            s.submit_nowait("r1", lambda: gate.wait(5))
            s.submit_nowait("r2", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 2
                )
            )

            closing = threading.Event()

            def close():
                closing.set()
                s.close()

            t = threading.Thread(target=close)
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().closing
                )
            )
            snap = s.runtime_snapshot()
            self.assertTrue(snap.closing)
            self.assertFalse(snap.closed)
            self.assertEqual(snap.running, 2)
            self.assertEqual(snap.admission_waiters, 0)

            gate.set()
            t.join(5)
            self.assertFalse(t.is_alive())
            snap = s.runtime_snapshot()
            self.assertTrue(snap.closing)
            self.assertTrue(snap.closed)
            self.assertEqual(
                (snap.queued, snap.running, snap.unfinished), (0, 0, 0)
            )
            self.assertEqual(
                (snap.admission_waiters, snap.admission_waiting_tasks),
                (0, 0),
            )
            self.assertEqual(snap.available_capacity, 4)

    def test_waiters_cleared_when_close_starts(self) -> None:
        from edge_sched import SchedulerClosedError
        gate = threading.Event()
        outcomes = []

        def wait_for_admission(scheduler):
            try:
                scheduler.submit_with_wait(
                    "w", lambda: None, admission_timeout_ms=None
                )
                outcomes.append("admitted")
            except SchedulerClosedError:
                outcomes.append("closed")

        with Scheduler(workers=1, max_pending=1) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            t = threading.Thread(target=wait_for_admission, args=(s,))
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            gate.set()
            # 不 join 等待者：close 开始时它要么已被接纳并随关闭执行完，
            # 要么直接得到 SchedulerClosedError；无论哪种路径，关闭开始后
            # 快照中都不再有等待者。
            s.close()
            t.join(5)
            self.assertFalse(t.is_alive())
            self.assertEqual(outcomes, [outcomes[0]])
            self.assertIn(outcomes[0], ("admitted", "closed"))
            snap = s.runtime_snapshot()
            self.assertEqual(snap.admission_waiters, 0)
            self.assertEqual(snap.admission_waiting_tasks, 0)
            self.assertTrue(snap.closed)

    def test_snapshot_after_close_is_stable_empty(self) -> None:
        with Scheduler(workers=2, max_pending=3) as s:
            s.submit("a", lambda: 1)
            s.close()
        s1 = s.runtime_snapshot()
        s2 = s.runtime_snapshot()
        for snap in (s1, s2):
            self.assertTrue(snap.closed)
            self.assertEqual(snap.unfinished, 0)
            self.assertEqual(snap.available_capacity, 3)
        # 关闭后的历史统计仍可读，运行快照不受其影响。
        self.assertEqual(s.snapshot().completed, 1)


class ExistingSnapshotStableTest(unittest.TestCase):
    def test_snapshot_value_frozen(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            s.submit_nowait("run", lambda: gate.wait(5))
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().running == 1
                )
            )
            s.submit_nowait("q1", lambda: None)
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().queued == 1
                )
            )
            frozen = s.runtime_snapshot()
            self.assertEqual((frozen.queued, frozen.running), (1, 1))

            s.submit_nowait("q2", lambda: None)
            gate.set()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().unfinished == 0
                )
            )
            # 既有快照不随后续事件变化。
            self.assertEqual((frozen.queued, frozen.running), (1, 1))
            self.assertEqual(frozen.unfinished, 2)
            self.assertEqual(frozen.available_capacity, 1)
            self.assertFalse(frozen.closing)
            self.assertFalse(frozen.closed)
            self.assertEqual(
                set(frozen.to_dict().keys()), set(_FIELDS)
            )


class ConcurrentInvariantTest(unittest.TestCase):
    def test_invariants_under_churn(self) -> None:
        gate = threading.Event()
        workers = 2
        max_pending = 6
        s = Scheduler(workers=workers, max_pending=max_pending)
        stop = threading.Event()
        violations = []

        def observer():
            while not stop.is_set():
                snap = s.runtime_snapshot()
                try:
                    assert snap.queued + snap.running == snap.unfinished
                    assert snap.available_capacity == max(
                        0, max_pending - snap.unfinished
                    )
                    assert 0 <= snap.unfinished <= max_pending
                    assert 0 <= snap.running <= workers
                    assert snap.admission_waiting_tasks >= snap.admission_waiters
                    assert snap.oldest_queued_age_ms >= 0.0
                    assert snap.oldest_admission_wait_ms >= 0.0
                    # 等待任务与已接纳任务永不重叠。
                    assert snap.unfinished <= max_pending
                except AssertionError:
                    violations.append(snap.to_dict())
                time.sleep(0.0005)

        def producer(i):
            deadline = time.monotonic() + 0.6
            while time.monotonic() < deadline:
                try:
                    s.submit_with_wait(
                        "p%d-%d" % (i, time.monotonic_ns()),
                        lambda: None,
                        admission_timeout_ms=50,
                    )
                except Exception:
                    pass

        observers = [
            threading.Thread(target=observer) for _ in range(3)
        ]
        # 先用长任务占住部分容量，制造排队与等待并存的状态。
        for i in range(workers):
            s.submit_nowait("hold%d" % i, lambda: gate.wait(5))
        for t in observers:
            t.start()
        producers = [
            threading.Thread(target=producer, args=(i,)) for i in range(3)
        ]
        for t in producers:
            t.start()
        time.sleep(0.3)
        gate.set()
        for t in producers:
            t.join(5)
        s.close()
        stop.set()
        for t in observers:
            t.join(5)
        self.assertEqual(violations, [])
        snap = s.runtime_snapshot()
        self.assertTrue(snap.closed)
        self.assertEqual(snap.unfinished, 0)
        self.assertEqual(snap.admission_waiters, 0)


if __name__ == "__main__":
    unittest.main()
