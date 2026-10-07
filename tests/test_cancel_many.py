"""Scheduler.cancel_many 批量取消语义测试。"""

import threading
import time
import unittest

from edge_sched import (
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    TaskCancelledError,
    TaskHandle,
)


def _wait_started(s: Scheduler, task_id: str) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        entry = s._tasks.get(task_id)  # type: ignore[attr-defined]
        if entry is not None and entry.started:
            return True
        time.sleep(0.002)
    return False


def _wait_runtime(s: Scheduler, **expected: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        snap = s.runtime_snapshot()
        if all(getattr(snap, k) == v for k, v in expected.items()):
            return True
        time.sleep(0.002)
    return False


def _wait_expired(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if s.snapshot().expired == n:
            return True
        time.sleep(0.005)
    return False


class CancelManyValidationTest(unittest.TestCase):
    def test_container_must_be_non_empty_list_or_tuple(self) -> None:
        release = threading.Event()
        with Scheduler(1, 4) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("a", lambda: None)
            for bad in ([], (), None, 1, "x", {h}, {0: h},
                        iter([h]), (x for x in [])):
                with self.subTest(bad=type(bad).__name__):
                    with self.assertRaises(InputValidationError):
                        s.cancel_many(bad)  # type: ignore[arg-type]
            # 全部校验失败均不改变任务：句柄仍排队未结束。
            self.assertFalse(h.done())
            release.set()
            held.result()
            self.assertIsNone(h.result())
        self.assertEqual(s.snapshot().cancelled, 0)
        self.assertEqual(s.snapshot().completed, 2)

    def test_elements_must_be_task_handles(self) -> None:
        release = threading.Event()
        with Scheduler(1, 8) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("a", lambda: None)
            for bad in (1, "h", None, object(), ["nope"], {"x": 1}):
                with self.subTest(bad=type(bad).__name__):
                    with self.assertRaises(InputValidationError):
                        s.cancel_many([h, bad])  # type: ignore[list-item]
            # 校验在加锁裁决前完成：合法的 h 不被部分取消。
            self.assertFalse(h.done())
            release.set()
            held.result()
            self.assertIsNone(h.result())
        self.assertEqual(s.snapshot().cancelled, 0)

    def test_handle_from_other_scheduler_rejected(self) -> None:
        release1 = threading.Event()
        release2 = threading.Event()
        with Scheduler(1, 4) as s1, Scheduler(1, 4) as s2:
            held1 = s1.submit_nowait("hold1", lambda: release1.wait(2.0))
            held2 = s2.submit_nowait("hold2", lambda: release2.wait(2.0))
            self.assertTrue(_wait_started(s1, "hold1"))
            self.assertTrue(_wait_started(s2, "hold2"))
            h1 = s1.submit_nowait("a", lambda: None)
            h2 = s2.submit_nowait("b", lambda: None)
            with self.assertRaises(InputValidationError):
                s1.cancel_many([h2])
            with self.assertRaises(InputValidationError):
                s1.cancel_many([h1, h2])
            self.assertFalse(h1.done())
            self.assertFalse(h2.done())
            release1.set()
            release2.set()
            held1.result()
            held2.result()
            self.assertIsNone(h1.result())
            self.assertIsNone(h2.result())
        self.assertEqual(s1.snapshot().cancelled, 0)
        self.assertEqual(s2.snapshot().cancelled, 0)

    def test_duplicate_handle_by_identity_rejected(self) -> None:
        release = threading.Event()
        with Scheduler(1, 8) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            ha = s.submit_nowait("a", lambda: None)
            hb = s.submit_nowait("b", lambda: None)
            with self.assertRaises(InputValidationError):
                s.cancel_many([ha, hb, ha])
            with self.assertRaises(InputValidationError):
                s.cancel_many((ha, ha))
            # 任何重复都在加锁前拒绝：两个任务都不受影响。
            self.assertFalse(ha.done())
            self.assertFalse(hb.done())
            release.set()
            held.result()
            self.assertIsNone(ha.result())
            self.assertIsNone(hb.result())
        self.assertEqual(s.snapshot().cancelled, 0)

    def test_distinct_handles_same_entry_are_not_duplicates(self) -> None:
        # 两个不同对象（身份不同）指向同一个任务条目是合法的：
        # 按输入顺序裁决，第一个成功 True，第二个看到已取消 -> False，
        # cancelled 只计一次。
        release = threading.Event()
        with Scheduler(1, 2) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h1 = s.submit_nowait("a", lambda: None)
            h2 = TaskHandle(s, h1._entry)
            self.assertIsNot(h1, h2)
            self.assertEqual(s.cancel_many([h1, h2]), (True, False))
            with self.assertRaises(TaskCancelledError):
                h1.result()
            with self.assertRaises(TaskCancelledError):
                h2.result()
            release.set()
            held.result()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled), (2, 1))

    def test_corrupted_handle_rejected(self) -> None:
        release = threading.Event()
        with Scheduler(1, 4) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            h = s.submit_nowait("a", lambda: None)
            corrupted = TaskHandle.__new__(TaskHandle)  # 无槽位属性
            with self.assertRaises(InputValidationError):
                s.cancel_many([h, corrupted])
            self.assertFalse(h.done())
            release.set()
            held.result()
            self.assertIsNone(h.result())
        self.assertEqual(s.snapshot().cancelled, 0)

    def test_failed_validation_changes_no_counts_or_pending(self) -> None:
        release = threading.Event()
        with Scheduler(1, 4) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            handles = [s.submit_nowait("a%d" % i, lambda: None)
                       for i in range(2)]
            before = s.snapshot().to_dict()
            rt_before = s.runtime_snapshot().to_dict()
            with self.assertRaises(InputValidationError):
                s.cancel_many([handles[0], object()])  # type: ignore[list-item]
            self.assertEqual(s.snapshot().to_dict(), before)
            self.assertEqual(s.runtime_snapshot().unfinished,
                             rt_before["unfinished"])
            for h in handles:
                self.assertFalse(h.done())
            release.set()
            held.result()
            for h in handles:
                h.result()


class CancelManyBasicTest(unittest.TestCase):
    def test_cancel_queued_batch_returns_true_tuple(self) -> None:
        release = threading.Event()
        ran: list[str] = []
        lock = threading.Lock()

        def record(tid: str) -> "any":
            def fn() -> str:
                release.wait(2.0)
                with lock:
                    ran.append(tid)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=8) as s:
            held = s.submit_nowait("hold", record("hold"))
            self.assertTrue(_wait_started(s, "hold"))
            queued = [s.submit_nowait("q%d" % i, record("q%d" % i))
                      for i in range(5)]
            results = s.cancel_many(list(queued))
            self.assertIsInstance(results, tuple)
            self.assertEqual(len(results), 5)
            self.assertEqual(results, (True,) * 5)
            for h in queued:
                self.assertTrue(h.done())
                with self.assertRaises(TaskCancelledError):
                    h.result()
            release.set()
            self.assertEqual(held.result(), "hold")
        self.assertEqual(ran, ["hold"])
        self.assertEqual(s.snapshot().cancelled, 5)

    def test_tuple_input_works_and_is_idempotent(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            held = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            q1 = s.submit_nowait("q1", lambda: None)
            q2 = s.submit_nowait("q2", lambda: None)
            self.assertEqual(s.cancel_many((q1, q2)), (True, True))
            # 再次批量取消已取消句柄：全部 False，计数不重复。
            self.assertEqual(s.cancel_many((q1, q2)), (False, False))
            for h in (q1, q2):
                self.assertTrue(h.done())
                with self.assertRaises(TaskCancelledError):
                    h.result()
                with self.assertRaises(TaskCancelledError):
                    s.result(h._entry.task_id)
            release.set()
            held.result()
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 3)
        self.assertEqual(snap.cancelled, 2)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)

    def test_mixed_outcomes_in_input_order(self) -> None:
        # 先制造已成功 / 已失败的终态任务（工作线程空闲时立即执行）。
        with Scheduler(workers=1, max_pending=8) as s:
            done_ok = s.submit_nowait("ok", lambda: 7)
            self.assertEqual(done_ok.result(), 7)

            def boom() -> None:
                raise RuntimeError("boom")

            done_bad = s.submit_nowait("bad", boom)
            with self.assertRaises(RuntimeError):
                done_bad.result()

            # 占住唯一工作线程，再造执行中、排队中、先取消的任务。
            entered = threading.Event()
            release = threading.Event()

            def slow() -> None:
                entered.set()
                release.wait(2.0)

            running = s.submit_nowait("run", slow)
            self.assertTrue(entered.wait(2.0))
            queued = s.submit_nowait("q", lambda: None)
            pre_cancelled = s.submit_nowait("pc", lambda: None)
            self.assertTrue(pre_cancelled.cancel())
            queued2 = s.submit_nowait("q2", lambda: None)

            results = s.cancel_many([
                running,        # 执行中 -> False
                queued,         # 排队中 -> True
                pre_cancelled,  # 已被单项取消 -> False
                done_ok,        # 已成功 -> False
                done_bad,       # 已失败 -> False
                queued2,        # 排队中 -> True
            ])
            self.assertEqual(results, (False, True, False, False, False, True))
            release.set()
            running.result()
            with self.assertRaises(TaskCancelledError):
                queued.result()
            with self.assertRaises(TaskCancelledError):
                queued2.result()
        snap = s.snapshot()
        self.assertEqual(snap.cancelled, 3)  # pre_cancelled + queued + queued2
        self.assertEqual(snap.completed, 2)  # done_ok 与 running 都成功结束
        self.assertEqual(snap.failed, 1)

    def test_true_results_never_execute(self) -> None:
        release = threading.Event()
        ran: list[str] = []
        lock = threading.Lock()

        def make(tid: str) -> "any":
            def fn() -> str:
                with lock:
                    ran.append(tid)
                release.wait(2.0)
                return tid
            return fn

        with Scheduler(workers=2, max_pending=16) as s:
            holders = [s.submit_nowait("h%d" % i, make("h%d" % i))
                       for i in range(2)]
            self.assertTrue(_wait_started(s, "h0"))
            self.assertTrue(_wait_started(s, "h1"))
            queued = [s.submit_nowait("q%d" % i, make("q%d" % i))
                      for i in range(6)]
            results = s.cancel_many(queued)
            self.assertTrue(all(results))
            release.set()
            for h in holders:
                self.assertEqual(h.result(), h._entry.task_id)
            for h in queued:
                with self.assertRaises(TaskCancelledError):
                    h.result()
        self.assertEqual(sorted(ran), ["h0", "h1"])

    def test_batch_does_not_affect_other_tasks(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            targets = [s.submit_nowait("t%d" % i, lambda: None)
                       for i in range(3)]
            bystander = s.submit_nowait("keep", lambda: "kept")
            self.assertEqual(s.cancel_many(targets), (True, True, True))
            release.set()
            self.assertEqual(bystander.result(), "kept")
            self.assertTrue(hold.result())
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled, snap.completed),
                         (5, 3, 2))

    def test_no_latency_samples_for_cancelled(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            queued = [s.submit_nowait("q%d" % i, lambda: 1)
                      for i in range(4)]
            self.assertEqual(s.cancel_many(queued), (True,) * 4)
            release.set()
            hold.result()
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 5)
        self.assertEqual(snap.cancelled, 4)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.expired, 0)
        self.assertEqual(snap.rejected, 0)
        # 仅 hold 一个成功任务贡献三类结束延迟样本；admission_wait 与每个
        # 被接纳任务一一对应（5 个 0.0 样本），取消不影响其保留。
        self.assertEqual(len(snap._queue_wait_samples), 1)
        self.assertEqual(len(snap._total_latency_samples), 1)
        self.assertEqual(len(snap._execution_samples), 1)
        self.assertEqual(len(snap._admission_wait_samples), 5)

    def test_expired_handle_returns_false(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            exp = s.submit_nowait("e", lambda: None, max_queue_wait_ms=20)
            self.assertTrue(_wait_expired(s, 1))
            self.assertEqual(s.cancel_many([exp]), (False,))
            with self.assertRaises(QueueTimeoutError):
                exp.result()
            fresh = s.submit_nowait("f", lambda: None)
            self.assertEqual(s.cancel_many([exp, fresh]), (False, True))
            release.set()
            hold.result()
        snap = s.snapshot()
        self.assertEqual((snap.cancelled, snap.expired), (1, 1))


class CancelManyCapacityTest(unittest.TestCase):
    def test_cancel_frees_slots_and_promotes_waiter_fifo(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            q = s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_runtime(s, unfinished=2, queued=1))

            admitted: dict[str, object] = {}

            def waiter() -> None:
                admitted["h"] = s.submit_with_wait(
                    "w", lambda: "w-result"
                )

            t = threading.Thread(target=waiter)
            t.start()
            self.assertTrue(_wait_runtime(s, admission_waiters=1))
            # 批量取消释放唯一排队名额：FIFO 等待者在同一原子边界被提升。
            self.assertEqual(s.cancel_many([q]), (True,))
            self.assertTrue(_wait_runtime(s, admission_waiters=0))
            t.join(2.0)
            self.assertFalse(t.is_alive())
            self.assertEqual(admitted.get("h"), "w-result")
            release.set()
            hold.result()
        snap = s.snapshot()
        self.assertEqual(snap.rejected, 0)
        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.completed, 2)

    def test_group_promoted_after_batch_frees_enough_slots(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            queued = [s.submit_nowait("q%d" % i, lambda: None)
                      for i in range(3)]
            self.assertTrue(_wait_runtime(s, unfinished=4, queued=3))

            group_result: dict[str, object] = {}

            def group_waiter() -> None:
                try:
                    group_result["handles"] = s.submit_batch_with_wait([
                        {"task_id": "g0", "fn": lambda: "g0"},
                        {"task_id": "g1", "fn": lambda: "g1"},
                    ])
                except BaseException as exc:  # pragma: no cover - 失败路径
                    group_result["error"] = exc

            t = threading.Thread(target=group_waiter)
            t.start()
            self.assertTrue(_wait_runtime(s, admission_waiters=1,
                                          admission_waiting_tasks=2))
            # 一次只释放 1 个名额：成组等待者仍排队。
            self.assertEqual(s.cancel_many([queued[0]]), (True,))
            self.assertTrue(_wait_runtime(s, admission_waiters=1))
            # 再释放 2 个：整组（2 任务）在同一次批量调用的原子边界内提升。
            self.assertEqual(s.cancel_many([queued[1], queued[2]]),
                             (True, True))
            self.assertTrue(_wait_runtime(s, admission_waiters=0))
            t.join(2.0)
            self.assertFalse(t.is_alive())
            handles = group_result.get("handles")
            self.assertIsNotNone(handles)
            self.assertEqual([h.result() for h in handles], ["g0", "g1"])
            release.set()
            hold.result()
        self.assertEqual(s.snapshot().rejected, 0)
        self.assertEqual(s.snapshot().cancelled, 3)

    def test_batch_itself_not_counted_as_rejected_or_accepted(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            qs = [s.submit_nowait("q%d" % i, lambda: None) for i in range(2)]
            before = s.snapshot()
            self.assertEqual(s.cancel_many(qs), (True, True))
            after = s.snapshot()
            self.assertEqual(after.accepted, before.accepted)
            self.assertEqual(after.rejected, before.rejected)
            self.assertEqual(after.cancelled, before.cancelled + 2)
            release.set()
            hold.result()


class CancelManyConcurrencyTest(unittest.TestCase):
    def test_batch_races_claims_single_cancel_and_expiry(self) -> None:
        n = 240
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
                "t%d" % i, make("t%d" % i), max_queue_wait_ms=2
            ))

        errors: list[BaseException] = []

        def batch(chunk: list[TaskHandle]) -> None:
            try:
                s.cancel_many(chunk)
            except BaseException as exc:  # pragma: no cover - 失败路径
                errors.append(exc)

        chunks = [handles[i:i + 7] for i in range(0, n, 7)]
        batch_threads = [threading.Thread(target=batch, args=(c,))
                         for c in chunks]
        single_targets = handles[::13]
        single_threads = [
            threading.Thread(target=h.cancel) for h in single_targets
        ]
        for t in batch_threads + single_threads:
            t.start()
        for t in batch_threads + single_threads:
            t.join()
        s.close()
        self.assertEqual(errors, [])

        cancelled = expired = succeeded = 0
        for h in handles:
            try:
                h.result()
                succeeded += 1
            except TaskCancelledError:
                cancelled += 1
            except QueueTimeoutError:
                expired += 1
        # 每个任务恰好一种终态。
        self.assertEqual(succeeded + cancelled + expired, n)
        snap = s.snapshot()
        self.assertEqual(
            snap.accepted,
            snap.completed + snap.failed + snap.cancelled + snap.expired,
        )
        self.assertEqual(snap.cancelled, cancelled)
        self.assertEqual(snap.expired, expired)
        self.assertEqual(snap.completed + snap.failed, succeeded)

    def test_concurrent_batches_single_outcome_per_task(self) -> None:
        n = 100
        release = threading.Event()
        s = Scheduler(workers=2, max_pending=n + 2)
        holders = [s.submit_nowait("h%d" % i, lambda: release.wait(2.0))
                   for i in range(2)]
        self.assertTrue(_wait_started(s, "h0"))
        self.assertTrue(_wait_started(s, "h1"))
        handles = [s.submit_nowait("t%d" % i, lambda: None)
                   for i in range(n)]

        outcomes: list[tuple[bool, ...]] = []
        lock = threading.Lock()

        def batch(chunk: list[TaskHandle]) -> None:
            result = s.cancel_many(chunk)
            with lock:
                outcomes.append(result)

        # 同一批句柄被两个线程以相同顺序并发裁决：每项恰有一个 True。
        t1 = threading.Thread(target=batch, args=(handles,))
        t2 = threading.Thread(target=batch, args=(handles,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        release.set()
        s.close()

        self.assertEqual(len(outcomes), 2)
        for i in range(n):
            self.assertEqual(
                outcomes[0][i] + outcomes[1][i], 1,
                "task t%d must be cancelled by exactly one batch" % i,
            )
        for h in handles:
            with self.assertRaises(TaskCancelledError):
                h.result()
        self.assertEqual(s.snapshot().cancelled, n)
        for h in holders:
            h.result()


class CancelManyCloseTest(unittest.TestCase):
    def test_close_after_cancel_many_does_not_block(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=8)
        hold = s.submit_nowait("hold", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "hold"))
        queued = [s.submit_nowait("q%d" % i, lambda: None) for i in range(4)]
        self.assertEqual(s.cancel_many(queued), (True,) * 4)
        release.set()
        s.close()
        self.assertTrue(s.runtime_snapshot().closed)
        for h in queued:
            with self.assertRaises(TaskCancelledError):
                h.result()
        hold.result()

    def test_cancel_many_races_close(self) -> None:
        for _ in range(10):
            release = threading.Event()
            s = Scheduler(workers=1, max_pending=16)
            hold = s.submit_nowait("hold", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "hold"))
            queued = [s.submit_nowait("q%d" % i, lambda: None)
                      for i in range(8)]

            ct = threading.Thread(target=s.close)
            ct.start()
            # 与 close 并发尝试取消；close 必须等待执行中的 hold。
            results = s.cancel_many(queued)
            release.set()
            self.assertTrue(ct.join(2.0) is None)
            self.assertFalse(ct.is_alive())

            for h, ok in zip(queued, results):
                if ok:
                    # True 的任务绝不执行。
                    with self.assertRaises(TaskCancelledError):
                        h.result()
                else:
                    # 未取消者必然在 close 排空阶段执行完成。
                    self.assertIsNone(h.result())
            hold.result()
            snap = s.snapshot()
            self.assertEqual(
                snap.accepted,
                snap.completed + snap.failed + snap.cancelled + snap.expired,
            )

    def test_cancel_many_after_close_returns_false_no_closed_error(self) -> None:
        s = Scheduler(workers=1, max_pending=4)
        h = s.submit_nowait("a", lambda: 42)
        self.assertEqual(h.result(), 42)
        s.close()
        # close 之后不抛 SchedulerClosedError：已完成句柄返回 False。
        self.assertEqual(s.cancel_many([h]), (False,))

    def test_validation_still_applies_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=4)
        h = s.submit_nowait("a", lambda: 42)
        self.assertEqual(h.result(), 42)
        s.close()
        with self.assertRaises(InputValidationError):
            s.cancel_many([h, h])
        with self.assertRaises(InputValidationError):
            s.cancel_many([])
        with self.assertRaises(InputValidationError):
            s.cancel_many(None)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
