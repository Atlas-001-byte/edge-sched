"""Scheduler.cancel_many 批量取消的验收测试。

覆盖：容器类型/空集合/非 TaskHandle/其他调度器句柄/重复句柄（按对象身份）
校验且失败时不改变任务、计数与延迟样本；整批在同一原子边界按输入顺序
裁决并返回等长同序布尔元组；已接纳未认领任务取消成功（callable 不执行、
TaskCancelledError、accepted/cancelled 各计一次、不贡献延迟样本、名额
立即释放并按 FIFO 提升等待者）；已开始/已结束/已取消/已到期句柄返回
False；混合批次互不影响；与认领、到期、单项取消交错只有唯一结果；
close 并发与 close 之后调用返回 False 而不抛 SchedulerClosedError；
批量调用不计入 accepted/rejected 或延迟分布。
"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    TaskCancelledError,
)


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


def _wait_admission_queue(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if s.runtime_snapshot().admission_waiters == n:
            return True
        time.sleep(0.002)
    return False


def _samples(snap: object, kind: str) -> list:
    attr = {
        "wait": "_queue_wait_samples",
        "total": "_total_latency_samples",
        "exec": "_execution_samples",
        "admission": "_admission_wait_samples",
    }[kind]
    return getattr(snap, attr)


class CancelManyValidationTest(unittest.TestCase):
    def test_container_must_be_non_empty_list_or_tuple(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in ([], (), {}, set(), "x", 123, None, True,
                        {"a": 1}):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.cancel_many(bad)  # type: ignore[arg-type]
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.cancelled, snap.rejected), (0, 0, 0)
        )

    def test_elements_must_be_task_handles(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("block", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "block"))
            handle = s.submit_nowait("a", lambda: "a")
            for bad in ([None], [42], ["h"], [handle, None],
                        [object()], [{"task_id": "a"}]):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.cancel_many(bad)  # type: ignore[arg-type]
            # 校验失败不改变任务与计数，合法句柄未被波及。
            self.assertEqual(s.snapshot().cancelled, 0)
            self.assertFalse(handle.done())
            self.assertTrue(handle.cancel())
            release.set()

    def test_handles_from_other_scheduler_rejected(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s1, \
                Scheduler(workers=1, max_pending=2) as s2:
            s1.submit_nowait("b1", lambda: release.wait(2.0))
            s2.submit_nowait("b2", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s1, "b1"))
            self.assertTrue(_wait_started(s2, "b2"))
            local = s1.submit_nowait("a", lambda: "a")
            foreign = s2.submit_nowait("f", lambda: "f")
            with self.assertRaises(InputValidationError):
                s1.cancel_many([foreign])
            with self.assertRaises(InputValidationError):
                s1.cancel_many([local, foreign])
            # 校验失败不改变任何一侧的任务与计数。
            self.assertEqual(s1.snapshot().cancelled, 0)
            self.assertEqual(s2.snapshot().cancelled, 0)
            self.assertFalse(foreign.done())
            self.assertFalse(local.done())
            self.assertTrue(local.cancel())
            self.assertTrue(foreign.cancel())
            release.set()

    def test_duplicate_handles_rejected_by_identity(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("block", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "block"))
            ha = s.submit_nowait("a", lambda: "a")
            hb = s.submit_nowait("b", lambda: "b")
            with self.assertRaises(InputValidationError):
                s.cancel_many([ha, ha])
            with self.assertRaises(InputValidationError):
                s.cancel_many((ha, hb, ha))
            # 校验失败整批不生效：两个任务都未被取消。
            self.assertEqual(s.snapshot().cancelled, 0)
            self.assertFalse(ha.done())
            self.assertFalse(hb.done())
            self.assertEqual(s.cancel_many([ha, hb]), (True, True))
            release.set()

    def test_validation_failure_leaves_no_stats_trace(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("block", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "block"))
            handle = s.submit_nowait("a", lambda: "a")
            before = s.snapshot().to_dict()
            for bad in ([], [handle, handle], [None], "nope"):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.cancel_many(bad)  # type: ignore[arg-type]
            self.assertEqual(s.snapshot().to_dict(), before)
            self.assertTrue(handle.cancel())
            release.set()


class CancelManyBasicTest(unittest.TestCase):
    def test_batch_cancel_queued_tasks(self) -> None:
        release = threading.Event()
        ran: list[str] = []
        lock = threading.Lock()

        def record(tid: str) -> "object":
            def fn() -> str:
                with lock:
                    ran.append(tid)
                return tid
            return fn

        # 单工作线程：a 占住线程，b/c/d 排队等待，批量取消必然先于执行。
        with Scheduler(workers=1, max_pending=8) as s:
            ha = s.submit_nowait("a", lambda: release.wait(2.0))
            hb = s.submit_nowait("b", record("b"))
            hc = s.submit_nowait("c", record("c"))
            hd = s.submit_nowait("d", record("d"))
            self.assertTrue(_wait_started(s, "a"))

            result = s.cancel_many([hb, hc, hd])
            self.assertIsInstance(result, tuple)
            self.assertEqual(result, (True, True, True))
            for h in (hb, hc, hd):
                self.assertTrue(h.done())
                with self.assertRaises(TaskCancelledError):
                    h.result()
            # 重复批量取消返回 False，不重复计数。
            self.assertEqual(s.cancel_many([hb, hc, hd]),
                             (False, False, False))

            release.set()
            self.assertTrue(ha.result())

        self.assertEqual(ran, [])
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 4)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.cancelled, 3)
        self.assertEqual(snap.expired, 0)
        self.assertEqual(snap.rejected, 0)
        # 取消任务不贡献任何延迟样本（仅完成的 a 贡献）。
        self.assertEqual(len(_samples(snap, "wait")), 1)
        self.assertEqual(len(_samples(snap, "total")), 1)
        self.assertEqual(len(_samples(snap, "exec")), 1)

    def test_tuple_container_accepted(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("block", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "block"))
            ha = s.submit_nowait("a", lambda: "a")
            hb = s.submit_nowait("b", lambda: "b")
            self.assertEqual(s.cancel_many((ha, hb)), (True, True))
            release.set()
        self.assertEqual(s.snapshot().cancelled, 2)

    def test_mixed_batch_adjudicated_in_input_order(self) -> None:
        release = threading.Event()

        def slow() -> str:
            release.wait(2.0)
            return "done"

        with Scheduler(workers=1, max_pending=8) as s:
            running = s.submit_nowait("run", slow)
            self.assertTrue(_wait_started(s, "run"))
            q1 = s.submit_nowait("q1", lambda: "q1")
            q2 = s.submit_nowait("q2", lambda: "q2")
            keep = s.submit_nowait("keep", lambda: "keep")
            pre_cancelled = s.submit_nowait("cx", lambda: "cx")
            self.assertTrue(pre_cancelled.cancel())

            result = s.cancel_many([q1, running, pre_cancelled, q2])
            self.assertEqual(result, (True, False, False, True))
            for h in (q1, q2):
                with self.assertRaises(TaskCancelledError):
                    h.result()
            # 批次之外的句柄不受影响。
            self.assertFalse(keep.done())
            release.set()
            self.assertEqual(running.result(), "done")
            self.assertEqual(keep.result(), "keep")
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled, snap.completed),
                         (5, 3, 2))

    def test_batch_does_not_touch_other_tasks(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            victim = s.submit_nowait("v", lambda: "v")
            bystander = s.submit_nowait("keep", lambda: "keep")
            self.assertEqual(s.cancel_many([victim]), (True,))
            self.assertFalse(bystander.done())
            release.set()
            self.assertEqual(bystander.result(), "keep")
        snap = s.snapshot()
        self.assertEqual((snap.completed, snap.cancelled), (2, 1))

    def test_batch_cancel_not_counted_as_accepted_or_rejected(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            handles = [
                s.submit_nowait("t%d" % i, lambda: None) for i in range(3)
            ]
            before = s.snapshot()
            self.assertEqual(s.cancel_many(handles), (True, True, True))
            after = s.snapshot()
            # accepted 只来自提交；批量取消不新增 accepted/rejected，
            # 也不贡献准入或执行延迟样本。
            self.assertEqual(after.accepted, before.accepted)
            self.assertEqual(after.rejected, before.rejected)
            self.assertEqual(after.cancelled, before.cancelled + 3)
            self.assertEqual(
                len(_samples(after, "admission")),
                len(_samples(before, "admission")),
            )
            self.assertEqual(
                len(_samples(after, "exec")), len(_samples(before, "exec"))
            )
            release.set()


class CancelManyCapacityTest(unittest.TestCase):
    def test_freed_slots_promote_waiters_fifo(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            q1 = s.submit_nowait("q1", lambda: "q1")
            self.assertTrue(_wait_pending(s, 2))

            admitted: list[str] = []

            def wait_submit() -> None:
                admitted.append(s.submit_with_wait("w", lambda: "w"))

            t = threading.Thread(target=wait_submit)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            # 批量取消释放名额：队首等待者 w 按 FIFO 被提升接纳。
            self.assertEqual(s.cancel_many([q1]), (True,))
            release.set()
            t.join(2.0)
            self.assertFalse(t.is_alive())
            self.assertEqual(admitted, ["w"])
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled, snap.completed),
                         (3, 1, 2))

    def test_capacity_available_immediately_after_batch(self) -> None:
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            queued = s.submit_nowait("b", lambda: "b")
            self.assertTrue(_wait_pending(s, 2))
            with self.assertRaises(BackpressureError):
                s.submit_nowait("c", lambda: None)
            self.assertEqual(s.cancel_many([queued]), (True,))
            hc = s.submit_nowait("c", lambda: "c")
            release.set()
            self.assertEqual(hc.result(), "c")
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled, snap.completed),
                         (3, 1, 2))


class CancelManyRaceTest(unittest.TestCase):
    def test_cancel_many_vs_start_race_single_outcome(self) -> None:
        # 大量任务在“即将开始”窗口与批量取消竞争：每个任务必须恰好
        # 有一种终态——要么执行到底，要么被取消，绝不两者兼得。
        for _ in range(5):
            with Scheduler(workers=2, max_pending=64) as s:
                n = 24
                handles = [
                    s.submit_nowait("t%d" % i, lambda i=i: i)
                    for i in range(n)
                ]

                def race_cancel() -> None:
                    s.cancel_many(handles)

                canceler = threading.Thread(target=race_cancel)
                canceler.start()
                canceler.join()

                cancelled_count = 0
                for h in handles:
                    try:
                        h.result()
                    except TaskCancelledError:
                        cancelled_count += 1
                snap = s.snapshot()
                self.assertEqual(snap.completed + snap.cancelled, n)
                self.assertEqual(snap.failed, 0)
                self.assertEqual(snap.cancelled, cancelled_count)

    def test_cancel_many_vs_single_cancel_same_handles(self) -> None:
        # 单项取消与批量取消竞争同一句柄：每句柄恰好一方成功。
        release = threading.Event()
        with Scheduler(workers=1, max_pending=8) as s:
            s.submit_nowait("a", lambda: release.wait(2.0))
            self.assertTrue(_wait_started(s, "a"))
            handles = [
                s.submit_nowait("t%d" % i, lambda: None) for i in range(4)
            ]
            results: list[tuple] = []

            def batch() -> None:
                results.append(s.cancel_many(handles))

            t = threading.Thread(target=batch)
            t.start()
            singles = [h.cancel() for h in handles]
            t.join(2.0)
            self.assertFalse(t.is_alive())
            for single_ok, batch_ok in zip(singles, results[0]):
                self.assertNotEqual(single_ok, batch_ok)
            for h in handles:
                with self.assertRaises(TaskCancelledError):
                    h.result()
            release.set()
        self.assertEqual(s.snapshot().cancelled, 4)

    def test_cancel_many_vs_expiry_single_outcome(self) -> None:
        # 排队到期与批量取消竞争：每个任务恰入一种终态。
        with Scheduler(workers=1, max_pending=64) as s:
            s.submit_nowait("block", lambda: time.sleep(0.3))
            self.assertTrue(_wait_started(s, "block"))
            handles = [
                s.submit_nowait("e%d" % i, lambda: None,
                                max_queue_wait_ms=50)
                for i in range(8)
            ]
            time.sleep(0.08)
            result = s.cancel_many(handles)
            for h, ok in zip(handles, result):
                if ok:
                    with self.assertRaises(TaskCancelledError):
                        h.result()
                else:
                    with self.assertRaises(QueueTimeoutError):
                        h.result()
            snap = s.snapshot()
            self.assertEqual(snap.cancelled, sum(1 for ok in result if ok))
            self.assertEqual(snap.expired, len(handles) - snap.cancelled)


class CancelManyCloseTest(unittest.TestCase):
    def test_cancel_many_during_close_still_works(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=8)
        s.submit_nowait("a", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "a"))
        queued = [s.submit_nowait("q%d" % i, lambda: i) for i in range(3)]

        closer = threading.Thread(target=s.close)
        closer.start()
        # close 开始后、执行中任务结束前：已排队句柄仍可被批量取消。
        result = s.cancel_many(queued)
        self.assertEqual(result, (True, True, True))
        release.set()
        closer.join(2.0)
        self.assertFalse(closer.is_alive())
        snap = s.snapshot()
        self.assertEqual((snap.completed, snap.cancelled), (1, 3))

    def test_cancel_many_after_close_returns_false(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=4)
        s.submit_nowait("block", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "block"))
        done = s.submit_nowait("a", lambda: 1)
        queued = s.submit_nowait("b", lambda: 2)
        # 工作线程仍被 block 占住：b 必然处于排队态，取消必然成功。
        self.assertTrue(queued.cancel())
        release.set()
        self.assertEqual(done.result(), 1)
        s.close()
        # close 之后：不抛 SchedulerClosedError，已终态句柄返回 False。
        self.assertEqual(s.cancel_many([done, queued]), (False, False))
        # 参数校验在 close 之后仍然生效。
        with self.assertRaises(InputValidationError):
            s.cancel_many([])
        with self.assertRaises(InputValidationError):
            s.cancel_many([done, done])

    def test_cancel_many_does_not_block_close(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=4)
        s.submit_nowait("block", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "block"))
        handle = s.submit_nowait("a", lambda: "a")
        self.assertEqual(s.cancel_many([handle]), (True,))
        release.set()
        start = time.monotonic()
        s.close()
        self.assertLess(time.monotonic() - start, 2.0)


if __name__ == "__main__":
    unittest.main()
