"""submit_batch_with_wait 成组原子准入的验收测试。

覆盖：任务列表与字段校验、组内重复、超 max_pending；有空位立即整组接纳、
按输入顺序返回句柄；与 submit_with_wait 共用 FIFO——整组在队首时余量不
足即等待、后续单任务/小组不得绕过、队首单项先于小组；准入超时
BackpressureError（整组 rejected 只加 1）、0 时限语义、close 时整组
SchedulerClosedError 且 rejected 不变；等待期间组内 task_id 占用与
DuplicateTaskError；max_queue_wait_ms 锚定整组接纳时刻；接纳后取消/
到期/成功/失败的单项独立性、组内 priority 降序与同级输入顺序；各类
失败不留任务/统计痕迹、callable 不执行；以及并发压力下“整组要么全部
接纳、要么整组被拒、每个 callable 恰好一次”。
"""

import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    SchedulerClosedError,
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


def _wait_accepted(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if s.snapshot().accepted == n:
            return True
        time.sleep(0.005)
    return False


def _wait_admission_queue(s: Scheduler, n: int) -> bool:
    # 队列长度按“准入请求”计：一个成组请求只占一个队列项。
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with s._cond:  # type: ignore[attr-defined]
            if len(s._admission_queue) == n:  # type: ignore[attr-defined]
                return True
        time.sleep(0.002)
    return False


def _item(task_id: str, fn=None, **kw) -> dict:
    return {"task_id": task_id, "fn": fn if fn is not None else (lambda: None), **kw}


class BatchValidationTest(unittest.TestCase):
    def test_tasks_must_be_nonempty_list(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in (None, {}, (), "x", 1, {"task_id": "a", "fn": lambda: None}):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait(bad)  # type: ignore[arg-type]
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([])
        self.assertEqual(s.snapshot().accepted, 0)

    def test_batch_size_over_max_pending_rejected(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [_item("a"), _item("b"), _item("c")]
                )
            # 恰好等于 max_pending 合法。
            handles = s.submit_batch_with_wait([_item("a"), _item("b")])
            self.assertEqual(len(handles), 2)
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.rejected, 0)

    def test_element_structure_validated(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(["not-a-dict"])
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([{"fn": lambda: None}])  # 缺 task_id
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([{"task_id": "a"}])  # 缺 fn
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [{"task_id": "a", "fn": lambda: None, "extra": 1}]
                )
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([{"task_id": "", "fn": lambda: None}])
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [{"task_id": 1, "fn": lambda: None}]  # type: ignore[list-item]
                )
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [{"task_id": "a", "fn": "not-callable"}]
                )
            for bad_prio in (True, False, 1.5, "1"):
                with self.assertRaises(InputValidationError):
                    s.submit_batch_with_wait(
                        [_item("a", priority=bad_prio)]  # type: ignore[arg-type]
                    )
            for bad_wait in (0, -1, True, 1.5, "100"):
                with self.assertRaises(InputValidationError):
                    s.submit_batch_with_wait(
                        [_item("a", max_queue_wait_ms=bad_wait)]  # type: ignore[arg-type]
                    )

    def test_duplicate_within_batch_is_input_error(self) -> None:
        called = threading.Event()
        with Scheduler(workers=2, max_pending=4) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [_item("a", called.set), _item("b"),
                     _item("a", called.set)]
                )
        self.assertFalse(called.wait(0.05))
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.rejected), (0, 0, 0)
        )

    def test_invalid_admission_timeout_ms(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in (True, False, -1, -100, 1.5, "100", 0.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait(
                            [_item("a")], admission_timeout_ms=bad  # type: ignore[arg-type]
                        )
        self.assertEqual(s.snapshot().accepted, 0)

    def test_none_zero_positive_timeout_when_capacity_free(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            h1 = s.submit_batch_with_wait(
                [_item("a", lambda: 1)], admission_timeout_ms=None
            )
            h2 = s.submit_batch_with_wait(
                [_item("b", lambda: 2)], admission_timeout_ms=0
            )
            h3 = s.submit_batch_with_wait(
                [_item("c", lambda: 3), _item("d", lambda: 4)],
                admission_timeout_ms=60_000,
            )
            self.assertEqual([h.result() for h in h1], [1])
            self.assertEqual([h.result() for h in h2], [2])
            self.assertEqual([h.result() for h in h3], [3, 4])

    def test_validation_precedes_close_check(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit_batch_with_wait([_item("a")], admission_timeout_ms=True)
        with self.assertRaises(InputValidationError):
            s.submit_batch_with_wait([])
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait([_item("a")])
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait([_item("a")], admission_timeout_ms=0)


class ImmediateBatchAdmissionTest(unittest.TestCase):
    def test_admitted_immediately_returns_handles_in_input_order(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            handles = s.submit_batch_with_wait(
                [_item("a", lambda: "A"),
                 _item("b", lambda: "B"),
                 _item("c", lambda: "C")]
            )
            self.assertIsInstance(handles, tuple)
            self.assertEqual(len(handles), 3)
            self.assertTrue(all(isinstance(h, TaskHandle) for h in handles))
            snap = s.snapshot()
            # 接纳是原子的：返回时 accepted 已增加整组任务数。
            self.assertEqual(snap.accepted, 3)
            self.assertEqual([h.result() for h in handles], ["A", "B", "C"])

    def test_default_priority_and_queue_wait(self) -> None:
        with Scheduler(workers=4, max_pending=8) as s:
            handles = s.submit_batch_with_wait(
                [_item("t%d" % i, lambda i=i: i) for i in range(5)]
            )
            self.assertEqual([h.result() for h in handles], list(range(5)))
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.completed, snap.failed), (5, 5, 0))
        self.assertEqual(len(snap._queue_wait_samples), 5)
        self.assertEqual(len(snap._total_latency_samples), 5)

    def test_item_failure_propagates_original_exception_only_for_that_item(self) -> None:
        def boom() -> None:
            raise ValueError("x")

        with Scheduler(workers=2, max_pending=4) as s:
            handles = s.submit_batch_with_wait(
                [_item("ok1", lambda: "ok1"),
                 _item("boom", boom),
                 _item("ok2", lambda: "ok2")]
            )
            self.assertEqual(handles[0].result(), "ok1")
            with self.assertRaises(ValueError) as cm:
                handles[1].result()
            self.assertIs(type(cm.exception), ValueError)
            self.assertEqual(str(cm.exception), "x")
            self.assertEqual(handles[2].result(), "ok2")
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.completed, snap.failed), (3, 2, 1))

    def test_cancel_one_item_does_not_affect_others(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        c1_called = threading.Event()

        def hold() -> None:
            entered.set()
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=3) as s:
            handles = s.submit_batch_with_wait(
                [_item("c0", hold),
                 _item("c1", c1_called.set),
                 _item("c2", lambda: "c2")]
            )
            self.assertTrue(entered.wait(2.0))
            self.assertTrue(handles[1].cancel())
            with self.assertRaises(TaskCancelledError):
                handles[1].result()
            self.assertFalse(handles[1].cancel())  # 终态后再取消返回 False
            release.set()
            self.assertEqual(handles[0].result(), None)
            self.assertEqual(handles[2].result(), "c2")
        self.assertFalse(c1_called.wait(0.05))
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.cancelled), (3, 2, 1)
        )

    def test_priority_desc_then_input_order_within_group(self) -> None:
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                time.sleep(0.005)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=6) as s:
            handles = s.submit_batch_with_wait(
                [_item("a", rec("a"), priority=0),
                 _item("b", rec("b"), priority=5),
                 _item("c", rec("c"), priority=5),
                 _item("d", rec("d"), priority=-1)]
            )
            self.assertEqual([h.result() for h in handles],
                             ["a", "b", "c", "d"])
        self.assertEqual(ran, ["b", "c", "a", "d"])

    def test_group_items_compete_in_global_priority_order(self) -> None:
        # 组与先接受的排队任务之间不享有成组亲和：高优先级组项先派发。
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str, hold: bool = False):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                if hold:
                    entered.set()
                    release.wait(2.0)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=4) as s:
            s.submit_nowait("h", rec("h", hold=True), priority=0)
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", rec("q"), priority=1)
            handles = s.submit_batch_with_wait(
                [_item("ghi", rec("ghi"), priority=10),
                 _item("glo", rec("glo"), priority=0)]
            )
            release.set()
            self.assertEqual([h.result() for h in handles], ["ghi", "glo"])
        self.assertEqual(ran, ["h", "ghi", "q", "glo"])


class FifoAtomicAdmissionTest(unittest.TestCase):
    def test_group_at_head_blocks_later_single_despite_free_slot(self) -> None:
        # max_pending=3, workers=1：h 执行中、q1/q2 排队占满；随后组(g0,g1)
        # 与单任务 s 排队。h 完成后只有 1 个空位，组需 2 个：组不得被提升，
        # s 也不得绕过组插队。q1 完成后空出 2 席，组整组接纳。
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str, hold: bool = False):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                if hold:
                    entered.set()
                    release.wait(2.0)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=3) as s:
            th = threading.Thread(target=s.submit, args=("h", rec("h", True)))
            th.start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q1", rec("q1"))
            s.submit_nowait("q2", rec("q2"))
            self.assertTrue(_wait_accepted(s, 3))

            def group_call() -> None:
                group_result["h"] = s.submit_batch_with_wait(
                    [_item("g0", rec("g0")), _item("g1", rec("g1"))],
                    admission_timeout_ms=5_000,
                )

            group_result: dict[str, tuple] = {}
            tg = threading.Thread(target=group_call)
            tg.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            ts = threading.Thread(
                target=lambda: single_result.setdefault(
                    "v", s.submit_with_wait("s", rec("s"),
                                            admission_timeout_ms=5_000)
                )
            )
            single_result: dict[str, str] = {}
            ts.start()
            self.assertTrue(_wait_admission_queue(s, 2))
            self.assertEqual(s.snapshot().accepted, 3)

            release.set()
            th.join()
            tg.join()
            ts.join()
            self.assertEqual(
                [h.result() for h in group_result["h"]], ["g0", "g1"]
            )
            self.assertEqual(single_result, {"v": "s"})

        # 严格 FIFO + 整组原子：g0/g1 必须都在 s 之前执行。
        self.assertEqual(ran, ["h", "q1", "q2", "g0", "g1", "s"])

    def test_single_at_head_served_before_group_behind(self) -> None:
        # max_pending=2：h 执行、q 排队；单任务 w 在队首，组(g0,g1) 在其后。
        # 每次只释放 1 席：w 先获容；组必须等到两个席位齐备。
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str, hold: bool = False):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                if hold:
                    entered.set()
                    release.wait(2.0)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=2) as s:
            th = threading.Thread(target=s.submit, args=("h", rec("h", True)))
            th.start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", rec("q"))
            self.assertTrue(_wait_accepted(s, 2))

            tw = threading.Thread(
                target=lambda: s.submit_with_wait("w", rec("w"),
                                                  admission_timeout_ms=5_000)
            )
            tw.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            tg = threading.Thread(
                target=lambda: group_result.setdefault(
                    "h",
                    s.submit_batch_with_wait(
                        [_item("g0", rec("g0")), _item("g1", rec("g1"))],
                        admission_timeout_ms=5_000,
                    ),
                )
            )
            group_result: dict[str, tuple] = {}
            tg.start()
            self.assertTrue(_wait_admission_queue(s, 2))

            release.set()
            th.join()
            tw.join()
            tg.join()
            self.assertEqual(
                [h.result() for h in group_result["h"]], ["g0", "g1"]
            )

        self.assertEqual(ran, ["h", "q", "w", "g0", "g1"])

    def test_smaller_group_behind_cannot_bypass_head_group(self) -> None:
        # 队首组需 3 席，其后的单任务始终空闲 1-2 席也不得绕过；
        # 三席齐备时队首整组先接纳，单任务最后。
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str, hold: bool = False):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                if hold:
                    entered.set()
                    release.wait(2.0)
                return tid
            return fn

        s = Scheduler(workers=1, max_pending=3)
        s.submit_nowait("h", rec("h", hold=True))
        self.assertTrue(entered.wait(2.0))

        def big_group() -> None:
            try:
                handles = s.submit_batch_with_wait(
                    [_item("b0", rec("b0")),
                     _item("b1", rec("b1")),
                     _item("b2", rec("b2"))],
                    admission_timeout_ms=5_000,
                )
                out["b"] = [h.result() for h in handles]
            except Exception as exc:  # pragma: no cover - 不应发生
                out["b"] = type(exc).__name__

        out: dict[str, object] = {}
        tb = threading.Thread(target=big_group)
        tb.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        ts = threading.Thread(
            target=lambda: out.setdefault(
                "s", s.submit_with_wait("s", rec("s"),
                                        admission_timeout_ms=5_000)
            )
        )
        ts.start()
        self.assertTrue(_wait_admission_queue(s, 2))

        release.set()
        tb.join()
        ts.join()
        s.close()
        self.assertEqual(out["b"], ["b0", "b1", "b2"])
        self.assertEqual(out["s"], "s")
        self.assertEqual(ran, ["h", "b0", "b1", "b2", "s"])


class BatchTimeoutTest(unittest.TestCase):
    def test_zero_rejected_immediately_when_group_cannot_fit(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))
            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [_item("a", called.set), _item("b", called.set)],
                    admission_timeout_ms=0,
                )
            self.assertLess(time.monotonic() - start, 0.1)
            release.set()
        self.assertFalse(called.wait(0.05))
        self.assertEqual(s.snapshot().rejected, 1)
        for tid in ("a", "b"):
            with self.assertRaises(KeyError):
                s.result(tid)

    def test_zero_admitted_when_whole_group_fits(self) -> None:
        with Scheduler(workers=2, max_pending=3) as s:
            handles = s.submit_batch_with_wait(
                [_item("a", lambda: 1), _item("b", lambda: 2)],
                admission_timeout_ms=0,
            )
            self.assertEqual([h.result() for h in handles], [1, 2])
        self.assertEqual(s.snapshot().rejected, 0)

    def test_timeout_counts_rejected_once_for_whole_group(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: None)
            s.submit_nowait("r", lambda: None)
            self.assertTrue(_wait_accepted(s, 3))
            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [_item("a", called.set),
                     _item("b", called.set),
                     _item("c", called.set)],
                    admission_timeout_ms=80,
                )
            elapsed = time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.07)
            self.assertLess(elapsed, 0.5)
            snap = s.snapshot()
            # 整组拒绝只计一次 rejected，accepted 不含组内任何任务。
            self.assertEqual((snap.accepted, snap.rejected), (3, 1))
            for tid in ("a", "b", "c"):
                with self.assertRaises(KeyError):
                    s.result(tid)
            release.set()
        self.assertFalse(called.wait(0.05))

    def test_timed_out_group_releases_ids_for_reuse(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [_item("z", lambda: "old")], admission_timeout_ms=20
                )
            release.set()
            handles = s.submit_batch_with_wait(
                [_item("z", lambda: "again")], admission_timeout_ms=2_000
            )
            self.assertEqual(handles[0].result(), "again")

    def test_group_timeout_frees_head_for_next_waiter(self) -> None:
        # 大小为 1 的组超时出队后，空出的队首单项在 h 结束时正常获容。
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [_item("x", lambda: None)], admission_timeout_ms=30
                )
            result: dict[str, str] = {}
            tw = threading.Thread(
                target=lambda: result.setdefault(
                    "v", s.submit_with_wait("w", lambda: "W",
                                            admission_timeout_ms=5_000)
                )
            )
            tw.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            release.set()
            tw.join()
        self.assertEqual(result, {"v": "W"})
        self.assertEqual(s.snapshot().rejected, 1)


class BatchCloseWhileWaitingTest(unittest.TestCase):
    def test_waiting_group_rejected_on_close_without_rejected_count(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        s.submit_nowait("q", lambda: None)
        self.assertTrue(_wait_accepted(s, 2))

        done = threading.Event()

        def group_waiter() -> None:
            try:
                s.submit_batch_with_wait(
                    [_item("g0", called.set), _item("g1", called.set)],
                    admission_timeout_ms=None,
                )
            except SchedulerClosedError:
                done.set()

        t = threading.Thread(target=group_waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        s.close()
        self.assertTrue(done.wait(2.0))
        t.join()
        self.assertFalse(called.wait(0.05))
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.rejected, 0)
        for tid in ("g0", "g1"):
            with self.assertRaises(KeyError):
                s.result(tid)
        release.set()

    def test_close_runs_admitted_group_to_completion(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=3)
        s.submit_nowait("a", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "a"))
        handles = s.submit_batch_with_wait(
            [_item("b", lambda: "b"), _item("c", lambda: "c")]
        )
        closing = threading.Thread(target=s.close)
        closing.start()
        release.set()
        closing.join()
        self.assertEqual([h.result() for h in handles], ["b", "c"])
        self.assertEqual(s.result("b"), "b")

    def test_batch_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait([_item("a", lambda: None)])
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait(
                [_item("a", lambda: None)], admission_timeout_ms=0
            )
        self.assertEqual(s.snapshot().accepted, 0)
        self.assertEqual(s.snapshot().rejected, 0)


class BatchDuplicateWhileWaitingTest(unittest.TestCase):
    def test_group_ids_block_other_submissions_while_waiting(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))

            tw = threading.Thread(
                target=lambda: s.submit_batch_with_wait(
                    [_item("dup", lambda: "first"),
                     _item("other", lambda: "second")]
                )
            )
            tw.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            with self.assertRaises(DuplicateTaskError):
                s.submit("dup", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("other", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_with_wait("dup", lambda: None,
                                   admission_timeout_ms=None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_batch_with_wait([_item("dup", lambda: None)])
            self.assertEqual(s.snapshot().rejected, 0)

            release.set()
            tw.join()
            # 句柄在整组接纳后即返回；等待组内任务实际执行结束再读结果。
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().completed == 4:
                    break
                time.sleep(0.005)
            self.assertEqual(s.result("dup"), "first")
            self.assertEqual(s.result("other"), "second")

    def test_conflicting_batch_does_not_reserve_any_of_its_ids(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        s.submit_nowait("q", lambda: None)
        self.assertTrue(_wait_accepted(s, 2))

        def first_group() -> None:
            try:
                s.submit_batch_with_wait(
                    [_item("a0", lambda: None), _item("a1", lambda: None)],
                    admission_timeout_ms=None,
                )
            except SchedulerClosedError:
                pass

        t = threading.Thread(target=first_group)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        # 第二组与等待中的组同名：整组失败，z 不得被登记。
        with self.assertRaises(DuplicateTaskError):
            s.submit_batch_with_wait(
                [_item("a1", lambda: None), _item("z", lambda: None)]
            )
        s.close()
        t.join()
        for tid in ("a0", "a1", "z"):
            with self.assertRaises(KeyError):
                s.result(tid)
        release.set()

    def test_duplicate_against_running_task(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("r", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(DuplicateTaskError):
                s.submit_batch_with_wait(
                    [_item("x", lambda: None), _item("r", lambda: None)]
                )
            self.assertEqual(s.snapshot().accepted, 1)
            release.set()


class BatchQueueDeadlineTest(unittest.TestCase):
    def test_group_queue_deadline_anchored_at_group_admission(self) -> None:
        # 两个任务都带 100ms 排队时限，但整组在约 300ms 后才获接纳：
        # 时限从整组接纳时刻起算，两任务都应被及时认领并成功。
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=2, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        result: dict[str, list] = {}

        def waiter() -> None:
            handles = s.submit_batch_with_wait(
                [_item("w0", lambda: "w0", max_queue_wait_ms=100),
                 _item("w1", lambda: "w1", max_queue_wait_ms=100)],
                admission_timeout_ms=5_000,
            )
            result["v"] = [h.result() for h in handles]

        t = threading.Thread(target=waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        time.sleep(0.30)  # 已超过各自 100ms，但整组尚未获名额
        release.set()
        t.join()
        s.close()
        self.assertEqual(result, {"v": ["w0", "w1"]})

    def test_one_item_expires_after_admission_others_unaffected(self) -> None:
        # 整组接纳后 w1 认领前到期：w1 不执行，释放名额并接纳排队者 s；
        # w0/w2 与 s 各自正常完成，单项终态互不影响。
        entered = threading.Event()
        release = threading.Event()
        w1_called = threading.Event()
        s = Scheduler(workers=1, max_pending=3)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        result: dict[str, object] = {}

        def w0_fn() -> str:
            time.sleep(0.15)
            return "w0"

        def group_waiter() -> None:
            handles = s.submit_batch_with_wait(
                [_item("w0", w0_fn),
                 _item("w1", w1_called.set, max_queue_wait_ms=30),
                 _item("w2", lambda: "w2")],
                admission_timeout_ms=5_000,
            )
            ends: list[str] = []
            for tid, h in zip(("w0", "w1", "w2"), handles):
                try:
                    ends.append("%s=%r" % (tid, h.result()))
                except QueueTimeoutError:
                    ends.append("%s=expired" % tid)
            result["g"] = ends

        tg = threading.Thread(target=group_waiter)
        tg.start()
        self.assertTrue(_wait_admission_queue(s, 1))

        def single_waiter() -> None:
            result["s"] = s.submit_with_wait("s", lambda: "s",
                                             admission_timeout_ms=5_000)

        ts = threading.Thread(target=single_waiter)
        ts.start()
        self.assertTrue(_wait_admission_queue(s, 2))

        release.set()
        tg.join()
        ts.join()
        s.close()
        self.assertFalse(w1_called.wait(0.05))
        self.assertEqual(result["g"], ["w0='w0'", "w1=expired", "w2='w2'"])
        self.assertEqual(result["s"], "s")
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 5)
        self.assertEqual(snap.expired, 1)
        self.assertEqual(
            (snap.completed, snap.failed, snap.cancelled), (4, 0, 0)
        )
        with self.assertRaises(QueueTimeoutError):
            s.result("w1")


class BatchStatsTest(unittest.TestCase):
    def test_rejected_group_leaves_no_samples_or_counts(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            cp = s.stats_checkpoint()
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [_item("x", lambda: None), _item("y", lambda: None)],
                    admission_timeout_ms=30,
                )
            release.set()
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.completed, 2)
        self.assertEqual(
            (snap.failed, snap.cancelled, snap.expired), (0, 0, 0)
        )
        interval = s.snapshot_since(cp)
        self.assertEqual((interval.accepted, interval.rejected), (2, 1))
        self.assertEqual(len(interval._queue_wait_samples), 2)
        self.assertEqual(len(interval._total_latency_samples), 2)

    def test_group_accepted_atomically_into_interval(self) -> None:
        with Scheduler(workers=4, max_pending=8) as s:
            cp = s.stats_checkpoint()
            handles = s.submit_batch_with_wait(
                [_item("t%d" % i, lambda i=i: i) for i in range(4)]
            )
            self.assertEqual([h.result() for h in handles], [0, 1, 2, 3])
            interval = s.snapshot_since(cp)
        self.assertEqual(interval.accepted, 4)
        self.assertEqual(interval.completed, 4)
        self.assertEqual(interval.rejected, 0)


class BatchStressTest(unittest.TestCase):
    def test_group_all_or_nothing_and_callable_runs_once(self) -> None:
        # 混合单任务与 1-3 元组、短准入时限：每个组要么整组接纳（全部句柄
        # 最终结束、callable 各执行一次），要么整组 BackpressureError；
        # 不允许部分接纳。
        n = 200
        s = Scheduler(workers=3, max_pending=4)
        call_counts: dict[str, int] = {}
        clock = threading.Lock()
        outcomes: dict[int, str] = {}
        olock = threading.Lock()
        counter = {"id": 0}

        def make_fn(tid: str):
            def fn() -> str:
                with clock:
                    call_counts[tid] = call_counts.get(tid, 0) + 1
                time.sleep(0.001)
                return tid
            return fn

        def caller(index: int) -> None:
            with clock:
                size = counter["id"] % 3 + 1
                counter["id"] += size
            tids = ["g%d-%d" % (index, j) for j in range(size)]
            tasks = [_item(tid, make_fn(tid)) for tid in tids]
            try:
                handles = s.submit_batch_with_wait(tasks, admission_timeout_ms=30)
            except BackpressureError:
                with olock:
                    outcomes[index] = "rejected:%d" % size
                return
            values = [h.result() for h in handles]
            with olock:
                outcomes[index] = "ok:%s" % ",".join(values)

        threads = [threading.Thread(target=caller, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        s.close()

        self.assertEqual(len(outcomes), n)
        accepted_items = 0
        rejected_groups = 0
        accepted_ids: set[str] = set()
        for outcome in outcomes.values():
            if outcome.startswith("rejected:"):
                rejected_groups += 1
                continue
            values = outcome[len("ok:"):].split(",")
            accepted_items += len(values)
            accepted_ids.update(values)
            for tid in values:
                self.assertEqual(call_counts.get(tid), 1)
        # 每个被执行的 callable 都属于被接纳的任务且恰好一次。
        self.assertEqual(set(call_counts), accepted_ids)

        snap = s.snapshot()
        self.assertEqual(snap.accepted, accepted_items)
        self.assertEqual(snap.rejected, rejected_groups)
        self.assertEqual(
            snap.completed + snap.failed + snap.cancelled + snap.expired,
            accepted_items,
        )
        self.assertEqual(len(call_counts), accepted_items)


if __name__ == "__main__":
    unittest.main()
