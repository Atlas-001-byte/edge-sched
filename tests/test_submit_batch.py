"""submit_batch_with_wait 成组原子准入的验收测试。

覆盖：任务列表与单项字段校验、组内重复、超 max_pending；整组立即/0/正超时
接纳；余量只够部分时整组等待且 accepted 不增加；与 submit_with_wait 共用
FIFO 队列，后续单任务/小组不得绕过队首大组；接纳后组内按 priority 降序、
同级按输入顺序派发；等待期间组内 task_id 占用（同名 DuplicateTaskError）；
超时整组被拒（rejected 恰加 1）、close 时整组 SchedulerClosedError
（rejected 不变）；任何失败不建任务、不执行 callable、不留延迟样本；
max_queue_wait_ms 自实际接纳时刻起算；组内单项的成功/失败/取消/到期互不
影响；snapshot/snapshot_since 的计数口径不变。
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
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with s._cond:  # type: ignore[attr-defined]
            if len(s._admission_queue) == n:  # type: ignore[attr-defined]
                return True
        time.sleep(0.002)
    return False


class BatchValidationTest(unittest.TestCase):
    def test_tasks_must_be_non_empty_list(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in ([], (), {}, "x", 123, None, True):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait(bad)  # type: ignore[arg-type]
        self.assertEqual(s.snapshot().accepted, 0)

    def test_element_must_be_dict_with_required_fields(self) -> None:
        with Scheduler(workers=1, max_pending=3) as s:
            for bad in (
                [["not-dict"]],
                [42],
                [{}],
                [{"fn": lambda: None}],
                [{"task_id": "a"}],
                [{"task_id": "a", "fn": lambda: None}, "nope"],
            ):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait(bad)  # type: ignore[arg-type]

    def test_unknown_fields_rejected(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([
                    {"task_id": "a", "fn": lambda: None, "timeout": 1},
                ])

    def test_per_item_field_validation_matches_submit(self) -> None:
        with Scheduler(workers=1, max_pending=3) as s:
            for bad_item in (
                {"task_id": "", "fn": lambda: None},
                {"task_id": 1, "fn": lambda: None},
                {"task_id": "a", "fn": "not-callable"},
                {"task_id": "a", "fn": lambda: None, "priority": True},
                {"task_id": "a", "fn": lambda: None, "priority": 1.5},
                {"task_id": "a", "fn": lambda: None, "priority": "x"},
                {"task_id": "a", "fn": lambda: None, "max_queue_wait_ms": 0},
                {"task_id": "a", "fn": lambda: None, "max_queue_wait_ms": True},
                {"task_id": "a", "fn": lambda: None, "max_queue_wait_ms": 1.5},
            ):
                with self.subTest(bad_item=bad_item):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait([bad_item])
        # 非法参数不留任何统计或任务痕迹。
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.rejected), (0, 0, 0)
        )

    def test_duplicate_task_id_within_batch(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait([
                    {"task_id": "a", "fn": lambda: None},
                    {"task_id": "b", "fn": lambda: None},
                    {"task_id": "a", "fn": lambda: None},
                ])
            self.assertEqual(s.snapshot().accepted, 0)

    def test_batch_larger_than_max_pending_rejected(self) -> None:
        with Scheduler(workers=2, max_pending=2) as s:
            with self.assertRaises(InputValidationError):
                s.submit_batch_with_wait(
                    [{"task_id": "a%d" % i, "fn": lambda: None}
                     for i in range(3)]
                )
            # 即使调度器完全空闲也不接纳；无统计痕迹。
            snap = s.snapshot()
            self.assertEqual((snap.accepted, snap.rejected), (0, 0))

    def test_invalid_admission_timeout_ms(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in (True, False, -1, -100, 1.5, "100", 0.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_batch_with_wait(
                            [{"task_id": "t", "fn": lambda: None}],
                            admission_timeout_ms=bad,  # type: ignore[arg-type]
                        )
        self.assertEqual(s.snapshot().rejected, 0)

    def test_validation_runs_before_close_check(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit_batch_with_wait([])
        with self.assertRaises(InputValidationError):
            s.submit_batch_with_wait(
                [{"task_id": "t", "fn": lambda: None}],
                admission_timeout_ms=True,
            )
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait(
                [{"task_id": "t", "fn": lambda: None}]
            )
        with self.assertRaises(SchedulerClosedError):
            s.submit_batch_with_wait(
                [{"task_id": "t", "fn": lambda: None}],
                admission_timeout_ms=0,
            )

    def test_single_item_batch_is_a_normal_batch(self) -> None:
        with Scheduler(workers=2, max_pending=2) as s:
            handles = s.submit_batch_with_wait(
                [{"task_id": "a", "fn": lambda: 42}]
            )
            self.assertEqual(len(handles), 1)
            self.assertIsInstance(handles[0], TaskHandle)
            self.assertEqual(handles[0].result(2.0), 42)


class ImmediateBatchAdmissionTest(unittest.TestCase):
    def test_whole_group_admitted_when_capacity_free(self) -> None:
        with Scheduler(workers=2, max_pending=5) as s:
            handles = s.submit_batch_with_wait([
                {"task_id": "a", "fn": lambda: 1},
                {"task_id": "b", "fn": lambda: 2},
                {"task_id": "c", "fn": lambda: 3},
            ])
            self.assertEqual(
                [h._entry.task_id for h in handles],  # type: ignore[attr-defined]
                ["a", "b", "c"],
            )
            self.assertIsInstance(handles, tuple)
            # 返回前整组已被接纳：accepted 一次增加组内任务数。
            self.assertEqual(s.snapshot().accepted, 3)
            self.assertEqual([h.result(2.0) for h in handles], [1, 2, 3])

    def test_batch_of_exactly_max_pending_from_empty(self) -> None:
        with Scheduler(workers=1, max_pending=3) as s:
            handles = s.submit_batch_with_wait(
                [{"task_id": "t%d" % i, "fn": lambda i=i: i}
                 for i in range(3)]
            )
            self.assertEqual(s.snapshot().accepted, 3)
            self.assertEqual([h.result(2.0) for h in handles], [0, 1, 2])

    def test_zero_timeout_admitted_when_whole_group_fits(self) -> None:
        with Scheduler(workers=4, max_pending=4) as s:
            handles = s.submit_batch_with_wait(
                [{"task_id": "a%d" % i, "fn": lambda i=i: i}
                 for i in range(4)],
                admission_timeout_ms=0,
            )
            self.assertEqual([h.result(2.0) for h in handles], [0, 1, 2, 3])
        self.assertEqual(s.snapshot().rejected, 0)

    def test_zero_timeout_rejected_when_group_does_not_fit(self) -> None:
        # 只有 1 个空位，组需要 2 个：即使部分放得下也整组立即拒绝。
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h0", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q0", lambda: None)
            s.submit_nowait("q1", lambda: None)
            self.assertTrue(_wait_accepted(s, 3))
            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [{"task_id": "a", "fn": called.set},
                     {"task_id": "b", "fn": called.set}],
                    admission_timeout_ms=0,
                )
            self.assertLess(time.monotonic() - start, 0.1)
            self.assertFalse(called.wait(0.05))
            release.set()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (3, 1))

    def test_one_failure_keeps_original_exception_and_others_run(self) -> None:
        def boom() -> None:
            raise ValueError("x")

        with Scheduler(workers=2, max_pending=4) as s:
            handles = s.submit_batch_with_wait([
                {"task_id": "ok", "fn": lambda: "fine"},
                {"task_id": "boom", "fn": boom},
                {"task_id": "ok2", "fn": lambda: 42},
            ])
            self.assertEqual(handles[0].result(2.0), "fine")
            self.assertEqual(handles[2].result(2.0), 42)
            with self.assertRaises(ValueError) as cm:
                handles[1].result(2.0)
            self.assertIs(type(cm.exception), ValueError)
            self.assertEqual(str(cm.exception), "x")
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.failed), (3, 2, 1)
        )


class AtomicGroupAdmissionTest(unittest.TestCase):
    def test_group_waits_until_enough_slots_for_all(self) -> None:
        # workers=1, max_pending=3：h 执行中占 1 名，组需 3 名（空 2）。
        # 只释放 1 名（空 2，仍放不下 3）时绝不能部分接纳；释放到空 3
        # 的那一刻整组原子接纳，accepted 从 1 跳到 4。
        entered = threading.Event()
        release = threading.Event()
        ran = threading.Event()
        s = Scheduler(workers=1, max_pending=3)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        admitted = threading.Event()

        def group_caller() -> None:
            handles = s.submit_batch_with_wait(
                [{"task_id": "g%d" % i,
                  "fn": (lambda: ran.set()) if i == 0 else (lambda: None)}
                 for i in range(3)],
                admission_timeout_ms=5_000,
            )
            admitted.set()
            for h in handles:
                h.result(2.0)

        t = threading.Thread(target=group_caller)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        self.assertEqual(s.snapshot().accepted, 1)

        release.set()  # 唯一阻塞任务结束：一次空出 3 名
        self.assertTrue(admitted.wait(2.0))
        t.join(2.0)
        s.close()
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 4)
        self.assertEqual(snap.rejected, 0)

    def test_no_partial_admission_on_single_release(self) -> None:
        # workers=1, max_pending=2 满员（h 执行中、q 排队中）；组需 2 名。
        # h 结束只空出 1 名（q 随即被认领执行，仍占 1 名），组不得接纳；
        # q 也结束后整组才被接纳。
        h_entered = threading.Event()
        release_h = threading.Event()
        release_q = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (h_entered.set(), release_h.wait(2.0))),
        ).start()
        self.assertTrue(h_entered.wait(2.0))
        threading.Thread(
            target=s.submit, args=("q", lambda: release_q.wait(2.0))
        ).start()
        self.assertTrue(_wait_accepted(s, 2))

        done = threading.Event()
        t = threading.Thread(
            target=lambda: (
                s.submit_batch_with_wait(
                    [{"task_id": "g0", "fn": lambda: 0},
                     {"task_id": "g1", "fn": lambda: 1}],
                    admission_timeout_ms=5_000,
                ),
                done.set(),
            )
        )
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))

        release_h.set()
        self.assertFalse(done.wait(0.15))  # q 仍占一名，继续等待
        self.assertEqual(s.snapshot().accepted, 2)

        release_q.set()
        self.assertTrue(done.wait(2.0))
        t.join(2.0)
        s.close()
        self.assertEqual(s.snapshot().accepted, 4)

    def test_later_single_cannot_bypass_head_group(self) -> None:
        # 队首是需要 2 名的组（仅空 1）；其后的单任务即使只要 1 名也必须
        # 排在后面，短超时后被拒。
        entered = threading.Event()
        release = threading.Event()
        q_release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            threading.Thread(
                target=s.submit, args=("q", lambda: q_release.wait(2.0))
            ).start()
            self.assertTrue(_wait_accepted(s, 2))

            group_box: dict[str, object] = {}
            tg = threading.Thread(
                target=lambda: group_box.setdefault(
                    "v",
                    s.submit_batch_with_wait(
                        [{"task_id": "G0", "fn": lambda: "G0"},
                         {"task_id": "G1", "fn": lambda: "G1"}],
                        admission_timeout_ms=5_000,
                    ),
                )
            )
            tg.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("late", lambda: "late",
                                   admission_timeout_ms=80)
            self.assertGreaterEqual(time.monotonic() - start, 0.06)

            q_release.set()
            self.assertTrue(tg.is_alive())  # 仍差一名，组继续等待
            # 再释放一名：整组获容并先于任何后来者执行。
            release.set()
            tg.join(2.0)
            handles = group_box["v"]
            self.assertIsNotNone(handles)
            self.assertEqual(
                [h.result(2.0) for h in handles],  # type: ignore[union-attr]
                ["G0", "G1"],
            )
        self.assertEqual(s.snapshot().rejected, 1)  # 只有被超时的 late

    def test_later_smaller_group_cannot_bypass_head_group(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            # 队首：需要 3 名的组（空 2，放不下）。
            head = threading.Thread(
                target=lambda: s.submit_batch_with_wait(
                    [{"task_id": "H%d" % i, "fn": lambda i=i: i}
                     for i in range(3)],
                    admission_timeout_ms=5_000,
                )
            )
            head.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            # 队尾：只需 2 名、放得进当前空位的小组——仍不得绕过。
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [{"task_id": "s0", "fn": lambda: 0},
                     {"task_id": "s1", "fn": lambda: 1}],
                    admission_timeout_ms=80,
                )
            release.set()
            head.join(2.0)
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (4, 1))

    def test_immediate_submit_cannot_use_slack_behind_waiting_group(self) -> None:
        # 队首是需要 3 名的组（仅空 2）；submit / submit_nowait 即使只要
        # 1 名、空位放得下，也不得绕过队首——它们按满员语义即时拒绝
        # （计入 rejected），而不是插队占用那 2 个空位。
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            head = threading.Thread(
                target=lambda: s.submit_batch_with_wait(
                    [{"task_id": "H%d" % i, "fn": lambda: None}
                     for i in range(3)],
                    admission_timeout_ms=5_000,
                )
            )
            head.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            # submit_nowait：即时背压，不入队。
            with self.assertRaises(BackpressureError):
                s.submit_nowait("i0", lambda: None)
            # submit：同样即时背压（submit 本就是满员即拒）。
            with self.assertRaises(BackpressureError):
                s.submit("i1", lambda: None)
            # 队首仍是那一个组；即时拒绝没有产生任务或推进准入队列。
            self.assertTrue(_wait_admission_queue(s, 1))
            self.assertEqual(s.snapshot().accepted, 1)

            release.set()
            head.join(2.0)
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 4)       # h + 组内 3 项
        self.assertEqual(snap.rejected, 2)       # 两次即时拒绝
        for tid in ("i0", "i1"):
            with self.assertRaises(KeyError):
                s.result(tid)

    def test_fifo_order_single_group_single(self) -> None:
        # 满员后准入队列依次为：单任务 w0、组 [g0,g1]、单任务 w2；
        # 逐名释放时接纳顺序必须严格为 w0 -> g0,g1 -> w2。
        entered = threading.Event()
        release = threading.Event()
        order: list[str] = []
        lk = threading.Lock()

        def rec(tid: str):
            def fn() -> str:
                with lk:
                    order.append(tid)
                return tid
            return fn

        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit, args=("h", lambda: (entered.set(), release.wait(2.0)))
        ).start()
        self.assertTrue(entered.wait(2.0))
        threading.Thread(target=s.submit, args=("q", rec("q"))).start()
        self.assertTrue(_wait_accepted(s, 2))

        box: dict[str, object] = {}
        t0 = threading.Thread(
            target=lambda: box.setdefault(
                "w0", s.submit_with_wait("w0", rec("w0"),
                                         admission_timeout_ms=5_000)
            )
        )
        t0.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        tg = threading.Thread(
            target=lambda: box.setdefault(
                "g",
                s.submit_batch_with_wait(
                    [{"task_id": "g0", "fn": rec("g0")},
                     {"task_id": "g1", "fn": rec("g1")}],
                    admission_timeout_ms=5_000,
                ),
            )
        )
        tg.start()
        self.assertTrue(_wait_admission_queue(s, 2))
        t2 = threading.Thread(
            target=lambda: box.setdefault(
                "w2", s.submit_with_wait("w2", rec("w2"),
                                         admission_timeout_ms=5_000)
            )
        )
        t2.start()
        self.assertTrue(_wait_admission_queue(s, 3))

        release.set()
        t0.join(2.0)
        tg.join(2.0)
        t2.join(2.0)
        s.close()
        self.assertEqual(box["w0"], "w0")
        self.assertEqual([h.result(1.0) for h in box["g"]], ["g0", "g1"])  # type: ignore[union-attr]
        self.assertEqual(box["w2"], "w2")
        self.assertEqual(order, ["q", "w0", "g0", "g1", "w2"])

    def test_group_timeout_drains_accumulated_slots_to_followers(self) -> None:
        # 队首大组等待期间累积出多个空位，大组超时退出后，后续 FIFO 等待者
        # 按序获得这些空位（不丢失名额）。
        entered = threading.Event()
        release = threading.Event()
        r1 = threading.Event()
        r2 = threading.Event()
        s = Scheduler(workers=1, max_pending=3)
        threading.Thread(
            target=s.submit, args=("h", lambda: (entered.set(), release.wait(2.0)))
        ).start()
        self.assertTrue(entered.wait(2.0))
        threading.Thread(target=s.submit, args=("q1", lambda: r1.wait(2.0))).start()
        threading.Thread(target=s.submit, args=("q2", lambda: r2.wait(2.0))).start()
        self.assertTrue(_wait_accepted(s, 3))

        def head() -> None:
            try:
                s.submit_batch_with_wait(
                    [{"task_id": "B%d" % i, "fn": lambda: None}
                     for i in range(3)],
                    admission_timeout_ms=100,
                )
            except BackpressureError:
                pass

        th = threading.Thread(target=head)
        outcomes: dict[str, str] = {}
        tz1 = threading.Thread(
            target=lambda: outcomes.setdefault(
                "z1", s.submit_with_wait("z1", lambda: "z1",
                                         admission_timeout_ms=5_000)
            )
        )
        tz2 = threading.Thread(
            target=lambda: outcomes.setdefault(
                "z2", s.submit_with_wait("z2", lambda: "z2",
                                         admission_timeout_ms=5_000)
            )
        )
        th.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        tz1.start()
        tz2.start()
        self.assertTrue(_wait_admission_queue(s, 3))
        # 大组先超时（此时仍满员），随后三名任务全部结束空出 3 名：
        # z1、z2 依次获容。
        th.join(1.0)
        release.set()
        r1.set()
        r2.set()
        tz1.join(2.0)
        tz2.join(2.0)
        s.close()
        self.assertEqual(outcomes, {"z1": "z1", "z2": "z2"})
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (5, 1))


class BatchDispatchOrderTest(unittest.TestCase):
    def test_within_group_priority_desc_then_input_order(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=5) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            handles = s.submit_batch_with_wait([
                {"task_id": "a", "fn": rec("a"), "priority": 0},
                {"task_id": "b", "fn": rec("b"), "priority": 5},
                {"task_id": "c", "fn": rec("c"), "priority": 5},
                {"task_id": "d", "fn": rec("d"), "priority": -1},
            ])
            # 句柄顺序始终等于输入顺序，与派发先后无关。
            self.assertEqual(
                [h._entry.task_id for h in handles],  # type: ignore[attr-defined]
                ["a", "b", "c", "d"],
            )
            release.set()
            self.assertEqual([h.result(2.0) for h in handles],
                             ["a", "b", "c", "d"])
        self.assertEqual(ran, ["b", "c", "a", "d"])

    def test_group_sequences_interleave_by_global_priority(self) -> None:
        # 两个同刻等待的组：组 1 在前但优先级低；全局按 priority 降序派发。
        entered = threading.Event()
        release = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def rec(tid: str):
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=6) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            h1 = s.submit_batch_with_wait([
                {"task_id": "lo1", "fn": rec("lo1"), "priority": 0},
                {"task_id": "hi", "fn": rec("hi"), "priority": 9},
            ])
            h2 = s.submit_batch_with_wait([
                {"task_id": "lo2", "fn": rec("lo2"), "priority": 0},
            ])
            release.set()
            for h in (*h1, *h2):
                h.result(2.0)
        self.assertEqual(ran, ["hi", "lo1", "lo2"])


class BatchTimeoutTest(unittest.TestCase):
    def test_timeout_rejects_whole_group_and_counts_once(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        r2 = threading.Event()
        ran: list[str] = []
        with Scheduler(workers=2, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            threading.Thread(
                target=s.submit, args=("q", lambda: r2.wait(2.0))
            ).start()
            self.assertTrue(_wait_accepted(s, 2))
            cp = s.stats_checkpoint()

            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [{"task_id": "a", "fn": lambda: ran.append("a")},
                     {"task_id": "b", "fn": lambda: ran.append("b")},
                     {"task_id": "c", "fn": lambda: ran.append("c")}][:2],
                    admission_timeout_ms=80,
                )
            elapsed = time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.06)
            self.assertLess(elapsed, 0.5)
            self.assertEqual(ran, [])
            # 拒绝发生的区间只有一次 rejected：无 accepted、无延迟样本。
            interval = s.snapshot_since(cp)
            self.assertEqual(
                (interval.accepted, interval.rejected), (0, 1)
            )
            self.assertEqual(
                len(interval._queue_wait_samples), 0  # type: ignore[attr-defined]
            )
            self.assertEqual(
                len(interval._execution_samples), 0  # type: ignore[attr-defined]
            )
            release.set()
            r2.set()
        snap = s.snapshot()
        # 一次调用一次拒绝：rejected 恰为 1；整组未产生任务。
        self.assertEqual((snap.accepted, snap.rejected), (2, 1))

    def test_none_timeout_waits_until_group_fits(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit, args=("h", lambda: (entered.set(), release.wait(2.0)))
        ).start()
        self.assertTrue(entered.wait(2.0))
        threading.Thread(target=s.submit, args=("q", lambda: None)).start()
        self.assertTrue(_wait_accepted(s, 2))

        done = threading.Event()

        def caller() -> None:
            handles = s.submit_batch_with_wait(
                [{"task_id": "g0", "fn": lambda: 0},
                 {"task_id": "g1", "fn": lambda: 1}],
                admission_timeout_ms=None,
            )
            done.set()
            for h in handles:
                h.result(2.0)

        t = threading.Thread(target=caller)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        self.assertFalse(done.wait(0.1))
        release.set()
        self.assertTrue(done.wait(2.0))
        t.join(2.0)
        s.close()
        self.assertEqual(s.snapshot().rejected, 0)

    def test_group_ids_reusable_after_timeout(self) -> None:
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
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [{"task_id": "x", "fn": lambda: None},
                     {"task_id": "y", "fn": lambda: None}],
                    admission_timeout_ms=30,
                )
            release.set()
            handles = s.submit_batch_with_wait(
                [{"task_id": "x", "fn": lambda: "x"},
                 {"task_id": "y", "fn": lambda: "y"}],
                admission_timeout_ms=2_000,
            )
            self.assertEqual([h.result(2.0) for h in handles], ["x", "y"])


class BatchCloseWhileWaitingTest(unittest.TestCase):
    def test_waiting_group_gets_closed_error_without_rejected(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        threading.Thread(target=s.submit, args=("q", lambda: None)).start()
        self.assertTrue(_wait_accepted(s, 2))

        errors: list[str] = []
        threads = []
        for ids in (("g0", "g1"), ("k0", "k1")):
            def worker(ids=ids) -> None:
                try:
                    s.submit_batch_with_wait(
                        [{"task_id": tid, "fn": called.set} for tid in ids],
                        admission_timeout_ms=None,
                    )
                except SchedulerClosedError:
                    errors.extend(ids)
                except Exception as exc:  # pragma: no cover - 不应发生
                    errors.append(type(exc).__name__)
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)
        self.assertTrue(_wait_admission_queue(s, 2))

        s.close()
        for t in threads:
            t.join(2.0)
        self.assertFalse(called.wait(0.05))
        self.assertEqual(sorted(errors), ["g0", "g1", "k0", "k1"])
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.rejected, 0)
        for tid in errors:
            with self.assertRaises(KeyError):
                s.result(tid)
        release.set()

    def test_close_waits_for_admitted_group_not_waiting_group(self) -> None:
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        s.submit_nowait("a", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "a"))

        closed_error = threading.Event()

        def waiter() -> None:
            try:
                s.submit_batch_with_wait(
                    [{"task_id": "w0", "fn": lambda: 0},
                     {"task_id": "w1", "fn": lambda: 1}],
                    admission_timeout_ms=None,
                )
            except SchedulerClosedError:
                closed_error.set()

        t = threading.Thread(target=waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        closing = threading.Thread(target=s.close)
        closing.start()
        self.assertTrue(closed_error.wait(2.0))  # 等待组不阻塞关闭
        release.set()
        closing.join(2.0)
        t.join(2.0)


class BatchDuplicateIdTest(unittest.TestCase):
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

            box: dict[str, object] = {}
            tg = threading.Thread(
                target=lambda: box.setdefault(
                    "v",
                    s.submit_batch_with_wait(
                        [{"task_id": "g0", "fn": lambda: "g0"},
                         {"task_id": "g1", "fn": lambda: "g1"}],
                        admission_timeout_ms=5_000,
                    ),
                )
            )
            tg.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            with self.assertRaises(DuplicateTaskError):
                s.submit("g0", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("g1", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_with_wait("g0", lambda: None,
                                   admission_timeout_ms=None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_batch_with_wait(
                    [{"task_id": "x", "fn": lambda: None},
                     {"task_id": "g1", "fn": lambda: None}]
                )
            self.assertEqual(s.snapshot().rejected, 0)

            release.set()
            tg.join(2.0)
            self.assertEqual(
                [h.result(2.0) for h in box["v"]],  # type: ignore[union-attr]
                ["g0", "g1"],
            )

    def test_duplicate_against_running_task(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=3) as s:
            threading.Thread(
                target=s.submit,
                args=("r", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(DuplicateTaskError):
                s.submit_batch_with_wait(
                    [{"task_id": "r", "fn": lambda: None},
                     {"task_id": "z", "fn": lambda: None}],
                    admission_timeout_ms=None,
                )
            release.set()
        self.assertEqual(s.snapshot().accepted, 1)

    def test_id_reusable_after_group_completes(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            handles = s.submit_batch_with_wait(
                [{"task_id": "a", "fn": lambda: "a"}]
            )
            self.assertEqual(handles[0].result(2.0), "a")
            self.assertEqual(
                s.submit_batch_with_wait(
                    [{"task_id": "a", "fn": lambda: "again"}]
                )[0].result(2.0),
                "again",
            )


class BatchQueueDeadlineTest(unittest.TestCase):
    def test_queue_deadline_anchored_at_group_admission(self) -> None:
        # 准入等待约 300ms，而 max_queue_wait_ms 只有 100ms：时限从接纳
        # 时刻起算，组接纳后任务仍正常执行，不在等待准入期间到期。
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        result: dict[str, object] = {}

        def caller() -> None:
            try:
                handles = s.submit_batch_with_wait(
                    [{"task_id": "w", "fn": lambda: "ok",
                      "max_queue_wait_ms": 100}],
                    admission_timeout_ms=None,
                )
                result["v"] = handles[0].result(2.0)
            except Exception as exc:  # pragma: no cover - 不应到期
                result["e"] = type(exc).__name__

        t = threading.Thread(target=caller)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        time.sleep(0.30)
        release.set()
        t.join(2.0)
        s.close()
        self.assertEqual(result, {"v": "ok"})
        # 准入等待（约 300ms）不计入 execution_ms：w 认领后立即返回。
        # 样本按结束顺序：h 先结束、w 认领后立即结束。
        self.assertLess(s.snapshot()._execution_samples[1], 50.0)  # type: ignore[attr-defined]

    def test_one_item_expires_after_admission_others_unaffected(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=4)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        handles = s.submit_batch_with_wait([
            {"task_id": "ok", "fn": lambda: "ok"},
            {"task_id": "slow", "fn": lambda: "slow",
             "max_queue_wait_ms": 30},
            {"task_id": "ok2", "fn": lambda: "ok2"},
        ])
        self.assertTrue(_wait_accepted(s, 4))
        # worker 被 h 挡住 0.2s：slow 在认领前到期；其余任务随后成功。
        time.sleep(0.2)
        release.set()
        self.assertEqual(handles[0].result(2.0), "ok")
        self.assertEqual(handles[2].result(2.0), "ok2")
        with self.assertRaises(QueueTimeoutError):
            handles[1].result(2.0)
        with self.assertRaises(QueueTimeoutError):
            s.result("slow")
        s.close()
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.expired), (4, 3, 1)
        )

    def test_expired_group_item_releases_slot_to_next_waiter(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        # 组占满剩余 1 个名额（max_pending=2），其中 g0 会在认领前到期。
        tg = threading.Thread(
            target=lambda: s.submit_batch_with_wait(
                [{"task_id": "g0", "fn": lambda: "g0",
                  "max_queue_wait_ms": 30}],
                admission_timeout_ms=5_000,
            )
        )
        tg.start()
        self.assertTrue(_wait_accepted(s, 2))

        result: dict[str, object] = {}
        tw = threading.Thread(
            target=lambda: result.setdefault(
                "v", s.submit_with_wait("w", lambda: "w",
                                        admission_timeout_ms=5_000)
            )
        )
        tw.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        # 不主动放行 h：g0 到期即接纳 w。
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            snap = s.snapshot()
            if snap.expired == 1 and snap.accepted == 3:
                break
            time.sleep(0.005)
        tg.join(2.0)
        release.set()
        tw.join(2.0)
        s.close()
        self.assertEqual(result, {"v": "w"})
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.expired), (3, 2, 1)
        )

    def test_cancel_one_group_item_before_claim(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=4) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            handles = s.submit_batch_with_wait([
                {"task_id": "c", "fn": lambda: "c"},
                {"task_id": "d", "fn": lambda: "d"},
            ])
            self.assertTrue(handles[0].cancel())
            self.assertFalse(handles[0].cancel())  # 终态唯一
            release.set()
            self.assertEqual(handles[1].result(2.0), "d")
            with self.assertRaises(TaskCancelledError):
                handles[0].result(2.0)
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.cancelled), (3, 2, 1)
        )


class BatchStatsTest(unittest.TestCase):
    def test_interval_checkpoint_accounts_whole_group(self) -> None:
        s = Scheduler(workers=4, max_pending=8)
        cp = s.stats_checkpoint()
        handles = s.submit_batch_with_wait(
            [{"task_id": "t%d" % i, "fn": lambda i=i: i} for i in range(5)]
        )
        self.assertEqual([h.result(2.0) for h in handles], list(range(5)))
        snap = s.snapshot_since(cp)
        self.assertEqual(
            (snap.accepted, snap.completed, snap.failed, snap.rejected),
            (5, 5, 0, 0),
        )
        self.assertEqual(len(snap._queue_wait_samples), 5)  # type: ignore[attr-defined]
        self.assertEqual(len(snap._execution_samples), 5)  # type: ignore[attr-defined]
        s.close()

    def test_rejected_group_contributes_no_interval_events(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        cp = s.stats_checkpoint()
        with self.assertRaises(BackpressureError):
            s.submit_batch_with_wait(
                [{"task_id": "a", "fn": lambda: None}],
                admission_timeout_ms=20,
            )
        snap = s.snapshot_since(cp)
        self.assertEqual(snap.accepted, 0)
        self.assertEqual(snap.rejected, 1)
        release.set()
        s.close()

    def test_mixed_terminal_states_counted_independently(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=5) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            handles = s.submit_batch_with_wait([
                {"task_id": "ok", "fn": lambda: "ok"},
                {"task_id": "boom",
                 "fn": lambda: (_ for _ in ()).throw(ValueError("z"))},
                {"task_id": "slow", "fn": lambda: "slow",
                 "max_queue_wait_ms": 30},
                {"task_id": "cc", "fn": lambda: "cc"},
            ])
            self.assertTrue(handles[3].cancel())  # 认领前取消
            time.sleep(0.2)                     # slow 认领前到期
            release.set()
            self.assertEqual(handles[0].result(2.0), "ok")
            with self.assertRaises(ValueError):
                handles[1].result(2.0)
            with self.assertRaises(QueueTimeoutError):
                handles[2].result(2.0)
            with self.assertRaises(TaskCancelledError):
                handles[3].result(2.0)
        snap = s.snapshot()
        # h + 组内 4 项全部接纳；终态各归其类。
        self.assertEqual(snap.accepted, 5)
        self.assertEqual(snap.completed, 2)   # h, ok
        self.assertEqual(snap.failed, 1)      # boom
        self.assertEqual(snap.expired, 1)     # slow
        self.assertEqual(snap.cancelled, 1)   # cc
        self.assertEqual(snap.rejected, 0)


class BatchStressTest(unittest.TestCase):
    def test_concurrent_groups_and_singles_have_single_outcome(self) -> None:
        # 小容量下多线程混合提交成组（尺寸 1..3）与单任务：每次调用唯一
        # 结局，每个被接纳 callable 恰好执行一次，统计恒等式成立。
        workers, max_pending = 4, 6
        n_groups = 24
        s = Scheduler(workers=workers, max_pending=max_pending)
        ran: set[str] = set()
        rlock = threading.Lock()
        rejected_lock = threading.Lock()
        admitted_lock = threading.Lock()

        def make_fn(tid: str):
            def fn() -> str:
                with rlock:
                    assert tid not in ran
                    ran.add(tid)
                time.sleep(0.001)
                return tid
            return fn

        def group_caller(g: int) -> None:
            size = (g % 3) + 1
            tids = ["g%d-%d" % (g, i) for i in range(size)]
            tasks = [
                {"task_id": tid, "fn": make_fn(tid)} for tid in tids
            ]
            try:
                handles = s.submit_batch_with_wait(
                    tasks, admission_timeout_ms=40
                )
            except BackpressureError:
                with rejected_lock:
                    nonlocal_rejected[0] += 1
                return
            with admitted_lock:
                nonlocal_admitted[0] += size
            # 句柄与输入等长、同序；每个结果恰好可读一次自己的值。
            self.assertEqual(
                [h._entry.task_id for h in handles],  # type: ignore[attr-defined]
                tids,
            )
            for tid, h in zip(tids, handles):
                self.assertEqual(h.result(5.0), tid)

        def single_caller(i: int) -> None:
            try:
                v = s.submit_with_wait(
                    "s%d" % i, make_fn("s%d" % i), admission_timeout_ms=40
                )
            except BackpressureError:
                with rejected_lock:
                    nonlocal_rejected[0] += 1
                return
            with admitted_lock:
                nonlocal_admitted[0] += 1
            self.assertEqual(v, "s%d" % i)

        nonlocal_rejected = [0]
        nonlocal_admitted = [0]
        threads = [threading.Thread(target=group_caller, args=(g,))
                   for g in range(n_groups)]
        threads += [threading.Thread(target=single_caller, args=(i,))
                    for i in range(n_groups)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        s.close()

        snap = s.snapshot()
        self.assertEqual(snap.accepted, nonlocal_admitted[0])
        self.assertEqual(snap.rejected, nonlocal_rejected[0])
        self.assertEqual(
            snap.completed + snap.failed + snap.cancelled + snap.expired,
            snap.accepted,
        )
        # 每个被接纳 callable 恰好执行一次。
        self.assertEqual(len(ran), snap.completed + snap.failed)
        self.assertEqual(
            len(ran), len(snap._queue_wait_samples)  # type: ignore[attr-defined]
        )
        self.assertEqual(
            len(ran), len(snap._execution_samples)  # type: ignore[attr-defined]
        )
        self.assertLessEqual(snap.accepted, 2 * n_groups * 3)


if __name__ == "__main__":
    unittest.main()
