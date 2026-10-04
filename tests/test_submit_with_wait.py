"""submit_with_wait 有界阻塞准入的验收测试。

覆盖：有空位立即接纳、满员后严格按发起先后（FIFO）获容、一次释放只接纳
队首一人、准入超时 BackpressureError（rejected 加一）、close 时等待者
SchedulerClosedError、等待期间同名 DuplicateTaskError、参数非法
InputValidationError、接纳时刻起算 max_queue_wait_ms，以及各类拒绝不留
任务/统计痕迹、callable 只执行一次。
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


def _samples(snap: "object", kind: str) -> list:
    attr = (
        "_queue_wait_samples" if kind == "wait" else "_total_latency_samples"
    )
    return getattr(snap, attr)


class AdmissionValidationTest(unittest.TestCase):
    def test_invalid_admission_timeout_ms(self) -> None:
        # 0 合法（只在有空位时接纳）；bool/负数/浮点/字符串均非法。
        with Scheduler(workers=1, max_pending=2) as s:
            for bad in (True, False, -1, -100, 1.5, "100", 0.0):
                with self.subTest(bad=bad):
                    with self.assertRaises(InputValidationError):
                        s.submit_with_wait(
                            "t", lambda: None,
                            admission_timeout_ms=bad,  # type: ignore[arg-type]
                        )
        # 校验失败不留任何统计痕迹。
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.rejected), (0, 0, 0)
        )

    def test_none_zero_and_positive_int_accepted(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(
                s.submit_with_wait("a", lambda: 1, admission_timeout_ms=None),
                1,
            )
            self.assertEqual(
                s.submit_with_wait("b", lambda: 2, admission_timeout_ms=0),
                2,
            )
            self.assertEqual(
                s.submit_with_wait("c", lambda: 3, admission_timeout_ms=60_000),
                3,
            )

    def test_shared_submit_validations_still_apply(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(InputValidationError):
                s.submit_with_wait("", lambda: None)
            with self.assertRaises(InputValidationError):
                s.submit_with_wait("t", "not-callable")  # type: ignore[arg-type]
            with self.assertRaises(InputValidationError):
                s.submit_with_wait("t", lambda: None, priority=True)
            with self.assertRaises(InputValidationError):
                s.submit_with_wait(
                    "t", lambda: None, max_queue_wait_ms=0
                )
            with self.assertRaises(InputValidationError):
                s.submit_with_wait("t", lambda: None, timeout=-1)

    def test_admission_timeout_validated_before_close_check(self) -> None:
        s = Scheduler(workers=1, max_pending=1)
        s.close()
        with self.assertRaises(InputValidationError):
            s.submit_with_wait("t", lambda: None, admission_timeout_ms=True)
        with self.assertRaises(SchedulerClosedError):
            s.submit_with_wait("t", lambda: None, admission_timeout_ms=1)
        with self.assertRaises(SchedulerClosedError):
            s.submit_with_wait("t", lambda: None, admission_timeout_ms=None)


class ImmediateAdmissionTest(unittest.TestCase):
    def test_admitted_immediately_when_capacity_free(self) -> None:
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(s.submit_with_wait("t", lambda: 42), 42)

    def test_failure_raises_original_exception(self) -> None:
        def boom() -> None:
            raise ValueError("x")

        with Scheduler(workers=2, max_pending=4) as s:
            with self.assertRaises(ValueError) as cm:
                s.submit_with_wait("boom", boom)
            self.assertIs(type(cm.exception), ValueError)
            self.assertEqual(str(cm.exception), "x")
            # 失败后调度器照常工作。
            self.assertEqual(s.submit_with_wait("ok", lambda: "fine"), "fine")
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.completed, snap.failed), (2, 1, 1))

    def test_zero_admitted_when_slot_free(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            self.assertEqual(
                s.submit_with_wait("t", lambda: "z", admission_timeout_ms=0),
                "z",
            )
        self.assertEqual(s.snapshot().rejected, 0)

    def test_zero_rejected_immediately_when_full(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        with Scheduler(workers=1, max_pending=1) as s:
            t = threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            )
            t.start()
            self.assertTrue(entered.wait(2.0))
            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_with_wait(
                    "z", lambda: None, admission_timeout_ms=0
                )
            self.assertLess(time.monotonic() - start, 0.1)
            release.set()
            t.join()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (1, 1))

    def test_result_timeout_leaves_task_running(self) -> None:
        release = threading.Event()

        def slow() -> None:
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(TimeoutError):
                s.submit_with_wait("slow", slow, timeout=0.05)
            release.set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().completed == 1:
                    break
                time.sleep(0.01)
            self.assertIsNone(s.result("slow"))


class FifoAdmissionTest(unittest.TestCase):
    def test_waiters_admitted_in_launch_order(self) -> None:
        # workers=1, max_pending=2：一个执行中、一个排队占满名额；
        # 三个等待者必须严格按发起顺序 w0 -> w1 -> w2 被接纳与执行。
        entered = threading.Event()
        release = threading.Event()
        order: list[str] = []
        lock = threading.Lock()

        def hold() -> None:
            entered.set()
            release.wait(2.0)

        def rec(tid: str) -> "any":
            def fn() -> str:
                with lock:
                    order.append(tid)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=2) as s:
            holder = threading.Thread(target=s.submit_with_wait, args=("h", hold))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            # 第二个名额由排队中的 q 占住。
            sq = threading.Thread(
                target=s.submit_with_wait, args=("q", rec("q"))
            )
            sq.start()
            self.assertTrue(_wait_accepted(s, 2))

            # 严格确定发起顺序：每启动一个等待者后，先确认它确实已按序
            # 进入准入队列（队列长度 +1），再启动下一个——不能依赖
            # thread.start() 与固定 sleep 来推断到达锁的先后。
            threads = []
            for index, tid in enumerate(("w0", "w1", "w2"), start=1):
                t = threading.Thread(
                    target=lambda tid=tid: s.submit_with_wait(tid, rec(tid))
                )
                t.start()
                threads.append(t)
                self.assertTrue(_wait_admission_queue(s, index))
            self.assertEqual(s.snapshot().accepted, 2)

            release.set()
            sq.join()
            holder.join()
            for t in threads:
                t.join()

        self.assertEqual(order, ["q", "w0", "w1", "w2"])

    def test_one_release_admits_only_head(self) -> None:
        # 一次名额释放只把队首提升为已接纳：在工作线程仍被占住时，
        # accepted 只增加一个，且被接纳者的 callable 尚未执行。
        h0_entered = threading.Event()
        release_h0 = threading.Event()
        release_h1 = threading.Event()
        ran: list[str] = []
        rlock = threading.Lock()

        def h0() -> None:
            h0_entered.set()
            release_h0.wait(2.0)

        def h1() -> None:
            release_h1.wait(2.0)

        def rec(tid: str) -> "any":
            def fn() -> str:
                with rlock:
                    ran.append(tid)
                return tid
            return fn

        with Scheduler(workers=1, max_pending=2) as s:
            th0 = threading.Thread(target=s.submit_with_wait, args=("h0", h0))
            th0.start()
            self.assertTrue(h0_entered.wait(2.0))
            # h1 排队占住第二个名额。
            th1 = threading.Thread(target=s.submit_with_wait, args=("h1", h1))
            th1.start()
            self.assertTrue(_wait_accepted(s, 2))

            tw0 = threading.Thread(
                target=s.submit_with_wait, args=("w0", rec("w0"))
            )
            tw1 = threading.Thread(
                target=s.submit_with_wait, args=("w1", rec("w1"))
            )
            tw0.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            tw1.start()
            self.assertTrue(_wait_admission_queue(s, 2))

            # h0 结束释放一个名额：h1 被认领（开始阻塞执行），同时只接纳
            # 队首 w0；w1 仍在准入队列，accepted 恰为 3，w0 尚未执行。
            release_h0.set()
            th0.join()
            self.assertTrue(_wait_started(s, "h1"))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().accepted == 3:
                    break
                time.sleep(0.005)
            self.assertEqual(s.snapshot().accepted, 3)
            self.assertTrue(_wait_admission_queue(s, 1))
            with rlock:
                self.assertEqual(ran, [])

            # h1 结束：w1 才被接纳；派发顺序仍为 w0（先接纳）再 w1。
            release_h1.set()
            th1.join()
            tw0.join()
            tw1.join()

        self.assertEqual(ran, ["w0", "w1"])

    def test_cancel_release_admits_waiter(self) -> None:
        # 取消排队中任务释放名额，等待者据此获容。
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("run", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            queued = s.submit_nowait("q", lambda: "q")
            self.assertTrue(_wait_accepted(s, 2))

            got: dict[str, object] = {}
            tw = threading.Thread(
                target=lambda: got.setdefault(
                    "v", s.submit_with_wait("w", lambda: "W")
                )
            )
            tw.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            self.assertTrue(queued.cancel())
            tw.join()
            release.set()
        self.assertEqual(got, {"v": "W"})

    def test_no_fairness_starvation_under_continuous_load(self) -> None:
        # 容量远小于并发、所有调用同时涌入并设置较长准入时限：每个调用
        # 最终都必须获容、恰好执行一次并返回自己的结果，不发生饥饿或
        # 死锁。两个工作线程下执行先后不保证等于提交先后，故只校验
        # “全部且仅执行一次”；严格 FIFO 顺序由单工作线程用例覆盖。
        n = 20
        s = Scheduler(workers=2, max_pending=3)
        order: list[int] = []
        olock = threading.Lock()
        threads = []

        def fn(i: int) -> int:
            with olock:
                order.append(i)
            time.sleep(0.002)
            return i

        for i in range(n):
            t = threading.Thread(
                target=lambda i=i: s.submit_with_wait(
                    "w%d" % i, lambda i=i: fn(i), admission_timeout_ms=10_000
                )
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        s.close()
        self.assertEqual(sorted(order), list(range(n)))


class AdmissionTimeoutTest(unittest.TestCase):
    def test_timeout_raises_backpressure_and_counts(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()

        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            start = time.monotonic()
            with self.assertRaises(BackpressureError):
                s.submit_with_wait(
                    "x", called.set, admission_timeout_ms=80
                )
            elapsed = time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.07)
            self.assertLess(elapsed, 0.5)
            # callable 不执行、不建任务、不占 id。
            self.assertFalse(called.wait(0.05))
            with self.assertRaises(KeyError):
                s.result("x")
            snap = s.snapshot()
            self.assertEqual((snap.accepted, snap.rejected), (1, 1))

            release.set()
            # 被拒 task_id 立即可复用。
            self.assertEqual(
                s.submit_with_wait(
                    "x", lambda: 7, admission_timeout_ms=2_000
                ),
                7,
            )

    def test_short_timeout_waiter_rejected_behind_long_head(self) -> None:
        # 队首时限长、队尾时限短：名额只给队首，队尾到期被拒，互不影响。
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            head: dict[str, object] = {}
            th = threading.Thread(
                target=lambda: head.setdefault(
                    "v", s.submit_with_wait(
                        "head", lambda: "H", admission_timeout_ms=5_000
                    )
                )
            )
            th.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            # 队尾等待者只给很短时限，必然先到期。
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("tail", lambda: "T",
                                   admission_timeout_ms=60)
            self.assertEqual(s.snapshot().rejected, 1)

            release.set()
            th.join()
        self.assertEqual(head, {"v": "H"})
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.completed, 2)


class CloseWhileWaitingTest(unittest.TestCase):
    def test_waiter_rejected_on_close(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        called = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit_with_wait,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        errors: list[str] = []
        threads = []
        for tid in ("w0", "w1"):
            def worker(tid: str = tid) -> None:
                try:
                    s.submit_with_wait(tid, called.set,
                                       admission_timeout_ms=None)
                except SchedulerClosedError:
                    errors.append(tid)
                except Exception as exc:  # pragma: no cover - 不应发生
                    errors.append(type(exc).__name__)
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)
            time.sleep(0.03)
        self.assertTrue(_wait_admission_queue(s, 2))

        s.close()  # close 开始即令等待者得到 SchedulerClosedError
        for t in threads:
            t.join()
        self.assertFalse(called.wait(0.05))
        self.assertEqual(sorted(errors), ["w0", "w1"])
        snap = s.snapshot()
        # 关闭拒绝不计 rejected，也不产生任务。
        self.assertEqual(snap.accepted, 1)
        self.assertEqual(snap.rejected, 0)
        for tid in ("w0", "w1"):
            with self.assertRaises(KeyError):
                s.result(tid)
        release.set()

    def test_close_waits_for_admitted_but_not_waiters(self) -> None:
        # 已接纳任务仍执行到底；等待者立即得到关闭异常，close 不被拖住。
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        s.submit_nowait("a", lambda: release.wait(2.0))
        self.assertTrue(_wait_started(s, "a"))
        s.submit_nowait("b", lambda: "b")
        self.assertTrue(_wait_accepted(s, 2))

        done = threading.Event()

        def waiter() -> None:
            try:
                s.submit_with_wait("w", lambda: None,
                                   admission_timeout_ms=None)
            except SchedulerClosedError:
                done.set()

        t = threading.Thread(target=waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        closing = threading.Thread(target=s.close)
        closing.start()
        self.assertTrue(done.wait(2.0))  # 等待者不阻塞关闭流程
        release.set()
        closing.join()
        t.join()
        self.assertEqual(s.result("b"), "b")

    def test_submit_with_wait_after_close(self) -> None:
        s = Scheduler(workers=1, max_pending=2)
        s.close()
        with self.assertRaises(SchedulerClosedError):
            s.submit_with_wait("late", lambda: None)
        with self.assertRaises(SchedulerClosedError):
            s.submit_with_wait("late", lambda: None, admission_timeout_ms=0)
        self.assertEqual(s.snapshot().accepted, 0)
        self.assertEqual(s.snapshot().rejected, 0)


class DuplicateWhileWaitingTest(unittest.TestCase):
    def test_duplicate_against_waiter(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            # "dup" 在准入队列中等待。
            tw = threading.Thread(
                target=s.submit_with_wait,
                args=("dup", lambda: "first"),
            )
            tw.start()
            self.assertTrue(_wait_admission_queue(s, 1))

            # 同名的 submit / submit_nowait / submit_with_wait 全部冲突。
            with self.assertRaises(DuplicateTaskError):
                s.submit("dup", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("dup", lambda: None)
            with self.assertRaises(DuplicateTaskError):
                s.submit_with_wait("dup", lambda: None,
                                   admission_timeout_ms=None)
            self.assertEqual(s.snapshot().rejected, 0)

            release.set()
            tw.join()
            # 等待者正常获容并返回原值。
            self.assertEqual(s.result("dup"), "first")

    def test_duplicate_against_running_task(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("r", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(DuplicateTaskError):
                s.submit_with_wait("r", lambda: None,
                                   admission_timeout_ms=None)
            release.set()

    def test_task_id_reusable_after_admission_timeout(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("z", lambda: None,
                                   admission_timeout_ms=20)
            release.set()
            # 超时拒绝释放标识：同名可再提交。
            self.assertEqual(
                s.submit_with_wait("z", lambda: "again",
                                   admission_timeout_ms=2_000),
                "again",
            )


class AdmissionQueueDeadlineTest(unittest.TestCase):
    def test_queue_deadline_anchored_at_admission(self) -> None:
        # max_queue_wait_ms=100，但准入发生在约 300ms 后：时限从接纳时刻
        # 起算，任务被接纳后仍应正常派发并成功，而不是在等待准入期间到期。
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit_with_wait,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        result: dict[str, object] = {}

        def waiter() -> None:
            try:
                result["v"] = s.submit_with_wait(
                    "w", lambda: "ok", max_queue_wait_ms=100
                )
            except Exception as exc:  # pragma: no cover - 不应到期
                result["e"] = type(exc).__name__

        t = threading.Thread(target=waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        time.sleep(0.30)  # 已超过 100ms，但尚未获名额
        release.set()
        t.join()
        s.close()
        self.assertEqual(result, {"v": "ok"})

    def test_expiry_after_admission_promotes_next_waiter(self) -> None:
        # 接纳后的任务在认领前到期：计入 expired，释放名额并接纳下一位。
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=2)
        threading.Thread(
            target=s.submit_with_wait,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        # w0 立即获第二个名额，但工作线程被占，30ms 后在认领前到期。
        def w0_waiter() -> None:
            try:
                s.submit_with_wait("w0", lambda: "w0",
                                   max_queue_wait_ms=30)
            except QueueTimeoutError:
                pass

        t0 = threading.Thread(target=w0_waiter)
        t0.start()
        self.assertTrue(_wait_accepted(s, 2))

        # w1 在准入队列等待 w0 到期释放名额。
        result: dict[str, object] = {}

        def w1_waiter() -> None:
            result["v"] = s.submit_with_wait(
                "w1", lambda: "w1", admission_timeout_ms=5_000
            )

        t1 = threading.Thread(target=w1_waiter)
        t1.start()
        self.assertTrue(_wait_admission_queue(s, 1))

        # 不主动放行 h：w0 到期即应接纳 w1（但 w1 仍排队等工作线程）。
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            snap = s.snapshot()
            if snap.expired == 1 and snap.accepted == 3:
                break
            time.sleep(0.005)
        t0.join()
        self.assertEqual(s.snapshot().expired, 1)
        with self.assertRaises(QueueTimeoutError):
            s.result("w0")

        release.set()
        t1.join()
        s.close()
        self.assertEqual(result, {"v": "w1"})
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.completed, snap.expired),
                         (3, 2, 1))


class AdmissionStatsTest(unittest.TestCase):
    def test_timed_out_admission_only_increments_rejected(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("x", lambda: None,
                                   admission_timeout_ms=30)
            release.set()
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 1)
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(
            (snap.completed, snap.failed, snap.cancelled, snap.expired),
            (1, 0, 0, 0),
        )
        # 被拒不贡献任何延迟样本。
        self.assertEqual(len(_samples(snap, "wait")), 1)
        self.assertEqual(len(_samples(snap, "total")), 1)

    def test_successful_path_matches_submit_accounting(self) -> None:
        with Scheduler(workers=4, max_pending=8) as s:
            for i in range(6):
                self.assertEqual(
                    s.submit_with_wait("t%d" % i, lambda i=i: i), i
                )
        snap = s.snapshot()
        self.assertEqual(
            (snap.accepted, snap.completed, snap.rejected), (6, 6, 0)
        )
        self.assertEqual(len(_samples(snap, "wait")), 6)
        self.assertEqual(len(_samples(snap, "total")), 6)


class AdmissionStressTest(unittest.TestCase):
    def test_each_caller_has_single_outcome_and_callable_runs_once(self) -> None:
        # 小容量 + 大量等待者 + 短时限混合：每个调用唯一结局
        # （成功返回或 BackpressureError），callable 至多执行一次。
        n = 400
        s = Scheduler(workers=4, max_pending=4)
        ran: set[int] = set()
        rlock = threading.Lock()
        outcomes: dict[int, str] = {}
        olock = threading.Lock()

        def make(i: int) -> "any":
            def fn() -> int:
                with rlock:
                    ran.add(i)
                time.sleep(0.001)
                return i
            return fn

        def caller(i: int) -> None:
            # 交错使用较短的准入时限，制造超时与获容混合。
            try:
                v = s.submit_with_wait(
                    "t%d" % i, make(i), admission_timeout_ms=30
                )
                with olock:
                    outcomes[i] = "ok:%s" % v
            except BackpressureError:
                with olock:
                    outcomes[i] = "rejected"

        threads = [threading.Thread(target=caller, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        s.close()

        self.assertEqual(len(outcomes), n)
        succeeded = [i for i, o in outcomes.items() if o == "ok:%d" % i]
        rejected = [i for i, o in outcomes.items() if o == "rejected"]
        self.assertEqual(len(succeeded) + len(rejected), n)
        # 成功的 callable 恰好执行一次；被拒的完全不执行。
        self.assertEqual(sorted(ran), sorted(succeeded))

        snap = s.snapshot()
        self.assertEqual(snap.accepted, len(succeeded))
        self.assertEqual(snap.rejected, len(rejected))
        self.assertEqual(
            snap.completed + snap.failed + snap.cancelled + snap.expired,
            len(succeeded),
        )
        finished = snap.completed + snap.failed
        self.assertEqual(len(_samples(snap, "wait")), finished)
        self.assertEqual(len(_samples(snap, "total")), finished)


if __name__ == "__main__":
    unittest.main()
