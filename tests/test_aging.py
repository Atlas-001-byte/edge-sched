"""排队优先级老化（aging_interval_ms）语义测试。"""

import threading
import time
import unittest

from edge_sched import (
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    TaskCancelledError,
)
from edge_sched.scheduler import _TaskEntry


class AgingValidationTest(unittest.TestCase):
    def test_invalid_values_raise(self) -> None:
        # 布尔值、零、负数、浮点数与其他类型一律抛 InputValidationError。
        for bad in (True, False, 0, -1, -100, 1.5, 2.0, "50", "1", [10]):
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(1, 1, aging_interval_ms=bad)  # type: ignore[arg-type]

    def test_none_and_positive_int_accepted(self) -> None:
        with Scheduler(1, 1):
            pass
        with Scheduler(1, 1, aging_interval_ms=None):
            pass
        with Scheduler(1, 1, aging_interval_ms=1):
            pass
        with Scheduler(1, 1, aging_interval_ms=250):
            pass

    def test_invalid_aging_checked_like_other_params(self) -> None:
        # 与 workers/max_pending 一样在构造期校验，非法即抛，无可用调度器。
        with self.assertRaises(InputValidationError):
            Scheduler(1, 1, aging_interval_ms=0)
        with self.assertRaises(InputValidationError):
            Scheduler(0, 1, aging_interval_ms=10)


class _Recorder:
    """记录 callable 实际执行顺序。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.order: "list[str]" = []

    def task(self, name: str, delay: float = 0.0):
        def run() -> str:
            if delay:
                time.sleep(delay)
            with self._lock:
                self.order.append(name)
            return name

        return run


class AgingDispatchTest(unittest.TestCase):
    def test_aging_lets_waiting_task_overtake_later_higher_priority(self) -> None:
        # 单工作线程被阻塞任务占住：先接受的低优先级任务 low 在队列中累计
        # 老化周期，阻塞结束时其有效优先级超过后接受的高优先级任务 high。
        rec = _Recorder()
        started = threading.Event()

        def blocker() -> None:
            started.set()
            rec.task("blocker", delay=0.5)()

        with Scheduler(1, 8, aging_interval_ms=40) as s:
            s.submit_nowait("blocker", blocker)
            self.assertTrue(started.wait(2.0))
            low = s.submit_nowait("low", rec.task("low"), priority=0)
            time.sleep(0.2)  # low 累计约 5 个老化周期
            high = s.submit_nowait("high", rec.task("high"), priority=1)
            low.result(5.0)
            high.result(5.0)
        # 阻塞结束时 low 有效优先级约 12，high 约 1+7=8：low 先派发；
        # 未启用老化时 high（priority 1）必先于 low（priority 0）。
        self.assertEqual(rec.order, ["blocker", "low", "high"])

    def test_default_no_aging_preserves_static_priority_order(self) -> None:
        # 缺省关闭老化：后到的高优先级任务仍然先于低优先级任务派发。
        rec = _Recorder()
        started = threading.Event()

        def blocker() -> None:
            started.set()
            rec.task("blocker", delay=0.4)()

        with Scheduler(1, 8) as s:
            s.submit_nowait("blocker", blocker)
            self.assertTrue(started.wait(2.0))
            low = s.submit_nowait("low", rec.task("low"), priority=0)
            time.sleep(0.2)
            high = s.submit_nowait("high", rec.task("high"), priority=1)
            low.result(5.0)
            high.result(5.0)
        self.assertEqual(rec.order, ["blocker", "high", "low"])

    def test_batch_group_ages_from_shared_admit_time(self) -> None:
        # 成组准入共享接纳时刻：同优先级组内有效优先级始终相同，
        # 同级次序由接受序号（输入顺序）决定，与老化累计无关。
        rec = _Recorder()
        started = threading.Event()

        def blocker() -> None:
            started.set()
            rec.task("blocker", delay=0.3)()

        with Scheduler(1, 8, aging_interval_ms=20) as s:
            s.submit_nowait("blocker", blocker)
            self.assertTrue(started.wait(2.0))
            handles = s.submit_batch_with_wait([
                {"task_id": "g0", "fn": rec.task("g0"), "priority": 0},
                {"task_id": "g1", "fn": rec.task("g1"), "priority": 0},
                {"task_id": "g2", "fn": rec.task("g2"), "priority": 0},
            ])
            for h in handles:
                h.result(5.0)
        self.assertEqual(rec.order, ["blocker", "g0", "g1", "g2"])

    def test_cancelled_task_not_dispatched_despite_aging(self) -> None:
        # 排队期间取消的任务即使有效优先级最高也不参与派发。
        rec = _Recorder()
        started = threading.Event()

        def blocker() -> None:
            started.set()
            rec.task("blocker", delay=0.3)()

        with Scheduler(1, 8, aging_interval_ms=20) as s:
            s.submit_nowait("blocker", blocker)
            self.assertTrue(started.wait(2.0))
            doomed = s.submit_nowait("doomed", rec.task("doomed"), priority=0)
            time.sleep(0.1)  # 让 doomed 累计若干老化周期
            self.assertTrue(doomed.cancel())
            other = s.submit_nowait("other", rec.task("other"), priority=0)
            other.result(5.0)
            with self.assertRaises(TaskCancelledError):
                doomed.result(5.0)
            snap = s.snapshot()
        self.assertEqual(rec.order, ["blocker", "other"])
        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.completed, 2)

    def test_expiry_unchanged_with_aging(self) -> None:
        # 老化不改变 max_queue_wait_ms：认领前到期的任务进入 expired 终态。
        rec = _Recorder()
        started = threading.Event()

        def blocker() -> None:
            started.set()
            rec.task("blocker", delay=0.3)()

        with Scheduler(1, 8, aging_interval_ms=10) as s:
            block = s.submit_nowait("blocker", blocker)
            self.assertTrue(started.wait(2.0))
            exp = s.submit_nowait(
                "exp", rec.task("exp"), priority=0, max_queue_wait_ms=50
            )
            with self.assertRaises(QueueTimeoutError):
                exp.result(5.0)
            block.result(5.0)
            snap = s.snapshot()
        self.assertEqual(rec.order, ["blocker"])
        self.assertEqual(snap.expired, 1)
        self.assertEqual(snap.completed, 1)


class AgingOrderUnitTest(unittest.TestCase):
    """以固定接纳时刻直接构造入站条目，确定性地验证派发比较规则。"""

    def setUp(self) -> None:
        self.s = Scheduler(1, 16, aging_interval_ms=100)
        self.addCleanup(self.s.close)

    def _entry(self, task_id: str, priority: int, seq: int,
               submit_time: float) -> _TaskEntry:
        return _TaskEntry(task_id, lambda: None, submit_time, priority, seq)

    def _pop_order(self, entries: "list[_TaskEntry]") -> "list[str]":
        with self.s._inbound_cond:
            for entry in entries:
                self.s._inbound.append((-entry.priority, entry.seq, entry))
            order = []
            while True:
                entry = self.s._pop_next_locked()
                if entry is None:
                    break
                order.append(entry.task_id)
        return order

    def test_effective_priority_dominates(self) -> None:
        # old 等待 350ms（3 个周期，有效优先级 3）超过新的 priority 1 任务。
        now = time.monotonic()
        old = self._entry("old", priority=0, seq=0, submit_time=now - 0.35)
        new = self._entry("new", priority=1, seq=1, submit_time=now)
        self.assertEqual(self._pop_order([new, old]), ["old", "new"])

    def test_equal_effective_falls_back_to_original_priority(self) -> None:
        # 有效优先级相同（均为 3）：原 priority 高者先派发。
        now = time.monotonic()
        aged = self._entry("aged", priority=0, seq=0, submit_time=now - 0.3)
        fresh = self._entry("fresh", priority=3, seq=1, submit_time=now)
        self.assertEqual(self._pop_order([aged, fresh]), ["fresh", "aged"])

    def test_equal_effective_and_priority_falls_back_to_accept_seq(self) -> None:
        # 有效优先级与原 priority 都相同：接受序号小者（先接受）先派发。
        now = time.monotonic()
        shared = now - 0.3
        first = self._entry("first", priority=1, seq=0, submit_time=shared)
        second = self._entry("second", priority=1, seq=1, submit_time=shared)
        self.assertEqual(
            self._pop_order([second, first]), ["first", "second"]
        )

    def test_full_ordering_chain(self) -> None:
        # 三者有效优先级均为 4：先比原 priority，再比接受序号。
        now = time.monotonic()
        shared = now - 0.3
        a = self._entry("a", priority=1, seq=2, submit_time=shared)  # eff 4
        b = self._entry("b", priority=1, seq=0, submit_time=shared)  # eff 4
        c = self._entry("c", priority=4, seq=1, submit_time=now)     # eff 4
        self.assertEqual(self._pop_order([a, b, c]), ["c", "b", "a"])

    def test_aging_periods_accumulate_from_admit_time(self) -> None:
        # 每完成一个老化周期有效优先级加 1：等待 250ms（周期 100ms）计 2。
        entry = self._entry(
            "x", priority=0, seq=0, submit_time=time.monotonic() - 0.25
        )
        key = self.s._dispatch_key(entry, time.monotonic())
        self.assertEqual(key, (-2, 0, 0))


if __name__ == "__main__":
    unittest.main()
