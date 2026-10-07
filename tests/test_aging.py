"""可选排队优先级老化（aging_interval_ms）的验收测试。

覆盖：

- 构造参数校验：None（缺省关闭）与 >=1 整数合法；布尔值、0、负数、
  浮点数、字符串等抛 InputValidationError，且不创建可用调度器。
- 缺省关闭时派发语义不变（原 priority 降序、同级接受先后）。
- 启用后长期等待的低优先级任务可越过后到的高优先级任务先派发；
  同一情形在关闭老化时高优先级仍先派发。
- 比较次序：有效优先级降序 -> 原 priority 降序 -> 接受先后升序；
  成组准入共享接纳时刻、同级按输入顺序。
- 老化只作用于已接纳、未认领、未终态任务：不抢占执行中任务，认领后
  冻结；排队中取消的任务不参与派发，认领前到期仍进入 expired 且
  callable 不执行，max_queue_wait_ms 口径不变。
- 六项统计与两类延迟样本的归属不变。
- CLI 新增 --aging-interval-ms：缺省关闭，非法值以退出码 2 报
  InputValidationError，输出 JSON 结构不变。
"""

import io
import json
import os
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

from edge_sched import (
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    TaskCancelledError,
)
from edge_sched.cli import run as cli_run


def _recorder(tid: str, order: list, lock: threading.Lock,
              hold: float = 0.0):
    def fn() -> str:
        with lock:
            order.append(tid)
        if hold:
            time.sleep(hold)
        return tid
    return fn


def _hold_worker():
    """占住唯一工作线程直到调用方放行：返回 (occupy_fn, entered, free)。"""
    entered = threading.Event()
    free = threading.Event()

    def occupy() -> None:
        entered.set()
        free.wait(5.0)

    return occupy, entered, free


class AgingValidationTest(unittest.TestCase):
    def test_none_and_positive_int_accepted(self) -> None:
        for value in (None, 1, 1000):
            with Scheduler(1, 2, aging_interval_ms=value) as s:
                self.assertEqual(s.submit("t", lambda: 1), 1)

    def test_invalid_values_raise(self) -> None:
        for bad in (True, False, 0, -1, -100, 1.5, 2.0, "10",
                    [], {}, object()):
            with self.subTest(bad=bad):
                with self.assertRaises(InputValidationError):
                    Scheduler(1, 2, aging_interval_ms=bad)  # type: ignore[arg-type]

    def test_invalid_value_creates_no_usable_scheduler(self) -> None:
        # 校验在任何线程启动前抛出：失败构造不留存活的调度器线程。
        before = threading.enumerate()
        with self.assertRaises(InputValidationError):
            Scheduler(1, 2, aging_interval_ms=0)
        time.sleep(0.05)
        after = threading.enumerate()
        leftovers = [
            t for t in after
            if t not in before and t.name.startswith("edge-sched-")
        ]
        self.assertEqual(leftovers, [])

    def test_default_positionally_unchanged(self) -> None:
        # 既有两参构造方式完全不变（老化缺省关闭）。
        with Scheduler(1, 2) as s:
            self.assertEqual(s.submit("t", lambda: 7), 7)


class AgingDispatchTest(unittest.TestCase):
    def test_long_waiting_low_priority_overtakes_newcomer(self) -> None:
        # 低优先级任务独自老化足够多周期后，有效优先级超过后到的高优先级
        # 新任务，先被派发。
        order: list = []
        lock = threading.Lock()
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8, aging_interval_ms=40) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            low = s.submit_nowait("low", _recorder("low", order, lock),
                                  priority=0)
            time.sleep(0.25)  # 约 6 个老化周期，有效优先级约 6
            high = s.submit_nowait("high", _recorder("high", order, lock),
                                   priority=3)  # 新任务，有效优先级 3
            time.sleep(0.02)
            free.set()
            self.assertEqual(low.result(), "low")
            self.assertEqual(high.result(), "high")
            holder.join()
        self.assertEqual(order, ["low", "high"])

    def test_without_aging_newcomer_keeps_priority(self) -> None:
        # 完全相同的等待结构，但缺省关闭老化：高优先级新任务仍先派发。
        order: list = []
        lock = threading.Lock()
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            low = s.submit_nowait("low", _recorder("low", order, lock),
                                  priority=0)
            time.sleep(0.25)
            high = s.submit_nowait("high", _recorder("high", order, lock),
                                   priority=3)
            time.sleep(0.02)
            free.set()
            self.assertEqual(low.result(), "low")
            self.assertEqual(high.result(), "high")
            holder.join()
        self.assertEqual(order, ["high", "low"])

    def test_effective_tie_broken_by_original_priority(self) -> None:
        # A(p0) 自接纳起老化 2 个周期 -> 有效 2；B(p2) 在此刻稍后才被接纳，
        # 尚未老化 -> 有效 2。有效优先级相同，按原 priority 降序，B 先派发。
        order: list = []
        lock = threading.Lock()
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8, aging_interval_ms=200) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            a = s.submit_nowait("a", _recorder("a", order, lock), priority=0)
            time.sleep(0.45)  # a 已完成 2 个周期（窗口 0.4~0.6s）
            b = s.submit_nowait("b", _recorder("b", order, lock), priority=2)
            free.set()        # 立即派发：a、b 有效优先级均为 2
            self.assertEqual(a.result(), "a")
            self.assertEqual(b.result(), "b")
            holder.join()
        self.assertEqual(order, ["b", "a"])

    def test_same_effective_and_original_keeps_accept_order(self) -> None:
        # 有效优先级与原 priority 都相同（同优先级、同刻接纳、同步老化）：
        # 仍由接受先后决定。成组准入共享接纳时刻，按输入顺序派发。
        order: list = []
        lock = threading.Lock()
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8, aging_interval_ms=50) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            handles = s.submit_batch_with_wait([
                {"task_id": "x", "fn": _recorder("x", order, lock),
                 "priority": 1},
                {"task_id": "y", "fn": _recorder("y", order, lock),
                 "priority": 1},
            ])
            time.sleep(0.12)  # 二者同样越过 2 个周期，键完全同序位
            free.set()
            for h in handles:
                h.result()
            holder.join()
        self.assertEqual(order, ["x", "y"])

    def test_aging_does_not_preempt_running_task(self) -> None:
        # 执行中的低优先级任务不被老化/后来者重排或抢占。
        started = threading.Event()
        release = threading.Event()
        order: list = []
        lock = threading.Lock()

        def low() -> None:
            with lock:
                order.append("low-start")
            started.set()
            release.wait(5.0)
            with lock:
                order.append("low-end")

        with Scheduler(1, 4, aging_interval_ms=10) as s:
            hl = s.submit_nowait("low", low, priority=-5)
            self.assertTrue(started.wait(2.0))
            # 后到的高优先级任务只能排队；执行中的 low 不会被打断。
            hh = s.submit_nowait("high", _recorder("high", order, lock),
                                 priority=100)
            time.sleep(0.1)
            with lock:
                self.assertEqual(order, ["low-start"])
            release.set()
            self.assertEqual(hh.result(), "high")
            self.assertIsNone(hl.result())
        self.assertEqual(order, ["low-start", "low-end", "high"])

    def test_effective_priority_frozen_after_claim(self) -> None:
        # 任务被工作线程认领即离开入站堆、有效优先级冻结：随后其执行顺序
        # 不再受继续老化影响。这里用两个工作线程保证一个任务在另一个仍在
        # 老化等待期间已被认领并执行；不抢占、不回插。
        order: list = []
        lock = threading.Lock()
        block_first = threading.Event()
        first_running = threading.Event()

        def first() -> str:
            first_running.set()
            block_first.wait(5.0)  # 占住一个工作线程，迫使其余任务排队
            with lock:
                order.append("first")
            return "first"

        with Scheduler(2, 8, aging_interval_ms=20) as s:
            h_first = s.submit_nowait("first", first, priority=10)
            self.assertTrue(first_running.wait(2.0))
            # 唯一剩余许可将立即认领 queued；它一旦被认领，即使之后长期
            # 等待（block_first 不放行），其执行位置也不再变化。
            queued = s.submit_nowait("q", _recorder("q", order, lock),
                                     priority=0)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s._tasks["q"].started:  # type: ignore[attr-defined]
                    break
                time.sleep(0.002)
            self.assertTrue(s._tasks["q"].started)  # type: ignore[attr-defined]
            block_first.set()
            self.assertEqual(queued.result(), "q")
            self.assertEqual(h_first.result(), "first")
        self.assertEqual(order[0], "q")  # 已认领的 q 不被任何老化回插


class AgingTerminalStateTest(unittest.TestCase):
    def test_cancelled_queued_task_skipped(self) -> None:
        # 老化让低优先级任务升到堆顶，但它在认领前被取消：不参与派发，
        # 下一个任务照常执行。
        order: list = []
        lock = threading.Lock()
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8, aging_interval_ms=20) as s:
            holder = threading.Thread(target=s.submit, args=("_", occupy))
            holder.start()
            self.assertTrue(entered.wait(2.0))
            aging_low = s.submit_nowait("low",
                                        _recorder("low", order, lock),
                                        priority=0)
            other = s.submit_nowait("other",
                                    _recorder("other", order, lock),
                                    priority=0)
            time.sleep(0.1)  # low 已老化到前面
            self.assertTrue(aging_low.cancel())
            with self.assertRaises(TaskCancelledError):
                aging_low.result()
            free.set()
            self.assertEqual(other.result(), "other")
            holder.join()
        self.assertEqual(order, ["other"])

    def test_aging_does_not_change_queue_deadline(self) -> None:
        # 老化不改变 max_queue_wait_ms：认领前到期的任务仍进入 expired，
        # callable 不执行，读取结果抛 QueueTimeoutError。
        ran: list = []
        occupy, entered, free = _hold_worker()
        with Scheduler(1, 8, aging_interval_ms=5) as s:
            holder = s.submit_nowait("hold", occupy)
            self.assertTrue(entered.wait(2.0))
            h = s.submit_nowait(
                "x", lambda: ran.append("x"),
                max_queue_wait_ms=30,
            )
            with self.assertRaises(QueueTimeoutError):
                h.result()
            free.set()
            self.assertIsNone(holder.result())
        self.assertEqual(ran, [])
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        self.assertEqual(snap.expired, 1)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.cancelled, 0)

    def test_stats_and_latency_unaffected(self) -> None:
        with Scheduler(2, 8, aging_interval_ms=10) as s:
            for i in range(6):
                self.assertEqual(
                    s.submit("t%d" % i, lambda i=i: i), i
                )
            snap = s.snapshot()
        self.assertEqual(snap.accepted, 6)
        self.assertEqual(snap.completed, 6)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(snap.cancelled, 0)
        self.assertEqual(snap.expired, 0)
        self.assertEqual(snap.rejected, 0)
        self.assertEqual(
            set(snap.to_dict()),
            {
                "accepted", "completed", "failed", "cancelled",
                "expired", "rejected", "admission_wait_ms",
                "queue_wait_ms", "total_latency_ms",
                "execution_ms",
            },
        )


class AgingCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def _write(self, tasks: object) -> str:
        path = os.path.join(self.dir, "tasks.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(tasks, f)
        return path

    def test_invalid_flag_exits_2(self) -> None:
        path = self._write([{"task_id": "a", "sleep_ms": 0}])
        for bad in ("0", "-3", "1.5", "true", "abc"):
            with self.subTest(bad=bad):
                err = io.StringIO()
                with redirect_stderr(err):
                    code = cli_run([
                        "--input", path, "--workers", "1",
                        "--max-pending", "1",
                        "--aging-interval-ms", bad,
                    ])
                self.assertEqual(code, 2)
                self.assertTrue(
                    err.getvalue().strip().startswith("InputValidationError:"),
                    err.getvalue(),
                )

    def test_flag_default_off_and_output_shape_unchanged(self) -> None:
        # 不传该参数：正常执行，结果数组与 stats 结构不变。
        path = self._write([
            {"task_id": "t%d" % i, "sleep_ms": 0} for i in range(3)
        ])
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli_run([
                "--input", path, "--workers", "2", "--max-pending", "4",
            ])
        self.assertEqual(code, 0)
        report = json.loads(out.getvalue())
        self.assertEqual([r["task_id"] for r in report["results"]],
                         ["t0", "t1", "t2"])
        self.assertEqual(
            set(report["stats"]),
            {"accepted", "completed", "failed", "cancelled", "expired",
             "rejected", "admission_wait_ms", "queue_wait_ms",
             "total_latency_ms", "execution_ms"},
        )

    def test_valid_flag_runs_and_preserves_input_order(self) -> None:
        # 合法老化值下结果数组仍严格按输入顺序返回，对象字段不新增。
        path = self._write([
            {"task_id": "low", "sleep_ms": 0, "priority": -2},
            {"task_id": "high", "sleep_ms": 0, "priority": 9},
        ])
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli_run([
                "--input", path, "--workers", "1", "--max-pending", "2",
                "--aging-interval-ms", "20",
            ])
        self.assertEqual(code, 0)
        report = json.loads(out.getvalue())
        self.assertEqual([r["task_id"] for r in report["results"]],
                         ["low", "high"])
        for r in report["results"]:
            self.assertEqual(set(r), {"task_id", "result"})
        self.assertEqual(report["stats"]["completed"], 2)


if __name__ == "__main__":
    unittest.main()
