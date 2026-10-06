"""execution_ms 执行耗时分布的测试。

口径：从工作线程原子认领后开始计时，到 callable 正常返回或抛出
Exception 结束；成功与失败任务各恰好贡献一个样本；取消、到期、拒绝、
校验失败的任务不贡献样本。分位口径与 queue_wait_ms / total_latency_ms
一致，空分布四项均为 0.0。
"""

import time
import unittest

from edge_sched import (
    BackpressureError,
    InputValidationError,
    QueueTimeoutError,
    Scheduler,
    TaskCancelledError,
)

_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


def _sleep(ms: int) -> None:
    time.sleep(ms / 1000.0)


class ExecutionTimingTest(unittest.TestCase):
    def test_success_contributes_exactly_one_sample(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            self.assertEqual(s.submit("a", lambda: 1), 1)
            snap = s.snapshot()
        self.assertEqual(snap.completed, 1)
        self.assertEqual(snap.failed, 0)
        self.assertEqual(len(snap._execution_samples), 1)
        # 空分布之外的样本非负，且执行耗时不超过总时延。
        self.assertGreaterEqual(snap.execution_ms["max"], 0.0)
        self.assertLessEqual(
            snap.execution_ms["max"], snap.total_latency_ms["max"]
        )

    def test_execution_reflects_callable_duration(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit("slow", lambda: _sleep(80))
            snap = s.snapshot()
        # 执行耗时应覆盖 callable 的真实睡眠（留调度抖动余量）。
        self.assertGreaterEqual(snap.execution_ms["max"], 60.0)
        self.assertGreaterEqual(
            snap.total_latency_ms["max"], snap.execution_ms["max"]
        )

    def test_execution_excludes_pre_claim_wait(self) -> None:
        # 单工作线程被长任务占住：第二个任务排队很久但执行极短，
        # 其执行耗时不得包含认领前的排队等待。
        with Scheduler(workers=1, max_pending=2) as s:
            h1 = s.submit_nowait("blocker", lambda: _sleep(300))
            h2 = s.submit_nowait("queued", lambda: None)
            h1.result()
            h2.result()
            snap = s.snapshot()
        queued_exec = sorted(snap._execution_samples)[0]
        queued_wait = sorted(snap._queue_wait_samples)[1]
        self.assertGreaterEqual(queued_wait, 200.0)
        self.assertLess(queued_exec, queued_wait)

    def test_failure_contributes_one_sample_and_reraises(self) -> None:
        def boom() -> None:
            _sleep(30)
            raise ValueError("nope")

        with Scheduler(workers=1, max_pending=2) as s:
            with self.assertRaises(ValueError):
                s.submit("bad", boom)
            snap = s.snapshot()
            # 失败任务的结果仍按原异常读出。
            with self.assertRaises(ValueError):
                s.result("bad")
        self.assertEqual(snap.failed, 1)
        self.assertEqual(snap.completed, 0)
        self.assertEqual(len(snap._execution_samples), 1)
        self.assertGreaterEqual(snap.execution_ms["max"], 20.0)

    def test_handle_result_reraises_after_failure_sample(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            h = s.submit_nowait("bad", lambda: 1 / 0)
            with self.assertRaises(ZeroDivisionError):
                h.result()
            snap = s.snapshot()
        self.assertEqual(snap.failed, 1)
        self.assertEqual(len(snap._execution_samples), 1)

    def test_cancelled_task_contributes_no_sample(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            blocker = s.submit_nowait("blocker", lambda: _sleep(200))
            h = s.submit_nowait("victim", lambda: None)
            self.assertTrue(h.cancel())
            with self.assertRaises(TaskCancelledError):
                h.result()
            blocker.result()
            snap = s.snapshot()
        self.assertEqual(snap.cancelled, 1)
        self.assertEqual(snap.completed, 1)
        # 只有真正执行的一个任务贡献样本。
        self.assertEqual(len(snap._execution_samples), 1)

    def test_expired_task_contributes_no_sample(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            blocker = s.submit_nowait("blocker", lambda: _sleep(200))
            h = s.submit_nowait("imp", lambda: None, max_queue_wait_ms=20)
            with self.assertRaises(QueueTimeoutError):
                h.result()
            blocker.result()
            snap = s.snapshot()
        self.assertEqual(snap.expired, 1)
        self.assertEqual(len(snap._execution_samples), 1)

    def test_rejected_and_invalid_contribute_no_sample(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            blocker = s.submit_nowait("blocker", lambda: _sleep(200))
            with self.assertRaises(BackpressureError):
                s.submit_nowait("drop", lambda: None)
            with self.assertRaises(InputValidationError):
                s.submit_nowait("bad", lambda: None, priority=True)  # type: ignore[arg-type]
            blocker.result()
            snap = s.snapshot()
        self.assertEqual(snap.rejected, 1)
        self.assertEqual(snap.completed, 1)
        self.assertEqual(len(snap._execution_samples), 1)

    def test_empty_snapshot_distribution_is_zero(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            snap = s.snapshot()
        self.assertEqual(snap.execution_ms, _EMPTY_DIST)
        self.assertEqual(snap.to_dict()["execution_ms"], _EMPTY_DIST)


class ExecutionCheckpointTest(unittest.TestCase):
    def test_cross_boundary_task_attributes_sample_to_end_interval(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            h = s.submit_nowait("a", lambda: _sleep(50))
            cp = s.stats_checkpoint()
            h.result()

            interval = s.snapshot_since(cp)
            # 接纳在边界前：区间内 accepted 为 0；结束在边界后：计 completed
            # 并贡献执行样本。
            self.assertEqual(interval.accepted, 0)
            self.assertEqual(interval.completed, 1)
            self.assertEqual(len(interval._execution_samples), 1)
            self.assertGreaterEqual(interval.execution_ms["max"], 30.0)

            total = s.snapshot()
            self.assertEqual(total.accepted, 1)
            self.assertEqual(total.completed, 1)

    def test_interval_only_counts_later_samples(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit("before", lambda: _sleep(40))
            cp = s.stats_checkpoint()
            s.submit("after", lambda: _sleep(40))

            interval = s.snapshot_since(cp)
            self.assertEqual(len(interval._execution_samples), 1)
            self.assertEqual(len(s.snapshot()._execution_samples), 2)

    def test_repeated_query_is_stable(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            cp = s.stats_checkpoint()
            s.submit("a", lambda: _sleep(30))
            first = s.snapshot_since(cp)
            second = s.snapshot_since(cp)
        self.assertEqual(first.execution_ms, second.execution_ms)
        self.assertEqual(first.to_dict(), second.to_dict())

    def test_empty_interval_distribution_is_zero(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            s.submit("a", lambda: None)
            cp = s.stats_checkpoint()
            interval = s.snapshot_since(cp)
        self.assertEqual(interval.execution_ms, _EMPTY_DIST)

    def test_concurrent_finishers_each_contribute_one_sample(self) -> None:
        with Scheduler(workers=4, max_pending=8) as s:
            cp = s.stats_checkpoint()
            handles = [
                s.submit_nowait("t%d" % i, lambda: _sleep(20))
                for i in range(6)
            ]
            for h in handles:
                h.result()
            interval = s.snapshot_since(cp)
        self.assertEqual(interval.completed, 6)
        self.assertEqual(len(interval._execution_samples), 6)
        self.assertGreaterEqual(interval.execution_ms["p50"], 10.0)


class ExecutionCliTest(unittest.TestCase):
    def test_cli_stats_include_execution_ms(self) -> None:
        import io
        import json
        import os
        import tempfile
        from contextlib import redirect_stdout

        from edge_sched.cli import run

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "tasks.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    [{"task_id": "a", "sleep_ms": 50},
                     {"task_id": "b", "sleep_ms": 0}],
                    f,
                )
            out = io.StringIO()
            with redirect_stdout(out):
                code = run(["--input", path, "--workers", "2",
                            "--max-pending", "4"])
        self.assertEqual(code, 0)
        report = json.loads(out.getvalue())
        # 结果数组与逐项结果对象形态不变。
        self.assertEqual(
            [set(r) for r in report["results"]],
            [{"task_id", "result"}, {"task_id", "result"}],
        )
        stats = report["stats"]
        self.assertEqual(
            set(stats["execution_ms"]), {"p50", "p95", "p99", "max"}
        )
        self.assertEqual(stats["completed"], 2)
        self.assertGreaterEqual(stats["execution_ms"]["max"], 30.0)
        self.assertLessEqual(
            stats["execution_ms"]["max"], stats["total_latency_ms"]["max"]
        )


if __name__ == "__main__":
    unittest.main()
