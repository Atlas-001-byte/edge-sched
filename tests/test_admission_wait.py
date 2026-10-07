"""admission_wait_ms 准入等待分布的调度器级测试。

口径：每次有效提交从进入容量准入判定到被原子接纳产生一个样本（毫秒，
三位小数）；调用瞬间取得名额记 0.0，进入过容量等待队列的从入队算到
接纳，成组接纳时组内每个任务样本相同。拒绝、校验失败、同名冲突与
close 后未接纳的提交不产生样本；接纳后无论成功、失败、取消或到期，
样本都保留。区间统计按接纳时刻归属。
"""

import json
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
)

_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


def _wait_until(pred, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.005)
    return False


class AdmissionWaitImmediateTest(unittest.TestCase):
    def test_immediate_submissions_record_zero(self) -> None:
        # submit / submit_nowait 在调用瞬间取得名额：样本为 0.0，
        # 每个被接纳任务恰好一个样本。
        with Scheduler(workers=2, max_pending=4) as s:
            self.assertEqual(s.submit("a", lambda: 1), 1)
            handle = s.submit_nowait("b", lambda: 2)
            self.assertEqual(handle.result(), 2)
            self.assertEqual(s.submit_with_wait("c", lambda: 3), 3)
            handles = s.submit_batch_with_wait(
                [{"task_id": "d", "fn": lambda: 4},
                 {"task_id": "e", "fn": lambda: 5}]
            )
            self.assertEqual([h.result() for h in handles], [4, 5])
            snap = s.snapshot()
            self.assertEqual(snap.accepted, 5)
            self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)

    def test_empty_snapshot_has_empty_distribution(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            self.assertEqual(s.snapshot().admission_wait_ms, _EMPTY_DIST)


class AdmissionWaitQueueTest(unittest.TestCase):
    def test_waited_submission_records_positive_sample(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            outcome: "dict[str, object]" = {}

            def queued_submit() -> None:
                try:
                    outcome["value"] = s.submit_with_wait(
                        "w", lambda: "done", admission_timeout_ms=5_000
                    )
                except Exception as exc:  # pragma: no cover - 防御
                    outcome["error"] = exc

            t = threading.Thread(target=queued_submit)
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            t.join()
            self.assertEqual(outcome.get("value"), "done")
            self.assertEqual(blocker.result(), None)

            snap = s.snapshot()
            self.assertEqual(snap.accepted, 2)
            # 等待者样本为正且明显大于 0（阻塞任务约 200ms）；即时接纳的
            # blocker 样本为 0.0，故 p50 为 0.0、max 为等待样本。
            self.assertGreater(snap.admission_wait_ms["max"], 50.0)
            self.assertEqual(snap.admission_wait_ms["p50"], 0.0)

    def test_batch_group_shares_one_sample(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            outcome: "dict[str, object]" = {}

            def batch_submit() -> None:
                try:
                    outcome["handles"] = s.submit_batch_with_wait(
                        [{"task_id": "g1", "fn": lambda: 1},
                         {"task_id": "g2", "fn": lambda: 2}],
                        admission_timeout_ms=5_000,
                    )
                except Exception as exc:  # pragma: no cover - 防御
                    outcome["error"] = exc

            t = threading.Thread(target=batch_submit)
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            t.join()
            self.assertNotIn("error", outcome)
            blocker.result()
            for h in outcome["handles"]:  # type: ignore[union-attr]
                h.result()

            snap = s.snapshot()
            self.assertEqual(snap.accepted, 3)
            dist = snap.admission_wait_ms
            # 样本为 [0.0, x, x]（组内两个任务共享同一等待样本）：
            # p50 与 max 同为 x，且 x 明显为正。
            self.assertGreater(dist["max"], 50.0)
            self.assertEqual(dist["p50"], dist["max"])
            self.assertEqual(dist["p95"], dist["max"])


class AdmissionWaitNoSampleTest(unittest.TestCase):
    def test_rejected_and_invalid_submissions_add_no_sample(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            before = s.snapshot()
            self.assertEqual(before.accepted, 1)

            # 背压拒绝：submit_nowait 满员即时拒绝。
            with self.assertRaises(BackpressureError):
                s.submit_nowait("rej1", lambda: None)
            # submit_with_wait admission_timeout_ms=0：无空位立即拒绝。
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("rej2", lambda: None,
                                   admission_timeout_ms=0)
            # 同名冲突与参数校验失败同样不产生样本。
            with self.assertRaises(DuplicateTaskError):
                s.submit_nowait("blocker", lambda: None)
            with self.assertRaises(InputValidationError):
                s.submit_nowait("bad", lambda: None, priority=True)  # type: ignore[arg-type]

            after = s.snapshot()
            self.assertEqual(after.accepted, 1)
            self.assertEqual(after.rejected, 2)
            self.assertEqual(after.admission_wait_ms,
                             before.admission_wait_ms)
            blocker.result()

    def test_admission_timeout_rejection_adds_no_sample(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            self.assertTrue(
                _wait_until(lambda: s.snapshot().accepted == 1)
            )
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("rej", lambda: None,
                                   admission_timeout_ms=30)
            snap = s.snapshot()
            self.assertEqual(snap.accepted, 1)
            self.assertEqual(snap.rejected, 1)
            self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)
            blocker.result()

    def test_close_with_waiter_adds_no_sample(self) -> None:
        s = Scheduler(workers=1, max_pending=1)
        blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
        outcome: "dict[str, object]" = {}

        def queued_submit() -> None:
            try:
                s.submit_with_wait("w", lambda: None,
                                   admission_timeout_ms=None)
            except SchedulerClosedError:
                outcome["closed"] = True
            except Exception as exc:  # pragma: no cover - 防御
                outcome["error"] = exc

        t = threading.Thread(target=queued_submit)
        t.start()
        self.assertTrue(
            _wait_until(lambda: s.runtime_snapshot().admission_waiters == 1)
        )
        s.close()
        t.join()
        self.assertEqual(outcome.get("closed"), True)
        self.assertEqual(blocker.result(), None)

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 1)
        self.assertEqual(snap.rejected, 0)
        self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)


class AdmissionWaitRetainedTest(unittest.TestCase):
    def test_sample_retained_after_cancel_and_expiry(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            # 即时接纳后被取消：0.0 样本保留。
            victim = s.submit_nowait("victim", lambda: None)
            self.assertTrue(victim.cancel())
            with self.assertRaises(TaskCancelledError):
                victim.result()
            # 即时接纳后排队到期：0.0 样本保留。
            exp = s.submit_nowait("exp", lambda: None, max_queue_wait_ms=20)
            with self.assertRaises(QueueTimeoutError):
                exp.result()
            blocker.result()

            snap = s.snapshot()
            self.assertEqual(snap.accepted, 3)
            self.assertEqual(snap.cancelled, 1)
            self.assertEqual(snap.expired, 1)
            self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)

    def test_waited_sample_retained_after_expiry(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            # 占满余量，使随后的 submit_with_wait 必须排队等名额。
            filler = s.submit_nowait("filler", lambda: time.sleep(0.4))
            outcome: "dict[str, object]" = {}

            def queued_submit() -> None:
                try:
                    s.submit_with_wait(
                        "w", lambda: None,
                        max_queue_wait_ms=20,
                        admission_timeout_ms=5_000,
                    )
                except QueueTimeoutError:
                    outcome["expired"] = True
                except Exception as exc:  # pragma: no cover - 防御
                    outcome["error"] = exc

            t = threading.Thread(target=queued_submit)
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            t.join()
            self.assertEqual(outcome.get("expired"), True)
            blocker.result()
            filler.result()

            snap = s.snapshot()
            self.assertEqual(snap.accepted, 3)
            self.assertEqual(snap.expired, 1)
            # 等待约 200ms 才获接纳的任务虽已到期，其准入样本仍保留。
            self.assertGreater(snap.admission_wait_ms["max"], 50.0)


class AdmissionWaitIntervalTest(unittest.TestCase):
    def test_interval_attribution_by_admission_moment(self) -> None:
        with Scheduler(workers=1, max_pending=1) as s:
            blocker = s.submit_nowait("blocker", lambda: time.sleep(0.2))
            outcome: "dict[str, object]" = {}

            def queued_submit() -> None:
                outcome["value"] = s.submit_with_wait(
                    "w", lambda: "done", admission_timeout_ms=5_000
                )

            t = threading.Thread(target=queued_submit)
            t.start()
            self.assertTrue(
                _wait_until(
                    lambda: s.runtime_snapshot().admission_waiters == 1
                )
            )
            # 边界落在 blocker 接纳之后、w 接纳之前。
            cp = s.stats_checkpoint()
            t.join()
            self.assertEqual(outcome.get("value"), "done")
            blocker.result()

            interval = s.snapshot_since(cp)
            # w 的接纳与结束都在区间内；blocker 的 0.0 准入样本在边界前，
            # 不进入本区间（否则 p50 会被拉成 0.0）。
            self.assertEqual(interval.accepted, 1)
            self.assertEqual(interval.completed, 2)
            self.assertGreater(interval.admission_wait_ms["p50"], 50.0)
            self.assertEqual(
                interval.admission_wait_ms["p50"],
                interval.admission_wait_ms["max"],
            )
            # 累计快照仍含两个准入样本（blocker 的 0.0 与 w 的等待样本）。
            total = s.snapshot()
            self.assertEqual(total.accepted, 2)
            self.assertEqual(total.admission_wait_ms["p50"], 0.0)
            self.assertEqual(
                total.admission_wait_ms["max"],
                interval.admission_wait_ms["max"],
            )


class AdmissionWaitCliTest(unittest.TestCase):
    def test_cli_stats_always_include_admission_wait_ms(self) -> None:
        import io
        import json as json_mod
        import os
        import tempfile
        from contextlib import redirect_stdout

        from edge_sched.cli import run

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "tasks.json")
            with open(path, "w", encoding="utf-8") as f:
                json_mod.dump(
                    [{"task_id": "t%d" % i, "sleep_ms": 0} for i in range(3)],
                    f,
                )
            out = io.StringIO()
            with redirect_stdout(out):
                code = run(["--input", path, "--workers", "2",
                            "--max-pending", "4"])
        self.assertEqual(code, 0)
        report = json.loads(out.getvalue())
        # 结果数组仍按输入顺序，结果对象形态不变。
        self.assertEqual([r["task_id"] for r in report["results"]],
                         ["t0", "t1", "t2"])
        for r in report["results"]:
            self.assertEqual(set(r), {"task_id", "result"})
        dist = report["stats"]["admission_wait_ms"]
        self.assertEqual(set(dist), {"p50", "p95", "p99", "max"})
        # CLI 即时提交全部在调用瞬间取得名额：样本全为 0.0。
        self.assertEqual(dist, _EMPTY_DIST)
        self.assertEqual(report["stats"]["accepted"], 3)


if __name__ == "__main__":
    unittest.main()
