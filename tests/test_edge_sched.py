"""Specification tests for edge_sched (stdlib unittest only)."""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    Scheduler,
    SchedulerClosedError,
)
from edge_sched.stats import percentile


class SchedulerTests(unittest.TestCase):
    def test_basic_result_and_order(self) -> None:
        with Scheduler(workers=2, max_pending=4) as scheduler:
            f1 = scheduler.submit("a", lambda: 1)
            f2 = scheduler.submit("b", lambda: 2)
            self.assertEqual(f1.result(), 1)
            self.assertEqual(f2.result(), 2)
            snap = scheduler.snapshot()
            self.assertEqual(snap.accepted, 2)
            self.assertEqual(snap.completed, 2)
            self.assertEqual(snap.failed, 0)
            self.assertEqual(snap.rejected, 0)

    def test_exception_is_original_and_keeps_scheduler_alive(self) -> None:
        boom = ValueError("boom")
        with Scheduler(workers=2, max_pending=4) as scheduler:
            bad = scheduler.submit("bad", lambda: (_ for _ in ()).throw(boom))
            good = scheduler.submit("good", lambda: "ok")
            with self.assertRaises(ValueError) as ctx:
                bad.result()
            self.assertIs(ctx.exception, boom)
            self.assertEqual(good.result(), "ok")
            snap = scheduler.snapshot()
            self.assertEqual(snap.completed, 1)
            self.assertEqual(snap.failed, 1)
            self.assertEqual(len(snap.total_latency_ms.samples), 2)

    def test_backpressure_rejects_and_counts(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def blocker() -> None:
            started.set()
            release.wait(2.0)

        with Scheduler(workers=1, max_pending=1) as scheduler:
            scheduler.submit("t1", blocker)
            self.assertTrue(started.wait(1.0))
            with self.assertRaises(BackpressureError):
                scheduler.submit("t2", lambda: None)
            self.assertNotIn("t2", scheduler._records)
            snap = scheduler.snapshot()
            self.assertEqual(snap.rejected, 1)
            self.assertEqual(snap.accepted, 1)
            self.assertEqual(snap.total_latency_ms.samples, [])
            release.set()
        # Capacity frees after tasks finish and the rejected id is usable.
        with Scheduler(workers=1, max_pending=1) as scheduler:
            self.assertEqual(scheduler.submit("t2", lambda: 7).result(), 7)

    def test_duplicate_unfinished_id(self) -> None:
        gate = threading.Event()
        with Scheduler(workers=1, max_pending=4) as scheduler:
            scheduler.submit("dup", lambda: gate.wait(2.0))
            with self.assertRaises(DuplicateTaskError):
                scheduler.submit("dup", lambda: None)
            gate.set()
            # Finished ids may be reused.
            scheduler.wait("dup")
            self.assertEqual(scheduler.submit("dup", lambda: "again").result(), "again")

    def test_closed_scheduler_rejects_then_drains(self) -> None:
        with Scheduler(workers=1, max_pending=4) as scheduler:
            future = scheduler.submit("late", lambda: (time.sleep(0.05), "done")[1])
            scheduler.close()
            with self.assertRaises(SchedulerClosedError):
                scheduler.submit("x", lambda: None)
            # Results of accepted tasks remain readable after shutdown.
            self.assertEqual(future.result(), "done")
        # close() is idempotent
        scheduler.close()

    def test_input_validation(self) -> None:
        for bad_id in (1, b"x", "", None):
            with self.assertRaises(InputValidationError):
                Scheduler(workers=1, max_pending=1).submit(bad_id, lambda: None)  # type: ignore[arg-type]
        with self.assertRaises(InputValidationError):
            Scheduler(workers=1, max_pending=1).submit("z", 42)  # type: ignore[arg-type]
        for workers, pending in ((0, 1), (1, 0), (-1, 1), (1, -2)):
            with self.assertRaises(InputValidationError):
                Scheduler(workers=workers, max_pending=pending)

    def test_concurrent_same_id_has_single_outcome(self) -> None:
        outcomes = {"ok": 0, "dup": 0}
        lock = threading.Lock()
        gate = threading.Event()
        start = threading.Barrier(9)

        def attempt(scheduler: Scheduler) -> None:
            start.wait()
            try:
                scheduler.submit("same", lambda: gate.wait(2.0))
                with lock:
                    outcomes["ok"] += 1
            except DuplicateTaskError:
                with lock:
                    outcomes["dup"] += 1

        with Scheduler(workers=2, max_pending=8) as scheduler:
            threads = [
                threading.Thread(target=attempt, args=(scheduler,))
                for _ in range(8)
            ]
            for t in threads:
                t.start()
            start.wait()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                with lock:
                    if outcomes["ok"] + outcomes["dup"] == 8:
                        break
                time.sleep(0.01)
            gate.set()
            for t in threads:
                t.join()
        self.assertEqual(outcomes["ok"], 1)
        self.assertEqual(outcomes["dup"], 7)

    def test_snapshot_is_cumulative_and_immutable(self) -> None:
        with Scheduler(workers=4, max_pending=8) as scheduler:
            for i in range(10):
                scheduler.submit(f"t{i}", lambda i=i: i).result()
            snap1 = scheduler.snapshot()
            self.assertEqual(snap1.completed, 10)
            scheduler.submit("more", lambda: 11).result()
            self.assertEqual(snap1.completed, 10)  # unchanged
            self.assertEqual(len(snap1.total_latency_ms.samples), 10)
            snap2 = scheduler.snapshot()
            self.assertEqual(snap2.completed, 11)

    def test_latency_samples_reflect_real_waits(self) -> None:
        with Scheduler(workers=1, max_pending=3) as scheduler:
            scheduler.submit("s", lambda: time.sleep(0.05)).result()
            snap = scheduler.snapshot()
            total = snap.total_latency_ms.samples[0]
            wait = snap.queue_wait_ms.samples[0]
            self.assertGreaterEqual(total, 45.0)
            self.assertGreaterEqual(wait, 0.0)
            self.assertLessEqual(wait, total)


class PercentileTests(unittest.TestCase):
    def test_nearest_rank(self) -> None:
        samples = [float(i) for i in range(1, 101)]  # 1..100
        self.assertEqual(percentile(samples, 0.50), 50.0)
        self.assertEqual(percentile(samples, 0.95), 95.0)
        self.assertEqual(percentile(samples, 0.99), 99.0)
        self.assertEqual(percentile(samples[:20], 0.99), 20.0)  # ceil(19.8)
        self.assertEqual(percentile([], 0.95), 0.0)

    def test_empty_snapshot_distribution_is_zero(self) -> None:
        with Scheduler(workers=1, max_pending=1) as scheduler:
            snap = scheduler.snapshot()
        for dist in (snap.queue_wait_ms, snap.total_latency_ms):
            self.assertEqual(dist.samples, [])
            self.assertEqual((dist.p50, dist.p95, dist.p99, dist.max), (0.0, 0.0, 0.0, 0.0))

    def test_rounding_three_decimals(self) -> None:
        with Scheduler(workers=1, max_pending=1) as scheduler:
            scheduler.submit("x", lambda: None).result()
            snap = scheduler.snapshot()
        for value in (*snap.total_latency_ms.samples, snap.total_latency_ms.max):
            self.assertEqual(round(value, 3), value)


class CliTests(unittest.TestCase):
    def run_cli(self, payload: object, workers: str = "2", pending: str = "4"):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "tasks-unique.json")
            if isinstance(payload, str):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(payload)
            else:
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle)
            return subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "edge_sched",
                    "--input",
                    path,
                    "--workers",
                    workers,
                    "--max-pending",
                    pending,
                ],
                capture_output=True,
                text=True,
                timeout=20,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            )

    def test_happy_path(self) -> None:
        proc = self.run_cli(
            [
                {"task_id": "a", "sleep_ms": 10},
                {"task_id": "b", "sleep_ms": 0},
            ]
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        data = json.loads(proc.stdout)
        self.assertEqual([r["task_id"] for r in data["results"]], ["a", "b"])
        stats = data["stats"]
        self.assertEqual((stats["accepted"], stats["completed"], stats["failed"]), (2, 2, 0))
        self.assertIn("p99", stats["queue_wait_ms"])

    def test_backpressure_small_limit_still_completes_all(self) -> None:
        proc = self.run_cli(
            [{"task_id": f"t{i}", "sleep_ms": 5} for i in range(6)],
            workers="1",
            pending="2",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        data = json.loads(proc.stdout)
        self.assertEqual(len(data["results"]), 6)

    def test_invalid_inputs_exit_2(self) -> None:
        cases = [
            ("{not json", "2", "4"),
            ([{"task_id": "a"}], "2", "4"),  # missing sleep_ms
            ([{"sleep_ms": 1}], "2", "4"),  # missing task_id
            ([{"task_id": "a", "sleep_ms": 1.5}], "2", "4"),  # non-integer
            ([{"task_id": "a", "sleep_ms": -1}], "2", "4"),  # negative
            ([{"task_id": "a", "sleep_ms": 1}, {"task_id": "a", "sleep_ms": 2}], "2", "4"),
            ([{"task_id": "", "sleep_ms": 1}], "2", "4"),
            ([{"task_id": "a", "sleep_ms": 1}], "0", "4"),
            ([{"task_id": "a", "sleep_ms": 1}], "2", "0"),
        ]
        for payload, workers, pending in cases:
            proc = self.run_cli(payload, workers, pending)
            self.assertEqual(proc.returncode, 2, payload)
            self.assertIn("InputValidationError", proc.stderr)


if __name__ == "__main__":
    unittest.main()
