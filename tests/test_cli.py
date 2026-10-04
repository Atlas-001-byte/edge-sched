"""命令行入口的端到端测试。"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from edge_sched.cli import run

_TOP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CliValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def _write(self, name: str, content: str) -> str:
        path = os.path.join(self.dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def _expect_exit_2(self, content: object, workers: str = "2",
                       max_pending: str = "4") -> str:
        import io
        from contextlib import redirect_stderr

        path = self._write("tasks.json",
                           content if isinstance(content, str)
                           else json.dumps(content))
        err = io.StringIO()
        with redirect_stderr(err):
            code = run(["--input", path, "--workers", workers,
                        "--max-pending", max_pending])
        self.assertEqual(code, 2)
        message = err.getvalue().strip()
        self.assertTrue(message.startswith("InputValidationError:"), message)
        return message

    def test_invalid_json(self) -> None:
        self._expect_exit_2("{not json")

    def test_root_not_array(self) -> None:
        self._expect_exit_2({"task_id": "a", "sleep_ms": 1})

    def test_missing_fields(self) -> None:
        self._expect_exit_2([{"task_id": "a"}])
        self._expect_exit_2([{"sleep_ms": 1}])

    def test_bad_sleep_types(self) -> None:
        self._expect_exit_2([{"task_id": "a", "sleep_ms": -1}])
        self._expect_exit_2([{"task_id": "a", "sleep_ms": 1.5}])
        self._expect_exit_2([{"task_id": "a", "sleep_ms": "2"}])
        self._expect_exit_2([{"task_id": "a", "sleep_ms": True}])
        self._expect_exit_2([{"task_id": "a", "sleep_ms": None}])

    def test_bad_task_id(self) -> None:
        self._expect_exit_2([{"task_id": "", "sleep_ms": 0}])
        self._expect_exit_2([{"task_id": 1, "sleep_ms": 0}])

    def test_duplicate_task_id(self) -> None:
        self._expect_exit_2([
            {"task_id": "a", "sleep_ms": 0},
            {"task_id": "a", "sleep_ms": 1},
        ])

    def test_bad_concurrency(self) -> None:
        good = [{"task_id": "a", "sleep_ms": 0}]
        self._expect_exit_2(good, workers="0")
        self._expect_exit_2(good, workers="-2")
        self._expect_exit_2(good, workers="1.5")
        self._expect_exit_2(good, max_pending="0")
        self._expect_exit_2(good, max_pending="x")

    def test_missing_file(self) -> None:
        import io
        from contextlib import redirect_stderr

        err = io.StringIO()
        with redirect_stderr(err):
            code = run(["--input", os.path.join(self.dir, "nope.json"),
                        "--workers", "1", "--max-pending", "1"])
        self.assertEqual(code, 2)
        self.assertIn("InputValidationError", err.getvalue())

    def test_bad_priority_types(self) -> None:
        for bad in (1.5, "2", True, False, None, [1]):
            self._expect_exit_2(
                [{"task_id": "a", "sleep_ms": 0, "priority": bad}]
            )

    def test_bad_max_queue_wait_ms_types(self) -> None:
        # bool、0、负数、浮点数、字符串等都在读取输入时报 InputValidationError。
        for bad in (True, False, 0, -2, 1.5, "10", [1], 0.0):
            self._expect_exit_2(
                [{"task_id": "a", "sleep_ms": 0, "max_queue_wait_ms": bad}]
            )

    def test_element_not_object(self) -> None:
        self._expect_exit_2([1, 2])


class CliSuccessTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def _execute(self, tasks: object, workers: int = 4,
                 max_pending: int = 8) -> dict:
        import io
        from contextlib import redirect_stdout

        path = os.path.join(self.dir, "tasks.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(tasks, f)

        out = io.StringIO()
        with redirect_stdout(out):
            code = run(["--input", path, "--workers", str(workers),
                        "--max-pending", str(max_pending)])
        self.assertEqual(code, 0)
        return json.loads(out.getvalue())

    def test_output_shape_and_stats(self) -> None:
        tasks = [{"task_id": "t%d" % i, "sleep_ms": 0} for i in range(5)]
        report = self._execute(tasks)
        self.assertEqual([r["task_id"] for r in report["results"]],
                         ["t0", "t1", "t2", "t3", "t4"])
        for r in report["results"]:
            self.assertIsNone(r["result"])
        stats = report["stats"]
        self.assertEqual(stats["accepted"], 5)
        self.assertEqual(stats["completed"], 5)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(stats["rejected"], 0)
        for group in ("queue_wait_ms", "total_latency_ms"):
            self.assertEqual(
                set(stats[group]), {"p50", "p95", "p99", "max"}
            )

    def test_tasks_actually_wait_and_run_parallel(self) -> None:
        # 4 个各 200ms 的任务跑在 4 线程上，总墙钟应明显小于串行 800ms。
        tasks = [{"task_id": "w%d" % i, "sleep_ms": 200} for i in range(4)]
        import time
        start = time.monotonic()
        report = self._execute(tasks, workers=4, max_pending=4)
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.6)
        self.assertEqual(report["stats"]["completed"], 4)

    def test_empty_array(self) -> None:
        report = self._execute([])
        self.assertEqual(report["results"], [])
        stats = report["stats"]
        self.assertEqual(
            (stats["accepted"], stats["completed"],
             stats["failed"], stats["rejected"]),
            (0, 0, 0, 0),
        )
        self.assertEqual(stats["queue_wait_ms"]["max"], 0.0)
        self.assertEqual(stats["total_latency_ms"]["p99"], 0.0)

    def test_priority_optional_default_zero_and_output_input_order(self) -> None:
        # priority 缺省或为负都合法；结果数组严格按输入顺序返回，
        # 不按优先级重排，且结果对象不新增字段。
        tasks = [
            {"task_id": "low", "sleep_ms": 0, "priority": -5},
            {"task_id": "mid", "sleep_ms": 0},
            {"task_id": "high", "sleep_ms": 0, "priority": 9},
        ]
        report = self._execute(tasks, workers=1, max_pending=3)
        self.assertEqual(
            [r["task_id"] for r in report["results"]],
            ["low", "mid", "high"],
        )
        for r in report["results"]:
            self.assertEqual(set(r), {"task_id", "result"})
        self.assertEqual(report["stats"]["completed"], 3)

    def test_max_queue_wait_ms_optional_and_valid(self) -> None:
        # 字段可省略，也可给 >= 1 的整数；统计快照新增 expired 口径。
        tasks = [
            {"task_id": "a", "sleep_ms": 0},
            {"task_id": "b", "sleep_ms": 0, "max_queue_wait_ms": 60000},
            {"task_id": "c", "sleep_ms": 0, "max_queue_wait_ms": 1000},
        ]
        report = self._execute(tasks, workers=3, max_pending=3)
        self.assertEqual(
            [r["task_id"] for r in report["results"]], ["a", "b", "c"]
        )
        for r in report["results"]:
            self.assertEqual(set(r), {"task_id", "result"})
        stats = report["stats"]
        self.assertEqual(stats["accepted"], 3)
        self.assertEqual(stats["completed"], 3)
        self.assertEqual(stats["expired"], 0)

    def test_expired_task_keeps_input_position_with_error_object(self) -> None:
        # 单工作线程被首个长任务占住，后续带短排队时限的任务到期：
        # 到期任务在 results 中保留输入位置，对象只含 task_id 与 error。
        tasks = [{"task_id": "slow", "sleep_ms": 800}]
        tasks += [
            {"task_id": "q%d" % i, "sleep_ms": 0, "max_queue_wait_ms": 40}
            for i in range(4)
        ]
        report = self._execute(tasks, workers=1, max_pending=8)
        results = report["results"]
        self.assertEqual(
            [r["task_id"] for r in results],
            ["slow", "q0", "q1", "q2", "q3"],
        )
        # 长任务不带时限，必然完成。
        self.assertEqual(results[0], {"task_id": "slow", "result": None})
        errors = [r for r in results if "error" in r]
        # 工作线程被占 800ms，排在后面的短时限任务到期（放宽下限以避免
        # 提交线程调度顺序带来的偶发波动）。
        self.assertGreaterEqual(len(errors), 2)
        for r in errors:
            self.assertEqual(set(r), {"task_id", "error"})
            self.assertEqual(r["error"], "QueueTimeoutError")
        for r in results:
            if "error" not in r:
                self.assertEqual(set(r), {"task_id", "result"})
        stats = report["stats"]
        self.assertEqual(stats["accepted"], 5)
        self.assertEqual(stats["expired"], len(errors))
        self.assertEqual(stats["completed"], 5 - len(errors))
        self.assertEqual(stats["cancelled"], 0)
        self.assertEqual(stats["failed"], 0)


class CliModuleTest(unittest.TestCase):
    """直接以子进程运行 python -m edge_sched，验证模块入口与退出码。"""

    def test_module_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "tasks.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump([{"task_id": "a", "sleep_ms": 0}], f)
            proc = subprocess.run(
                [sys.executable, "-m", "edge_sched", "--input", path,
                 "--workers", "1", "--max-pending", "1"],
                cwd=_TOP, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertEqual(report["results"][0]["task_id"], "a")

    def test_module_exit_code_2(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "tasks.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write("not-json")
            proc = subprocess.run(
                [sys.executable, "-m", "edge_sched", "--input", path,
                 "--workers", "1", "--max-pending", "1"],
                cwd=_TOP, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertIn("InputValidationError", proc.stderr)
            self.assertEqual(proc.stdout, "")


if __name__ == "__main__":
    unittest.main()
