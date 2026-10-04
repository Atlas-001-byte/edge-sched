"""命令行入口。

用法::

    python -m edge_sched --input tasks.json --workers N --max-pending M

输入文件为 JSON 数组，每个元素形如
``{"task_id": "a", "sleep_ms": 10, "priority": 1, "max_queue_wait_ms": 50}``，
其中 ``sleep_ms`` 为非负整数，``priority`` 为可选整数（缺省 0，不接受
布尔值）；priority 数值较大的任务先派发给工作线程，相同数值按文件中的
接受先后派发。``max_queue_wait_ms`` 为可选排队时限（缺省 None 表示不限；
只能为 >= 1 的整数，不接受布尔值）：任务被接受后若在时限内未被工作线程
认领，则不执行对应等待而进入到期终态。每个任务执行一次对应的空等待
（``time.sleep``），完成后向标准输出打印任务结果（仍按输入顺序）与
调度器统计快照（JSON）。到期任务的结果对象只含 ``task_id`` 与
``error``（固定为 ``"QueueTimeoutError"``），成功对象只含 ``task_id``
与 ``result``。

以下情况在标准错误打印 ``InputValidationError`` 消息并以退出码 2 结束:
JSON 非法、字段缺失或类型错误、sleep_ms 不是非负整数、priority 不是整数
或为布尔值、max_queue_wait_ms 不是 >= 1 的整数或为布尔值、task_id 重复
或非法、并发参数非法，以及输入文件无法读取。
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .errors import InputValidationError, QueueTimeoutError
from .scheduler import Scheduler

_EXIT_INVALID = 2


def _parse_concurrency(value: str, name: str) -> int:
    """把 --workers/--max-pending 的字符串参数解析为 >=1 的整数。"""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise InputValidationError(
            "%s must be an integer >= 1, got %r" % (name, value)
        ) from None
    # int("1.5") 本身会抛 ValueError；这里再拦截布尔式与越界值。
    if parsed < 1:
        raise InputValidationError(
            "%s must be an integer >= 1, got %r" % (name, value)
        )
    return parsed


def _is_nonneg_int(value: Any) -> bool:
    # bool 是 int 子类，但语义上不是“非负整数”输入，予以拒绝。
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_int(value: Any) -> bool:
    # priority 接受任意整数（含负数），但 bool 予以拒绝。
    return isinstance(value, int) and not isinstance(value, bool)


def _is_positive_int(value: Any) -> bool:
    # max_queue_wait_ms 只接受 >= 1 的整数；bool 同样予以拒绝。
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _load_tasks(path: str) -> "List[Tuple[str, int, int, Optional[int]]]":
    """读取并校验任务文件。

    返回 (task_id, sleep_ms, priority, max_queue_wait_ms) 列表（保持文件顺序）。
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except InputValidationError:
        raise
    except FileNotFoundError:
        raise InputValidationError("input file not found: %s" % path) from None
    except OSError as exc:
        raise InputValidationError(
            "cannot read input file %s: %s" % (path, exc)
        ) from None
    except json.JSONDecodeError as exc:
        raise InputValidationError("invalid JSON: %s" % exc) from None

    if not isinstance(data, list):
        raise InputValidationError(
            "input JSON must be an array of tasks, got %s"
            % type(data).__name__
        )

    tasks: "List[Tuple[str, int, int, Optional[int]]]" = []
    seen: set[str] = set()
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InputValidationError(
                "task at index %d must be a JSON object" % index
            )
        if "task_id" not in item:
            raise InputValidationError(
                "task at index %d is missing field 'task_id'" % index
            )
        if "sleep_ms" not in item:
            raise InputValidationError(
                "task at index %d is missing field 'sleep_ms'" % index
            )
        task_id = item["task_id"]
        sleep_ms = item["sleep_ms"]
        priority_raw = item.get("priority", 0)
        max_queue_wait_raw = item.get("max_queue_wait_ms", None)
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task at index %d has invalid task_id: %r" % (index, task_id)
            )
        if not _is_nonneg_int(sleep_ms):
            raise InputValidationError(
                "task %r has invalid sleep_ms: %r (expected non-negative integer)"
                % (task_id, sleep_ms)
            )
        if not _is_int(priority_raw):
            raise InputValidationError(
                "task %r has invalid priority: %r (expected integer, bool not allowed)"
                % (task_id, priority_raw)
            )
        if (max_queue_wait_raw is not None
                and not _is_positive_int(max_queue_wait_raw)):
            raise InputValidationError(
                "task %r has invalid max_queue_wait_ms: %r "
                "(expected integer >= 1, bool not allowed)"
                % (task_id, max_queue_wait_raw)
            )
        if task_id in seen:
            raise InputValidationError("duplicate task_id: %r" % task_id)
        seen.add(task_id)
        tasks.append((task_id, sleep_ms, priority_raw, max_queue_wait_raw))
    return tasks


def _make_wait(sleep_ms: int) -> "Any":
    """构造与任务对应的空等待 callable。"""

    def wait() -> None:
        time.sleep(sleep_ms / 1000.0)

    return wait


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edge_sched",
        description="高并发调度框架：按 JSON 任务文件执行空等待并输出统计。",
    )
    # 先按字符串接收，再统一用 InputValidationError 完成校验与报错。
    parser.add_argument("--input", required=True, help="任务 JSON 文件路径")
    parser.add_argument("--workers", required=True, help="工作线程数（>=1）")
    parser.add_argument(
        "--max-pending", required=True, help="最大未完成任务数（>=1）"
    )
    return parser


def run(argv: "List[str] | None" = None) -> int:
    """CLI 主逻辑，返回进程退出码。"""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        workers = _parse_concurrency(args.workers, "--workers")
        max_pending = _parse_concurrency(args.max_pending, "--max-pending")
        tasks = _load_tasks(args.input)
    except InputValidationError as exc:
        print("InputValidationError: %s" % exc, file=sys.stderr)
        return _EXIT_INVALID

    # 生产端以 max_pending 为闸门并发提交，保证框架不会因 CLI 自身的
    # 突发提交而产生背压拒绝；任务在 workers 个线程上真正并发执行。
    # 结果表记录 ("result", 返回值) 或 ("error", 异常类型名) 二元组。
    results: "Dict[str, Tuple[str, Any]]" = {}
    gate = threading.BoundedSemaphore(max_pending)

    def run_one(task_id: str, sleep_ms: int, priority: int,
                max_queue_wait_ms: "Optional[int]") -> None:
        gate.acquire()
        try:
            try:
                value = scheduler.submit(
                    task_id, _make_wait(sleep_ms),
                    priority=priority,
                    max_queue_wait_ms=max_queue_wait_ms,
                )
            except QueueTimeoutError:
                results[task_id] = ("error", "QueueTimeoutError")
            else:
                results[task_id] = ("result", value)
        finally:
            gate.release()

    threads: List[threading.Thread] = []
    with Scheduler(workers=workers, max_pending=max_pending) as scheduler:
        for task_id, sleep_ms, priority, max_queue_wait_ms in tasks:
            t = threading.Thread(
                target=run_one,
                args=(task_id, sleep_ms, priority, max_queue_wait_ms),
                name="cli-submit",
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        snapshot = scheduler.snapshot()

    output = {
        "results": [
            _result_object(task_id, results[task_id])
            for task_id, _, _, _ in tasks
        ],
        "stats": snapshot.to_dict(),
    }
    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


def _result_object(task_id: str, outcome: "Tuple[str, Any]") -> Dict[str, Any]:
    """构造单个任务的结果对象：成功只含 task_id/result，到期只含 task_id/error。"""
    kind, payload = outcome
    if kind == "error":
        return {"task_id": task_id, "error": payload}
    return {"task_id": task_id, "result": payload}


def main() -> None:  # pragma: no cover - 进程入口薄封装
    sys.exit(run())


if __name__ == "__main__":  # pragma: no cover
    main()
