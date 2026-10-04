"""命令行入口。

用法::

    python -m edge_sched --input tasks.json --workers N --max-pending M [--priority]

输入文件为 JSON 数组，每个元素形如
``{"task_id": "a", "sleep_ms": 10, "priority": 2}``，其中 ``sleep_ms`` 为
非负整数，``priority`` 为可选整数（缺省 0，不能是布尔值）。每个任务执行
一次对应的空等待（``time.sleep``），完成后向标准输出打印任务结果与调度器
统计快照（JSON）。默认按接受顺序 FCFS 派发；加上 ``--priority`` 后启用
优先级调度，priority 数值较大的任务优先派发，同值按输入先后派发。
结果数组始终按输入顺序返回。

以下情况在标准错误打印 ``InputValidationError`` 消息并以退出码 2 结束:
JSON 非法、字段缺失或类型错误、sleep_ms 不是非负整数、priority 不是整数
或为布尔值、task_id 重复或非法、并发参数非法，以及输入文件无法读取。
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from typing import Any, Dict, List, Tuple

from .errors import InputValidationError
from .scheduler import Scheduler, TaskHandle

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
    # bool 是 int 子类，但 priority 不接受布尔值。
    return isinstance(value, int) and not isinstance(value, bool)


def _load_tasks(path: str) -> List[Tuple[str, int, int]]:
    """读取并校验任务文件，返回 (task_id, sleep_ms, priority) 列表（保持文件顺序）。"""
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

    tasks: List[Tuple[str, int, int]] = []
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
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task at index %d has invalid task_id: %r" % (index, task_id)
            )
        if not _is_nonneg_int(sleep_ms):
            raise InputValidationError(
                "task %r has invalid sleep_ms: %r (expected non-negative integer)"
                % (task_id, sleep_ms)
            )
        # priority 缺省 0；显式给出时必须是整数且不能是布尔值。
        priority = item.get("priority", 0)
        if not _is_int(priority):
            raise InputValidationError(
                "task %r has invalid priority: %r (expected integer)"
                % (task_id, priority)
            )
        if task_id in seen:
            raise InputValidationError("duplicate task_id: %r" % task_id)
        seen.add(task_id)
        tasks.append((task_id, sleep_ms, priority))
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
    parser.add_argument(
        "--priority",
        action="store_true",
        help="启用优先级调度：priority 数值大的任务优先派发（默认 FCFS）",
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

    # 主线程严格按文件顺序完成准入：这样默认 FCFS 与优先级模式下同值任务的
    # 派发先后都是确定的输入顺序（submit_nowait 返回前任务已计入 accepted）。
    # 以 max_pending 为在途闸门，保证框架不会因突发提交产生背压拒绝；
    # 每个任务由独立观察线程等待结果，执行本身仍在 workers 个线程上并发。
    results: Dict[str, Any] = {}
    gate = threading.BoundedSemaphore(max_pending)
    watchers: List[threading.Thread] = []

    def watch(handle: "TaskHandle", task_id: str) -> None:
        try:
            results[task_id] = handle.result()
        finally:
            gate.release()

    with Scheduler(
        workers=workers, max_pending=max_pending, priority=args.priority
    ) as scheduler:
        for task_id, sleep_ms, priority in tasks:
            gate.acquire()
            handle = scheduler.submit_nowait(
                task_id, _make_wait(sleep_ms), priority=priority
            )
            t = threading.Thread(
                target=watch, args=(handle, task_id), name="cli-wait"
            )
            t.start()
            watchers.append(t)
        for t in watchers:
            t.join()

        snapshot = scheduler.snapshot()

    output = {
        "results": [
            {"task_id": task_id, "result": results[task_id]}
            for task_id, _, _ in tasks
        ],
        "stats": snapshot.to_dict(),
    }
    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


def main() -> None:  # pragma: no cover - 进程入口薄封装
    sys.exit(run())


if __name__ == "__main__":  # pragma: no cover
    main()
