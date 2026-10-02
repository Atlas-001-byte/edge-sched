"""Command-line entry point: ``python -m edge_sched``.

Reads a JSON array of ``{"task_id": ..., "sleep_ms": ...}`` entries, runs
each as a no-arg callable that simply waits, then prints task results and
the statistics snapshot as JSON on stdout.

Input/usage problems are reported as ``InputValidationError: ...`` on stderr
with exit code 2.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, List, Tuple

from .exceptions import InputValidationError
from .scheduler import Scheduler, TaskFuture


def main(argv: List[str] | None = None) -> int:
    try:
        input_path, workers, max_pending = _parse_args(
            sys.argv[1:] if argv is None else argv
        )
        entries = _load_entries(input_path)
        results, snapshot = _run(entries, workers, max_pending)
    except InputValidationError as exc:
        print(f"InputValidationError: {exc}", file=sys.stderr)
        return 2

    output = {
        "results": [
            {"task_id": task_id, "result": value} for task_id, value in results
        ],
        "stats": snapshot.to_dict(),
    }
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def _parse_args(argv: List[str]) -> Tuple[str, int, int]:
    options: Dict[str, str] = {}
    index = 0
    while index < len(argv):
        arg = argv[index]
        if not arg.startswith("--"):
            raise InputValidationError(f"unrecognized argument {arg!r}")
        body = arg[2:]
        if "=" in body:
            key, _, value = body.partition("=")
            index += 1
        else:
            key = body
            if index + 1 >= len(argv):
                raise InputValidationError(f"option --{key} requires a value")
            value = argv[index + 1]
            index += 2
        options[key] = value

    for name in ("input", "workers", "max-pending"):
        if name not in options or options[name] == "":
            raise InputValidationError(f"missing required option --{name}")

    try:
        workers = int(options["workers"])
    except ValueError:
        raise InputValidationError("workers must be an integer >= 1") from None
    try:
        max_pending = int(options["max-pending"])
    except ValueError:
        raise InputValidationError(
            "max-pending must be an integer >= 1"
        ) from None
    if workers < 1:
        raise InputValidationError("workers must be an integer >= 1")
    if max_pending < 1:
        raise InputValidationError("max-pending must be an integer >= 1")

    return options["input"], workers, max_pending


def _load_entries(path: str) -> List[Tuple[str, int]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as exc:
        raise InputValidationError(f"cannot read input file {path!r}: {exc}")
    except json.JSONDecodeError as exc:
        raise InputValidationError(f"invalid JSON: {exc.msg}") from None

    if not isinstance(data, list):
        raise InputValidationError("input must be a JSON array")

    entries: List[Tuple[str, int]] = []
    seen: set[str] = set()
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise InputValidationError(f"entry {index} must be an object")
        if "task_id" not in item:
            raise InputValidationError(f"entry {index} missing field 'task_id'")
        if "sleep_ms" not in item:
            raise InputValidationError(f"entry {index} missing field 'sleep_ms'")

        task_id = item["task_id"]
        if not isinstance(task_id, str) or len(task_id) == 0:
            raise InputValidationError(
                f"entry {index}: task_id must be a non-empty string"
            )
        sleep_ms = item["sleep_ms"]
        # bool is an int subclass; reject it to keep the integer rule strict.
        if isinstance(sleep_ms, bool) or not isinstance(sleep_ms, int):
            raise InputValidationError(
                f"entry {index}: sleep_ms must be a non-negative integer"
            )
        if sleep_ms < 0:
            raise InputValidationError(
                f"entry {index}: sleep_ms must be a non-negative integer"
            )
        if task_id in seen:
            raise InputValidationError(f"duplicate task_id {task_id!r}")
        seen.add(task_id)
        entries.append((task_id, sleep_ms))
    return entries


def _run(
    entries: List[Tuple[str, int]], workers: int, max_pending: int
) -> Tuple[List[Tuple[str, Any]], Any]:
    # Sliding-window submission: keep at most ``max_pending`` tasks in flight
    # so every input task is accepted once and none is rejected.
    futures: List[Tuple[str, TaskFuture]] = []
    pending: List[TaskFuture] = []
    with Scheduler(workers=workers, max_pending=max_pending) as scheduler:
        for task_id, sleep_ms in entries:
            if len(pending) >= max_pending:
                pending.pop(0).result()
            future = scheduler.submit(
                task_id, lambda ms=sleep_ms: time.sleep(ms / 1000.0)
            )
            futures.append((task_id, future))
            if not future.done():
                pending.append(future)

        results = [(task_id, future.result()) for task_id, future in futures]
        snapshot = scheduler.snapshot()
    return results, snapshot


if __name__ == "__main__":
    sys.exit(main())
