"""The :class:`Scheduler` entry point and its event loop / worker pool."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from queue import Queue
from typing import Any, Callable, Dict, Optional

from .exceptions import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    SchedulerClosedError,
)
from .stats import Snapshot, StatsCollector

_SHUTDOWN = object()


@dataclass
class _TaskRecord:
    task_id: str
    func: Callable[[], Any]
    submitted_at: float
    event: threading.Event
    status: str = "pending"  # pending -> running -> succeeded | failed
    started_at: float = 0.0
    finished_at: float = 0.0
    value: Any = None
    exc: Optional[BaseException] = None

    @property
    def done(self) -> bool:
        return self.status in ("succeeded", "failed")


class TaskFuture:
    """Handle to a submitted task.

    The task result is read exactly once per task through :meth:`result`;
    a failed task re-raises the original exception.
    """

    def __init__(self, record: _TaskRecord) -> None:
        self._record = record

    @property
    def task_id(self) -> str:
        return self._record.task_id

    def done(self) -> bool:
        return self._record.done

    def result(self, timeout: Optional[float] = None) -> Any:
        """Block until the task finishes and return its value.

        ``timeout`` is in seconds. Raises the original exception for a
        failed task and :class:`TimeoutError` if ``timeout`` elapses first.
        Results stay readable after the scheduler has closed.
        """
        record = self._record
        if not record.event.wait(timeout):
            raise TimeoutError(f"task {record.task_id!r} did not finish in time")
        if record.exc is not None:
            raise record.exc
        return record.value


class Scheduler:
    """FCFS task scheduler with worker threads and backpressure.

    Parameters
    ----------
    workers:
        Number of worker threads executing callables (>= 1).
    max_pending:
        Maximum number of unfinished (queued or running) tasks. When the
        limit is reached, :meth:`submit` raises :class:`BackpressureError`.
    """

    def __init__(self, workers: int, max_pending: int) -> None:
        _positive_int(workers, "workers")
        _positive_int(max_pending, "max_pending")
        self._workers = workers
        self._max_pending = max_pending

        self._lock = threading.Lock()
        self._records: Dict[str, _TaskRecord] = {}
        self._unfinished = 0
        self._closing = False
        self._closed = False

        self._stats = StatsCollector()
        self._incoming: Queue = Queue()  # event-loop input, FCFS
        self._work: Queue = Queue()  # dispatched tasks for workers

        self._loop_thread = threading.Thread(
            target=self._run_loop, name="edge-sched-loop", daemon=True
        )
        self._worker_threads = [
            threading.Thread(
                target=self._run_worker,
                name=f"edge-sched-worker-{i + 1}",
                daemon=True,
            )
            for i in range(workers)
        ]
        self._loop_thread.start()
        for worker in self._worker_threads:
            worker.start()

    # ------------------------------------------------------------------ submit

    def submit(self, task_id: str, func: Callable[[], Any]) -> TaskFuture:
        """Submit a zero-argument callable under a unique ``task_id``.

        Validation order: input validation, closed scheduler, duplicate
        unfinished task id, then backpressure capacity.
        """
        if not isinstance(task_id, str) or len(task_id) == 0:
            raise InputValidationError("task_id must be a non-empty string")
        if not callable(func):
            raise InputValidationError("func must be callable")

        with self._lock:
            if self._closing or self._closed:
                raise SchedulerClosedError("scheduler is closed")
            existing = self._records.get(task_id)
            if existing is not None and not existing.done:
                raise DuplicateTaskError(f"task_id {task_id!r} is already pending")
            if self._unfinished >= self._max_pending:
                self._stats.record_rejected()
                raise BackpressureError(
                    f"unfinished task limit reached ({self._max_pending})"
                )

            record = _TaskRecord(
                task_id=task_id,
                func=func,
                submitted_at=time.monotonic(),
                event=threading.Event(),
            )
            self._records[task_id] = record
            self._unfinished += 1
            self._stats.record_accepted()
            self._incoming.put(task_id)
            return TaskFuture(record)

    def wait(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """Wait for a submitted task and return its value or raise its error."""
        with self._lock:
            record = self._records.get(task_id)
        if record is None:
            raise KeyError(f"unknown task_id {task_id!r}")
        return TaskFuture(record).result(timeout)

    # ------------------------------------------------------------------ stats

    def snapshot(self) -> Snapshot:
        """Return an immutable cumulative statistics snapshot."""
        return self._stats.snapshot()

    # ------------------------------------------------------------------ close

    def close(self) -> None:
        """Reject new submissions, then wait for all accepted tasks."""
        with self._lock:
            if self._closing or self._closed:
                closing = False
            else:
                self._closing = True
                closing = True
        if not closing:
            self._loop_thread.join()
            for worker in self._worker_threads:
                worker.join()
            return

        # FIFO sentinel: everything accepted before close() is ahead of it.
        self._incoming.put(_SHUTDOWN)
        self._loop_thread.join()
        for worker in self._worker_threads:
            worker.join()
        with self._lock:
            self._closed = True

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------- internals

    def _run_loop(self) -> None:
        """Dispatch accepted tasks first-come-first-served to workers."""
        while True:
            item = self._incoming.get()
            if item is _SHUTDOWN:
                break
            with self._lock:
                record = self._records[item]
            self._work.put(record)
        # Every real task was dispatched before the sentinel; the matching
        # worker sentinels therefore sit behind all remaining work.
        for _ in range(self._workers):
            self._work.put(_SHUTDOWN)

    def _run_worker(self) -> None:
        while True:
            item = self._work.get()
            if item is _SHUTDOWN:
                return

            record: _TaskRecord = item
            start = time.monotonic()
            record.started_at = start
            record.status = "running"
            try:
                value = record.func()
                ok = True
            except BaseException as exc:  # scheduler keeps working; deliver as-is
                value = None
                error = exc
                ok = False
            end = time.monotonic()

            queue_wait_ms = (start - record.submitted_at) * 1000.0
            total_ms = (end - record.submitted_at) * 1000.0
            with self._lock:
                record.finished_at = end
                if ok:
                    record.value = value
                    record.status = "succeeded"
                    self._stats.record_completion(queue_wait_ms, total_ms)
                else:
                    record.exc = error
                    record.status = "failed"
                    self._stats.record_failure(queue_wait_ms, total_ms)
                self._unfinished -= 1
            record.event.set()


def _positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InputValidationError(f"{name} must be an integer >= 1")
