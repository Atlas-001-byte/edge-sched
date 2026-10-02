"""Edge Sched: a high-concurrency task scheduler.

The public entry point is :class:`Scheduler`. Tasks are submitted as
zero-argument callables with a unique string ``task_id``; a pool of worker
threads executes them in first-come-first-served order while an event-loop
thread handles dispatch and completion notification.

The scheduler applies backpressure based on the number of unfinished
(incomplete) tasks and records latency distribution snapshots.
"""

from .exceptions import (
    BackpressureError,
    DuplicateTaskError,
    EdgeSchedError,
    InputValidationError,
    SchedulerClosedError,
)
from .scheduler import Scheduler
from .stats import Snapshot, percentile

__all__ = [
    "Scheduler",
    "Snapshot",
    "EdgeSchedError",
    "BackpressureError",
    "DuplicateTaskError",
    "InputValidationError",
    "SchedulerClosedError",
    "percentile",
]

__version__ = "0.1.0"
