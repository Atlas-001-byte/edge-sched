"""Edge Sched: 高并发调度框架。

公开入口:

- :class:`~edge_sched.scheduler.Scheduler` -- 调度器入口。
- :func:`~edge_sched.scheduler.Scheduler.submit` -- 提交任务并等待结果。
- :func:`~edge_sched.scheduler.Scheduler.snapshot` -- 累计统计快照。
- :class:`~edge_sched.errors.BackpressureError` / :class:`~edge_sched.errors.DuplicateTaskError`
  / :class:`~edge_sched.errors.SchedulerClosedError` /
  :class:`~edge_sched.errors.InputValidationError`
"""

from .errors import (
    BackpressureError,
    DuplicateTaskError,
    EdgeSchedError,
    InputValidationError,
    SchedulerClosedError,
)
from .scheduler import Scheduler
from .stats import StatsSnapshot, percentile

__all__ = [
    "Scheduler",
    "StatsSnapshot",
    "percentile",
    "EdgeSchedError",
    "BackpressureError",
    "DuplicateTaskError",
    "SchedulerClosedError",
    "InputValidationError",
]

__version__ = "0.1.0"
