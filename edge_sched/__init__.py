"""Edge Sched: 高并发调度框架。

公开入口:

- :class:`~edge_sched.scheduler.Scheduler` -- 调度器入口。
- :func:`~edge_sched.scheduler.Scheduler.submit` -- 提交任务并等待结果。
- :func:`~edge_sched.scheduler.Scheduler.submit_nowait` -- 非阻塞提交，返回
  :class:`~edge_sched.scheduler.TaskHandle`。
- :func:`~edge_sched.scheduler.Scheduler.submit_with_wait` -- 容量满时按
  发起先后排队等待名额（可带 admission_timeout_ms），再等待结果。
- :func:`~edge_sched.scheduler.Scheduler.submit_batch_with_wait` --
  成组原子准入：整组任务要么一次全部获得 max_pending 名额，要么继续在同一条
  FIFO 准入队列中等待（后续单任务/小组不得绕过），按输入顺序返回
  :class:`~edge_sched.scheduler.TaskHandle` 元组。
- :func:`~edge_sched.scheduler.Scheduler.snapshot` -- 累计统计快照。
- :func:`~edge_sched.scheduler.Scheduler.runtime_snapshot` -- 即时运行观测
  快照 :class:`~edge_sched.scheduler.RuntimeSnapshot`（只读、不可变、
  不改变任务与统计）。
- :func:`~edge_sched.scheduler.Scheduler.stats_checkpoint` -- 创建区间统计
  观测边界 :class:`~edge_sched.stats.StatsCheckpoint`。
- :func:`~edge_sched.scheduler.Scheduler.snapshot_since` -- 返回边界之后事件
  的区间统计快照（形态同累计快照）。
- :class:`~edge_sched.errors.BackpressureError` / :class:`~edge_sched.errors.DuplicateTaskError`
  / :class:`~edge_sched.errors.SchedulerClosedError` /
  :class:`~edge_sched.errors.InputValidationError` /
  :class:`~edge_sched.errors.TaskCancelledError` /
  :class:`~edge_sched.errors.QueueTimeoutError`
"""

from .errors import (
    BackpressureError,
    DuplicateTaskError,
    EdgeSchedError,
    InputValidationError,
    QueueTimeoutError,
    SchedulerClosedError,
    TaskCancelledError,
)
from .scheduler import RuntimeSnapshot, Scheduler, TaskHandle
from .stats import StatsCheckpoint, StatsSnapshot, percentile

__all__ = [
    "Scheduler",
    "TaskHandle",
    "RuntimeSnapshot",
    "StatsSnapshot",
    "StatsCheckpoint",
    "percentile",
    "EdgeSchedError",
    "BackpressureError",
    "DuplicateTaskError",
    "SchedulerClosedError",
    "InputValidationError",
    "TaskCancelledError",
    "QueueTimeoutError",
]

__version__ = "0.1.0"
