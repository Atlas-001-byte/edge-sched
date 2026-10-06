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
- :func:`~edge_sched.scheduler.Scheduler.runtime_snapshot` -- 同一逻辑时刻的
  不可变即时运行观测 :class:`~edge_sched.scheduler.RuntimeSnapshot`
  （workers/容量/queued/running/unfinished、准入等待者与其占用任务数、
  最早排队与等待耗时、closing/closed），只读、不改任务与统计。
- :func:`~edge_sched.scheduler.Scheduler.stats_checkpoint` -- 创建区间统计
  观测边界 :class:`~edge_sched.stats.StatsCheckpoint`。
- :func:`~edge_sched.scheduler.Scheduler.snapshot_since` -- 返回边界之后事件
  的区间统计快照（形态同累计快照）。
- :func:`~edge_sched.scheduler.Scheduler.rolling_snapshot` -- 返回滑动延迟
  窗口的不可变 :class:`~edge_sched.stats.RollingStatsSnapshot`
  （window_size/sampled_finished 与 queue_wait_ms、total_latency_ms、
  execution_ms 三个最近任务分布）；构造时以 latency_window_tasks 启用，
  未启用时窗口容量与样本数均为 0。
- :func:`~edge_sched.scheduler.Scheduler.resize_workers` -- 运行时调整
  工作线程容量（>= 1 的整数）：扩容先补足线程再补充许可，缩容立即降低
  并发上限但不打断执行中任务；``runtime_snapshot().workers`` 报告当前
  有效目标容量。
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
from .stats import RollingStatsSnapshot, StatsCheckpoint, StatsSnapshot, percentile

__all__ = [
    "Scheduler",
    "TaskHandle",
    "RuntimeSnapshot",
    "StatsSnapshot",
    "RollingStatsSnapshot",
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
