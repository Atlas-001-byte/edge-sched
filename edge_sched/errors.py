"""edge_sched 的异常类型。

所有公开异常均继承自 :class:`EdgeSchedError`，便于调用方统一捕获。
并发边界上每个任务只可能产生一种确定的结果。
"""


class EdgeSchedError(Exception):
    """edge_sched 所有异常的基类。"""


class BackpressureError(EdgeSchedError):
    """未完成任务数达到 ``max_pending`` 上限，submit 被拒绝。

    被拒绝的任务不会产生结果，也不计入任何统计。
    """


class DuplicateTaskError(EdgeSchedError):
    """提交的 task_id 与某个尚未完成的任务重复。

    只有未完成任务占用 task_id；已完成任务（结果仍可读取）不再阻止复用。
    """


class SchedulerClosedError(EdgeSchedError):
    """调度器已关闭，不再接受新任务。"""


class TaskCancelledError(EdgeSchedError):
    """任务在开始执行前被取消，进入取消终态。

    被取消的任务其 callable 完全不会执行；通过句柄或调度器读取结果时
    抛出本异常。取消仅对尚未开始执行的任务生效。
    """


class InputValidationError(EdgeSchedError):
    """构造参数或 submit 参数未通过校验。"""
