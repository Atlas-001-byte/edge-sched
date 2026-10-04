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


class TaskCancelledError(EdgeSchedError):
    """任务在开始执行前被 :meth:`TaskHandle.cancel` 取消。

    callable 完全不会执行；任务计入 accepted 与 cancelled，
    但不计入 completed/failed，也不贡献延迟样本。
    """


class QueueTimeoutError(EdgeSchedError):
    """已接受任务在排队等待期间超过 ``max_queue_wait_ms``，未被工作线程认领。

    callable 完全不会执行；任务计入 accepted 与 expired，
    但不计入 completed/failed/cancelled，也不贡献延迟样本。
    """


class SchedulerClosedError(EdgeSchedError):
    """调度器已关闭，不再接受新任务。"""


class InputValidationError(EdgeSchedError):
    """构造参数或 submit 参数未通过校验。"""
