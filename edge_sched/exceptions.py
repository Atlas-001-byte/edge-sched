"""Exception types raised by Edge Sched."""


class EdgeSchedError(Exception):
    """Base class for all Edge Sched errors."""


class InputValidationError(EdgeSchedError):
    """Raised when an argument fails validation.

    Covers non-string or empty task ids, non-positive worker counts or
    pending limits, and non-callable task objects.
    """


class BackpressureError(EdgeSchedError):
    """Raised when submitting would exceed the unfinished-task limit.

    The rejected task id produces neither a result nor any statistics.
    """


class DuplicateTaskError(EdgeSchedError):
    """Raised when a task id is already in use by an unfinished task."""


class SchedulerClosedError(EdgeSchedError):
    """Raised when submitting to a scheduler that is closing or closed."""
