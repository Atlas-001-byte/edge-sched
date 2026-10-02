"""调度器核心：事件循环派发 + 工作线程执行 + 背压。

结构:

- submit 在调用方线程通过校验后，任务进入有界的入站队列（先到先服务）。
- 一个事件循环线程按入队顺序取出任务，在有空闲工作线程时派发到就绪队列。
- 工作线程执行无参数 callable，记录排队/总延迟，唤醒 submit 的等待方。
- 未完成（已接受但未结束）任务数达到 ``max_pending`` 时触发背压拒绝。
- close 先令新 submit 失败，再等待全部已接受任务结束，最后停止线程；
  已完成任务的结果在关闭后仍可读取。
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Any, Callable, Optional, TypeVar

from .errors import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    SchedulerClosedError,
)
from .stats import Stats, StatsSnapshot, to_ms

T = TypeVar("T")

# 就绪队列中的停止哨兵；工作线程取到即退出。
_SENTINEL = object()

# 事件循环在空闲时轮询停止信号的间隔（秒）。
_DISPATCH_POLL = 0.05


class _TaskEntry:
    """一个任务的全部可变状态。"""

    __slots__ = (
        "task_id",
        "fn",
        "submit_time",
        "done",
        "success",
        "value",
        "exception",
    )

    def __init__(self, task_id: str, fn: Callable[[], Any],
                 submit_time: float) -> None:
        self.task_id = task_id
        self.fn = fn
        self.submit_time = submit_time
        self.done = threading.Event()
        self.success = False
        self.value: Any = None
        self.exception: Optional[BaseException] = None


class Scheduler:
    """高并发任务调度器。

    :param workers: 工作线程数，必须为 >= 1 的整数。
    :param max_pending: 未完成任务（含排队中与执行中）上限，必须为 >= 1 的整数。
    """

    def __init__(self, workers: int, max_pending: int) -> None:
        if not self._is_positive_int(workers):
            raise InputValidationError(
                "workers must be an integer >= 1, got %r" % (workers,)
            )
        if not self._is_positive_int(max_pending):
            raise InputValidationError(
                "max_pending must be an integer >= 1, got %r" % (max_pending,)
            )
        self._workers = workers
        self._max_pending = max_pending

        # _cond 同时承担状态锁：_closing/_closed/_pending/_unfinished 的读写。
        self._cond = threading.Condition()
        self._closing = False
        self._closed = False
        self._pending = 0
        self._unfinished: set[str] = set()
        # 全部已接受任务的条目长期保留，关闭后结果仍可读取。
        self._tasks: dict[str, _TaskEntry] = {}

        self._stats = Stats()
        self._inbound: "queue.Queue[Any]" = queue.Queue()
        self._ready: "queue.Queue[Any]" = queue.Queue()
        # 空闲工作线程许可：事件循环派发前必须先取得一个许可。
        self._worker_slots = threading.BoundedSemaphore(workers)
        self._stop_dispatcher = threading.Event()

        self._dispatcher = threading.Thread(
            target=self._run_dispatcher, name="edge-sched-loop", daemon=True
        )
        self._worker_threads = [
            threading.Thread(
                target=self._run_worker,
                name="edge-sched-worker-%d" % i,
                daemon=True,
            )
            for i in range(workers)
        ]
        self._dispatcher.start()
        for t in self._worker_threads:
            t.start()

    @staticmethod
    def _is_positive_int(value: Any) -> bool:
        # bool 是 int 的子类，但并发参数不接受布尔值。
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1

    # ------------------------------------------------------------------ public

    def submit(self, task_id: str, fn: Callable[[], T],
               timeout: Optional[float] = None) -> T:
        """提交任务并阻塞等待结果。

        结果语义（每个任务只可能有一种）:

        - 成功：返回 callable 的返回值。
        - 失败：抛出 callable 抛出的**原始异常**；任务计入 failed。
        - 未完成任务数已达上限：抛 :class:`BackpressureError`，无结果无统计。
        - task_id 与未完成任务重复：抛 :class:`DuplicateTaskError`。
        - 调度器已关闭：抛 :class:`SchedulerClosedError`。
        - 参数非法：抛 :class:`InputValidationError`。
        - 等待超过 ``timeout`` 秒：抛 :class:`TimeoutError`，任务仍继续执行，
          其最终结果不受影响。

        :param task_id: 非空字符串，任务唯一标识。
        :param fn: 无参数 callable。
        :param timeout: 可选等待超时（秒），None 表示一直等待。
        """
        if not isinstance(task_id, str) or task_id == "":
            raise InputValidationError(
                "task_id must be a non-empty str, got %r" % (task_id,)
            )
        if not callable(fn):
            raise InputValidationError("fn must be callable, got %r" % (fn,))
        if timeout is not None and timeout < 0:
            raise InputValidationError(
                "timeout must be >= 0 or None, got %r" % (timeout,)
            )

        with self._cond:
            if self._closing:
                raise SchedulerClosedError("scheduler is closed")
            if task_id in self._unfinished:
                raise DuplicateTaskError(
                    "task_id %r is already pending" % (task_id,)
                )
            if self._pending >= self._max_pending:
                self._stats.record_rejected()
                raise BackpressureError(
                    "pending task limit %d reached" % self._max_pending
                )

            entry = _TaskEntry(task_id, fn, time.monotonic())
            self._tasks[task_id] = entry
            self._unfinished.add(task_id)
            self._pending += 1
            self._stats.record_accepted()

        # 入队在锁外：Queue 本身线程安全，入队顺序即接受顺序（FCFS）。
        self._inbound.put(task_id)

        if not entry.done.wait(timeout):
            raise TimeoutError(
                "task %r did not finish within %s seconds" % (task_id, timeout)
            )
        if entry.success:
            return entry.value  # type: ignore[no-any-return]
        assert entry.exception is not None
        raise entry.exception

    def result(self, task_id: str) -> Any:
        """读取一个已结束任务的结果（非阻塞）。

        - 成功：返回 callable 的返回值；失败：重新抛出其原始异常。
        - 任务尚未结束：抛 :class:`RuntimeError`。
        - 从未接受过该 task_id：抛 :class:`KeyError`。

        调度器关闭后已完成任务的结果仍可通过本方法读取。
        """
        with self._cond:
            entry = self._tasks.get(task_id)
            if entry is None:
                raise KeyError(task_id)
            if not entry.done.is_set():
                raise RuntimeError("task %r is not finished" % (task_id,))
        if entry.success:
            return entry.value
        assert entry.exception is not None
        raise entry.exception

    def snapshot(self) -> StatsSnapshot:
        """返回累计统计快照；快照为值拷贝，不随后续任务变化。"""
        return self._stats.snapshot()

    def close(self) -> None:
        """关闭调度器。

        先令此后的 submit 立即抛 :class:`SchedulerClosedError`，
        再等待全部已接受任务执行结束，最后停止事件循环与工作线程。
        可重复调用，且并发调用安全。
        """
        with self._cond:
            if self._closed:
                return
            if self._closing:
                # 另一个线程正在执行关闭，等待其完成即可。
                teardown_by_other = True
            else:
                teardown_by_other = False
                self._closing = True
            if teardown_by_other:
                while not self._closed:
                    self._cond.wait()
                return

        # 等待全部已接受任务结束（关闭期间事件循环与工作线程照常运转）。
        with self._cond:
            while self._pending > 0:
                self._cond.wait()

        # 此时入站队列必为空，停止事件循环。
        self._stop_dispatcher.set()
        self._dispatcher.join()

        # 每个工作线程一个停止哨兵。
        for _ in range(self._workers):
            self._ready.put(_SENTINEL)
        for t in self._worker_threads:
            t.join()

        with self._cond:
            self._closed = True
            self._cond.notify_all()

    # ------------------------------------------------------------- internals

    def _run_dispatcher(self) -> None:
        """事件循环：按 FCFS 顺序把任务派发给空闲工作线程。"""
        while not self._stop_dispatcher.is_set():
            try:
                item = self._inbound.get(timeout=_DISPATCH_POLL)
            except queue.Empty:
                continue
            # 取得一个空闲工作线程许可后再派发，保证就绪队列中至多有
            # workers 个待执行任务，且任务不会在就绪队列里无限堆积。
            self._worker_slots.acquire()
            self._ready.put(item)

    def _run_worker(self) -> None:
        """工作线程：取任务 -> 执行 callable -> 记录统计 -> 唤醒等待方。"""
        while True:
            item = self._ready.get()
            if item is _SENTINEL:
                return
            task_id = item
            entry = self._tasks[task_id]

            start_time = time.monotonic()
            try:
                value = entry.fn()
            except Exception as exc:  # 调度器吞掉异常以隔离任务，继续工作
                end_time = time.monotonic()
                entry.success = False
                entry.exception = exc
                success = False
            else:
                end_time = time.monotonic()
                entry.success = True
                entry.value = value
                success = True

            queue_wait_ms = to_ms(start_time - entry.submit_time)
            total_latency_ms = to_ms(end_time - entry.submit_time)

            # 先记账再唤醒：被唤醒的 submit 调用方返回后即可读到一致统计。
            self._stats.record_finished(
                queue_wait_ms, total_latency_ms, success
            )
            with self._cond:
                self._pending -= 1
                self._unfinished.discard(task_id)
                self._cond.notify_all()
            entry.done.set()

            # 执行结束才释放许可，许可数即并发执行数。
            self._worker_slots.release()

    # ------------------------------------------------------------- context mgr

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
