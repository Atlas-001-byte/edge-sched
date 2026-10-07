"""admission_wait_ms 容量准入等待分布的验收测试。

口径：

- 每个被原子接纳的任务（submit / submit_nowait / submit_with_wait /
  submit_batch_with_wait）恰好贡献一个样本：调用瞬间取得名额为 0.0；
  只有进入容量等待队列后才从入队时刻算到被原子接纳时刻（毫秒，三位小数）。
- submit_batch_with_wait 整组接纳时组内每个任务产生相同样本。
- BackpressureError / InputValidationError / DuplicateTaskError /
  close 后未接纳的 SchedulerClosedError 不产生样本。
- 任务接纳后无论成功、失败、取消或排队到期，样本都保留。
- 样本按接纳时刻归属 stats_checkpoint 区间；跨边界任务的准入样本在接纳
  区间，结束样本（queue_wait_ms / total_latency_ms / execution_ms）在
  结束区间。
"""

import json
import threading
import time
import unittest

from edge_sched import (
    BackpressureError,
    DuplicateTaskError,
    InputValidationError,
    Scheduler,
    SchedulerClosedError,
)

_EMPTY_DIST = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
_DIST_KEYS = {"p50", "p95", "p99", "max"}


def _wait_started(s: Scheduler, task_id: str) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        entry = s._tasks.get(task_id)  # type: ignore[attr-defined]
        if entry is not None and entry.started:
            return True
        time.sleep(0.002)
    return False


def _wait_accepted(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if s.snapshot().accepted == n:
            return True
        time.sleep(0.005)
    return False


def _wait_admission_queue(s: Scheduler, n: int) -> bool:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with s._cond:  # type: ignore[attr-defined]
            if len(s._admission_queue) == n:  # type: ignore[attr-defined]
                return True
        time.sleep(0.002)
    return False


def _admission_samples(snap: "object") -> list:
    return getattr(snap, "_admission_wait_samples")


class ImmediateAdmissionZeroTest(unittest.TestCase):
    def test_all_four_entry_points_record_zero_when_capacity_free(self) -> None:
        with Scheduler(workers=2, max_pending=8) as s:
            self.assertEqual(s.submit("a", lambda: "a"), "a")
            self.assertEqual(
                s.submit_with_wait("b", lambda: "b",
                                   admission_timeout_ms=0),
                "b",
            )
            h = s.submit_nowait("c", lambda: "c")
            self.assertEqual(h.result(2.0), "c")
            handles = s.submit_batch_with_wait(
                [
                    {"task_id": "d", "fn": lambda: "d"},
                    {"task_id": "e", "fn": lambda: "e"},
                ],
                admission_timeout_ms=0,
            )
            self.assertEqual([x.result(2.0) for x in handles], ["d", "e"])

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 5)
        self.assertEqual(_admission_samples(snap), [0.0] * 5)
        self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)
        # to_dict 在既有字段之外增加同名字段且可 JSON 序列化。
        data = snap.to_dict()
        self.assertEqual(set(data["admission_wait_ms"]), _DIST_KEYS)
        json.dumps(data)

    def test_snapshot_is_value_copy_and_repeatable(self) -> None:
        with Scheduler(workers=1, max_pending=4) as s:
            s.submit("a", lambda: None)
            first = s.snapshot()
            s.submit("b", lambda: None)
            second = s.snapshot()
            self.assertEqual(_admission_samples(first), [0.0])
            self.assertEqual(_admission_samples(second), [0.0, 0.0])
            # 早先快照不随后续接纳变化，重复读取稳定。
            dist_a = first.admission_wait_ms
            dist_b = first.admission_wait_ms
            self.assertEqual(dist_a, dist_b)
            self.assertIsNot(dist_a, dist_b)
            json.dumps(first.to_dict())


class QueuedAdmissionWaitTest(unittest.TestCase):
    def test_waiting_submit_with_wait_measures_enqueue_to_admission(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            result: dict[str, object] = {}

            def waiter() -> None:
                result["v"] = s.submit_with_wait(
                    "w", lambda: "w", admission_timeout_ms=5_000
                )

            t = threading.Thread(target=waiter)
            start = time.monotonic()
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.20)
            release.set()
            t.join()
            wall_ms = (time.monotonic() - start) * 1000.0

        self.assertEqual(result, {"v": "w"})
        snap = s.snapshot()
        self.assertEqual(snap.accepted, 2)
        samples = _admission_samples(snap)
        self.assertEqual(len(samples), 2)
        self.assertEqual(samples[0], 0.0)  # 持有者调用瞬间获名额。
        # 等待者的样本从入队时刻算到接纳时刻：约 200ms，且明显不把等待
        # 结果返回的调度空隙计入上限之外。
        self.assertGreaterEqual(samples[1], 150.0)
        self.assertLessEqual(samples[1], wall_ms + 5.0)
        dist = snap.admission_wait_ms
        self.assertEqual(set(dist), _DIST_KEYS)
        self.assertEqual(dist["max"], samples[1])
        # 两个样本 [0.0, w]：p50 取 rank 1（0.0），p95/p99 取 rank 2（w）。
        self.assertEqual(dist["p50"], 0.0)
        self.assertEqual(dist["p95"], samples[1])
        self.assertEqual(dist["p99"], samples[1])
        # 三位小数。
        self.assertEqual(round(samples[1], 3), samples[1])

    def test_all_positive_distribution_percentiles(self) -> None:
        # 全部等待接纳（无 0.0 样本）：分布分位随样本数走 ceil(n*q)。
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            def waiter(tid: str) -> None:
                s.submit_with_wait(tid, lambda: None,
                                   admission_timeout_ms=5_000)

            threads = [
                threading.Thread(target=waiter, args=(tid,))
                for tid in ("w0", "w1", "w2", "w3")
            ]
            for i, t in enumerate(threads, start=1):
                t.start()
                self.assertTrue(_wait_admission_queue(s, i))
                time.sleep(0.05)
            release.set()
            for t in threads:
                t.join()

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 5)
        waited = _admission_samples(snap)[1:]
        # 每个等待者都至少经过一次 50ms 的入队间隔。
        self.assertEqual(len(waited), 4)
        self.assertTrue(all(v >= 40.0 for v in waited), waited)
        ordered = sorted(waited)
        dist = snap.admission_wait_ms
        self.assertEqual(dist["max"], ordered[-1])
        self.assertEqual(dist["p50"], ordered[1])  # ceil(4*0.5)=2
        self.assertEqual(dist["p95"], ordered[3])  # ceil(4*0.95)=4
        self.assertEqual(dist["p99"], ordered[3])

    def test_waiting_batch_produces_one_equal_sample_per_task(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        # max_pending=2：h 占 1 个名额后只余 1，整组（2 个）必须等待。
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            handles_holder: dict[str, object] = {}

            def submit_batch() -> None:
                handles_holder["hs"] = s.submit_batch_with_wait(
                    [
                        {"task_id": "b0", "fn": lambda: 0},
                        {"task_id": "b1", "fn": lambda: 1},
                    ],
                    admission_timeout_ms=5_000,
                )

            t = threading.Thread(target=submit_batch)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.18)
            release.set()
            t.join()
            for h in handles_holder["hs"]:  # type: ignore[index]
                h.result(2.0)

        snap = s.snapshot()
        self.assertEqual(snap.accepted, 3)
        samples = _admission_samples(snap)
        self.assertEqual(len(samples), 3)
        self.assertEqual(samples[0], 0.0)
        # 整组接纳：组内两个任务样本完全相同且约为 180ms。
        self.assertEqual(samples[1], samples[2])
        self.assertGreaterEqual(samples[1], 130.0)

    def test_multiple_waiters_each_keep_their_own_sample(self) -> None:
        entered = threading.Event()
        release_holder = threading.Event()
        release_w0 = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release_holder.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            # w0 被接纳后仍占住唯一名额（阻塞在 release_w0），w1 因此继续
            # 在准入队列等待；二者的样本都从各自入队时刻起算。
            def w0_call() -> None:
                release_w0.wait(2.0)

            t0 = threading.Thread(
                target=lambda: s.submit_with_wait(
                    "w0", w0_call, admission_timeout_ms=5_000
                )
            )
            t1 = threading.Thread(
                target=lambda: s.submit_with_wait(
                    "w1", lambda: "w1", admission_timeout_ms=5_000
                )
            )
            t0.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.12)
            t1.start()
            self.assertTrue(_wait_admission_queue(s, 2))
            release_holder.set()  # h 结束：w0 获名额，w1 仍排队。
            self.assertTrue(_wait_started(s, "w0"))
            time.sleep(0.10)  # w1 自入队起再等约 100ms。
            release_w0.set()  # w0 结束：w1 才获名额。
            t0.join()
            t1.join()

        samples = _admission_samples(s.snapshot())
        self.assertEqual(len(samples), 3)
        self.assertEqual(samples[0], 0.0)
        self.assertGreaterEqual(samples[1], 90.0)   # w0 等待约 120ms+
        self.assertGreaterEqual(samples[2], 60.0)   # w1 自入队起等待
        # 两者入队有先后，w0 等得更久。
        self.assertGreaterEqual(samples[1], samples[2])


class RejectionsProduceNoSampleTest(unittest.TestCase):
    def test_immediate_backpressure_produces_no_sample(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit("x", lambda: None)
            with self.assertRaises(BackpressureError):
                s.submit_nowait("y", lambda: None)
            release.set()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (1, 2))
        self.assertEqual(_admission_samples(snap), [0.0])

    def test_admission_timeout_produces_no_sample(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=1) as s:
            threading.Thread(
                target=s.submit_with_wait,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            with self.assertRaises(BackpressureError):
                s.submit_with_wait("x", lambda: None,
                                   admission_timeout_ms=30)
            release.set()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (1, 1))
        self.assertEqual(_admission_samples(snap), [0.0])

    def test_batch_timeout_produces_no_sample(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        with Scheduler(workers=1, max_pending=2) as s:
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))
            s.submit_nowait("q", lambda: None)
            self.assertTrue(_wait_accepted(s, 2))
            with self.assertRaises(BackpressureError):
                s.submit_batch_with_wait(
                    [
                        {"task_id": "b0", "fn": lambda: None},
                        {"task_id": "b1", "fn": lambda: None},
                    ],
                    admission_timeout_ms=30,
                )
            release.set()
        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (2, 1))
        self.assertEqual(_admission_samples(snap), [0.0, 0.0])

    def test_duplicate_validation_and_close_produce_no_sample(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit_with_wait,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        with self.assertRaises(DuplicateTaskError):
            s.submit_with_wait("h", lambda: None,
                               admission_timeout_ms=None)
        with self.assertRaises(InputValidationError):
            s.submit_with_wait("", lambda: None)
        with self.assertRaises(InputValidationError):
            s.submit_with_wait("z", lambda: None, priority=True)

        # close 时仍在准入队列的等待者不产生样本。
        def closed_waiter() -> None:
            with self.assertRaises(SchedulerClosedError):
                s.submit_with_wait("w", lambda: None,
                                   admission_timeout_ms=None)

        t = threading.Thread(target=closed_waiter)
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))
        s.close()
        t.join()
        release.set()

        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.rejected), (1, 0))
        self.assertEqual(_admission_samples(snap), [0.0])
        self.assertEqual(snap.admission_wait_ms, _EMPTY_DIST)


class SampleRetainedAfterTerminalStateTest(unittest.TestCase):
    def _fill_to_pending(self, s: Scheduler) -> "object":
        """占满 1 running + 1 queued（workers=1, max_pending=2），返回
        排队任务句柄（取消它即释放一个 pending 名额）。"""
        entered = threading.Event()
        release = threading.Event()
        s.submit_nowait("h", lambda: (entered.set(), release.wait(2.0)))
        self.assertTrue(_wait_started(s, "h"))
        queued = s.submit_nowait("q", lambda: "q")
        self.assertTrue(_wait_accepted(s, 2))
        return queued, release  # type: ignore[return-value]

    def test_sample_retained_after_cancel(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            queued, release = self._fill_to_pending(s)
            handles: dict[str, object] = {}

            def submit_batch() -> None:
                handles["h"] = s.submit_batch_with_wait(
                    [{"task_id": "w", "fn": lambda: "w"}],
                    admission_timeout_ms=5_000,
                )

            t = threading.Thread(target=submit_batch)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.15)
            self.assertTrue(queued.cancel())  # 释放名额：w 被接纳但仍排队。
            self.assertTrue(_wait_accepted(s, 3))
            t.join()
            w_handle = handles["h"][0]  # type: ignore[index]
            self.assertTrue(w_handle.cancel())  # 认领前取消。
            release.set()

        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.cancelled), (3, 2))
        samples = _admission_samples(snap)
        self.assertEqual(len(samples), 3)
        # 两个即时接纳为 0.0，等待后接纳的 w 样本保留（取消不删除）。
        self.assertEqual(samples[0], 0.0)
        self.assertEqual(samples[1], 0.0)
        self.assertGreaterEqual(samples[2], 100.0)

    def test_sample_retained_after_expiry(self) -> None:
        with Scheduler(workers=1, max_pending=2) as s:
            queued, release = self._fill_to_pending(s)
            done = threading.Event()

            def submit_expiring() -> None:
                try:
                    s.submit_with_wait(
                        "w", lambda: "w",
                        max_queue_wait_ms=30,
                        admission_timeout_ms=5_000,
                    )
                except Exception:  # 到期经 result 路径；此处不应抛在提交上
                    done.set()
                else:
                    done.set()

            t = threading.Thread(target=submit_expiring)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.15)
            self.assertTrue(queued.cancel())  # w 获名额：排队 30ms 后到期。
            t.join()
            self.assertTrue(done.wait(2.0))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if s.snapshot().expired == 1:
                    break
                time.sleep(0.005)
            release.set()

        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.expired), (3, 1))
        samples = _admission_samples(snap)
        self.assertEqual(len(samples), 3)
        self.assertGreaterEqual(samples[2], 100.0)
        # 到期不贡献执行样本，但准入样本保留。
        self.assertEqual(len(snap._execution_samples), 1)  # type: ignore[attr-defined]

    def test_sample_retained_after_failure(self) -> None:
        def boom() -> None:
            raise ValueError("x")

        with Scheduler(workers=1, max_pending=1) as s:
            entered = threading.Event()
            release = threading.Event()
            threading.Thread(
                target=s.submit,
                args=("h", lambda: (entered.set(), release.wait(2.0))),
            ).start()
            self.assertTrue(entered.wait(2.0))

            def failing_waiter() -> None:
                with self.assertRaises(ValueError):
                    s.submit_with_wait("w", boom,
                                       admission_timeout_ms=5_000)

            t = threading.Thread(target=failing_waiter)
            t.start()
            self.assertTrue(_wait_admission_queue(s, 1))
            time.sleep(0.1)
            release.set()
            t.join()

        snap = s.snapshot()
        self.assertEqual((snap.accepted, snap.failed), (2, 1))
        samples = _admission_samples(snap)
        self.assertGreaterEqual(samples[1], 60.0)


class CheckpointAttributionTest(unittest.TestCase):
    def test_admission_sample_in_admission_interval_finish_in_finish_interval(
        self,
    ) -> None:
        entered = threading.Event()
        hold = threading.Event()
        running = threading.Event()
        finish = threading.Event()

        def w_fn() -> None:
            running.set()
            finish.wait(2.0)

        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), hold.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))

        t = threading.Thread(
            target=lambda: s.submit_with_wait(
                "w", w_fn, admission_timeout_ms=5_000
            )
        )
        t.start()
        self.assertTrue(_wait_admission_queue(s, 1))

        # 边界 1：w 尚未接纳。
        cp_queued = s.stats_checkpoint()
        hold.set()  # h 结束：w 被原子接纳（准入样本落定），随即被认领执行。
        self.assertTrue(running.wait(2.0))

        # 边界 2：w 已接纳、执行中（尚未结束）。
        cp_running = s.stats_checkpoint()
        finish.set()
        t.join()
        s.close()

        admit_interval = s.snapshot_since(cp_queued)
        # 区间内 w 接纳且随后（仍在本区间结束时刻之前）完成：准入样本
        # 属于该区间。
        self.assertEqual(admit_interval.accepted, 1)
        admission = _admission_samples(admit_interval)
        self.assertEqual(len(admission), 1)
        self.assertGreaterEqual(admission[0], 0.0)

        finish_interval = s.snapshot_since(cp_running)
        self.assertEqual(finish_interval.accepted, 0)
        self.assertEqual(_admission_samples(finish_interval), [])
        self.assertEqual(finish_interval.admission_wait_ms, _EMPTY_DIST)
        self.assertEqual(finish_interval.completed, 1)
        self.assertEqual(
            len(finish_interval._execution_samples), 1  # type: ignore[attr-defined]
        )
        self.assertEqual(
            len(finish_interval._queue_wait_samples), 1  # type: ignore[attr-defined]
        )

        # 累计视图两者都在。
        total = s.snapshot()
        self.assertEqual(total.accepted, 2)
        self.assertEqual(len(_admission_samples(total)), 2)

    def test_repeated_interval_reads_are_stable(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        s = Scheduler(workers=1, max_pending=1)
        threading.Thread(
            target=s.submit,
            args=("h", lambda: (entered.set(), release.wait(2.0))),
        ).start()
        self.assertTrue(entered.wait(2.0))
        cp = s.stats_checkpoint()

        with self.assertRaises(BackpressureError):
            s.submit_with_wait("x", lambda: None, admission_timeout_ms=20)
        release.set()
        s.close()

        first = s.snapshot_since(cp)
        second = s.snapshot_since(cp)
        # 区间内只有一次被拒：无接纳、无准入样本，反复读取一致。
        self.assertEqual(first.rejected, 1)
        self.assertEqual(first.accepted, 0)
        self.assertEqual(_admission_samples(first), [])
        self.assertEqual(first.to_dict(), second.to_dict())
        json.dumps(first.to_dict())


if __name__ == "__main__":
    unittest.main()
