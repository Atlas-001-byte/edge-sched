# Edge Sched

高并发调度框架：事件循环、背压与延迟分布统计。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 功能

- `submit` 阻塞等待结果，`submit_nowait` 立即返回 `TaskHandle`。
- 有界 `max_pending` 背压（FCFS 入站队列 + 工作线程池），超限抛 `BackpressureError`。
- 延迟分布统计：`queue_wait_ms` / `total_latency_ms` 的 p50/p95/p99/max。
- 任务取消：`submit_nowait` 返回的句柄可在任务**开始执行前**调用 `cancel()`。
  - 取消成功返回 `True`，句柄 `done()` 变为 `True`，callable 完全不执行；
    句柄 `result()` 与 `Scheduler.result(task_id)` 抛 `TaskCancelledError`。
  - 任务已开始执行或已有终态时返回 `False`，原终态保持不变。
  - 取消与“开始执行”在同一锁上原子裁决，结果唯一：不会部分执行后取消。
  - 取消任务计入 `accepted` 与新增的 `cancelled`，不计入 `completed`/`failed`，
    不贡献延迟样本；立即释放未完成额度与 task_id 占用，同名 task_id 可再提交。
  - 已取消任务不阻塞 `close`，关闭后其取消结果仍可读取。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
