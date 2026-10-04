# Edge Sched

高并发调度框架：事件循环、背压与延迟分布统计。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

- 基线：Scheduler、submit / submit_nowait / TaskHandle、result、close、
  snapshot、背压与取消；`python -m edge_sched` 读取 JSON 任务数组执行空等待。
- 增量：可选优先级调度。默认仍为 FCFS；`Scheduler(workers, max_pending,
  priority=True)` 启用后，提交可带整数 `priority`（缺省 0），数值较大的
  已接受但未开始执行任务优先派发到工作线程，同值按接受顺序派发。
  执行中、已完成、已取消或已拒绝任务不重排，任务只执行一次。
  CLI 任务可提供 `priority` 字段，加 `--priority` 开关启用；
  结果数组仍按输入顺序返回，统计字段与延迟口径不变。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
