# Edge Sched

高并发调度框架：事件循环、背压与延迟分布统计。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

- 初始基线：事件循环派发、工作线程执行、背压、取消、统计延迟分布与 CLI。
- 增量：可选优先级调度。`submit` / `submit_nowait` 支持可选整数 `priority`
  （缺省 0，`bool` 非法）；数值大的任务先派发给工作线程，相同数值按接受
  先后派发。优先级只影响已接受但尚未开始执行的任务，不抢占执行中任务；
  CLI 任务可提供 `priority` 字段，结果数组仍按输入顺序返回。
- 增量：可选排队时限。`submit` / `submit_nowait` 支持可选
  `max_queue_wait_ms`（置于 `priority` 后，缺省 `None` 表示不限；只能为
  >= 1 的整数，`bool` 非法）。按单调时钟从任务被接受计到工作线程原子认领：
  到期未认领的任务进入 expired 终态（callable 不执行，立即释放
  `max_pending` 与 task_id），读取其结果抛 `QueueTimeoutError`；认领先
  发生则执行到底。到期任务计入 `accepted` 与新增的 `expired` 统计，不
  贡献延迟样本；CLI 任务可提供 `max_queue_wait_ms` 字段，到期任务的
  结果对象只含 `task_id` 与 `error`（固定为 `"QueueTimeoutError"`）。
- 增量：有界阻塞准入。新增 `Scheduler.submit_with_wait`，沿用 `submit`
  的参数位置与结果语义（成功返回原值、失败抛原异常、callable 只执行
  一次），只在末尾增加 `admission_timeout_ms`：`None`（缺省）无限等待，
  `0` 只在调用瞬间有空位时接纳，其他值必须是非负整数毫秒（`bool`、负数
  非法），参数非法抛 `InputValidationError`。达到 `max_pending` 后调用按
  发起先后 FIFO 排队，等待期间即登记 task_id（同名提交抛
  `DuplicateTaskError`）；成功、失败、取消或到期每释放一个名额只接纳队首
  一人，再按现有“priority 降序、同级接受先后”派发，不抢占执行中任务。
  容量不足直到准入时限抛 `BackpressureError`（计入 `rejected`），`close`
  开始时尚未接纳者抛 `SchedulerClosedError`（不计 `rejected`）；二者都
  释放标识、不创建任务、不执行 callable、不改变其他计数或延迟样本。被
  接纳任务的 `max_queue_wait_ms` 从实际接纳时刻起算，认领前到期仍抛
  `QueueTimeoutError` 并计入 `expired`。`TaskHandle.result`、
  `Scheduler.result` 与 `snapshot` 的既有结果、计数及两类延迟分位不变；
  现有公开入口与 CLI 不变。
- 增量：区间统计观测。新增不可变 `StatsCheckpoint`（已导出到
  `edge_sched` 公开命名空间）与 `Scheduler.stats_checkpoint`、
  `Scheduler.snapshot_since`。checkpoint 只能由对应调度器创建；
  `snapshot_since` 返回 `StatsSnapshot`，字段、分位口径与 `to_dict`
  形态与 `snapshot` 一致。创建 checkpoint 与统计事件在同一把锁的原子
  顺序上裁决：`accepted`/`rejected`/`cancelled`/`expired` 按接纳、拒绝、
  取消、到期时刻归属，`completed`/`failed` 与两类延迟样本按结束时刻
  归属；跨边界任务在接纳区间计 `accepted`，在结束区间计 `completed` 或
  `failed` 并贡献 `queue_wait_ms`/`total_latency_ms` 样本，边界前的事件
  不计入。`queue_wait_ms` 与 `total_latency_ms` 只收集边界后成功或失败
  结束的任务样本；空区间六项计数全为 0，两个分布的 p50/p95/p99/max
  均为 0.0。同一 checkpoint 可反复查询，不改变累计统计或后续区间；
  `snapshot`、`TaskHandle.result`、`Scheduler.result`、提交入口、关闭后
  结果读取与 CLI 的输出和异常语义不变。调度器关闭后仍可创建 checkpoint、
  查询历史统计；传入非 `StatsCheckpoint`、其他调度器的 checkpoint 或损坏
  对象时抛 `InputValidationError`，且不改变计数、延迟样本或任务状态。
- 增量：成组原子准入。新增 `Scheduler.submit_batch_with_wait(tasks,
  admission_timeout_ms=None)`，`tasks` 为非空列表，每项是含 `task_id`、
  `fn`、可选 `priority` 与可选 `max_queue_wait_ms` 的字典（字段口径与
  `submit` 一致，`bool` 非法；任务列表为空或不是列表、元素非字典、
  缺字段或字段值非法、组内 `task_id` 重复或任务数超过 `max_pending`
  均抛 `InputValidationError`）。整组与 `submit_with_wait` 按调用先后
  共用同一条 FIFO 准入队列：只有空缺席位数不少于组内任务数时才在同一
  原子边界一次性接纳整组，释放名额只考察队首，余量不足即等待，后续单
  任务或小组不得绕过。接纳时 `accepted` 一次增加组内任务数、全部
  `max_queue_wait_ms` 同以接纳时刻起算，组内按 `priority` 降序、同级
  按输入顺序派发。等待期间组内全部 `task_id` 已占用（冲突抛
  `DuplicateTaskError`）；`admission_timeout_ms` 为 `None` 无限等待、
  `0` 仅在调用瞬间容纳整组、正整数为毫秒时限，`bool` 或负数抛
  `InputValidationError`；超时整组抛 `BackpressureError` 且 `rejected`
  只加 1，`close` 时未接纳抛 `SchedulerClosedError` 且 `rejected` 不变；
  任何等待期失败都不创建任务、不执行 callable、不改变延迟样本。接纳后
  按输入顺序返回 `TaskHandle` 元组，不抢占执行中任务，认领前取消抛
  `TaskCancelledError`、排队到期抛 `QueueTimeoutError` 且 callable 不
  执行、成功返回原值、失败传播原始异常，单项终态不影响其他任务；
  `snapshot`、`stats_checkpoint`、`snapshot_since`、CLI 输出与退出码、
  `submit`/`submit_nowait`/`submit_with_wait` 行为均不变，新失败不改计数。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
