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
  admission_timeout_ms=None)`：`tasks` 为非空列表，每项是含 `task_id`、
  `fn` 与可选 `priority`、`max_queue_wait_ms` 的字典（字段口径与
  `submit` 一致）；返回与输入等长、同序的 `TaskHandle` 元组。整组要么
  全部获得名额，要么继续等待，不存在部分接纳；它与 `submit_with_wait`
  按调用先后共用同一条 FIFO 准入队列，释放名额只考察队首——余量不足以
  容纳队首整组时继续等待，后续单任务或更小的组即使放得下也不得绕过。
  接纳瞬间组内任务共享同一接纳时刻、按输入顺序预留连续接受序号，
  `accepted` 一次增加组内任务数，`max_queue_wait_ms` 自该刻起算，随后
  组内（以及与其他已接纳任务之间）仍按 `priority` 降序、同级按接受先后
  派发。等待期间组内全部 task_id 即被占用（与已接受任务或其他等待者
  同名抛 `DuplicateTaskError`，组内重复抛 `InputValidationError`）；
  任务列表为空或不是列表、元素结构或字段值非法、任务数超过
  `max_pending` 一律抛 `InputValidationError`，不入队、不留计数。
  `admission_timeout_ms` 为 `None` 时无限等待，`0` 时仅在调用瞬间容纳
  整组，正整数时等待相应毫秒数，布尔值或负数抛 `InputValidationError`；
  超时整组被拒抛 `BackpressureError`（一次调用 `rejected` 只加 1），
  `close` 开始时未接纳抛 `SchedulerClosedError`（`rejected` 不变）。
  任何失败都不创建任务、不执行 callable、不改变延迟样本。接纳后各项
  保持既有终态语义（成功原值、失败原异常、认领前取消
  `TaskCancelledError`、排队到期 `QueueTimeoutError` 且 callable 不执行），
  单项终态不影响其他任务；`snapshot`、`stats_checkpoint`、
  `snapshot_since`、CLI 输出与退出码，以及 `submit`、`submit_nowait`、
  `submit_with_wait` 的行为均不变。
- 增量：可选排队优先级老化。`Scheduler` 构造新增
  `aging_interval_ms`（置于 `workers`、`max_pending` 之后，缺省 `None`
  关闭）；启用时只接受 >= 1 的整数毫秒，布尔值、零、负数、浮点数或
  其他类型抛 `InputValidationError`，且不创建可用调度器。老化只作用于
  已接纳、尚未被工作线程认领且未进入取消/到期终态的任务：任务自实际
  接纳时刻起按单调时钟累计老化周期，派发比较时有效优先级等于原
  `priority` 加已完成周期数。比较依次按有效优先级降序、原 `priority`
  降序、接受先后升序；成组准入任务共享接纳时刻并按输入顺序确定同级
  次序。越过老化边界后立即比较，后接受的任务可因有效优先级更高而先
  派发，有效优先级与原 `priority` 都相同时仍由接受先后决定。工作线程
  原子认领后有效优先级冻结，老化不抢占、不重排执行中任务，也不改变
  `max_queue_wait_ms` 的起算与到期口径。未配置老化时派发语义与之前
  完全一致。四个提交入口的参数位置、返回值、异常、task_id 占用、准入
  FIFO、取消、排队到期、执行结果与统计口径不变；`snapshot`、
  `stats_checkpoint`、`snapshot_since` 的累计/区间值、分位定义与
  `to_dict` 形态不变。CLI 新增 `--aging-interval-ms`（缺省关闭），原有
  调用方式、结果数组与 JSON 结构不变；非法值在标准错误打印
  `InputValidationError` 并以退出码 2 结束。
- 增量：执行耗时分布。`StatsSnapshot.to_dict()` 在既有计数与
  `queue_wait_ms`、`total_latency_ms` 之外新增同形态（p50/p95/p99/max，
  毫秒保留三位小数，空分布四个值均为 0.0）的 `execution_ms`，
  `StatsSnapshot` 同时暴露 `execution_ms` 属性。计时从任务被工作线程
  在锁内原子认领（置 `started`）之后开始，到 callable 正常返回或抛出
  `Exception` 为止，因此既不含认领前的排队等待，也不含结束后的记账/
  唤醒开销；成功任务与失败任务都恰好贡献一个执行耗时样本，失败时
  `TaskHandle.result` / `Scheduler.result` 仍原样抛出 callable 的异常。
  未开始执行就被取消（`TaskCancelledError`）或排队到期
  （`QueueTimeoutError`）的任务不贡献 `execution_ms`，提交被拒、参数
  校验失败、关闭期间未接纳的任务同样不贡献。`stats_checkpoint` /
  `snapshot_since` 按任务结束时刻把执行耗时样本归属到对应区间：跨
  checkpoint 的任务仍在接纳区间计 `accepted`，在结束区间计
  `completed`/`failed` 并贡献执行样本；分位计算只使用当前快照或当前
  区间内的样本，同一 checkpoint 反复查询结果稳定。
  `python -m edge_sched` 的结果数组、逐项结果对象、退出码与既有 stats
  字段保持不变，只在 stats 中新增 `execution_ms`；提交、返回、取消、
  到期、背压、task_id 占用、结果读取、关闭语义、输入校验与优先级派发
  顺序均不变，也不另建落盘文件。
- 增量：即时运行观测。新增 `Scheduler.runtime_snapshot()`，从 `edge_sched`
  导出不可变 `RuntimeSnapshot`；在与提交、认领、取消、到期、名额释放及
  close 相同的 `_cond` 锁内、按同一单调时刻只读状态，不改变任务、等待者
  与统计，生命周期内（含 closing/closed）均可调用。固定属性为 `workers`、
  `max_pending`、`queued`、`running`、`unfinished`、
  `admission_waiters`、`admission_waiting_tasks`、`available_capacity`、
  `oldest_queued_age_ms`、`oldest_admission_wait_ms`、`closing`、`closed`；
  `to_dict()` 只返回同名且 JSON 可序列化的字典，属性不可赋值或删除。
  `queued` 为已接纳、未被工作线程认领且仍可执行的任务，`running` 为已
  认领未终态的任务，`unfinished` 恒为二者之和（取消、到期、已结束不计）；
  `admission_waiters` 只计仍在等容量的 `submit_with_wait` /
  `submit_batch_with_wait` 调用，`admission_waiting_tasks` 计其占用的
  task_id（成组按组内任务数计，接纳即退出，不与已接纳任务重复计数）；
  `available_capacity = max(0, max_pending - unfinished)`。
  `oldest_queued_age_ms` 与 `oldest_admission_wait_ms` 分别从最早未认领
  任务的接纳时刻、最早等待调用的发起时刻算至快照时刻，无对象为 0.0，
  单调时钟毫秒保留三位小数，执行中任务不计前者。事件与 close 交错时
  快照仍一致；close 后无 queued/running/admission_waiters，
  `closing`/`closed` 反映当时状态，已返回的快照不随后续事件变化。现有
  公开 API、统计、异常与 CLI 不变，且不产生任何落盘文件。
- 增量：滑动窗口延迟观测。`Scheduler` 构造在 `aging_interval_ms` 之后新增
  `latency_window_tasks`（缺省 `None` 关闭）；启用值只能是 >= 1 的整数
  任务数，布尔值、零、负数、浮点数或其他类型抛 `InputValidationError`，
  且不创建调度器（参数校验先于任何状态与线程创建）。启用后按任务**结束
  顺序**保留最近 N 个成功/失败结束任务的 `queue_wait_ms` /
  `total_latency_ms` / `execution_ms` 三元样本（`deque(maxlen=N)`，三类
  样本在统计同一把锁内同步进出）；取消、排队到期、拒绝、close 期间未
  接纳及执行中任务不入窗。累计统计与区间统计（`stats_checkpoint` /
  `snapshot_since`）的口径、分位定义与 `to_dict` 形态完全不变，窗口只在
  `record_finished` 的同一原子顺序上额外记账。从 `edge_sched` 导出不可变
  `RollingStatsSnapshot`，`Scheduler.rolling_snapshot()` 返回它：固定字段
  为 `window_size`、`sampled_finished`、`queue_wait_ms`、
  `total_latency_ms`、`execution_ms`；三个分布沿用既有键、`ceil(n*q)`
  分位与三位小数，空分布四个值均为 0.0。`sampled_finished` 为窗内结束
  任务数，窗口未满时等于启用后结束任务总数，满窗后等于 N。取样与既有
  统计记账同序、只含已记账结束，`rolling_snapshot` 与 `snapshot` /
  `snapshot_since` 同刻边界一致；快照为值拷贝，对象不可修改或删除，
  `to_dict()` 只含上述字段且可 JSON 序列化，重复读取稳定，close 后窗口
  仍可读。未启用时 `rolling_snapshot()` 返回 `window_size=0`、
  `sampled_finished=0` 与三个空分布。四个提交入口、准入 FIFO、老化、
  取消、到期、背压、关闭、执行结果与其余公开输出均不变。CLI 新增
  `--latency-window-tasks`（缺省关闭，输出不含 `rolling`）；启用后最终
  stats 新增 `rolling`，其值等于 `RollingStatsSnapshot.to_dict()`，结果
  数组与既有 stats 字段、退出码与输入顺序输出不变；非法值在标准错误
  打印 `InputValidationError` 并以退出码 2 结束，不落盘。
- 增量：运行时工作线程容量调整。新增 `Scheduler.resize_workers(workers)`：
  `workers` 只能是 >= 1 的整数，布尔值、0、负数、浮点数或其他类型抛
  `InputValidationError`；相同容量调用是无操作并正常返回。目标值在
  `_cond` 状态锁内原子生效，与提交、认领、取消、到期、名额释放及
  `close` 并发安全；并发调整按取得锁的先后串行，后一次覆盖前一次。
  扩容先补足新工作线程再补充许可，新增线程立即可接收任务；缩容把并发
  上限立即降到目标值——空闲许可当场退出流通，被占用的许可记为退役
  额度随归还逐步吸收，等量工作线程在取到停止哨兵后退出——但不打断、
  不取消、不提前结束已认领执行中的 callable（仍按原值或原异常结束），
  任务结束后不再按旧上限启动新任务；未认领任务继续排队，仍按
  priority 降序、同级接受先后及 aging 语义派发。容量变化不改变
  `max_pending`、准入 FIFO、成组原子性、排队时限、取消、到期、背压、
  task_id 占用与释放、结果读取和 `close` 语义；`runtime_snapshot().workers`
  与 `RuntimeSnapshot.to_dict()` 的 `workers` 报告当前有效目标容量，
  已返回快照不随后续调整变化，其余快照字段与统计口径不变。调度器
  closing 或 closed 时调用抛 `SchedulerClosedError`，容量与任务状态
  不变；若调整先于 `close` 取得状态锁，则按新容量收尾，`close` 仍等待
  全部已接受任务结束并安全停止所有线程。`python -m edge_sched` 与现有
  CLI 参数、输出、退出码不变，不新增落盘行为。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
