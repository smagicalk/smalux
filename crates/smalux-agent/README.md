# smalux-agent

`smalux-agent` 是 Smalux 的 Agent 侧任务执行库，负责本机指标采集、固定 Task、Job
命令校验和 Scheduler 运行时。它依赖 [`smalux-protocol`](../smalux-protocol/README.md)
提供的 Proto Job/Task、Noise 和 gRPC 类型，但不把连接状态塞进采集器或调度器。

## 当前状态

当前实现已经完成 Agent 的核心库能力：

- 类型安全的 Scheduler Actor、Trigger、队列、并发、超时、重试和优雅关闭；
- `JobController` 对 Server 下发的 Proto `JobCommand` 做校验、幂等和远程 Job 所有权管理；
- CPU、内存、负载、主机、磁盘 IO、网卡 IO、本地 IP、公网 IP、进程、Socket 采集；
- 多节点 ICMP Echo、TCP Connect 和 HTTP 探测；
- Process/Socket 的 `SUMMARY`、`BASIC`、`DETAILED` 成本模式；
- Task 直接返回 `TaskResult`，再由 `TaskReportSink` 选择 Channel、Callback、数据库或网络出口；
- Agent gRPC Client 的连接、HealthCheck、OpenSession 和断开日志。

当前 `smalux-agent` 二进制入口仍是开发骨架：它初始化 tracing 后退出，尚未把配置解析、
Noise 注册/IK、SessionDriver、Scheduler 和结果上报接入同一个常驻进程。因此第一次联调
请运行 Protocol 的 `noise_shared_port_server` 和 `noise_shared_port_client` 示例，而不是
把 `cargo run -p smalux-agent` 当作完整探针进程。

## 目录结构

```text
crates/smalux-agent/
├── src/main.rs                 # 当前二进制入口，仅初始化日志
├── src/client.rs               # Agent 侧 gRPC Client 包装和连接边界
├── src/cli.rs                  # CLI 预留模块，尚未接入 main
├── src/config/                 # Agent 配置预留和默认值
├── src/job_control.rs          # Proto JobCommand -> Scheduler 的控制器
├── src/scheduler/
│   ├── config.rs               # Scheduler 容量和安全限制
│   ├── model.rs                # Trigger、JobOptions、快照和更新 Patch
│   ├── queue.rs                # 到期项和 Pending 队列
│   ├── runtime/                # Actor、调度、执行、计时和管理
│   ├── task.rs                 # Task、结果出口和类型擦除适配器
│   └── event.rs                # 生命周期和执行事件
└── src/tasks/collect/
    ├── collectors/             # sysinfo/netstat2 等原始读取和采样状态
    ├── probe/                  # ICMP/TCP/HTTP 多节点探测实现
    └── *.rs                    # 固定采集 Task 和 Proto 转换
```

职责边界保持为：

```text
Protocol/Server JobDefinition
        -> JobController 校验和编译
        -> Scheduler 决定何时执行、如何排队和重试
        -> 固定 Task 读取本机并返回 TaskResult
        -> TaskReportSink 负责保存或发送结果
        -> 连接层负责 gRPC/Noise 上报
```

Task 不应自行发送 gRPC、写数据库或依赖 Scheduler；Collector 也不应知道 Job、Token 或
网络会话。

## 构建与测试

在仓库根目录执行：

```powershell
cargo check -p smalux-agent --all-targets
cargo test -p smalux-agent --all-targets
cargo clippy -p smalux-agent --all-targets -- -D warnings
```

完整 workspace 检查：

```powershell
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

运行当前二进制只能看到启动日志后进程退出：

```powershell
$env:RUST_LOG = "smalux_agent=info"
cargo run -p smalux-agent
```

真实协议联调使用：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
cargo run -p smalux-protocol --example noise_shared_port_client
```

## gRPC Client 边界

Agent 层的 `SmaluxClient` 只包装连接边界，不实现注册状态机：

| 方法 | 作用 |
| --- | --- |
| `connect(endpoint, grpc_prefix)` | 创建路径感知 gRPC Client，支持 `/api/v1/grpc` 前缀。 |
| `health_check()` | 调用未认证 HealthCheck，验证 HTTP/2 路由和 Server 进程。 |
| `open_session(stream)` | 打开 `OpenSession` 双向流；握手帧由协议层决定。 |
| `disconnect()` | 丢弃当前 gRPC Client，记录断开日志。 |
| `Drop` | 只记录仍有 channel 的释放，不负责猜测重连策略。 |

正式注册和后续 IK 应优先使用 Protocol 的高层方法：

```rust,ignore
use smalux_protocol::tonic_transport::AgentProtocolClient;

let mut client = AgentProtocolClient::new("http://127.0.0.1:12345");
client.set_grpc_prefix("/api/v1/grpc");

// 首次注册：Token 格式为 token_id.psk，Client 不需要预置 Server 公钥。
let pending = client
    .prepare_registration(identity, &registration_psk, registration_token, agent_name)
    .await?;

// 先保存身份、公钥、Agent ID 和 registration_id，再 commit。
save_pending(&pending)?;
let registered = pending.commit().await?;
save_committed(&registered)?;

// 当前 XX session 已经可用；进程重启后再用保存的身份和 Server 公钥执行 connect(IK)。
run_session(registered.session).await?;
```

实时 `TonicNoiseSession`/`SessionDriver` 不能持久化。它们包含当前 gRPC 流、Noise nonce、
心跳和 rekey 状态；断线后应关闭旧流并重新建连，不能把内存对象序列化后跨进程恢复。

## Scheduler

`SchedulerRuntime` 是唯一 Actor 生命周期所有者，`Scheduler` 是可以跨线程 Clone 的命令句柄。
启动和关闭骨架：

```rust,ignore
let runtime = SchedulerRuntime::start(SchedulerConfig::default())?;
let scheduler = runtime.scheduler();

// 业务层提供结果出口；它可以写有界 Channel、数据库或上报会话。
let sink: Arc<dyn TaskReportSink> = Arc::new(|report: TaskReport| async move {
    persist_or_send(report).await.map_err(CallbackError::Transient)
});
let controller = JobController::new(scheduler.clone(), sink);

// Server 下发的完整 Proto 命令必须通过控制器，而不是直接修改 Scheduler。
let result = controller.apply(command).await;

// 进程退出前请求优雅关闭，取消待执行项并等待运行中的 Task 退出。
runtime.shutdown().await?;
```

`SchedulerConfig` 默认提供全局并发、单 Job 并发、Pending 上限、最小周期、重试上限和关闭
等待时间。违反限制的 Trigger、JobOptions 或 Task 配置在安装前失败，不会留下半安装 Job。

### JobController 命令

| 命令 | 作用 | 版本要求 |
| --- | --- | --- |
| `ReplaceAllJobs` | 首次同步或检测到目录断档后完整对账。 | 接受不低于当前版本的权威快照。 |
| `UpsertJob` | 创建或完整替换一个远程 Job。 | `revision` 必须严格递增。 |
| `DeleteJob` | 删除一个远程所有权 Job。 | 使用 `expected_revision` 乐观锁。 |
| `RunJobNow` | 立即执行一次，不改变正常周期相位。 | 不修改 Job `revision`。 |

每条命令必须有稳定的 `command_id`。相同 `command_id` 重试时返回第一次结果，不会重复执行
`RunJobNow`。增量命令的 `catalog_revision` 必须严格连续；`ReplaceAllJobs` 使用 Server 的
权威快照版本，可以跨过断档，但不能回滚。`catalog_revision`、Job `revision` 和 Scheduler
`generation` 是三个不同版本，不能互相替代。

### 本地 Job 与远程 Job

本地 Job 可以由 Agent 内置策略或本地配置创建；远程 Job 只由 `JobController` 管理。两者可以
共用一个 Scheduler，但 `ReplaceAllJobs`、`DeleteJob` 和 `clear()` 只操作远程所有权 Job，
不会删除本地 Job。

Scheduler 不会因为连接断开自动暂停远程 Job。连接管理层应根据业务需要决定离线运行窗口，
超过窗口后再通过 Scheduler 关闭或更新对应 Job。

## 内置采集 Task

所有固定 Task 都实现 `ReportingTask`，返回协议 `TaskResult`。常用类型如下：

| Task | 结果 | 主要配置 |
| --- | --- | --- |
| `SystemTask` | CPU、内存、负载、主机、磁盘、网卡、IP、Socket、进程组合快照。 | 可嵌套磁盘、网卡和本地 IP 选择。 |
| `CpuTask` | 总体和每核 CPU 快照。 | 当前无业务配置。 |
| `MemoryTask` | 物理内存和交换区容量/使用量。 | 当前无业务配置。 |
| `LoadTask` | 系统平均负载。 | 当前无业务配置。 |
| `HostTask` | 主机名、系统和启动信息。 | 当前无业务配置。 |
| `DiskIoTask` | 磁盘读写累计量和速率。 | 设备名/挂载点 include/exclude。 |
| `NetworkIoTask` | 网卡收发累计量和速率。 | 网卡 include/exclude；include 优先。 |
| `LocalIpTask` | 本地网卡地址。 | 网卡 include/exclude。 |
| `PublicIpTask` | 外部服务解析的公网 IPv4/IPv6。 | 超时、地址族和服务端点。 |
| `ProcessTask` | 进程总数、状态统计和进程列表。 | 模式、PID/名称筛选、CPU/内存排序、条目上限。 |
| `SocketTask` | TCP/UDP 总数、状态统计和连接明细。 | 模式、TCP/UDP、IPv4/IPv6、条目上限。 |
| `ProbeTask` | 多节点 ICMP、TCP Connect、HTTP 探测。 | 节点列表、协议目标、尝试次数、超时、间隔、并发。 |

### 采集成本模式

`SUMMARY` 只返回总数或聚合统计，`BASIC` 返回基础条目，`DETAILED` 执行更昂贵的进程关联、
命令行或 Socket PID 关联。Process 和 Socket 的 DETAILED 模式应使用较低频率和有限的
`max_entries`，避免在大量进程/连接主机上产生过大开销。

### 选择优先级

- include 列表非空时先进入白名单；
- exclude 最终生效，命中 include 和 exclude 时会被排除；
- 磁盘可按设备名或挂载点筛选；
- 网卡和本地 IP 按完整接口名筛选；
- 未提供筛选时磁盘、网卡和本地 IP 默认采集全部可见对象；
- Probe 节点必须明确协议，单个节点失败不会隐藏其他节点结果。

## 结果出口

Task 只返回值，不直接发送网络：

```text
ReportingTask::run(TaskContext)
    -> TaskResult
    -> Scheduler 生成 TaskReport（Job ID、revision、run ID、尝试次数、时间）
    -> TaskReportSink::report(report)
    -> Channel / Callback / 数据库 / gRPC Session
```

Task 错误和结果交付错误分开处理：临时 Task 错误可按 Job 重试策略重试；临时交付错误不会
重新执行已经完成的 Task；永久交付错误可以停用 Job。关闭、删除和超时通过 `TaskContext`
中的 `CancellationToken` 协作取消，进入 `spawn_blocking` 的非可取消 Task 会等待实际返回。

## 日志与数据目录

Agent 模块只产生 `tracing` 事件，进程入口负责初始化 subscriber。通用设置：

```powershell
$env:RUST_LOG = "smalux_agent=info,smalux_protocol=info"
$env:SMALUX_LOG_COMPONENT = "agent"
```

日志写入公共数据目录的 `logs/<component>/smalux.log`，按日期和 10 MiB 大小滚动，同时输出
控制台。当前 Agent 尚未接入身份/Job 本地持久化；正式运行时应把 Noise 私钥、Server 公钥、
Agent ID、注册事务和需要恢复的远程 Job 目录保存到数据目录或外部存储，不能把它们放进 cache。

## 扩展边界

新增原始系统能力时先放到 `tasks/collect/collectors/`，只返回领域快照或明确错误；新增固定
Task 时在 `tasks/collect/` 添加 Proto 配置转换、采样状态和 `TaskResult` 映射；需要跨 Agent
和 Server 的配置/结果时先修改 `smalux-protocol/proto/.../task/`，再更新 `JobController::build`。

不要在 Task 中直接读取 Token、持有 gRPC Client、操作 Scheduler 或写数据库。Plus 模块应依赖
稳定的 Task/Proto 边界，通过单独的 TaskFactory/feature 注册，不修改核心 Scheduler 的业务判断。

## 相关文档

- [Protocol README](../smalux-protocol/README.md)：gRPC、Noise、Session 和 rekey；
- [Job 与 Task](../../website/docs/usage/job-task.md)：Proto Job、触发器、队列和版本模型；
- [进程与 Socket](../../website/docs/usage/process-socket.md)：详细模式和筛选策略；
- [Workspace 开发](../../website/docs/development/workspace.md)：修改位置和提交前检查。
