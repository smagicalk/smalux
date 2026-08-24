# smalux-agent

`smalux-agent` 是 Smalux 的 Agent 侧任务执行库，负责本机指标采集、固定 Task、Job
命令校验和 Scheduler 运行时。它依赖 [`smalux-protocol`](../smalux-protocol/README.md)
提供的 Proto Job/Task、Noise 和 gRPC 类型，但不把连接状态塞进采集器或调度器。

## 当前状态

当前实现已经完成 Agent 的核心库能力：

- 类型安全的 Scheduler Actor、Trigger、队列、并发、超时、重试和优雅关闭；
- `RemoteJobController` 对 Server 下发的 Proto `JobCommand` 做校验、幂等和远程 Job 所有权管理；
- CPU、内存、负载、主机、磁盘 IO、网卡 IO、本地 IP、公网 IP、进程、Socket 采集；
- 多节点 ICMP Echo、TCP Connect、UDP Request 和 HTTP 探测；
- Process/Socket 的 `SUMMARY`、`BASIC`、`DETAILED` 成本模式；
- Task 直接返回 `TaskResult`，再由 `TaskReportSink` 选择 Channel、Callback、数据库或网络出口；
- 自动选择 XXpsk3 首次注册或 IK 恢复连接的 `SmaluxClient`；
- SessionDriver 自动心跳、Pong、会话 rekey 和断线指数退避重连；
- 安全文件状态存储、能力同步、远程 Job 下发、结果上报和优雅关闭的常驻进程入口。

当前二进制已经接通 Agent 的主链路。Server 端仍在开发中，实际联调要求 Server 已实现
注册 Token 签发、XX 提交、IK 授权和 JobCommand 下发。

## 目录结构

```text
crates/smalux-agent/
├── src/lib.rs                  # 可嵌入的 Agent Client、Job 和 Scheduler 库入口
├── src/main.rs                 # Client、RemoteJobController 和 Scheduler 生命周期装配
├── src/client.rs               # 对外 SmaluxClient 门面和稳定发送句柄
├── src/client/connection.rs    # 注册、pending 恢复和 IK 连接建立
├── src/client/config.rs        # 端点、Token、心跳、rekey 和重连配置
├── src/client/state/           # 认证状态模型与默认原子文件存储
├── src/client/supervisor.rs    # SessionDriver、授权验证和 IK 自动重连
├── src/cli.rs                  # Clap 参数、配置覆盖和只读查询命令模型
├── src/commands.rs             # status/jobs/tasks/plugins/config/identity 命令执行
├── src/management/              # 常驻 Agent 的本地管理边界
│   ├── mod.rs                   # 兼容性重导出和 IPC 装配
│   ├── protocol.rs              # 请求、响应、状态和 Job 策略 DTO
│   ├── service.rs               # 状态聚合、Job 查询和策略修改业务
│   └── ipc.rs                   # Named Pipe/Unix Socket 有界 JSON 帧
├── src/config/                 # Agent 配置预留和默认值
├── src/remote_jobs.rs          # Proto JobCommand -> Scheduler 的远程 Job 控制器
├── src/remote_jobs/compiler.rs # Proto JobDefinition -> Task/Trigger 编译
├── src/outbox.rs               # 断线期间的 Job 结果与 TaskReport 内存队列
├── src/scheduler/
│   ├── config.rs               # Scheduler 容量和安全限制
│   ├── model.rs                # Trigger、JobOptions、快照和更新 Patch
│   ├── queue.rs                # 到期项和 Pending 队列
│   ├── runtime/                # Actor、调度、执行、计时和管理
│   ├── task.rs                 # Task、结果出口和类型擦除适配器
│   └── event.rs                # 生命周期和执行事件
└── src/tasks/collect/
    ├── collectors/             # sysinfo/netstat2 等原始读取和采样状态
    ├── probe/                  # ICMP/TCP/UDP/HTTP 多节点探测实现
    └── *.rs                    # 固定采集 Task 和 Proto 转换
```

职责边界保持为：

```text
Protocol/Server JobDefinition
        -> RemoteJobController 校验和编译
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

运行 Agent：

```powershell
$env:RUST_LOG = "smalux_agent=info,smalux_protocol=info"
cargo run -p smalux-agent -- run `
  --server-endpoint http://127.0.0.1:12345 `
  --token "token-id.64位十六进制PSK"
```

`--token` 适合临时注册，但可能出现在命令历史或本机进程列表。需要避免这一点时使用
`--token-file C:/smalux/registration-token.txt` 或 `SMALUX_REGISTRATION_TOKEN`。Agent 已处于
`registered` 阶段时 Client 自动执行 IK，配置中存在的 Token 不会被读取。

### 启动参数

配置优先级固定为 `CLI > 环境变量 > 默认值`：

| CLI | 环境变量 | 默认值/作用 |
| --- | --- | --- |
| `--server-endpoint` | `SMALUX_SERVER_ENDPOINT` | `http://127.0.0.1:12345` |
| `--grpc-prefix` | `SMALUX_GRPC_PREFIX` | `/api/v1/grpc` |
| `--no-grpc-prefix` | - | 禁用反向代理路径前缀，与 `--grpc-prefix` 互斥 |
| `--token` | `SMALUX_REGISTRATION_TOKEN` | 可选的一次性注册 Token |
| `--token-file` | - | 从 UTF-8 文件读取 Token，与 `--token` 互斥 |
| `--state-file` | - | `<data_dir>/agent/connection-state.json` |
| `--handshake-timeout` | `SMALUX_HANDSHAKE_TIMEOUT` | `5s` |
| `--heartbeat-interval` | `SMALUX_HEARTBEAT_INTERVAL` | `30s` |
| `--heartbeat-timeout` | `SMALUX_HEARTBEAT_TIMEOUT` | `90s`，必须大于心跳间隔 |
| `--reconnect-initial-delay` | `SMALUX_RECONNECT_INITIAL_DELAY` | `1s` |
| `--reconnect-max-delay` | `SMALUX_RECONNECT_MAX_DELAY` | `30s` |
| `--offline-job-timeout` | `SMALUX_OFFLINE_JOB_TIMEOUT` | `30m`，持续断线到期后清空远程 Job |
| `--task-report-buffer-capacity` | `SMALUX_TASK_REPORT_BUFFER_CAPACITY` | `1024`，满载时丢弃最旧报告并记录日志 |
| `--job-result-buffer-capacity` | `SMALUX_JOB_RESULT_BUFFER_CAPACITY` | `1024`，满载时丢弃最旧 Job 结果 |
| `--shutdown-drain-timeout` | `SMALUX_SHUTDOWN_DRAIN_TIMEOUT` | `5s`，退出前补发内存结果的总等待时间 |
| `--scheduler-global-concurrency` | `SMALUX_SCHEDULER_GLOBAL_CONCURRENCY` | 逻辑 CPU 数的 4 倍 |
| `--scheduler-global-max-pending` | `SMALUX_SCHEDULER_GLOBAL_MAX_PENDING` | `8192` |
| `--scheduler-default-job-concurrency` | `SMALUX_SCHEDULER_DEFAULT_JOB_CONCURRENCY` | `1` |
| `--scheduler-default-job-max-pending` | `SMALUX_SCHEDULER_DEFAULT_JOB_MAX_PENDING` | `1024` |
| `--scheduler-max-jobs` | `SMALUX_SCHEDULER_MAX_JOBS` | `1024` |
| `--scheduler-shutdown-timeout` | `SMALUX_SCHEDULER_SHUTDOWN_TIMEOUT` | `30s` |
| `--control-endpoint` | `SMALUX_CONTROL_ENDPOINT` | Windows named pipe 或 Unix socket |
| `--plugin-max-workers` | `SMALUX_PLUGIN_MAX_WORKERS` | `16`，Agent 本地 Worker 数量上限 |
| `--plugin-max-concurrency` | `SMALUX_PLUGIN_MAX_CONCURRENCY` | `4`，单 Worker 并发上限 |
| `--plugin-task-timeout` | `SMALUX_PLUGIN_TASK_TIMEOUT` | `5m`，插件 Task 本地超时 |
| `--plugin-shutdown-timeout` | `SMALUX_PLUGIN_SHUTDOWN_TIMEOUT` | `5s`，旧 Worker 关闭等待时间 |
| `--plugin-startup-timeout` | `SMALUX_PLUGIN_STARTUP_TIMEOUT` | `10s`，Worker Hello/Initialize 握手超时 |
| `--plugin-restart-max-attempts` | `SMALUX_PLUGIN_RESTART_MAX_ATTEMPTS` | `3`，失败窗口内最大重启失败次数 |
| `--plugin-restart-window` | `SMALUX_PLUGIN_RESTART_WINDOW` | `10m`，Worker 崩溃统计窗口 |

默认管理端点会直接显示在 `smalux-agent --help` 中：Windows 使用
`\\.\pipe\smalux-agent`，Linux/macOS 使用 `<data_dir>/agent/control.sock`。

### 状态查询

常驻 Agent 启动本机管理 IPC。Windows 管道拒绝远程客户端并使用受限 ACL；Unix socket
权限为 `0600`。IPC 不开放 TCP，不传输私钥、Token 或裸 Scheduler 对象；只有 Job 策略
命令可以修改状态，其余查询保持只读。

```powershell
smalux-agent status
smalux-agent status --watch --interval 2s --output json
smalux-agent jobs list
smalux-agent jobs show 550e8400-e29b-41d4-a716-446655440000
smalux-agent jobs policy show
smalux-agent jobs policy add-task smalux.collect.process.v1
smalux-agent jobs policy remove-task smalux.collect.process.v1
smalux-agent jobs policy deny-all
smalux-agent jobs policy allow-all
smalux-agent tasks list
smalux-agent tasks show smalux.collect.cpu.v1
smalux-agent plugins list
smalux-agent config show
smalux-agent identity show
```

- `status` 显示连接、认证模式、心跳 RTT、Scheduler、Job 数和插件子系统状态。
- `status` 同时显示断线期间待发送的 Job 结果数量和因容量限制丢弃的结果数量。
- `jobs list/show` 查询当前运行进程内的远程 Job；Agent 未运行时不会伪造空列表。
- `tasks list/show` 从唯一 Capability Registry 离线读取，可直接复制完整 kind 写入拒绝策略。
- `plugins list/show` 显示已安装插件、激活状态、Worker PID、配置 revision、实际并发和熔断原因。
- `config show` 查询运行进程的最终配置，Token 永远为 `<redacted>`。
- `identity show` 离线读取状态文件，只展示注册阶段与公钥 ID。

IPC 当前采用协议版本 `2`、`u32` 长度前缀和最大 1 MiB JSON 帧，一次连接只处理一个
请求。本版本不包含事件订阅、Task 结果历史或 TUI。

### 动态远程 Job 策略

策略由 Agent 本地拥有并持久化到 `<data_dir>/agent/job-policy.json`，只能通过本机 CLI
修改。它只约束 Server 下发的远程 Job，不影响 Agent 内部代码创建的本地任务。

```text
smalux.collect.cpu.v1
smalux.collect.process.v1
smalux.collect.socket.v1
smalux.probe.network.v1
```

完整列表以 `smalux-agent tasks list` 输出为准。加入 Task kind 后，Agent 立即停用并取消
所有匹配的远程 Job，同时保留定义用于诊断；`deny-all` 对全部远程 Job执行同一处理。
解除规则不会直接运行旧定义，而是通知 Server 重新读取权威目录并下发 `ReplaceAllJobs`。
`allow-all` 只解除全局拒绝，保留逐项 Task 黑名单。

Agent 在每次认证连接建立后主动上报完整策略，Server 也会主动查询。CLI 返回的
`server_sync=pending` 表示本地策略已经生效但 Server 尚未确认；ACK 只表示 Server 已接收
策略，不代表 Job 已重新下发。断线期间修改仍会成功，重连后只发送最新完整快照。

### Plus AgentContext 与本地持久化

Agent 启动 Worker 时注入只读 `AgentContext`，包括 Agent 版本、操作系统、架构、数据目录、
配置目录、插件目录和当前插件专属数据目录。插件可以在自己的目录中保存业务状态：

```text
<data_dir>/plus/<plugin_id>/<plugin_version>/
```

Token、私钥、Noise 状态和 Server 凭据不会进入 AgentContext。Plus 的业务幂等数据由插件自己
持久化，Agent 只负责 Worker 生命周期、超时、并发和结果上报。

### Plus Worker 崩溃和 Server 暂停

Agent 每秒检查 Worker 子进程和 stdout reader。异常退出会在本地失败窗口内按 `1s、2s、4s`
退避重启；正常配置替换或主动关闭不会计入失败。默认在 `10m` 内第 `3` 次失败后进入
`paused`，停止该插件 Worker，并通过加密会话发送 `PluginPauseNotice`。

Server 按 `agent_id + plugin_id + version` 保存暂停状态，并从该 Agent 的远程 Job 快照中移除
对应插件任务；其他 Agent 不受影响。暂停通知收到确认后不会重复发送，断线重连会重发未确认通知。
只有 Server 下发更高的 `PluginRuntimeSnapshot.revision` 才能清除暂停并尝试启动新 Worker；
Agent 不会自动重试原任务，原任务是否重试仍由 Job 的 Scheduler 策略决定。

Plus Worker 的实际并发会取插件自身上限和 Agent 本地上限中的较小值。共享 runtime 配置会在
Worker Ready 前交给每个 Plus Task 初始化；初始化失败时整个 Worker 不会进入可执行状态。

策略文件无法解析时不会静默放行远程 Job。交互启动会询问是否备份并重置；服务模式直接
失败并提示执行 `smalux-agent jobs policy repair --reset`。修复命令检测到 Agent 正在运行时
会拒绝离线改文件。

### 能力同步

每次 XXpsk3 注册或 IK 重连成功后，Agent 都会主动发送 `AgentCapabilitySnapshot`。快照包含
Agent 版本、排序后的内置 Task kind，以及 ICMP/TCP/HTTP/UDP Probe 协议。Server 也可以发送
`AgentCapabilityQuery` 要求当前会话立即重发。该消息没有 ACK：它描述当前二进制能力，不代表
Job 已执行；Job 是否应用成功仍由 `JobCommandResult` 确认。

真实协议联调使用：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
cargo run -p smalux-protocol --example noise_shared_port_client
```

## Client 边界与调用流程

Agent 层的 `SmaluxClient` 是认证连接的编排入口，底层 Noise 状态机仍由
`smalux-protocol::tonic_transport::AgentProtocolClient` 执行。Agent Client 不暴露裸
`open_session`，避免调用方绕过 XXpsk3/IK 直接发送未认证帧：

| 方法 | 作用 |
| --- | --- |
| `SmaluxClientConfig::new` / `from_env` | 配置端点、gRPC 前缀、可选 Token、心跳和重连策略。 |
| `SmaluxClient::new` | 注入配置和 `AgentStateStore`，尚不访问网络。 |
| `connect()` | 加载状态，自动执行 XXpsk3、pending 恢复或 IK，并启动后台 Driver。 |
| `health_check()` | 未认证 HealthCheck；只表示端点可达，不表示 Agent 已获授权。 |
| `handle()` | 获取可克隆发送句柄，适合注入 Scheduler 回调或其他后台任务。 |
| `send_task_report()` | 通过当前已认证 Noise 会话发送一次采集结果。 |
| `send_job_command_result()` | 返回 JobCommand 的幂等执行结果。 |
| `send_agent_capability()` | 发送 Agent 能力查询或完整快照。 |
| `heartbeat_stats()` | 查询后台自动心跳的收发计数和最近 RTT。 |
| `next_event()` | 接收解密后的 JobCommand、Diagnostic 或密钥轮换事件。 |
| `disconnect()` | 优雅关闭 Driver；长期身份仍保留，下一次 `connect()` 使用 IK。 |
| `forget_registration()` | 断开并清除本地身份；不会撤销 Server 数据，调用前应先完成服务端吊销。 |

最小调用示例：

```rust,ignore
let mut config = SmaluxClientConfig::new(server_endpoint)?;
config.set_grpc_prefix(Some("/api/v1/grpc".to_owned()));
config.set_registration_token(configured_token.map(RegistrationToken::new).transpose()?);

let store = Arc::new(FileAgentStateStore::new(state_path));
let mut client = SmaluxClient::new(config, store);
client.connect().await?;

let sender = client.handle()?;
while let Some(event) = client.next_event().await? {
    if let SmaluxClientEvent::Session(SessionEvent::JobCommand(command)) = event {
        let result = remote_jobs.apply_command(command).await;
        sender.send_job_command_result(result).await?;
    }
}
```

注册 Token 采用 `token_id.64位十六进制PSK`。Agent 展示名称由 Server 签发 Token 时绑定；
没有指定名称时，Server 使用新生成的 `agent_id` 作为默认展示名称，Agent 不上报或覆盖该字段。
Token 只在状态为 `IdentityPrepared` 或 pending 确认
未提交时才解析。状态为 `Registered` 时直接使用长期 Agent 私钥与已固定的 Server 公钥执行
IK，配置中的 Token 不会被读取。Token 和 PSK 的 Debug 输出始终脱敏。

持久化状态分为三阶段：

1. `IdentityPrepared`：已生成 Agent 身份，但尚未获得 Server 注册事务；
2. `RegistrationPending`：Server 已 prepare，本地先原子保存 Agent ID、双方公钥和事务 ID；
3. `Registered`：Server 已确认 commit，后续连接只使用 IK。

进程若在 Server commit 后、本地保存 `Registered` 前崩溃，重启会先尝试 IK 并通过加密
Ping/Pong 验证授权；只有 Server 明确返回 `AgentNotAuthorized` 才恢复 XX pending 流程，
不会因普通网络错误降级到 Token 注册。

实时 gRPC 流、Noise nonce、心跳和会话 rekey 状态不会持久化。首次连接失败或活动流断开后，
监督器均按 1 秒起步、最大 30 秒的指数退避重新连接；`SessionDriver` 默认自动发送心跳并响应
Pong。Server 公告新静态公钥时，Client 会验证 key ID，先把新旧公钥候选原子保存，再发送确认，
确保轮换窗口内的后续 IK 可以尝试两把 key。

JobCommandResult 在短暂断线时会保存在进程内 FIFO；TaskReport 使用可配置的有界 FIFO，满载时
丢弃最旧报告并累计告警。重连后两者按原顺序补发。收到 Ctrl+C 或 Unix SIGTERM 后，Agent 先停止
Scheduler，再在总超时内补发队列，随后关闭 Noise Session 和本地 IPC。这些队列不跨进程恢复，且
TaskReport 当前没有 Server 业务 ACK；需要承受断电或崩溃时仍应加入持久化 outbox。

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
let controller = RemoteJobController::new(scheduler.clone(), sink);

// Server 下发的完整 Proto 命令必须通过控制器，而不是直接修改 Scheduler。
let result = controller.apply_command(command).await;

// 进程退出前请求优雅关闭，取消待执行项并等待运行中的 Task 退出。
runtime.shutdown().await?;
```

`SchedulerConfig` 默认提供全局并发、单 Job 并发、Pending 上限、最小周期、重试上限和关闭
等待时间。违反限制的 Trigger、JobOptions 或 Task 配置在安装前失败，不会留下半安装 Job。

### RemoteJobController 命令

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

本地 Job 可以由 Agent 内置策略或本地配置创建；远程 Job 只由 `RemoteJobController` 管理。两者可以
共用一个 Scheduler，但 `ReplaceAllJobs`、`DeleteJob` 和 `clear()` 只操作远程所有权 Job，
不会删除本地 Job。

短暂断线不会暂停远程 Job；从首次断线开始持续超过 `--offline-job-timeout`（默认 `30m`）后，
Agent 会取消并清空全部远程 Job、重置内存目录 revision，本地 Job 不受影响。重连会取消尚未到期的
计时器；若目录已清空，Agent 会照常上报本地策略和能力，由 Server 重新下发权威
`ReplaceAllJobs` 快照。

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
| `ProbeTask` | 多节点 ICMP、TCP Connect、UDP Request、HTTP 探测。 | 节点列表、协议目标、尝试次数、超时、间隔、并发。 |

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
控制台。默认身份状态保存在公共数据目录的 `agent/connection-state.json`，写入时使用
`.new` + `.bak` 原子替换和崩溃恢复。Unix 创建权限为 `0600`；Windows 使用仅 SYSTEM、Administrators
和当前所有者可完全访问的保护 ACL，加载旧文件时也会检查权限。该文件包含 Agent 长期私钥；
它不能放进 cache，也不能提交到版本库。远程 Job 目录目前仍是内存状态，进程重启后由
Server 通过 `ReplaceAllJobs` 重新下发权威快照。

## 扩展边界

新增原始系统能力时先放到 `tasks/collect/collectors/`，只返回领域快照或明确错误；新增固定
Task 时在 `tasks/collect/` 添加 Proto 配置转换、采样状态和 `TaskResult` 映射；需要跨 Agent
和 Server 的配置/结果时先修改 `smalux-protocol/proto/.../task/`，再更新
`remote_jobs/compiler.rs` 中对应的 `compile_*` 和 `TaskFactory::build` 分支。

不要在 Task 中直接读取 Token、持有 gRPC Client、操作 Scheduler 或写数据库。Plus 模块应依赖
稳定的 Task/Proto 边界，通过单独的 TaskFactory/feature 注册，不修改核心 Scheduler 的业务判断。

## 相关文档

- [Protocol README](../smalux-protocol/README.md)：gRPC、Noise、Session 和 rekey；
- [Job 与 Task](../../website/docs/usage/job-task.md)：Proto Job、触发器、队列和版本模型；
- [进程与 Socket](../../website/docs/usage/process-socket.md)：详细模式和筛选策略；
- [Workspace 开发](../../website/docs/development/workspace.md)：修改位置和提交前检查。
