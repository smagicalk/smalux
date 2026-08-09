# smalux-protocol

`smalux-protocol` 是 Agent 与 Server 共用的版本化 gRPC、Noise 会话和密钥轮换 crate。

当前提供：

- Protobuf package：`smalux.agent.v1`
- gRPC service：`AgentTransport`
- Rust 模块：`smalux_protocol::agent::v1`
- Noise XXpsk3 首次注册和 IK 后续双向认证
- 加密业务消息、心跳和同步 rekey
- Agent/Server 静态密钥轮换状态及可持久化快照
- Tonic Client/Server 会话适配方法
- 可组合的会话维护方法与可选 `SessionDriver`
- 可断线恢复的四阶段首次注册状态机
- 可拆分维护的 Job 定义、控制命令、固定 Task 配置和强类型结果

`.proto` 是 wire 契约的唯一事实来源；业务 crate 不应自行维护重复的握手或消息结构。

完整交互顺序、方法调用和持久化时点单独整理在
[`PROTOCOL_FLOW.md`](PROTOCOL_FLOW.md)，阅读或接入协议时建议先看该文档。

## 目录结构

```text
smalux-protocol/
├── proto/smalux/agent/v1/
│   ├── job/                # 调度、完整定义、命令、状态和事件
│   ├── task/               # 固定 Task 配置与强类型 Snapshot
│   └── *.proto             # 传输、错误和会话消息
├── src/noise/
│   ├── client/             # Agent/initiator 的 XXpsk3 与 IK 握手状态机
│   ├── server/             # Server/responder 的 XXpsk3 与 IK 握手状态机
│   └── *.rs                # 共用身份、加密会话、错误和轮换状态
├── src/tonic_transport/    # Tonic Client/Server 和会话方法
├── src/lib.rs              # 公开模块入口
└── build.rs                # Protobuf/Tonic 代码生成
```

生成代码位于 Cargo 的 `OUT_DIR`，不提交到仓库。业务 crate 应引用
`smalux_protocol::agent::v1`，不要依赖生成文件的物理路径。

## 职责边界

本 crate 负责：

- 维护 Agent 与 Server 共用的 Protobuf package 和 gRPC service；
- 执行 Noise XXpsk3/IK、业务加解密、心跳与同步 rekey；
- 返回 Agent/Server 静态换钥状态和 snapshot；
- 驱动 Tonic 双向流的握手及加密消息。

本 crate 不负责：

- 网络监听、HTTP 路由、TLS 证书和 Server 生命周期；
- Token 发放、Agent 授权和注册表；
- 数据库、本地文件、HSM/KMS 或集群密钥同步；
- RPC 重试、业务消息持久化和幂等；
- Agent 调度、采集任务或 Server 业务处理。

## 协议日志

协议代码只产生 tracing 事件，不负责安装全局 subscriber。宿主程序决定日志输出到
控制台、滚动文件还是其他采集系统，并通过通用的 RUST_LOG 环境变量选择级别。

日志级别按诊断粒度划分：

| 级别 | 内容 |
| --- | --- |
| error | Driver、RPC 或会话无法继续，通常需要关闭当前连接。 |
| warn | Token/公钥不匹配、握手超时、无效帧、未匹配 Pong、授权失败等可恢复或安全拒绝。 |
| info | XXpsk3/IK 完成、注册阶段变化、会话建立/关闭、心跳超时、rekey 完成。 |
| debug | 选择密钥候选、策略变更、注册准备、Driver 命令和业务阶段。 |
| trace | 单帧收发、握手 payload 长度、Noise nonce、心跳 nonce 和 RTT 样本。 |

协议日志不会记录 PSK、注册 Token、Noise 私钥或解密后的业务明文。key_id、Agent ID、
帧长度、generation 和计数只用于关联诊断；Agent ID 和 endpoint 仍可能属于部署敏感信息，
生产环境应按实际采集策略配置过滤和脱敏。

常用过滤示例：

    # 只看协议生命周期和失败原因
    $env:RUST_LOG = "smalux_protocol=info"

    # 查看握手、注册、授权和 rekey 的详细步骤
    $env:RUST_LOG = "smalux_protocol=debug"

    # 排查单帧顺序、nonce 或心跳 RTT；只建议短时间使用
    $env:RUST_LOG = "smalux_protocol=trace"

    # 同时保留应用日志，压低底层依赖噪声
    $env:RUST_LOG = "smalux_protocol=debug,tonic=warn,h2=warn,tokio=warn"

调用方可以在业务日志中使用相同的 session_id、Agent ID 或 request ID 建立关联；协议层
不会自行生成并传播业务 request ID。

## Job 与 Task 协议

`.proto` 是 Job 配置和采集结果的唯一公开模型。Agent 不再维护可从 JSON 反序列化的
第二套 `Job`、`TaskConfig` 或 Snapshot；本地持久化也应保存 `JobDefinition` 的 protobuf
字节或由它无损转换出的数据库字段。

### 固定调用流程

```text
Server 或本地存储生成 JobDefinition
    -> Agent JobController::apply(JobCommand)
    -> 校验 job_id、revision、trigger、options 和具体 TaskConfig
    -> TaskFactory 创建固定 ReportingTask
    -> Scheduler 使用 Server UUID 和私有 generation 执行
    -> ReportingTask 返回 TaskResult
    -> TaskReportSink 选择持久化或发送方式
```

`JobDefinition.revision` 是 Server 业务配置版本；Scheduler 的 generation 只用于隔离已经
启动的旧执行，两者不能混用。`ReplaceAllJobs` 只替换远程所有权 Job，不应删除 Agent
本地 Job。相同 `command_id` 必须返回缓存结果，尤其不能重复执行 `RunJobNow`。

### JobController 方法

| 方法 | 输入 | 行为 |
| --- | --- | --- |
| `new(scheduler, sink)` | Scheduler 与结果出口 | 创建不包含连接逻辑的控制器。 |
| `apply(command)` | `JobCommand` | 幂等处理 ReplaceAll、Upsert、Delete 或 RunNow，返回结构化结果。 |
| `clear()` | 无 | 删除全部远程所有权 Job，本地 Job 保持不变。 |

调用方只需要提供 Scheduler 和报告出口，连接与落盘策略可以独立替换：

```rust,ignore
// SchedulerRuntime 拥有后台运行循环；Scheduler 是可克隆的控制句柄。
let runtime = SchedulerRuntime::start(SchedulerConfig::default())?;

// Sink 收到的是完整 TaskReport，可在这里写本地队列、数据库或 gRPC 长流。
let sink: Arc<dyn TaskReportSink> = Arc::new(|report: TaskReport| async move {
    save_or_send(report)
        .await
        .map_err(CallbackError::Transient)
});

// JobController 只管理通过自身安装的远程 Job。
let controller = JobController::new(runtime.scheduler(), sink);

// 网络层解码出 JobCommand 后直接交给控制器；返回值应原样关联到 command_id 上报。
let result = controller.apply(command).await;
send_command_result(result).await?;
```

一次 `apply` 的内部顺序如下：

```text
1. 校验 16 字节 command_id。
2. 命中幂等缓存时直接返回首次结果。
3. 锁定远程目录状态，检查 catalog_revision 和 expected_revision。
4. 把 Proto trigger/options/task 编译为 Scheduler 强类型。
5. 安装、更新、删除或立即触发 Scheduler Job。
6. Scheduler 成功后才更新本地 revision/generation 索引。
7. 缓存 APPLIED 或 REJECTED 结果并返回调用方。
```

`UpsertJob` 和 `DeleteJob` 的单 Job Scheduler 修改是原子的。`ReplaceAllJobs` 会先校验全部
定义，再逐个修改 Scheduler，但当前不是跨多个 Job 的数据库式事务；如果运行中途发生
Scheduler 故障，连接层应读取返回错误并用新的完整目录重新对账，不能假定整批自动回滚。

连接断开不会自动删除 Scheduler 中已经安装的 Job，因此短时网络波动期间仍会继续采集。
连接层可以把 `TaskReportSink` 实现为本地缓冲，再在会话恢复后上报；缓冲上限、过期策略
和重连退避不属于协议 crate 或 Scheduler 的职责。

## 扩展固定 Task 与 Plus 模块

新增采集能力或 `plus` 功能时，继续使用“固定 Proto 类型映射到固定 Rust 实现”的方式。
Server 只能选择 Agent 已编译并声明支持的能力，不能通过 Proto 指定任意 Rust 类型、命令、
动态库或可执行代码。

推荐扩展链路：

```text
Server JobDefinition
    -> TaskDefinition.oneof
    -> Agent TaskFactory
        -> 内置 Collect Task
        -> PlusTaskFactory
            -> RusticBackupTask
            -> 其他固定 Plus Task
    -> ReportingTask::run
    -> TaskResult.oneof
    -> TaskReportSink
```

### Proto 扩展步骤

以新增 Rustic 备份能力为例：

1. 在 `proto/smalux/agent/v1/task/` 下按领域维护独立 `.proto` 文件；
2. 定义该 Task 的完整配置消息和强类型结果消息；
3. 在 `TaskDefinition.oneof task` 中分配新的配置字段；
4. 在 `TaskResult.oneof result` 中分配对应的结果字段；
5. 在 Agent 工厂中把该 Proto 分支注册到唯一的本地 Task 实现；
6. 为配置解析、执行结果、协议 round-trip 和不支持能力补充测试。

已发布的 Proto 字段编号不能改变或分配给其他含义。功能删除后应使用 `reserved` 保留原编号
和字段名，避免旧消息被新版本错误解释。配置消息应只包含执行业务所需的稳定参数，不复制
Scheduler 已经提供的触发、超时、并发、队列和重试字段。

概念上的配置与结果如下：

```proto
message RusticBackupTaskConfig {
  string repository_id = 1;
  string source_id = 2;
}

message RusticBackupResult {
  bool succeeded = 1;
  string snapshot_id = 2;
}
```

`repository_id` 和 `source_id` 是 Agent 本地配置或安全存储的引用，不是仓库密码、访问 Token、
完整 shell 命令或任意路径。Agent 根据引用读取凭据并执行本地授权检查，敏感信息不得进入
Job Proto、TaskReport 或普通日志。

### 工厂职责

顶层 `TaskFactory` 只做稳定的类型分发，不直接实现 Plus 业务：

```text
Task::Cpu(config)          -> 内置 CpuTask
Task::Process(config)      -> 内置 ProcessTask
Task::RusticBackup(config) -> PlusTaskFactory -> RusticBackupTask
```

具体 Plus 工厂负责：

- 校验 Plus 配置和本地资源引用；
- 注入仓库、凭据存储、文件系统策略等运行依赖；
- 创建实现 `ReportingTask` 的固定 Task；
- 把领域结果包装为对应的 `TaskResult` 分支。

Scheduler、`JobController` 和 `TaskReportSink` 不应理解 Rustic 或其他 Plus 的业务细节。这样
新增 Plus 能力时，变化只集中在 Proto、Plus 实现和工厂注册位置。

### 能力协商

不同 Agent 版本或安装类型可能不包含相同 Plus 模块。Agent 注册或建立会话后应上报能力，
至少包括协议版本、支持的 Task 类型、Task 配置版本和启用的 Plus 功能。Server 只向声明
支持该能力的 Agent 下发 Job。

如果 Agent 收到未编译、未启用或版本不支持的 Task，必须返回稳定的结构化错误，例如
`UNSUPPORTED_TASK`，不能静默忽略、猜测配置或退化为其他 Task。错误消息可用于诊断，
Server 的恢复逻辑应依据错误码和能力列表，而不是匹配错误文本。

### 只读采集与有副作用任务

CPU、内存、网络等采集通常是只读操作；备份、更新、脚本和修复操作会产生外部副作用。
有副作用的 Plus Task 在接入统一 Job 模型前，必须额外确定：

- 是否允许 Server 远程创建和 `RunNow`；
- Agent 本地授权范围以及允许访问的目录、仓库和凭据；
- 重复执行是否安全，使用什么业务幂等键；
- Agent 重启或断联后是否继续执行，以及最长离线执行时间；
- 最大运行时间、并发限制和取消能否真正终止底层操作；
- 运行日志、安全审计、结果保留和失败恢复方式。

`JobController` 当前的 `command_id` 缓存是进程内有限窗口，适合防止网络重发导致的重复
`RunNow`，但不能替代有副作用任务的持久化幂等。备份等任务需要把业务运行 ID、执行状态
和最终结果保存在数据库或本地文件中，Agent 重启后仍应能够识别已经开始或完成的操作。

### 推荐模块边界

```text
smalux-protocol
    稳定的配置、命令、状态、结果和能力契约

smalux-agent/tasks/collect
    内置只读采集 Task

smalux-plus-rustic
    Rustic 领域配置校验、执行逻辑和结果构造

smalux-agent/task_factory
    将协议 Task 分支映射到内置或 Plus 实现

smalux-agent/job_control
    处理命令幂等、业务版本、所有权和 Scheduler 装配
```

协议 crate 不访问数据库、文件或 KMS；Plus crate 不处理 gRPC 会话；`JobController` 不实现
具体任务。持久化、连接和业务实现通过明确的方法与 trait 组合，避免后期扩展反向耦合到
Scheduler 或传输层。

## 使用层级

一般业务只使用 Tonic 适配层：

```text
AgentProtocolClient
    -> XXpsk3 首次注册或 IK 后续连接
    -> TonicNoiseSession
    -> 加密收发、心跳、rekey、静态密钥轮换消息

ServerSessionAcceptor
    -> 接受 Tonic 双向流并完成 XXpsk3/IK
    -> ServerPendingSession
    -> 业务授权
    -> TonicNoiseSession
```

只有在不使用 Tonic、需要把 Noise 放进其他传输协议时，才直接使用
`ClientXxHandshake`、`ServerXxHandshake` 和 `SecureSession`。两层不能同时驱动同一个会话，
因为 Noise nonce 必须严格按帧顺序递增。

## Wire 方法与消息

`smalux.agent.v1.AgentTransport` 提供两个 gRPC 方法：

| RPC | 类型 | 用途 |
| --- | --- | --- |
| `HealthCheck` | unary | 检查 gRPC 服务是否可达，不建立 Noise 会话。 |
| `OpenSession` | 双向 stream | 在同一条流中完成 Noise 握手，并持续收发加密业务消息。 |

`OpenSession` 外层只允许三种 `ProtocolFrame`：

| 帧 | 使用阶段 | 是否包含业务明文 |
| --- | --- | --- |
| `NoiseHandshake` | XXpsk3 或 IK 握手 | 否，但可观察握手类型和 Server key ID。 |
| `ciphertext` | 握手成功后 | 否，内容是 Noise AEAD 密文。 |
| `ProtocolError` | 无法建立加密会话时 | 否，只能携带通用、安全的错误描述。 |

握手成功后，`ciphertext` 解密为 `SecureMessage`。当前内层消息包括：

| 消息 | 用途 |
| --- | --- |
| `RegistrationMessage` | XXpsk3 后执行 request、prepared、commit、committed 四阶段注册。 |
| `Messages` | 上报、命令、应答等业务数据。 |
| `SecureError` | 已加密的 Token、授权或业务错误。 |
| `SessionControl` | Ping/Pong 和同步 rekey。 |
| `KeyRotationMessage` | Agent/Server 长期静态密钥轮换。 |

## 身份与标识方法

### `NoiseIdentity`

`NoiseIdentity` 是一对长期 X25519 静态密钥。它故意不实现 `Debug`，避免日志意外输出私钥。

| 方法 | 作用 | 调用方接下来做什么 |
| --- | --- | --- |
| `generate()` | 生成新的 32 字节私钥和公钥。 | 立即加密持久化，不能只保存在内存。 |
| `from_parts(private, public)` | 从数据库或本地文件恢复身份，并验证长度。 | 用恢复结果创建 Client、Server keyring。 |
| `public_key()` | 返回可复制的 `NoisePublicKey`。 | 用于注册表、固定 Server 身份或发送轮换消息。 |
| `key_id()` | 返回公钥的 BLAKE2s-256 标识。 | IK 握手和 Server 多密钥选择使用。 |
| `export_private_key()` | 显式导出 `SecretKeyBytes`。 | 只交给受保护存储，不发送到网络。 |

`SecretKeyBytes::as_bytes()` 返回私钥的 32 字节视图；`SecretKeyBytes` 同样不实现 `Debug`。

### `NoisePublicKey`

| 方法 | 作用 |
| --- | --- |
| `from_bytes(bytes)` | 从持久化或 Protobuf 字节恢复 32 字节公钥。 |
| `as_bytes()` | 获取固定长度字节，用于存库或构造消息。 |
| `key_id()` | 计算稳定的 `KeyId`。 |

### `KeyId` 与 `RotationId`

| 方法 | 作用 |
| --- | --- |
| `KeyId::from_bytes()` / `as_bytes()` | 解析或导出 32 字节公钥标识。 |
| `RotationId::generate()` | 生成新的 16 字节换钥事务 ID。 |
| `RotationId::from_bytes()` / `as_bytes()` | 从消息恢复或导出换钥事务 ID。 |

不要把 `KeyId` 当成秘密；它只是公钥指纹。`RotationId` 也不是认证凭据，换钥消息的可信度
来自已经认证和加密的 Noise 会话。

## Agent Client 方法

### `AgentProtocolClient`

| 方法 | 作用 | 重要行为 |
| --- | --- | --- |
| `new(endpoint)` | 创建 Client 配置。 | `http://` 使用 h2c；`https://` 使用系统根证书验证 TLS。 |
| `set_handshake_timeout(duration)` | 修改连接和每一步握手超时。 | 默认 5 秒，只限制建连/握手，不限制长期业务流。 |
| `set_grpc_prefix(prefix)` | 设置 Axum/Nginx 下的统一 gRPC 前缀。 | 示例使用 `/api/v1/grpc`。 |
| `prepare_registration(identity, psk, token, agent_name)` | 执行 XXpsk3 并等待 Server 保存 pending 注册。 | `token` 使用 `token_id.psk`；返回后应先持久化结果，再调用 `commit()`。 |
| `register_agent(identity, psk, token, agent_name)` | 依次执行 prepare 和 commit 的便捷方法。 | 自动从 `token_id.psk` 提取 Token ID；适合测试。生产持久化应使用分步方法。 |
| `connect(identity, server_key)` | 使用固定 Server 公钥执行 IK。 | 成功后返回可持续使用的 `TonicNoiseSession`。 |
| `connect_with_candidates(identity, candidates)` | 依次尝试多把 Server 公钥。 | Server 换钥期间通常传 `PinnedServerKeys::connection_candidates()`。 |

`prepare_registration` 返回 `AgentPendingRegistration`，其中的 `agent_identity`、
`server_public_key`、`agent_id` 和 `registration_id` 都必须先持久化。随后调用 `commit()`，
成功后得到 `AgentRegistration`：

| 字段 | 含义 | 是否需要持久化 |
| --- | --- | --- |
| `agent_id` | Server 确认的业务身份。 | 是。 |
| `agent_identity` | 本次注册使用的 Agent 长期身份。 | 是，尤其是私钥。 |
| `server_public_key` | XXpsk3 认证后学到的 Server 公钥。 | 是，后续 IK 必需。 |
| `registration_id` | Server 分配的 16 字节幂等注册事务 ID。 | 是，用于恢复和审计。 |
| `session` | 已完成注册授权的 XXpsk3 加密会话，可立即承载业务。 | 否，只在当前进程和连接内有效。 |

### Session 生命周期与持久化

`TonicNoiseSession` 不是可以写入数据库的业务对象。它持有当前 gRPC stream、Noise cipher
状态、nonce 和未完成的 rekey/心跳状态，只能在当前进程内顺序使用；`SessionDriver` 也只是
围绕该连接运行的后台任务。进程退出、连接断开或切换网络后，必须创建新的 session，不能从
旧对象恢复密文状态。

首次注册应保存的是长期材料和注册事务，而不是 session：

```text
prepare_registration()
    -> Server 返回 pending
    -> 原子保存 agent_identity、server_public_key、agent_id、registration_id
    -> commit()
    -> 原子保存 committed 状态
    -> 直接使用返回的 registration.session
```

对应的最小调用骨架如下：

```rust,ignore
let pending = client
    .prepare_registration(identity, &psk, token, agent_name)
    .await?;

// 这里保存四项长期材料；保存失败时不要发送 commit。
store.save_pending_registration(
    &pending.agent_identity,
    &pending.server_public_key,
    &pending.agent_id,
    pending.registration_id,
)?;

// Server 激活 Agent 并消费 Token；成功后当前 XX session 已经可以传业务。
let registered = pending.commit().await?;
store.mark_registration_committed(registered.registration_id)?;
run_business_loop(registered.session).await?;
```

如果在 `prepare` 后、`commit` 前崩溃，重启时加载保存的身份和 pending 事务，再重新执行
注册流程即可；Server 会按 Token、Agent 公钥和事务 ID 做幂等处理。如果 `commit` 已发送但
确认丢失，保留同一组状态并重试，不要生成新的 Agent identity。只有后续建立连接时，才从
持久化的 `agent_identity` 和 `server_public_key` 调用 `connect()` 执行 IK。

### 首次注册调用流程

```rust,ignore
// 引入长期 Noise 身份和封装完整 Tonic/Noise 流程的高层 Client。
use smalux_protocol::{
    noise::NoiseIdentity,
    tonic_transport::AgentProtocolClient,
};

# async fn register_agent() -> Result<(), Box<dyn std::error::Error>> {
// 首次运行生成 Agent 长期静态身份；生产代码应立即加密持久化私钥。
let identity = NoiseIdentity::generate()?;
// XXpsk3 要求恰好 32 字节 PSK；示例常由一次性 Token 解码得到。
let psk = [7_u8; 32];

// endpoint 只写 scheme + authority，service path 由生成的 Tonic Client 维护。
let mut client = AgentProtocolClient::new("https://agent.example.com");
// Server 使用 Axum nest 或反向代理前缀时，Client 必须配置同一前缀。
client.set_grpc_prefix("/api/v1/grpc");

// prepare_registration 完成 XXpsk3、发送 RegistrationRequest，并等待 RegistrationPrepared。
let pending = client
    .prepare_registration(
        // 方法取得身份所有权，并在成功结果中通过 agent_identity 交还。
        identity,
        // PSK 只参与首次握手，不用于后续 IK。
        &psk,
        // Token 使用公开 ID 加秘密 PSK 的格式；完整值只进入加密注册请求。
        "token-001.0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
        // Agent 名称只是允许重复的展示字段，不参与身份判断。
        "agent-001".to_owned(),
    )
    // 成功只表示 Server 已保存 pending，还不能使用 IK。
    .await?;

// 这里由调用方开启数据库事务并保存 pending 中的四项长期状态：
// pending.agent_id
// pending.registration_id
// pending.agent_identity（含私钥）
// pending.server_public_key
save_pending_registration(&pending)?;

// 保存成功后再发送 RegistrationCommit，并等待 Server 的 RegistrationCommitted。
let mut registration = pending.commit().await?;
mark_registration_committed(registration.registration_id)?;

// 持久化成功后直接复用 registration.session 发送业务消息；不需要立即重连 IK。
// registration.session.send(message).await?;
# Ok(())
# }
```

完整顺序：

```text
1. Agent 从安全渠道取得一次性 Token/PSK。
2. Agent 生成或恢复自己的 NoiseIdentity。
3. `prepare_registration` 执行 XXpsk3 三消息握手。
4. 双方确认持有相同 PSK，Client 得到已认证的 Server 公钥。
5. Client 在 Noise 密文内发送 `RegistrationRequest`。
6. Server 保存 pending 事务并返回 `RegistrationPrepared`。
7. Client 持久化身份、Server 公钥、`agent_id` 和 `registration_id`。
8. Client 发送 `RegistrationCommit`；Server 激活 Agent、消费 Token 并返回 `RegistrationCommitted`。
9. Client 标记本地注册完成，当前 XX Session 直接进入业务循环。
10. 只有当前流断开、进程重启或网络切换后，下一条连接才使用 IK。
```

如果 Server 返回加密 `SecureError`，`register_agent` 会返回
`TransportError::RemoteSecure(code, message)`。此时不得保存 Server 公钥或把 Agent 标记为注册成功。

### 后续 IK 连接

```rust,no_run
// Messages 是加密业务 envelope；NoiseIdentity 和 Server 公钥来自持久化状态。
use smalux_protocol::{
    agent::v1::{Messages, MessagesRequest, SecureMessage, messages, secure_message},
    noise::{NoiseIdentity, NoisePublicKey},
    tonic_transport::AgentProtocolClient,
};

# async fn connect(
#     identity: NoiseIdentity,
#     server_key: NoisePublicKey,
# ) -> Result<(), Box<dyn std::error::Error>> {
// 普通连接不再读取注册 Token，只依赖双方已经保存的静态身份。
let client = AgentProtocolClient::new("https://agent.example.com");
// connect 发送 IK message 1、验证 message 2，并返回长期双向加密流。
let mut session = client.connect(&identity, server_key).await?;

// 所有业务消息都必须交给 session.send，由它维护严格递增的 Noise nonce。
session
    .send(SecureMessage {
        // SecureMessage 的 oneof 指明这是一条普通业务 Messages。
        body: Some(secure_message::Body::Messages(Messages {
            // Messages oneof 再区分 Request 与 Response。
            body: Some(messages::Body::Request(MessagesRequest {
                // sequence 用于业务确认和幂等，不是 Noise 密码学 nonce。
                sequence: 1,
                // 示例省略 payload；实际可放 bytes、string 或 typed EchoRequest。
                payload: None,
            })),
        })),
    })
    // channel 发送失败后不能复用同一密文帧，应关闭会话并重新 IK。
    .await?;

// receive 内部会自动处理 Ping/Pong 和 responder rekey，这里只得到业务消息。
let response = session.receive().await?;
// None 表示对端正常结束了 gRPC 流，而不是一条空业务响应。
if response.is_none() {
    // Server 正常关闭了流；调用方根据重连策略重新执行 IK。
}
# Ok(())
# }
```

## Server 方法

### `ServerKeyRing`

Server 启动时先从存储恢复 `NoiseIdentity` 或 `ServerKeyRingSnapshot`：

```rust,no_run
// NoiseIdentity 负责密钥强类型校验，ServerKeyRing 管理轮换期多把私钥。
use smalux_protocol::noise::{NoiseIdentity, ServerKeyRing};

# fn load(private: &[u8], public: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
// 从数据库、文件或 KMS 返回的字节恢复同一对长期静态密钥。
let identity = NoiseIdentity::from_parts(private, public)?;
// 初次构造只有 current；换钥时再通过 prepare_rotation 增加 next。
let keyring = ServerKeyRing::new(identity);
# let _ = keyring;
# Ok(())
# }
```

### `ServerSessionAcceptor`

| 方法 | 作用 |
| --- | --- |
| `default()` | 创建默认 5 秒握手超时的接收器。 |
| `new(handshake_timeout)` | 使用自定义握手超时。 |
| `accept_session(inbound, sender, keyring, registration_psk)` | 读取首帧，选择 XXpsk3/IK 和对应 Server 私钥，完成握手。 |
| `accept_incoming(inbound, sender, keyring, registration_psk)` | 完成同一握手并返回强类型注册或认证阶段。 |

`accept_session` 返回 `ServerPendingSession`，此时 Noise 已认证，但业务授权还没有自动完成：

| 方法 | 作用 | 使用时机 |
| --- | --- | --- |
| `handshake_mode()` | 返回 `RegistrationXxPsk3` 或 `AuthenticatedIk`。 | 决定执行首次注册还是已注册授权。 |
| `peer_public_key()` | 返回握手认证得到的 Agent 静态公钥。 | 注册时写入；IK 时查询授权表。 |
| `authorize()` | 消费 pending 状态并返回 `TonicNoiseSession`。 | 业务层确认允许继续处理时。 |
| `reject(secure_error)` | 在已建立的 Noise 会话中发送加密错误。 | Token、吊销、租户或业务授权失败时。 |

推荐使用强类型 Server 处理骨架；原始 `accept_session` 和 `ServerPendingSession` 仍保留：

```rust,ignore
let incoming = ServerSessionAcceptor::default()
    .accept_incoming(inbound, sender, &keyring, &registration_psk)
    .await?;

let (agent_id, session) = match incoming {
    IncomingSession::Registration(mut registration) => {
        let agent_key = registration.peer_public_key();
        let request = registration.receive_request().await?;
        // 数据库方法必须对 token + agent_key 的重复请求返回同一事务。
        let prepared = registry.prepare(request, agent_key)?;
        registration.prepare(prepared.id, prepared.agent_id.clone()).await?;
        registration.wait_for_commit(prepared.id, commit_timeout).await?;
        // 先提交数据库并消费 Token，再发送最终成功响应。
        registry.commit(prepared.id, agent_key)?;
        let session = registration.complete(prepared.id).await?;
        (prepared.agent_id, session)
    }
    IncomingSession::Authentication(authentication) => {
        let Some(agent_id) = registry.authorize(authentication.peer_public_key()) else {
            authentication.reject(not_authorized_error).await?;
            return Ok(());
        };
        (agent_id, authentication.authorize())
    }
};

// 注册成功的 XX 和已授权 IK 在这里汇合，共用同一业务循环。
messages_loop(&mut session, &agent_id).await?;
```

握手失败时还没有安全的 Noise 会话，Server 可以调用
`TransportError::protocol_error()` 生成不包含 Token、密钥和业务内容的外层 `ProtocolError`。

## 加密会话方法

### `TonicNoiseSession`

该类型必须由一个任务顺序持有；不要把它拆给多个并发 reader/writer。业务需要并发时可以直接使用
下文的 `SessionDriver`，也可以自行用 channel 汇聚后调用这些小方法。

| 方法 | 哪端调用 | 作用 |
| --- | --- | --- |
| `set_heartbeat_policy(policy)` | 两端 | 修改 Ping 间隔和失联超时。 |
| `set_rekey_policy(policy)` | 两端 | 修改自动 rekey 的时间、帧数和开关。 |
| `heartbeat_policy()` | 两端 | 读取当前心跳策略。 |
| `heartbeat_stats()` | 两端 | 读取发送数、匹配 Pong 数、丢失数和 RTT 极值。 |
| `last_heartbeat()` | 两端 | 读取最近一次成功心跳的 nonce、RTT 和诊断时间。 |
| `should_ping()` | 两端 | 判断距离上次发送是否超过心跳间隔。 |
| `heartbeat_expired()` | 两端 | 判断距离上次接收是否超过失联上限。 |
| `should_rekey()` | 两端 | 判断自动 rekey 的时间或帧数条件是否满足。 |
| `maintenance_status()` | 两端 | 无副作用查询心跳超时、Ping 和 rekey 是否到期。 |
| `perform_maintenance()` | 两端 | 执行一次到期的 Ping 或 initiator rekey。 |
| `send(message)` | 两端 | Prost 编码、Noise 加密并发送一条 `SecureMessage`。 |
| `send_task_report(report)` | Agent | 发送强类型采集结果。 |
| `send_job_command(command)` | Server | 发送强类型 Job 命令。 |
| `send_job_command_result(result)` | Agent | 发送强类型 Job 处理结果。 |
| `receive()` | 两端 | 等待下一条业务消息；内部处理 Ping/Pong 和 responder rekey。 |
| `receive_event()` | 两端 | 返回 `SessionEvent`，无需手动匹配 `SecureMessage.oneof`。 |
| `ping(nonce)` | 两端 | 手动发送加密 Ping；通常无需直接调用。 |
| `request_rekey()` | Agent/initiator | 发起同步 rekey，等待 Ack 后切换双向 cipher state。 |
| `require_rekey()` | Server/responder | 通知 Agent 应发起 rekey，本身不立即切换密钥。 |
| `generation()` | 两端 | 返回当前会话 rekey 代数，初始为 0。 |
| `encrypted_frames()` | 两端 | 返回本代已处理的加密帧数，rekey 后归零。 |
| `request_agent_key_rotation(prepared)` | Agent | 发送 Agent 新静态公钥申请。 |
| `accept_agent_key_rotation(rotation_id)` | Server | 接受 Agent 静态公钥申请。 |
| `announce_server_key(prepared)` | Server | 宣布下一把 Server 静态公钥。 |
| `acknowledge_server_key(rotation_id, key_id)` | Agent | 确认已保存 Server 新公钥。 |

`HeartbeatPolicy::default()` 是 30 秒发送间隔、90 秒无入站消息超时。每个 Ping 会记录 nonce 和
发送时间，Pong 会回显发送时间并附带 responder 的接收/发送时间；`heartbeat_stats()` 使用本地
单调时钟计算 RTT，不依赖两台机器的系统时钟同步。
`RekeyPolicy::default()` 是 1 小时或 `2^20` 个加密帧后自动 rekey，且 `automatic = true`。

`receive()` 的返回值含义：

| 返回值 | 含义 |
| --- | --- |
| `Ok(Some(message))` | 收到一条非会话控制的加密消息。 |
| `Ok(None)` | 对端正常关闭 gRPC 流。 |
| `Err(HeartbeatTimeout)` | 超过心跳失联上限。 |
| 其他 `Err` | gRPC、Noise、帧格式或远端协议错误。 |

自动模式下 `RekeyRequired` 会在 `receive()` 或 Driver 内部触发 rekey，不再作为普通业务错误返回。
rekey 等待 Ack 时提前到达的业务消息会被缓存，并在换钥完成后按原顺序交付。

### `SessionDriver`

`SessionDriver::new(session, config)` 返回尚未启动的 Driver、可克隆 `SessionHandle` 和单消费者
`SessionEventReceiver`；调用方可自行 spawn `driver.run()`。`SessionDriver::spawn` 是对应的便捷入口，
返回 `RunningSession { handle, events, task }`。

`SessionHandle` 提供 `send`、`send_messages`、`send_task_report`、`send_job_command`、
`send_job_command_result`、`ping`、`request_rekey`、`require_rekey`、`heartbeat_stats`、四个静态密钥
轮换发送方法和幂等 `shutdown`。所有请求经过有界队列，真正的 Prost 编码、Noise 加密、rekey 和 nonce 推进只发生
在 Driver task 中。事件队列同样有界，消费过慢会形成反压而不是静默丢包。

手动 rekey 流程：

```text
Agent request_rekey()
  -> 发送加密 RekeyRequest(generation + 1)
Server receive()
  -> 自动切换 incoming
  -> 用旧 outgoing 发送 RekeyAck
  -> 切换 outgoing，完成新 generation
Agent request_rekey()
  -> 收到 Ack 后切换 incoming/outgoing
  -> 返回新的 generation
双方继续使用同一条 gRPC 流，不重新建立 TCP/TLS 连接
```

## 底层 Noise 方法

Tonic 适配层已经调用这些方法。自定义 QUIC、WebSocket 或其他传输时才直接使用。

### XXpsk3

| 方法 | 输入 | 返回 |
| --- | --- | --- |
| `ClientXxHandshake::start(identity, psk)` | Agent 身份和 32 字节 PSK | 等待状态与 message 1。 |
| `ServerXxHandshake::receive_message1(identity, psk, frame)` | Server 身份、PSK、message 1 | 等待状态与 message 2。 |
| `ClientXxAwaitMessage2::receive_message2(frame)` | message 2 | `EstablishedNoise` 与 message 3。 |
| `ServerXxAwaitMessage3::receive_message3(frame)` | message 3 | Server 侧 `EstablishedNoise`。 |

### IK

| 方法 | 输入 | 返回 |
| --- | --- | --- |
| `ClientIkHandshake::start(identity, server_key)` | Agent 身份和已固定 Server 公钥 | 等待状态与 message 1。 |
| `ServerIkHandshake::receive_message1(identity, frame)` | 与 key ID 对应的 Server 身份、message 1 | Server `EstablishedNoise` 与 message 2。 |
| `ClientIkAwaitMessage2::receive_message2(frame)` | message 2 | Client `EstablishedNoise`。 |

`EstablishedNoise` 包含：

| 字段 | 含义 |
| --- | --- |
| `session` | 底层 `SecureSession`。 |
| `mode` | `RegistrationXxPsk3` 或 `AuthenticatedIk`。 |
| `remote_static_key` | 握手认证得到的对端静态公钥。 |
| `responder_key_id` | 本次实际使用的 Server 公钥标识。 |

### `SecureSession`

| 方法 | 作用 |
| --- | --- |
| `encrypt(message)` | 把 `SecureMessage` 编码并加密成 `ProtocolFrame::ciphertext`。 |
| `decrypt(frame)` | 验证并解密 ciphertext，再解码为 `SecureMessage`。 |
| `generation()` | 当前 rekey 代数。 |
| `encrypted_frames()` | 本代双向累计处理帧数。 |
| `sending_nonce()` / `receiving_nonce()` | 用于诊断帧顺序；不能手动修改。 |

底层 `SecureSession` 不公开 rekey 原语；同步 rekey 必须通过 `TonicNoiseSession`，避免一端提前切换
造成永久失步。

## 静态密钥轮换

会话 rekey 只更新当前连接的对称密钥；下面的方法更新跨重启使用的长期 Noise 静态密钥。
所有状态对象只修改内存，不会自动写数据库或文件。

统一持久化规则：

```text
调用 prepare/stage/promote/retire/cancel
-> 立即取得 snapshot()
-> 调用方原子持久化 snapshot
-> 持久化成功后才发送网络确认或进入下一阶段
```

### Agent 私钥：`AgentKeySet`

| 方法 | 作用 |
| --- | --- |
| `new(current)` | 用当前 Agent 身份创建状态。 |
| `from_snapshot(snapshot)` | 从存储恢复并验证 pending 与 rotation ID 是否一致。 |
| `prepare_rotation()` | 生成 pending 身份和 rotation ID，返回可发送的 `AgentRotationPrepared`。 |
| `connection_candidates()` | 返回 pending、current；优先尝试新身份，失败可回退旧身份。 |
| `promote_pending(rotation_id)` | 把 pending 提升为 current。 |
| `cancel_rotation()` | 丢弃尚未完成的 pending。 |
| `snapshot()` | 返回包含 current、pending、rotation ID 的可持久化状态。 |

`prepare_rotation()` 返回的 `AgentRotationPrepared` 包含 `rotation_id`、新生成的
`new_identity`，以及可以直接交给 `request_agent_key_rotation()` 的 Protobuf `request`。
`new_identity` 含私钥，必须和 `AgentKeySetSnapshot` 一样受保护。

### Server 保存的 Agent 公钥：`AgentPublicKeySet`

| 方法 | 作用 |
| --- | --- |
| `new(current)` / `from_snapshot(snapshot)` | 创建或恢复 Agent 授权公钥状态。 |
| `stage(request)` | 校验 request 的公钥、key ID 和 rotation ID，加入 pending。 |
| `authorize(key)` | current 或 pending 匹配时返回 `true`。 |
| `promote_pending(rotation_id)` | 新 Agent 身份成功 IK 后提升 pending。 |
| `cancel_rotation()` | 拒绝或回滚未完成轮换。 |
| `snapshot()` | 返回可持久化状态。 |

Agent 换钥推荐流程：

```text
1. Agent: prepare_rotation -> snapshot -> 保存。
2. Agent: request_agent_key_rotation(prepared)。
3. Server: receive KeyRotation::AgentRequest。
4. Server: AgentPublicKeySet::stage -> snapshot -> 保存。
5. Server: accept_agent_key_rotation(rotation_id)。
6. Agent: 收到 AgentAccepted -> promote_pending -> snapshot -> 保存。
7. Agent: 用 connection_candidates() 发起新的 IK，pending 优先。
8. Server: authorize(new_key) 成功后 promote_pending -> snapshot -> 保存。
```

### Server 私钥：`ServerKeyRing`

| 方法 | 作用 |
| --- | --- |
| `new(current)` / `from_snapshot(snapshot)` | 创建或恢复 Server 私钥环。 |
| `prepare_rotation()` | 生成 next 身份和 announcement。previous 未退休时禁止再次轮换。 |
| `active_keys()` | 返回 current、next、previous，供握手接收器选择。 |
| `find_active(key_id)` | 按 Client 首帧携带的 key ID 查找 Server 私钥。 |
| `promote_next(rotation_id)` | next 变 current，旧 current 进入 previous。 |
| `retire_previous()` | 确认迁移完成后删除 previous。 |
| `cancel_rotation()` | 在 promote 前丢弃 next。 |
| `snapshot()` | 返回 current、next、previous 和 rotation ID。 |

`prepare_rotation()` 返回的 `ServerRotationPrepared` 包含 `rotation_id`、带私钥的
`next_identity`，以及可以直接交给 `announce_server_key()` 的 Protobuf `announcement`。

### Agent 固定的 Server 公钥：`PinnedServerKeys`

| 方法 | 作用 |
| --- | --- |
| `new(current)` / `from_snapshot(snapshot)` | 创建或恢复固定 Server 公钥状态。 |
| `stage(announcement)` | 校验并保存 pending Server 公钥。 |
| `connection_candidates()` | 返回 pending、current、previous，按该顺序尝试 IK。 |
| `promote_pending(rotation_id)` | 新 Server 公钥成功 IK 后提升为 current，并保留 previous。 |
| `retire_previous()` | 迁移结束后删除旧 Server 公钥。 |
| `cancel_pending()` | 在提升前取消未完成轮换。 |
| `snapshot()` | 返回可持久化状态。 |

Server 换钥推荐流程：

```text
1. Server: prepare_rotation -> snapshot -> 保存，current 与 next 同时可接受 IK。
2. Server: announce_server_key(prepared)。
3. Agent: receive ServerAnnouncement。
4. Agent: PinnedServerKeys::stage -> snapshot -> 保存。
5. Agent: acknowledge_server_key(rotation_id, new_key_id)。
6. Server: 收到确认后 promote_next -> snapshot -> 保存，旧 key 进入 previous。
7. Agent: connect_with_candidates，优先使用 pending 新公钥。
8. 新公钥 IK 成功后 Agent promote_pending -> snapshot -> 保存。
9. 观察期结束后两端 retire_previous -> snapshot -> 保存。
```

## 持久化边界

协议层不自行访问数据库、本地文件或 KMS。调用方至少需要保存：

| 所在端 | 状态 | 建议事务边界 |
| --- | --- | --- |
| Agent | `AgentKeySetSnapshot` | 每次 prepare/promote/cancel 后立即保存。 |
| Agent | `PinnedServerKeysSnapshot` | 每次 stage/promote/retire/cancel 后立即保存。 |
| Server | `ServerKeyRingSnapshot` | 每次 prepare/promote/retire/cancel 后立即保存。 |
| Server | 每个 Agent 的 `AgentPublicKeySetSnapshot` | 每次 stage/promote/cancel 后立即保存。 |
| Server | Token 状态和 Agent 业务身份 | 注册确认发送前原子提交。 |

私钥 snapshot 中包含 `NoiseIdentity`，写数据库前仍需调用 `export_private_key()` 取得字节。
生产环境应使用 envelope encryption、系统密钥库或 KMS 保护私钥，并确保 snapshot 与业务授权记录
在同一事务或可恢复的状态机中提交。

## 错误处理

`NoiseError` 表示不依赖具体网络传输的协议错误：

| 分类 | 常见原因 | 建议处理 |
| --- | --- | --- |
| `InvalidKeyLength` / `InvalidPskLength` | 持久化数据损坏或配置错误。 | fail-fast，不要重试握手。 |
| `InvalidHandshakeType` / `InvalidFrame` | 对端帧顺序或类型错误。 | 关闭当前会话并记录安全审计。 |
| `AuthenticationFailed` / `MissingRemoteKey` | PSK、公钥不匹配或握手被修改。 | 不泄露具体认证细节，不自动降级。 |
| `UnknownKeyId` | Agent 固定的 Server key 已不在 keyring。 | 刷新受信任配置或按轮换恢复流程处理。 |
| `RotationAlreadyInProgress` | 上一次轮换尚未结束。 | 从 snapshot 恢复原事务，不要覆盖 pending。 |
| `NoPendingRotation` / `RotationIdMismatch` | 状态机顺序或数据库版本错误。 | 拒绝操作并重新读取持久化状态。 |
| `Crypto` / `Encode` / `Random` | 加密库、密文或系统随机源失败。 | 终止当前操作，保留原 snapshot。 |

`TransportError` 在 `NoiseError` 外增加网络和会话错误：

| 分类 | 含义 | 是否适合重连 |
| --- | --- | --- |
| `Status` / `Transport` / `Closed` | gRPC 状态、网络失败或流关闭。 | 通常可以退避后重新 IK。 |
| `InvalidUri` | Endpoint 配置错误。 | 不应重试，先修正配置。 |
| `Timeout` | 建连或握手阶段超时。 | 可以有限次退避重试。 |
| `HeartbeatTimeout` | 长流超过失联上限。 | 关闭旧流并重新 IK。 |
| `RekeyRequired` | Server 要求 Agent 发起同步 rekey。 | 在当前流调用 `request_rekey()`。 |
| `UnknownKeyId` | Server 不接受 Client 指定的 key ID。 | 尝试轮换候选公钥或停止连接。 |
| `Protocol` / `RemoteProtocol` | 本地或远端发现外层协议错误。 | 关闭会话，不按普通网络抖动无限重试。 |
| `RemoteSecure(code, message)` | 已建立 Noise 后收到的加密业务错误。 | 按 `SecureErrorCode` 处理，例如重新注册或停止授权。 |

Server 只有在握手尚未完成、不能发送 `SecureError` 时才调用 `protocol_error()`。握手成功后应通过
`ServerPendingSession::reject()` 或 `TonicNoiseSession::send()` 返回加密错误。

## CDN 与反向代理

Noise 位于 gRPC 消息内部，因此外层 TLS 可以由 Rust、Nginx 或 Cloudflare 终止。代理只能
看到 gRPC 元数据、握手帧和 Noise 密文，不能读取 Token、业务消息或换钥控制消息。

- Cloudflare 标准代理需要 443、TLS、HTTP/2、ALPN h2，并启用 gRPC；
- Nginx 对 gRPC 路径使用 `grpc_pass`，到本地 Rust 可使用 h2c；
- 长流通过 30 秒加密 Ping/Pong 保活，90 秒无入站消息视为失联；
- 连接被代理关闭后，Agent 使用保存的 Server 公钥重新 IK；
- Cloudflare Access 和 public-hostname Tunnel 不作为该协议的认证或部署前提。

## TLS + Noise 单端口示例

独立示例位于 `examples/noise_shared_port/`。Server 与 Client 均直接使用正式协议层方法，
`tests/official_protocol.rs` 另外做真实 gRPC 端到端验证。示例演示：

- Axum REST、WebSocket 与 Tonic gRPC 共用端口；
- Noise XXpsk3 首次注册：Client 只持有一次性 Token，不预置 Server 公钥；
- Noise IK 恢复已登记 Agent 的双向加密流；
- 可选外层 TLS，以及 Cloudflare/Nginx 终止 TLS 时的职责边界。

运行入口：

```text
cargo run -p smalux-protocol --example noise_shared_port_server
cargo run -p smalux-protocol --example noise_shared_port_client
```

完整流程、环境变量、文件布局和安全边界见
[`examples/noise_shared_port/README.md`](examples/noise_shared_port/README.md)。

## 验证

```text
cargo check -p smalux-protocol
cargo test -p smalux-protocol --all-targets
cargo clippy -p smalux-protocol --all-targets -- -D warnings
cargo rustdoc -p smalux-protocol -- -D warnings
```
