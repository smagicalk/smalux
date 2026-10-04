---
title: Agent 与 Server 运行闭环
description: 正式二进制的注册、重连、对账、Job 下发、上报、重启与最小联调说明。
---

# Agent 与 Server 运行闭环

本文只描述正式 `smalux-agent` 与 `smalux-server` 二进制已经实现的最小运行闭环。它与
`noise_shared_port` Protocol Example 的关系是：Example 用于学习低层握手和共端口路由，正式二进制
用于验证 Agent 注册、加密会话、Job 对账、TaskReport/JobEvent 入库和本地管理 IPC。

当前目标是让 Agent 保持轻量：身份和本地 Job policy 会写入独立文件，Plus Worker 可在自己的专属目录持久化业务数据；
远程 Job 当前目录、执行中的 Task、Scheduler 队列以及待发送报告/event/命令结果仍只存在于当前进程。Agent 重启后，Server
依据新的进程实例摘要重新对账，并从数据库中的权威目录生成过滤后的会话快照。

## 1. 当前闭环的范围

已经接通的路径如下：

```text
Server CLI
  -> 本地管理 IPC
  -> Server 数据库：注册 Token、Agent 身份、Job 目录、报告和事件
  -> AgentTransport gRPC 长流
  -> Noise XXpsk3 注册或 IK 重连
  -> Agent 上报策略、能力、插件清单和运行摘要
  -> Server 下发权威 Job/runtime 快照
  -> RemoteJobController
  -> Scheduler
  -> ReportingTask
  -> TaskReport / JobEvent
  -> Server 幂等校验并写入数据库
```

不在本轮闭环中的能力：管理 REST API、浏览器管理端、指标聚合/告警、跨进程报告重放、Agent
安装服务、自动升级、插件下载与分发、多 Server 节点一致性。

## 2. 运行模块与职责

| 模块 | 运行时职责 | 不负责的事 |
| --- | --- | --- |
| `smalux-agent` | 维护认证身份、连接、重连、Scheduler、远程 Job、采集、探测、插件 Worker 和本地 IPC。 | 不保存 Server 权威 Job 目录，不把报告直接写数据库。 |
| `smalux-protocol` | 定义 Proto、Noise XXpsk3/IK、加密帧、心跳、rekey、Tonic 双向流和强类型会话事件。 | 不访问 Server 数据库，不解释采集业务。 |
| `smalux-server` | 维护注册表、密钥环、会话、权威 Job/runtime、报告和事件；提供本地管理 IPC。 | 尚不提供面向浏览器的管理 API 或聚合查询。 |
| `RemoteJobController` | 校验、幂等应用 `JobCommand`，把 Proto `JobDefinition` 编译为 Scheduler Job。 | 不拥有 gRPC Session，不保存跨重启目录。 |
| `Scheduler` | 管理触发器、并发、队列、超时、重试、取消和执行生命周期。 | 不知道 Token、Noise、Server 地址或数据库。 |
| `ReportingTask` | 读取本机或执行探测，返回 Proto `TaskResult`。 | 不发送网络消息，不决定触发频率。 |

因此调用链应保持为：`Server JobDefinition -> RemoteJobController -> Scheduler -> Task ->
TaskReportSink -> 加密连接`。Task 不应绕过调度器直接建立网络连接。

## 3. 状态所有权

### 3.1 Agent 本地持久化状态

Agent 默认状态文件为 `<data_dir>/agent/connection-state.json`。Unix 使用 `0600`；Windows
使用受限 ACL。该文件只保存认证和密钥轮换需要的数据：

| 状态 | 内容 | 作用 |
| --- | --- | --- |
| `IdentityPrepared` | Agent Noise 静态身份。 | 首次 XXpsk3 注册前保持同一 Agent 密钥。 |
| `RegistrationPending` | Agent ID、Agent 私钥、Server 公钥候选、注册事务 ID。 | 注册在 Prepared/Commit 间中断后恢复。 |
| `Registered` | Agent ID、Agent 私钥、Server 公钥候选、已完成注册事务 ID。 | 后续使用 IK 认证，不再读取注册 Token。 |
| `JobPolicy` | 独立的 `<data_dir>/agent/job-policy.json`，包含全局拒绝和逐项 Task 拒绝。 | 本地 CLI 修改；每次认证作为会话快照上报，不由 Server 持久化。 |
| `PlusBusinessData` | `<data_dir>/plus/<plugin_id>/<plugin_version>/` 下的插件自有文件。 | 由 Plus 插件自行持久化业务状态和幂等数据。 |
Server 公钥候选可以包含 `current`、`pending` 和 `previous`，用于 Server 密钥轮换。Agent 收到
公告后先落盘 pending 公钥；使用它完成授权后的 IK 连接才提升为 current。这样不会因为半途断线而
丢失原来的可用公钥。

### 3.2 Agent 进程内状态

以下状态故意不跨进程保存：

| 状态 | 原因 | 重启后的恢复方式 |
| --- | --- | --- |
| 当前远程 Job 目录和 revision | Server 是权威来源，避免本地目录和 Server 分叉。 | 新进程上报摘要，Server 完整对账并发送 `ReplaceAllJobs`。 |
| 当前 Scheduler 执行、队列、重试计时器 | 运行期对象不可可靠地直接恢复。 | 旧执行停止；重新下发后的新 Scheduler Job 按 Server 配置运行。 |
| TaskReport、JobCommandResult、JobEvent 内存队列 | 保持 Agent 轻量，避免高频采集带来磁盘写放大。 | 未发送项丢失；后续执行产生新报告。 |
| 进程 `instance_id` 和 JobEvent sequence | 用于识别当前进程和检测事件缺口。 | 每次启动生成新的 UUID 和新的事件序列。 |

内存 outbox 容量由 `--task-report-buffer-capacity`、`--job-result-buffer-capacity` 控制，满时丢弃
最旧项并写 `warn` 日志。短暂断线会按 FIFO 补发；进程崩溃、强制结束或超过容量则不保证保留。

### 3.3 Server 持久化状态

Server 数据库是 Agent 业务状态的权威来源，至少保存：

| 数据 | 用途 |
| --- | --- |
| Server keyring | 恢复 Server Noise 静态身份并执行轮换。 |
| 注册 Token 和注册事务 | 验证一次性 Token，支持 Prepared/Commit 的原子激活。 |
| Agent 记录与公钥 | 将 IK 的对端公钥映射为 Agent ID，并支持吊销。 |
| Agent Job catalog 与历史 revision | 为每个 Agent 保存权威 Job；校验迟到报告属于该 Agent。 |
| Agent capability、插件 inventory/runtime | 按 Agent 的能力和已安装插件过滤 Job；保存已接收的能力与运行时状态。 |
| TaskReport | 以执行身份幂等保存成功报告。 |
| JobEvent | 以 `agent_id + instance_id + sequence` 幂等保存生命周期事件。 |
`AgentJobPolicySnapshot` 是每次认证会话接受的输入，不是 Server 持久化的策略副本；Server 使用它生成该会话的有效 Job 目录，Agent 重连时必须重新上报完整快照。

实时 SessionRegistry 和“上一次见到的 `instance_id`”在当前 Server 进程内存中。Server 重启后没有
这个缓存，因此下次收到摘要也会走完整对账，这符合“Server 可重启后恢复权威状态”的预期。

## 4. 首次注册：XXpsk3

首次启动时 Agent 没有 Server Noise 公钥，必须提供由 Server 签发的一次性注册 Token。Token 格式是
`token_id.psk`：点号前的 ID 用来查询，后半段 32 字节 PSK 用于 Noise XXpsk3；不要把完整 Token
记录到日志、提交到配置文件或放入截图。

```text
Agent                                                   Server
  |  XXpsk3 handshake message 1  --------------------> |
  |  <-------------------- handshake message 2         |
  |  handshake message 3  ----------------------------> |
  |  encrypted RegistrationRequest -------------------> | 校验 Token，写 pending 注册事务
  |  <------------------------ RegistrationPrepared    |
  |  原子保存 Pending 身份状态                          |
  |  encrypted RegistrationCommit --------------------> | 激活 Agent，消费 Token
  |  <------------------------ RegistrationCommitted   |
  |  保存 Registered 身份状态                            |
  |  上报运行摘要、策略、能力、插件清单                 |
```

关键顺序不能颠倒：Agent 必须先写入 `RegistrationPending`，再发送 Commit；收到匹配
`RegistrationCommitted` 后才写入 `Registered`。如果在 Commit 后崩溃，下一次启动会优先尝试 IK；
若 Server 已激活，则把 pending 升级为 registered；若明确仍未授权，才使用同一个 Token 恢复注册。

Server 不信任 Noise 握手本身的业务语义。它在加密 RegistrationRequest 中读取 Token ID，查出 PSK，
写 pending 事务，然后在 Commit 时原子完成“激活 Agent + 消费 Token”。因此 Token 不会因为仅完成
握手而提前消费。

Agent 展示名称不由 Agent 上报。Server 创建 Token 时可通过 `--agent-name` 绑定展示名称；未设置时
使用 Server 生成的 Agent ID 作为默认展示名称。

## 5. 后续连接：IK

`Registered` Agent 启动或断线重连时读取本地 Agent 私钥和保存的 Server 公钥候选，依次尝试 IK。
IK 不发送 Token，双方静态密钥完成认证后，Server 仍会按 Agent 公钥查询注册表：只有存在、未吊销的
Agent 才可进入业务流。

```text
Agent                                       Server
  |  IK message 1  ----------------------> |
  |  <---------------------- IK message 2  |
  |  Server 用 Agent 公钥查询授权状态       |
  |  <========== encrypted business flow ==========> |
```

这意味着：Noise IK 成功只说明对端拥有私钥，不等于当前仍被业务授权。Token 已过期、已消费或不存在
不会影响已注册 Agent 的 IK；Agent 被 Server 吊销后，IK 授权会被拒绝。

## 6. 连接后的对账与 Job 下发

认证成功后 Agent 会按以下顺序发送状态：

1. `AgentReconcileSummary`：当前进程 UUID、已应用 Job catalog/runtime revision 和 BLAKE2s-256 摘要。
2. `AgentJobPolicySnapshot`：本地允许或拒绝的远程 Task 策略。
3. `AgentCapabilitySnapshot`：Agent 版本、内置 Task kind 与 Probe 协议。
4. `AgentPluginInventory`：本地发现的 Plus Worker 和 Schema 信息。

Server 会验证这些消息，并读取数据库中的权威 catalog/runtime。摘要、revision 和 digest 均相同且
进程实例未变化时，可跳过重复完整快照；以下任一情况会强制完整同步：

- Agent 刚重启，`instance_id` 变化；
- Server 刚重启，未保存上一次进程实例缓存；
- 摘要缺失、字段非法、revision/digest 不匹配；
- Server 已更新 Job catalog 或插件 runtime；
- Agent 返回 `ResyncRequired`。

Server 下发 `JobCommand::ReplaceAll` 后，`RemoteJobController` 先校验 revision、Task 类型、本地策略、
Agent capability 与插件状态，再统一对 Scheduler 执行新增、更新和删除。Agent 返回
`JobCommandResult`：`Applied` 表示当前进程已经应用，`ResyncRequired` 表示目录存在缺口，需要 Server
再发送完整快照。该结果不表示某一次 Task 已经执行完成。

Agent 本地策略修改返回 `server_sync=pending` 时，或 Server ACK 该策略快照时，只能说明快照已被接收；它不表示 Server 已完成目录过滤、`ReplaceAll` 已应用，或 Job 已经执行。
### 断线超过离线期限

短暂断线时 Agent 继续执行当前远程 Job；重连采用指数退避。连续断线超过
`--offline-job-timeout`（默认 `30m`）后，Agent 取消并清空远程 Job，重置内存 catalog revision。
下一次连接一定由 Server 重新发送权威完整目录。这个策略避免离线 Agent 无限期执行已经被 Server
撤销的配置。

## 7. 执行与上报

每个 Scheduler Job 运行一个 `ReportingTask`。Task 返回 `TaskResult` 后，Agent 加上 Job ID、业务
revision、运行 ID、attempt 和时间信息，形成 `TaskReport`。报告首先尝试写入当前加密 Session；临时
传输错误进入 FIFO 内存队列。重连或优雅关闭时，Agent 先按 FIFO 刷新积压项。

Scheduler 生命周期还会生成 `JobEvent`，例如排队、开始、成功、失败、超时、取消和重试。每条实际
发出的事件带有当前 `instance_id` 和递增 `sequence`；没有发送的诊断不会占用序号。

Server 接收后的校验如下：

| 消息 | 幂等身份 | 额外校验 | 持久化结果 |
| --- | --- | --- | --- |
| `TaskReport` | `agent_id + job_id + job_revision + run_id + attempt` | Job 历史必须证明该 revision 属于 Agent，结果 oneof 必须匹配 Job。 | 相同 payload 重复投递不新增；相同身份不同 payload 拒绝。 |
| `JobEvent` | `agent_id + instance_id + sequence` | Job revision 归属、字段范围和事件类型。 | 相同 payload 重复投递不新增；跳号记录 `gap_detected`。 |
| `JobCommandResult` | Server 命令记录关联 | catalog revision 与命令状态。 | 更新命令处理状态；`ResyncRequired` 触发完整目录重发。 |

Server 成功写数据库表示本次消息已经被持久化，但当前 Protocol 没有向 Agent 回传跨 Session 的业务 ACK。
因此 Agent 在“写入当前 Session 成功”后会从内存队列移除该项；连接在写入成功与 Server 落库之间中断时，
Server 端幂等键会保护重复，但 Agent 也可能不知道应该重发。该取舍是当前轻量设计的一部分。

## 8. 最小人工联调

以下流程使用三个终端，先复用 `target/debug` 中已构建的二进制，再生成一个真实可提交的 CPU `JobDefinition` protobuf 文件；不要求安装 `protoc` 或其他额外工具。

### 8.1 启动 Server

在仓库根目录的第一个终端执行；先完成一次 `cargo build -p smalux-agent -p smalux-server`，再直接运行产物，避免三个终端同时触发 Cargo 构建：

```powershell
$serverExe = 'target/debug/smalux-server.exe'
$env:RUST_LOG = "smalux_server=info,smalux_protocol=info"
& $serverExe run
```

默认监听 `http://127.0.0.1:12345`，启动时会创建/迁移默认 SQLite 数据库、恢复或生成 Server
keyring，并启动本机管理 IPC。第二个终端可先确认：

```powershell
& $serverExe status
Invoke-WebRequest http://127.0.0.1:12345/api/v1/health
```

### 8.2 创建一次性 Token

在第二个终端执行。先创建目录；`--credential-file` 要求目标文件不存在，成功后只写入新文件，不再打印完整凭据：

```powershell
$serverExe = 'target/debug/smalux-server.exe'
New-Item -ItemType Directory -Force C:/smalux | Out-Null
& $serverExe registration-token create `
  --agent-name edge-agent `
  --expires-in 30m `
  --credential-file C:/smalux/edge-agent.token
```

不要把 Token 放进长期环境变量；它只用于首次注册。

### 8.3 启动 Agent

在第三个终端执行，优先使用 `--token-file`：

```powershell
$agentExe = 'target/debug/smalux-agent.exe'
$env:RUST_LOG = "smalux_agent=info,smalux_protocol=info"
& $agentExe run `
  --server-endpoint http://127.0.0.1:12345 `
  --token-file C:/smalux/edge-agent.token
```

预期日志顺序是：建立 XXpsk3、获得 Agent ID、保存注册身份、进入连接状态、发送摘要/策略/能力/插件
清单。Server 端应显示注册完成和加密业务会话建立。

### 8.4 查询双方状态
```powershell
$serverExe = 'target/debug/smalux-server.exe'
$agentExe = 'target/debug/smalux-agent.exe'

# Server 侧
& $serverExe agent list --online
& $serverExe session list --state authenticated
& $serverExe registration-token list

# Agent 侧
& $agentExe status
& $agentExe identity show
& $agentExe tasks list
& $agentExe jobs list
```

Server 的 `session list` 应出现已认证 Agent；Agent `status` 应显示已连接和认证模式。首次应为
`RegistrationXxPsk3`，下一次重连为 `ReconnectIk`。

### 8.5 验证 IK 重连与重启对账

1. 使用 `Ctrl+C` 正常停止 Agent。
2. 再次启动 Agent，**不再传入 Token**：

   ```powershell
   & $agentExe run --server-endpoint http://127.0.0.1:12345
   ```
3. Agent 应使用保存的身份与 Server 公钥执行 IK。
4. 因为是新进程，`instance_id` 改变；Server 应强制完整对账并重新下发权威 Job/runtime。

如果同一进程只经历短暂网络断线，instance ID 不变，摘要相同则 Server 可以避免重复发送同一配置。

### 8.6 生成 CPU Job、下发与查询报告 {#cpu-example}

以下示例在管理终端中执行。Server 管理 CLI 默认可能把 tracing 日志写到 stdout，和 `--output json` 的 JSON 混在一起；
脚本解析前必须在该管理终端设置 `$env:RUST_LOG = 'off'`，并检查每个 CLI 的 `$LASTEXITCODE`。此设置只影响当前终端及其启动的短 CLI
进程，不影响另一个终端中运行的 Server/Agent daemon。JSON 应从命令的完整 stdout 收集后用 `-join "`n"` 再解析，不要过滤日志行来猜 JSON。

```powershell
$env:RUST_LOG = 'off'
$serverExe = 'target/debug/smalux-server.exe'
$agentExe = 'target/debug/smalux-agent.exe'
$agentStatusOutput = @(& $agentExe status --output json)
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Agent status' }
$agentStatus = ($agentStatusOutput -join "`n") | ConvertFrom-Json
$agentId = $agentStatus.agent_id
if (-not $agentId) { throw 'Agent has not registered' }
$catalogOutput = @(& $serverExe job list $agentId --output json)
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Server catalog' }
$catalog = (($catalogOutput -join "`n") | ConvertFrom-Json).data
if ($catalog -and $catalog.definitions.Count -gt 0) { throw 'Use a fresh test Agent with an empty catalog' }
$expectedRevision = if ($catalog) { $catalog.catalog_revision } else { 0 }
```

生成一个每 5 秒运行一次的 CPU `JobDefinition`：Job revision 为 1，enabled 为 true，
misfire 为 SKIP，其他执行配置使用默认值。下面固定编码仅适用于这个最小示例，
不是通用 Job 编辑器；复杂任务应按仓库 Proto 使用 protobuf 工具生成，不要盲改长度或字段号。

```powershell
$jobId = [guid]::NewGuid().ToString()
$jobHex = $jobId.Replace('-', '')
[byte[]]$jobBytes = for ($i = 0; $i -lt 32; $i += 2) {
    [Convert]::ToByte($jobHex.Substring($i, 2), 16)
}
# job_id(1), revision(2), enabled(3), trigger(4), task(6: cpu=11)
[byte[]]$definition = @(0x0a, 0x10) + $jobBytes + @(
    0x10, 0x01, 0x18, 0x01,
    0x22, 0x0a, 0x12, 0x02, 0x08, 0x01,
    0x5a, 0x04, 0x0a, 0x02, 0x08, 0x05,
    0x32, 0x02, 0x5a, 0x00
)
$jobFile = Join-Path ([IO.Path]::GetTempPath()) ("smalux-cpu-$jobId.pb")
[IO.File]::WriteAllBytes($jobFile, $definition)
& $serverExe job replace $agentId --definition $jobFile --expected-revision $expectedRevision
if ($LASTEXITCODE -ne 0) { throw 'Job catalog was not replaced' }
& $agentExe jobs list
Start-Sleep -Seconds 10
& $serverExe report --agent-id $agentId
```

Agent 应出现该 `$jobId` 的 `smalux.collect.cpu.v1` Job，状态为 `enabled`；
Server 的 `report` 应出现对应 Job/revision 的 CPU 报告。下发是异步过程，若尚未出现可再次查询，
并检查连接与 `jobs policy show`。`event` 用于异常生命周期诊断，成功采样不要求有非空事件。
`server job list` 显示原始权威目录；Agent `jobs list` 显示策略/能力过滤后实际安装的目录，两者可不同。

结束本示例时清空测试目录（保留 Server 历史报告），并删除临时文件：

```powershell
$catalogOutput = @(& $serverExe job list $agentId --output json)
if ($LASTEXITCODE -ne 0) { throw 'Cannot read catalog revision' }
$catalog = (($catalogOutput -join "`n") | ConvertFrom-Json).data
if (-not $catalog) { throw 'Cannot read catalog revision' }
& $serverExe job clear $agentId --expected-revision $catalog.catalog_revision --yes
if ($LASTEXITCODE -ne 0) { throw 'Catalog changed; inspect before retrying' }
& $agentExe jobs list
Remove-Item -LiteralPath $jobFile
```

等待目录同步后 Agent Job 数应为 0。`--expected-revision` 是目录乐观锁；出现冲突时应先重新查询，
不要跳过校验覆盖其他操作者的修改。更新同一 Job 配置还必须提高 Job 自身 revision。


## 9. 常见故障与定位

| 现象 | 首先检查 | 说明 |
| --- | --- | --- |
| Agent 第一次启动要求 Token | `identity show` 是否没有 Registered 状态。 | 合理现象；提供未过期、未消费 Token。 |
| Token 错误或已过期 | `registration-token list`、Server 日志。 | XXpsk3 故意只返回粗粒度认证失败，不泄露具体 PSK 原因。 |
| 注册后仍反复 XX | Agent 状态文件无法写入、被删除或只保留了一部分。 | 不要手工删除单个身份字段；要重置时删除完整身份状态后签发新 Token。 |
| IK 握手成功但随后被断开 | `agent list`、吊销状态、Server 授权日志。 | IK 认证不绕过 Agent 注册表的业务授权。 |
| Agent 连上但没有 Job | `jobs list`、Agent policy、capability、插件 inventory、Server catalog。 | Server 只下发该 Agent 支持且未被本地策略拒绝的 Job。 |
| 短暂断线后报告未立即出现 | Agent `status` 的 pending/dropped 统计、网络和 Server 日志。 | 队列按 FIFO 补发；满载会丢弃最旧项。 |
| Agent 重启后旧 Job 消失 | 查看 Server 是否仍保存该 Agent 的 catalog。 | 这是轻量设计的正常行为，Server 会重新下发权威目录。 |
| Server 重启后 Agent 再次对账 | Server 日志中的 reconcile 信息。 | 正常；实时 instance 缓存不持久化。 |

建议将日志级别设置为：

```powershell
$env:RUST_LOG = "smalux_agent=debug,smalux_server=debug,smalux_protocol=debug"
```

日志同时输出到控制台和 `<data_dir>/logs/<component>/smalux.log`，按日期和大小滚动。不要开启会记录
秘密的自定义调试日志；正常 tracing 字段不应输出 Token、PSK 或私钥。

## 10. 当前保证与明确不保证

| 行为 | 当前保证 | 当前不保证 |
| --- | --- | --- |
| 身份恢复 | 注册状态和 Server 公钥候选可跨 Agent 重启恢复。 | 身份文件被手工破坏后的自动修复。 |
| 连接安全 | 首次 XXpsk3、后续 IK，业务帧均在 Noise 加密层中。 | 直接 TLS listener；可由 Nginx/Cloudflare 终止外层 TLS。 |
| 重连 | 短暂断线指数退避；身份有效时可 IK 重连。 | 永久授权错误的无限自动恢复。 |
| Job 一致性 | 重启或摘要不一致时，Server 重新下发权威目录。 | 本地 Job 目录跨进程保留。 |
| 报告入库 | Server 对重复执行身份进行幂等校验并持久化。 | Agent 跨崩溃的报告不丢失，或端到端业务 ACK。 |
| 事件 | Server 检测同实例序号缺口和重复 payload 冲突。 | 因 Agent 内存队列淘汰导致的事件补洞。 |
| 插件 | Agent 可发现本地 Worker，Server 可按 inventory/runtime 过滤 Job。 | 插件下载、签名、灰度发布和回滚。 |

## 11. 自动化验证

恢复 Rust toolchain 后，推荐按以下顺序验证当前分支：

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

最关键的最小闭环测试：

```powershell
cargo test -p smalux-server encrypted_agent_executes_server_job_and_persists_report -- --nocapture
cargo test -p smalux-server official_agent_client_registers_then_reconnects_with_saved_identity -- --nocapture
```

当前机器的 Rust 验证受环境限制：rustup 没有已安装 toolchain，且创建
`C:/Users/19766/.rustup/tmp` 临时文件被拒绝。该问题需要先恢复 Rust toolchain/目录权限；它不是
本次文档修改导致的编译错误。文档站可用以下命令验证：

```powershell
pnpm --dir website build
```

## 12. 源码阅读入口

| 想了解什么 | 首选位置 |
| --- | --- |
| Agent 进程装配、重连、对账和上报 | `crates/smalux-agent/src/main.rs` |
| Agent 身份状态与密钥轮换 | `crates/smalux-agent/src/client/state/` |
| 首次注册、pending 恢复与 IK 选择 | `crates/smalux-agent/src/client/connection.rs` |
| 内存报告/事件/命令结果队列 | `crates/smalux-agent/src/outbox.rs` |
| Job 编译、revision、幂等和 Scheduler 更新 | `crates/smalux-agent/src/remote_jobs.rs` |
| Server gRPC Session 业务循环 | `crates/smalux-server/src/service/agent/transport/session/business.rs` |
| Server 注册与 IK 授权 | `crates/smalux-server/src/service/agent/transport/session/` |
| Server 对账实例缓存 | `crates/smalux-server/src/service/agent/session_registry.rs` |
| Report/Event 幂等入库 | `crates/smalux-server/src/database/task_report.rs`、`job_event.rs` |
| Wire 字段定义 | `crates/smalux-protocol/proto/smalux/agent/v1/` |

协议字段的最终事实来源是 `.proto`，运行行为的最终事实来源是公开 Rust API 与自动化测试。本文描述的是
当前实现状态；未来补上持久化 outbox 或业务 ACK 时，应同步更新“状态所有权”“执行与上报”和
“当前保证与明确不保证”三节。
