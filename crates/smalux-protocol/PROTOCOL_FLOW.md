# Agent 与 Server 协议调用流程

本文只描述当前 `smalux-protocol` 已实现的交互流程、公开方法和持久化边界。
Protobuf 字段定义以 `proto/smalux/agent/v1/` 为准，API 详细说明见同目录 `README.md`。

## 1. 整体分层

协议由三层组成：

| 层级 | 主要类型 | 职责 |
| --- | --- | --- |
| Wire 层 | `ProtocolFrame`、`SecureMessage`、Job/Task Proto | 定义跨进程消息格式和字段编号。 |
| 会话层 | `AgentProtocolClient`、`ServerSessionAcceptor`、`TonicNoiseSession` | 建立 gRPC + Noise 会话，完成注册、认证、加密收发和维护。 |
| Driver 层 | `SessionDriver`、`SessionHandle`、`SessionEventReceiver` | 独占会话状态，在后台串行处理收发、心跳、rekey 和事件派发。 |

Noise 底层握手和密钥状态仍然公开。调用方可以使用高层流程，也可以自行组合小方法，
但同一个会话只能由一种方式驱动，不能同时交给手动循环和 Driver。

## 快速代码导读

下面的代码省略 `use`、日志和具体数据库类型，只展示协议调用的先后关系。
其中 `store.*` 代表调用方自己的数据库或本地文件操作，不是 `smalux-protocol` 提供的方法。

### Agent 首次注册

```rust
// 1. Agent 本地生成长期身份；私钥不会发送给 Server。
let identity = NoiseIdentity::generate()?;

// 2. 创建协议 Client。endpoint 可以是 h2c，也可以是经过 CF/Nginx 的 HTTPS。
let mut client = AgentProtocolClient::new("https://agent.example.com");
client.set_grpc_prefix("/api/v1/grpc");
client.set_handshake_timeout(Duration::from_secs(5));

// 3. XXpsk3 建立首次可信会话，并等待 Server 保存 pending 注册记录。
//    此时 Agent 不需要预先知道 Server 静态公钥。
let pending = client
    .prepare_registration(
        identity,
        &registration_psk,
        registration_token,
        "agent-01".to_owned(),
    )
    .await?;

// 4. 必须先保存 pending 中的长期材料，成功后才能通知 Server 提交。
store.save_pending(
    &pending.agent_identity,
    &pending.server_public_key,
    &pending.agent_id,
    pending.registration_id,
)?;

// 5. commit 通知 Server 激活 Agent，并等待 RegistrationCommitted。
let registered = pending.commit().await?;
store.mark_committed(registered.registration_id)?;

// 6. 注册用的 XX 会话已经是可用的加密业务会话，不需要立即断开再建 IK。
let session = registered.session;
```

关键边界是 `save_pending()` 必须发生在 `commit()` 前。进程在两者之间退出时，Agent 能恢复
相同身份继续注册；Server 也能根据 Token、Agent 公钥和事务 ID 返回原 pending 事务。

### Agent 后续重连

```rust
// 1. 从本地恢复首次注册时保存的 Agent 私钥和 Server 固定公钥。
let identity = store.load_agent_identity()?;
let server_key = store.load_server_public_key()?;

// 2. IK 在两条握手消息内完成双方静态身份认证。
let mut client = AgentProtocolClient::new("https://agent.example.com");
client.set_grpc_prefix("/api/v1/grpc");
let session = client.connect(&identity, server_key).await?;

// 3. 后续只在当前会话上收发业务消息，不再提交注册 Token。
let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());
```

如果 Server 正在轮换静态密钥，可把本地仍可信的 `pending/current/previous` 公钥传给
`connect_with_candidates()`。每个候选公钥会创建一条独立 RPC，首个 IK 成功后停止尝试。

### Server 接受注册或重连

```rust
// sender 和 inbound 来自同一次 OpenSession RPC，不能跨连接混用。
let incoming = acceptor
    .accept_incoming(inbound, sender, &server_keyring, &registration_psk)
    .await?;

let (agent_id, session) = match incoming {
    IncomingSession::Registration(mut registration) => {
        // XXpsk3 已认证 Agent 公钥，但 Token 和名称仍由业务层验证。
        let agent_key = registration.peer_public_key();
        let request = registration.receive_request().await?;
        let prepared = store.prepare_registration(
            &request.token,
            &request.agent_name,
            agent_key.as_bytes(),
        )?;

        // Server 先保存 pending，再告诉 Agent 本次事务 ID。
        registration
            .prepare(prepared.registration_id, prepared.agent_id.clone())
            .await?;
        registration
            .wait_for_commit(prepared.registration_id, Duration::from_secs(10))
            .await?;

        // 收到 Agent commit 后，Server 先落库激活，再发送最终确认。
        store.commit_registration(prepared.registration_id, agent_key.as_bytes())?;
        let session = registration.complete(prepared.registration_id).await?;
        (prepared.agent_id, session)
    }
    IncomingSession::Authentication(authentication) => {
        // IK 只证明私钥持有关系；业务层仍需检查注册、吊销和租户权限。
        let agent_id = store.authorize_agent(authentication.peer_public_key().as_bytes())?;
        (agent_id, authentication.authorize())
    }
};

// 两条分支最终都得到已授权 session，后面的业务处理完全相同。
run_business_session(agent_id, session).await?;
```

Token 无效、Agent 已吊销或落库失败时，不应调用 `authorize()` 或 `complete()`；应调用对应对象的
`reject(secure_error)`，让对端在 Noise 密文中收到结构化拒绝原因。

### Driver 收发业务

```rust
// Driver 独占 TonicNoiseSession，串行推进 Noise nonce、心跳和 rekey。
let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());

// handle 可以克隆给采集任务；队列满时 send_task_report 会等待并形成反压。
let report_sender = running.handle.clone();
tokio::spawn(async move {
    while let Some(report) = next_task_report().await {
        report_sender.send_task_report(report).await?;
    }
    Ok::<_, TransportError>(())
});

// events 只能由一个调度循环消费。Server 下发 Job 后交给本地 JobController。
while let Some(event) = running.events.recv().await {
    match event? {
        SessionEvent::JobCommand(command) => {
            let result = job_controller.apply(command).await;
            running.handle.send_job_command_result(result).await?;
        }
        other => handle_other_event(other).await?,
    }
}

// 主动退出时先通知 Driver，再等待它释放 gRPC 流。
running.handle.shutdown().await?;
running.task.await?;
```

这段流程只有一个对象实际操作 `TonicNoiseSession`：`SessionDriver`。采集任务、Job 调度器和其他
业务模块只持有 `SessionHandle`，因此不会并发修改 Noise 状态。

## 2. 外层连接

Agent 首先创建 `AgentProtocolClient`：

| 方法 | 作用 |
| --- | --- |
| `new(endpoint)` | 设置 Server 的 scheme、域名和端口。 |
| `set_grpc_prefix(prefix)` | 设置 Axum 或反向代理增加的 gRPC 路径前缀。 |
| `set_handshake_timeout(duration)` | 设置建连、Noise 握手和注册确认的等待上限。 |

`http://` 使用 h2c，`https://` 使用 TLS。Noise 位于 gRPC 流内部，无论是否启用 TLS都会运行。
Cloudflare 或 Nginx 可以终止外层 TLS，但只能看到 Noise 握手元数据和密文长度，不能读取业务明文。

Server 的 `OpenSession` 为每条 gRPC 双向流创建独立 sender、inbound stream 和 Noise 状态。
同一次 RPC 的 sender 与 inbound 必须一起交给 `ServerSessionAcceptor`，不能跨 RPC 混用。

## 3. 首次注册

首次注册使用 XXpsk3。Agent 不需要提前保存 Server 静态公钥，但 Agent 和 Server 必须通过
可信渠道持有同一个注册 PSK。示例将固定 Token 的 32 字节值同时作为注册 PSK。

### 3.1 Wire 顺序

```text
Agent                                      Server
  | ------ XXpsk3 message 1 -------------> |
  | <----- XXpsk3 message 2 -------------- |
  | ------ XXpsk3 message 3 -------------> |
  | ------ RegistrationRequest ----------> | 保存 pending 注册
  | <----- RegistrationPrepared ---------- | 返回 agent_id 和 registration_id
  |         Agent 保存 pending 材料         |
  | ------ RegistrationCommit -----------> | 激活 Agent，消费 Token
  | <----- RegistrationCommitted --------- | Agent 标记本地注册完成
  | <===== 当前 XX 加密业务长流 ==========> |
```

`RegistrationPrepared` 不表示注册已经完成。只有 Agent 收到匹配事务 ID 的
`RegistrationCommitted`，本地状态才可以进入 committed，后续启动才可以使用 IK。

### 3.2 Agent 调用顺序

1. 生成或恢复长期 `NoiseIdentity`。
2. 调用 `AgentProtocolClient::prepare_registration()`。
3. 协议层自动完成 XXpsk3，发送 `RegistrationRequest` 并等待 `RegistrationPrepared`。
4. 获得 `AgentPendingRegistration`。
5. 调用方持久化 `agent_identity`、`server_public_key`、`agent_id`、`registration_id` 和 pending 状态。
6. 持久化成功后调用 `AgentPendingRegistration::commit()`。
7. 协议层发送 `RegistrationCommit`，等待并校验 `RegistrationCommitted`。
8. 获得 `AgentRegistration` 后，把本地状态从 pending 更新为 committed。
9. 继续使用 `AgentRegistration::session` 处理业务，不主动切换到新的 IK 连接。

`AgentProtocolClient::register_agent()` 是便捷入口，会连续执行 prepare 和 commit。
它没有给调用方留下 commit 前持久化 pending 的时点，因此生产注册应优先使用分步方法。

### 3.3 Server 调用顺序

1. 调用 `ServerSessionAcceptor::accept_incoming()`。
2. XXpsk3 成功后获得 `IncomingSession::Registration(ServerRegistration)`。
3. 调用 `ServerRegistration::peer_public_key()`取得 Noise 已认证的 Agent 静态公钥。
4. 调用 `receive_request()`取得 Token 和 Agent 名称。
5. 业务层验证 Token、名称和公钥，并在数据库或本地存储中创建或恢复 pending 事务。
6. 存储成功后调用 `prepare(registration_id, agent_id)`。
7. 调用 `wait_for_commit(registration_id, timeout)`等待 Agent 持久化确认。
8. 收到匹配 commit 后，业务层先激活 Agent并最终消费 Token。
9. 数据库提交成功后调用 `complete(registration_id)`。
10. `complete()`发送最终确认并返回可继续使用的 `TonicNoiseSession`。

Token、Agent 注册表和事务存储不属于协议 crate。Server 必须先成功写入自己的存储，再发送对应阶段响应。

### 3.4 注册拒绝

Server 调用 `ServerRegistration::reject()`发送 Noise 加密的 `SecureError`。常见原因包括：

- Token 无效、过期或已绑定其他公钥；
- Agent 名称无效或已占用；
- 注册事务 ID 不匹配；
- Server 存储失败。

Agent 会收到 `TransportError::RemoteSecure`。外层代理不会看到 Token 和具体注册资料。

## 4. 注册断线恢复

Server 的注册存储应以 Token、Agent 公钥和注册事务 ID保证幂等。当前示例已实现以下行为：

| 断线位置 | Agent 下次操作 | Server 行为 |
| --- | --- | --- |
| XXpsk3 完成前 | 重新执行 `prepare_registration()` | 创建一条全新流和握手状态。 |
| Request 已发送，Prepared 未收到 | 使用原身份和 Token 重试 | 返回原 pending 事务或创建一次 pending。 |
| Prepared 已收到，Commit 未发送 | 恢复本地 pending，重新注册 | 相同 Token、名称和公钥返回原事务 ID。 |
| Commit 已发送，Committed 未收到 | 保留 pending 并重试 | 已提交事务仍返回原事务，重复 commit 成功。 |
| Committed 已收到 | 使用 IK 连接 | Server 根据已登记 Agent 公钥授权。 |

相同 Token 配合不同 Agent 公钥必须拒绝。pending Agent 不能使用 IK。

当前 XXpsk3 接收接口每次接收一个已经选定的注册 PSK。示例使用固定 PSK；生产系统如果同时存在
多组注册 PSK，需要在上层增加可安全选择 PSK 的入口或后续扩展公开 Token 标识，不能在握手完成后
才决定本次 XXpsk3 应使用哪一个 PSK。

## 5. 后续 IK 认证

已 committed 的 Agent 重启或断线后使用 IK，不再发送 Token。

### 5.1 Agent 调用顺序

1. 恢复 Agent `NoiseIdentity` 和已固定的 Server 公钥。
2. 调用 `AgentProtocolClient::connect(identity, server_key)`。
3. Server 换钥窗口内可以调用 `connect_with_candidates()`按顺序尝试多把可信 Server 公钥。
4. IK 两消息握手成功后获得 `TonicNoiseSession`。

### 5.2 Server 调用顺序

1. `accept_incoming()`自动识别 IK 并完成 responder 握手。
2. 获得 `IncomingSession::Authentication(ServerAuthentication)`。
3. 调用 `peer_public_key()`取得 IK 认证的 Agent 公钥。
4. 业务层查询注册表、吊销状态、租户和业务权限。
5. 授权成功调用 `authorize()`取得 `TonicNoiseSession`。
6. 授权失败调用 `reject()`发送加密错误并结束当前流。

Noise 只证明对端持有对应静态私钥。Agent 是否仍有业务权限，必须由 Server 每次连接重新判断。

## 6. 手动业务会话

手动模式由一个任务独占 `TonicNoiseSession`。

### 6.1 收发方法

| 方法 | 适用端 | 作用 |
| --- | --- | --- |
| `send(SecureMessage)` | 两端 | 发送原始 envelope，保留给高级扩展。 |
| `receive()` | 两端 | 返回原始非控制 `SecureMessage`。 |
| `receive_event()` | 两端 | 返回强类型 `SessionEvent`。 |
| `send_task_report()` | Agent | 上报强类型 Task 结果。 |
| `send_job_command()` | Server | 下发 Job 控制命令。 |
| `send_job_command_result()` | Agent | 返回 Job 命令执行结果。 |

`SessionEvent` 当前区分注册消息、通用 `Messages`、Job 命令、Job 结果、TaskReport 和静态密钥轮换。
Ping、Pong 和连接 rekey 控制帧由会话层消费，不会作为普通业务事件返回。

### 6.2 手动循环责任

Noise nonce 必须严格串行，因此不能把同一个 `TonicNoiseSession` 同时交给多个 Tokio task。
调用方应在一个 `select!` 循环中汇聚采集结果、Server 命令、维护定时器和关闭信号。

持续调用 `receive()`或 `receive_event()`时，会话会自动响应 Ping、处理 responder rekey，
并在空闲时发送心跳。只发送而不接收时，调用方必须定期调用 `perform_maintenance()`，否则无法
及时接收 Server 命令、检测关闭或执行自动 rekey。

## 7. 自动 SessionDriver

Driver 模式适合高频上报和 Server 持续下发命令的 Agent。

### 7.1 创建方式

| 方法 | 行为 |
| --- | --- |
| `SessionDriver::new(session, config)` | 返回尚未运行的 Driver、`SessionHandle` 和 `SessionEventReceiver`。 |
| `SessionDriver::run()` | 在调用方选择的 task 中运行会话循环。 |
| `SessionDriver::spawn(session, config)` | 立即启动 Tokio task，返回 `RunningSession`。 |

`RunningSession` 包含可克隆 `handle`、单消费者 `events` 和用于等待退出的 `task`。

### 7.2 SessionHandle 方法

| 方法组 | 方法 |
| --- | --- |
| 原始与通用消息 | `send()`、`send_messages()` |
| Job 与上报 | `send_task_report()`、`send_job_command()`、`send_job_command_result()` |
| 心跳与连接换钥 | `ping()`、`request_rekey()`、`require_rekey()` |
| 长期静态换钥 | `request_agent_key_rotation()`、`accept_agent_key_rotation()`、`announce_server_key()`、`acknowledge_server_key()` |
| 生命周期 | `shutdown()` |

多个采集任务可以克隆 `SessionHandle` 并并发提交消息。Driver 内部依次执行 Prost 编码、Noise 加密、
nonce 推进和网络发送。命令队列和事件队列都有容量上限，队列满时异步等待，形成反压而不是丢包。

`SessionEventReceiver::recv()`返回业务事件或终止错误。`None` 表示 Driver 已结束且事件队列已排空。
收到远端关闭、心跳超时、gRPC 错误或 Noise 错误后，Driver 发送终止错误并退出。

## 8. 心跳与连接状态

| 方法 | 作用 |
| --- | --- |
| `set_heartbeat_policy()` | 修改心跳间隔和失联超时。 |
| `heartbeat_policy()` | 读取当前策略。 |
| `should_ping()` | 判断是否到达 Ping 时点。 |
| `heartbeat_expired()` | 判断是否超过无入站上限。 |
| `maintenance_status()` | 无副作用查询心跳、Ping 和 rekey 状态。 |
| `perform_maintenance()` | 执行一次到期维护。 |
| `ping()` | 手动发送指定 nonce 的加密 Ping。 |

默认 30 秒发送间隔、90 秒无入站消息超时。任何成功解密的入站帧都会刷新存活时间。

## 9. 当前连接 rekey

连接 rekey 只更新当前 gRPC 流内的对称 cipher key，不改变 Agent 或 Server 长期身份。

1. Agent/initiator 调用 `request_rekey()`，或 Server 调用 `require_rekey()`要求 Agent 发起。
2. Agent 发送下一 generation 的 `RekeyRequest`。
3. Server 切换 incoming key，使用旧 outgoing key 发送 `RekeyAck`，再切换 outgoing key。
4. Agent 验证 Ack 后依次切换 incoming 和 outgoing key。
5. 双方继续使用原 TCP、TLS 和 gRPC 流。

`set_rekey_policy()`配置最大时长、最大帧数和自动开关。默认一小时或 `2^20` 帧触发。
`generation()`返回当前代数，`encrypted_frames()`返回当前代处理帧数。

rekey 等待 Ack 时提前到达的业务消息会被缓存。换钥完成后按原接收顺序交付，不会因为控制帧与
业务帧交错而断开连接。

## 10. 长期静态密钥轮换

长期密钥轮换会影响后续 IK，必须先持久化 snapshot，再发送网络消息。

### 10.1 Agent 静态密钥

1. `AgentKeySet::prepare_rotation()`生成 pending 身份、请求和 snapshot。
2. Agent 保存包含 pending 私钥的 snapshot。
3. 调用 `request_agent_key_rotation()`发送新公钥。
4. Server 校验并保存 Agent pending 公钥。
5. Server 调用 `accept_agent_key_rotation()`确认。
6. Agent 调用 `promote_pending()`。
7. 稳定窗口结束后双方退休 previous 公钥。

### 10.2 Server 静态密钥

1. `ServerKeyRing::prepare_rotation()`生成 next 身份、公告和 snapshot。
2. Server 保存包含 next 私钥的 snapshot。
3. 调用 `announce_server_key()`发送公告。
4. Agent 调用 `PinnedServerKeys::stage()`验证并保存 pending Server 公钥。
5. Agent 调用 `acknowledge_server_key()`。
6. Server 调用 `promote_next()`，Agent 调用 `promote_pending()`。
7. 稳定窗口结束后调用 `retire_previous()`。

私钥不会进入 Protobuf。协议层只返回状态和 snapshot，不决定保存到数据库、文件还是 KMS。

## 11. Job 与 TaskReport 流程

Server 通过 `send_job_command()`下发 `JobCommand`。Agent 收到 `SessionEvent::JobCommand` 后：

1. 交给本地 `JobController`校验并应用。
2. 使用 `send_job_command_result()`返回结构化结果。
3. Scheduler 按 Job 定义持续运行 Task。
4. Task 产生 `TaskReport` 后，通过 `send_task_report()`上报。

连接断开不会自动删除 Agent 已安装的 Job，短时波动期间仍可继续采集。

当前协议没有为 `TaskReport`定义跨连接持久化 ACK 和重放状态机。需要保证断线不丢数据时，Agent
应先把 TaskReport 写入本地队列，连接恢复后再发送；去重 ID、确认水位和过期策略需要后续单独设计。

## 12. 错误和关闭

| 阶段 | 错误形式 |
| --- | --- |
| Noise 建立前 | `ProtocolError`，只包含安全的通用分类。 |
| Noise 建立后 | 加密 `SecureError` 或本地 `TransportError`。 |
| 对端关闭流 | `TransportError::Closed` 或手动 `receive()`返回 `None`。 |
| 心跳超时 | `TransportError::HeartbeatTimeout`。 |
| gRPC/TLS 失败 | `TransportError::Status` 或 `TransportError::Transport`。 |

一旦某帧已经成功加密但发送失败，nonce 已经推进，不能重新发送同一密文或继续复用该会话。
调用方应关闭当前流，并使用已持久化身份重新执行 IK。

## 13. Example 对应关系

`examples/noise_shared_port/` 提供 Client 和 Server 两个独立程序：

- `--mode manual`：展示 `TonicNoiseSession` 小方法的逐步调用；
- `--mode driver`：展示 `SessionDriver`、`SessionHandle` 和事件接收器；
- Server 同时提供 Axum REST、WebSocket 和 Tonic gRPC；
- h2c、Server 直接 TLS、Cloudflare/Nginx 前置终止 TLS 均不改变 Noise 流程；
- 示例注册表会把 pending/committed 事务和 Agent 公钥写入独立文件，展示断线恢复所需字段。

Client 和 Server 可以独立选择 manual 或 driver，因为两种模式使用完全相同的 wire protocol。
