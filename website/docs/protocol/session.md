---
title: 注册、重连与会话
description: XXpsk3 首次注册、IK 后续认证和 SessionDriver 调用流程。
---

# 注册、重连与会话

## 首次注册：XXpsk3

首次启动时 Agent 没有 Server Noise 公钥。Agent 与 Server 通过可信渠道共享一个 32 字节注册 PSK，
然后执行 XXpsk3：

```text
Agent                                      Server
  | ------ XXpsk3 message 1 -------------> |
  | <----- XXpsk3 message 2 -------------- |
  | ------ XXpsk3 message 3 -------------> |
  | ------ RegistrationRequest ----------> | 保存 pending
  | <----- RegistrationPrepared ---------- |
  |         Agent 保存 pending 材料         |
  | ------ RegistrationCommit -----------> | 激活 Agent
  | <----- RegistrationCommitted --------- |
  | <===== 继续复用当前加密业务流 =========> |
```

Agent 侧的可靠调用顺序：

```rust
let identity = NoiseIdentity::generate()?;
let mut client = AgentProtocolClient::new("https://agent.example.com");
client.set_grpc_prefix("/api/v1/grpc");

// token 使用公开 Token ID 与秘密 PSK 的组合格式；完整值只会进入 Noise 加密消息。
let token =
    "token-001.0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned();

let pending = client
    .prepare_registration(identity, &psk, token, agent_name)
    .await?;

// 业务存储：先保存私钥、Server 公钥、Agent ID 和事务 ID。
store.save_pending(&pending)?;

let registered = pending.commit().await?;
store.mark_committed(registered.registration_id)?;

// XX Session 已授权，当前连接直接进入业务阶段。
let session = registered.session;
```

`RegistrationPrepared` 只表示双方都保存了 pending 所需信息，不表示注册完成。只有收到匹配事务 ID 的
`RegistrationCommitted` 后，本地状态才能进入 committed 并允许后续 IK。

### 注册中断如何恢复

| 中断位置 | Agent 本地状态 | Server 状态 | 下次动作 |
| --- | --- | --- | --- |
| XX 握手未完成 | 无 pending | 无业务事务 | 关闭旧流，用同一有效 Token 重新开始。 |
| Request 后、Prepared 前 | 通常无 pending | 可能正在校验或落库 | 查询或等待旧事务过期，再重新注册。 |
| Prepared 后、Commit 前 | 已保存 pending | pending | 使用事务 ID 恢复提交，不能生成另一套 identity。 |
| Commit 后、Committed 前 | pending | 可能已激活 | 先查询事务结果；确认成功后再标记 committed。 |
| Committed 已收到 | committed | active | 后续连接使用 IK，不再提交 Token。 |

应用存储需要让 pending identity、Server 公钥、Agent ID 和事务 ID 一起原子提交。只保存其中一部分会导致
Agent 无法判断应恢复注册、重新注册还是进入 IK。

Server 侧依次调用：

```text
accept_incoming
  -> Registration
  -> peer_public_key
  -> receive_request
  -> 业务层验证 Token 并保存 pending
  -> prepare
  -> wait_for_commit
  -> 业务层激活 Agent 并消费 Token
  -> complete
```

任何落库失败都应调用 `reject(SecureError)` 或关闭当前流，不能提前发送成功阶段。

## 后续重连：IK

首次连接断开后，Agent 恢复自己的长期 identity 和固定的 Server 公钥：

```rust
let session = client.connect(&agent_identity, server_public_key).await?;
```

IK 使用两条消息完成双方静态密钥认证，不再发送注册 Token。Server 收到
`IncomingSession::Authentication` 后，仍必须根据 `peer_public_key()` 查询注册、吊销、租户和权限：

```rust
let agent_id = store.authorize_agent(authentication.peer_public_key())?;
let session = authentication.authorize();
```

Noise 证明对端持有对应私钥，不证明该 Agent 当前仍有业务权限。

## Driver 模式

`TonicNoiseSession` 的方法使用 `&mut self`，不能让多个 Tokio task 同时调用。高频上报和命令接收使用
`SessionDriver`：

```rust
let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());

// 可克隆给不同采集任务，真正加密仍在 Driver 中串行执行。
let sender = running.handle.clone();
sender.send_task_report(report).await?;

// 只能有一个事件消费者。
while let Some(event) = running.events.recv().await {
    match event? {
        SessionEvent::JobCommand(command) => {
            let result = controller.apply(command).await;
            running.handle.send_job_command_result(result).await?;
        }
        event => handle_event(event).await?,
    }
}
```

Driver 自动处理 Ping/Pong、心跳超时、响应方 rekey 和强类型事件派发。有界命令队列和事件队列在满时
异步等待，形成反压而不是静默丢包。

`SessionHandle` 可以克隆，`SessionEventReceiver` 只能有一个 owner。应用关闭时先停止产生新上报，再
调用 handle 的 shutdown，随后等待 Driver 退出；直接丢弃所有 handle 也会结束命令来源，但无法表达有序
关闭和剩余队列处理策略。

## 手动模式

简单顺序程序可以直接独占 Session：

```rust
session.send(message).await?;
let event = session.receive_event().await?;
```

持续调用 `receive`/`receive_event` 时会自动消费 Ping/Pong 和 responder rekey。只发送不接收的调用方
必须定期执行 `perform_maintenance()`，否则不能及时检测关闭、响应控制帧或触发自动 rekey。

## 关闭与重连

远端关闭、心跳超时、gRPC 错误或 Noise 错误都会结束当前 Driver。已经加密但发送失败的帧会推进 nonce，
因此不能继续复用原 Session 或简单重发同一密文。正确做法是关闭当前流，用持久化 identity 重新执行 IK。

Protocol 当前不提供跨 Session 的 TaskReport ACK。重连只恢复身份和加密通道，不自动恢复尚未确认的业务
消息；可靠上报队列必须由 Agent 应用层实现。

推荐的连接管理循环是：读取已提交 identity 和 Server 公钥，创建一次新的 IK Session，启动 Driver，
持续消费事件；任一终止错误都销毁整个 Session，按有上限的指数退避重新连接。授权拒绝、密钥被吊销等
永久错误不能无限快速重试，应进入需要本地干预或重新注册的状态。

## 错误归属

| 错误阶段 | 示例 | 负责处理的层 |
| --- | --- | --- |
| Endpoint/TLS | DNS、证书、HTTP/2 失败 | 连接管理器重连或报告配置错误。 |
| 握手 | 超时、错误 PSK、未知 Server key | 终止流；根据本地注册状态决定重试或恢复。 |
| 授权 | Token 无效、Agent 吊销、租户拒绝 | 业务注册表，不应由 Noise 自动放行。 |
| 会话 | 心跳超时、解密失败、远端关闭 | 丢弃 Session，重新建立 IK。 |
| 业务 | revision 冲突、不支持 Task | 返回结构化结果，会话通常可以继续。 |

## 从 RPC 到业务会话的逐步调用

### Client 侧入口

`AgentProtocolClient` 是应用应该优先使用的接口。它保存 endpoint、gRPC 前缀和握手超时，
不会替调用方保存长期 identity 或数据库状态：

```rust
let mut client = AgentProtocolClient::new("https://agent.example.com");
client.set_grpc_prefix("/api/v1/grpc");
client.set_handshake_timeout(Duration::from_secs(5));

// 首次注册：prepare 返回 pending，持久化完成后再 commit。
let pending = client
    .prepare_registration(identity, &psk, credential, "edge-01".to_owned())
    .await?;
store.save_pending(&pending)?;
let registered = pending.commit().await?;
store.save_committed(&registered)?;

// 后续连接：只读取本地 identity 和已保存的 Server 公钥。
let session = client.connect(&identity, server_public_key).await?;
```

实际调用顺序是：

```text
AgentProtocolClient::prepare_registration
  -> validate_registration_token_id
  -> ClientXxHandshake::start
  -> AgentProtocolClient::open
  -> AgentTransportRpcClient::open_session
  -> next_handshake(message 2)
  -> ClientXxHandshake::receive_message2
  -> send(message 3)
  -> TonicNoiseSession::send(RegistrationRequest)
  -> TonicNoiseSession::receive(RegistrationPrepared)
  -> AgentPendingRegistration
  -> AgentPendingRegistration::commit
```

`register_agent` 是 `prepare_registration(...).commit()` 的便捷组合，适合不需要在两阶段之间
自行落库的简单调用；需要崩溃恢复的正式 Agent 应使用显式 prepare/保存/commit 顺序。

### Server 侧入口

Server 的 Tonic adapter 只处理 RPC 资源和握手前错误；握手完成后的规则集中在 Session Policy：

```text
AgentTransport::open_session(request)
  -> AgentState::try_acquire_session
  -> create bounded outbound channel
  -> ServerSessionAcceptor::accept_incoming_with_psk_resolver
       -> next_handshake(first frame)
       -> XXpsk3: AgentRegistrar::resolve_registration_psk(token_id)
       -> IK: ServerKeyRing::find_active(responder_key_id)
       -> IncomingSession::Registration / Authentication
  -> AgentServer::handle_established_session
```

握手失败时 Server 只能通过未加密的 `ProtocolError` 返回粗粒度错误；握手成功后，注册、授权和
业务错误才通过加密 `SecureError` 返回。Token、PSK、私钥和数据库错误的详细文本不会进入外层帧。

## 注册四阶段的真实状态

XXpsk3 建立加密通道后，`server_service/session.rs` 按以下顺序调用：

```text
IncomingSession::Registration
  -> AgentState::try_acquire_registration
  -> ServerRegistration::receive_request
  -> AgentRegistrar::prepare_registration
       -> ServerDatabase::load_active_registration_psk / prepare_agent_registration
       -> 写入 pending registration
  -> ServerRegistration::prepare(registration_id, agent_id)
  -> ServerRegistration::wait_for_commit(registration_id, 10s)
  -> AgentRegistrar::commit_registration
       -> 原子激活 Agent + 提交 registration + 消费 Token
  -> ServerRegistration::complete(registration_id)
  -> handle_business_messages
```

四个协议阶段的含义不同：

| 阶段 | Server 持久化状态 | Agent 可以做什么 |
| --- | --- | --- |
| Request | 尚未生成可恢复事务，或正在校验 | 等待 Prepared；失败时重新建立 XXpsk3。 |
| Prepared | 有 `registration_id`、Agent ID 和过期时间 | 必须先保存 identity、Server 公钥、Agent ID、事务 ID。 |
| Commit | Server 校验事务 ID 并原子激活 | 可在断线后查询/重试同一事务，不应生成另一 identity。 |
| Committed | Agent active，Token 已消费 | 标记本地 committed，后续使用 IK。 |

`wait_for_commit` 超时不会把 pending 直接当成成功；数据库 commit 失败也只返回固定的
`registration service is unavailable`。这样不会出现“客户端已经收到成功、Server 却没有激活”的假状态。

## IK 授权调用

IK 只证明 Agent 拥有长期私钥，业务授权仍由 Server 的注册中心完成。当前实现使用一次数据库
快照同时得到三种结果，避免先查吊销再查授权的重复查询：

```text
IncomingSession::Authentication
  -> authentication.peer_public_key()
  -> AgentRegistrar::authorize_agent(public_key)
       -> ServerDatabase::find_agent_authorization(public_key)
          -> Authorized(agent_id)
          -> Revoked
          -> Unauthorized
  -> Authorized: authentication.authorize()
  -> Revoked/Unauthorized: reject(AgentNotAuthorized)
  -> Database error: reject(Internal)
```

即使 Noise IK 成功，`active` 状态和 `revoked_at == None` 仍是进入业务循环的必要条件。授权结果
不会把数据库实体泄漏到 Protocol；`AgentAuthorizationError` 是 Server service 的稳定错误接口，
数据库 adapter 只负责返回持久化快照。

## Driver 的调用和所有权

建立好的 `TonicNoiseSession` 只能由一个 owner 推进，因为每一帧都会改变 Noise nonce、心跳时间和
rekey generation。并发业务应把它交给 `SessionDriver`：

```rust
let running = SessionDriver::spawn(session, SessionDriverConfig::default());
let handle = running.handle.clone();
let mut events = running.events;

handle.send_task_report(report).await?;
handle.send_job_command(command).await?;
handle.request_rekey().await?;

while let Some(event) = events.recv().await {
    match event? {
        SessionEvent::JobCommand(command) => {
            let result = job_controller.apply(command).await;
            handle.send_job_command_result(result).await?;
        }
        SessionEvent::TaskReport(report) => store_report(report).await?,
        SessionEvent::Messages(messages) => handle_messages(messages).await?,
        SessionEvent::KeyRotation(message) => handle_rotation(message).await?,
        SessionEvent::Registration(_) | SessionEvent::JobCommandResult(_) => {}
    }
}
```

`SessionHandle` 可以 clone，但 `SessionEventReceiver` 只有一个消费者。发送接口把业务消息放入有界
队列，Driver 在内部串行调用 `TonicNoiseSession::send`；队列满时等待形成反压。手动模式则由调用方
独占 `&mut TonicNoiseSession`，适合示例和严格顺序的流程，不适合多个采集任务共享。

## 心跳、rekey 和关闭顺序

Driver 或手动循环接收消息时，会调用 `receive` 并在需要时执行维护动作：

```text
receive / receive_event
  -> 读取 ProtocolFrame
  -> 解密 SecureMessage
  -> 消费 Ping/Pong 或 rekey 控制帧
  -> 返回 SessionEvent

perform_maintenance
  -> maintenance_status
  -> ping (interval 到期)
  -> request_rekey (initiator 达到 max_age/max_frames)
  -> HeartbeatTimeout / TransportError
```

应用关闭时应先停止新的 `send_task_report`，再调用 `SessionHandle::shutdown`，等待 Driver 退出，
最后销毁当前 Session。任何发送失败、远端关闭或心跳超时都应丢弃整条 Session；不要复用已经推进
过 nonce 的对象，也不要把同一密文重新发送到新连接。
