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
