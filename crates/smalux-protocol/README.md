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

`.proto` 是 wire 契约的唯一事实来源；业务 crate 不应自行维护重复的握手或消息结构。

## 目录结构

```text
smalux-protocol/
├── proto/smalux/agent/v1/  # 正式 wire 契约
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
| `TokenMessage` | XXpsk3 后提交一次性 Token 和返回 `agent_id`。 |
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
| `enroll(identity, psk, token, agent_name)` | 执行 XXpsk3、发送加密 Token 请求。 | Client 不需要预置 Server 公钥。 |
| `connect(identity, server_key)` | 使用固定 Server 公钥执行 IK。 | 成功后返回可持续使用的 `TonicNoiseSession`。 |
| `connect_with_candidates(identity, candidates)` | 依次尝试多把 Server 公钥。 | Server 换钥期间通常传 `PinnedServerKeys::connection_candidates()`。 |

`enroll` 返回 `EnrollmentOutcome`：

| 字段 | 含义 | 是否需要持久化 |
| --- | --- | --- |
| `agent_id` | Server 确认的业务身份。 | 是。 |
| `agent_identity` | 本次注册使用的 Agent 长期身份。 | 是，尤其是私钥。 |
| `server_public_key` | XXpsk3 认证后学到的 Server 公钥。 | 是，后续 IK 必需。 |
| `session` | 已建立的 XXpsk3 加密会话。 | 否，只在当前进程和连接内有效。 |

### 首次注册调用流程

```rust,no_run
// 引入长期 Noise 身份和封装完整 Tonic/Noise 流程的高层 Client。
use smalux_protocol::{
    noise::NoiseIdentity,
    tonic_transport::AgentProtocolClient,
};

# async fn enroll() -> Result<(), Box<dyn std::error::Error>> {
// 首次运行生成 Agent 长期静态身份；生产代码应立即加密持久化私钥。
let identity = NoiseIdentity::generate()?;
// XXpsk3 要求恰好 32 字节 PSK；示例常由一次性 Token 解码得到。
let psk = [7_u8; 32];

// endpoint 只写 scheme + authority，service path 由生成的 Tonic Client 维护。
let mut client = AgentProtocolClient::new("https://agent.example.com");
// Server 使用 Axum nest 或反向代理前缀时，Client 必须配置同一前缀。
client.set_grpc_prefix("/api/v1/grpc");

// enroll 内部完成 XXpsk3 三消息握手和加密 TokenRequest/TokenResponse。
let enrolled = client
    .enroll(
        // 方法取得身份所有权，并在成功结果中通过 agent_identity 交还。
        identity,
        // PSK 只参与首次握手，不用于后续 IK。
        &psk,
        // Token 放在 Noise ciphertext 内，不进入 URL、日志或 gRPC metadata。
        "one-time-token".to_owned(),
        // Agent 名称是业务注册标识，不代替静态公钥认证。
        "agent-001".to_owned(),
    )
    // 只有 await 成功才表示收到了 Server 的加密注册确认。
    .await?;

// 这里由调用方开启数据库事务并保存：
// enrolled.agent_id
// enrolled.agent_identity.export_private_key().as_bytes()
// enrolled.agent_identity.public_key().as_bytes()
// enrolled.server_public_key.as_bytes()
# Ok(())
# }
```

完整顺序：

```text
1. Agent 从安全渠道取得一次性 Token/PSK。
2. Agent 生成或恢复自己的 NoiseIdentity。
3. AgentProtocolClient::enroll 执行 XXpsk3 三消息握手。
4. 双方确认持有相同 PSK，Client 得到已认证的 Server 公钥。
5. Client 在 Noise 密文内发送 TokenRequest。
6. Server 保存 Agent 公钥并返回加密 TokenResponse。
7. Client 收到 EnrollmentOutcome 后才持久化 Server 公钥和 agent_id。
8. 当前注册会话可以关闭；后续连接统一使用 IK。
```

如果 Server 返回加密 `SecureError`，`enroll` 会返回
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
| `accept_session(inbound, sender, keyring, enrollment_psk)` | 读取首帧，选择 XXpsk3/IK 和对应 Server 私钥，完成握手。 |

`accept_session` 返回 `ServerPendingSession`，此时 Noise 已认证，但业务授权还没有自动完成：

| 方法 | 作用 | 使用时机 |
| --- | --- | --- |
| `handshake_mode()` | 返回 `EnrollmentXxPsk3` 或 `AuthenticatedIk`。 | 决定进入注册还是已注册会话。 |
| `peer_public_key()` | 返回握手认证得到的 Agent 静态公钥。 | 注册时写入；IK 时查询授权表。 |
| `authorize()` | 消费 pending 状态并返回 `TonicNoiseSession`。 | 业务层确认允许继续处理时。 |
| `reject(secure_error)` | 在已建立的 Noise 会话中发送加密错误。 | Token、吊销、租户或业务授权失败时。 |

推荐的 Server 处理骨架：

```rust,ignore
// accept_session 负责密码学握手，但故意不替业务层决定 Agent 是否有权限。
let pending = ServerSessionAcceptor::default()
    // inbound/sender 来自同一个 OpenSession RPC；PSK 仅在 XXpsk3 分支使用。
    .accept_session(inbound, sender, &keyring, &enrollment_psk)
    .await?;

// 握手模式决定这是首次注册还是已注册 IK。
let mode = pending.handshake_mode();
// 该公钥来自 Noise 握手认证，不能用请求 body 中自报的公钥替代。
let agent_key = pending.peer_public_key();

match mode {
    HandshakeMode::EnrollmentXxPsk3 => {
        // XXpsk3 只证明双方持有 PSK；下一步还要验证加密 TokenRequest。
        let mut session = pending.authorize();
        // receive() 读取加密 TokenRequest。
        // 数据库事务成功保存 agent_key 后，send() 返回 TokenResponse。
    }
    HandshakeMode::AuthenticatedIk => {
        // Server 用握手得到的 Agent 公钥查询注册表、吊销状态和租户授权。
        if !agent_registry.authorize(agent_key) {
            // Noise 已建立，拒绝原因应作为加密 SecureError 返回。
            pending.reject(not_authorized_error).await?;
            return Ok(());
        }
        // 业务授权成功后才消费 pending，取得可持续收发的会话。
        let mut session = pending.authorize();
        // 循环 receive()/send() 处理业务消息。
    }
}
```

握手失败时还没有安全的 Noise 会话，Server 可以调用
`TransportError::protocol_error()` 生成不包含 Token、密钥和业务内容的外层 `ProtocolError`。

## 加密会话方法

### `TonicNoiseSession`

该类型必须由一个任务顺序持有；不要把它拆给多个并发 reader/writer。若业务需要并发，应在会话外
使用 channel 汇聚消息，再由单一 actor 调用 `send` 和 `receive`。

| 方法 | 哪端调用 | 作用 |
| --- | --- | --- |
| `set_heartbeat_policy(policy)` | 两端 | 修改 Ping 间隔和失联超时。 |
| `set_rekey_policy(policy)` | 两端 | 修改自动 rekey 的时间、帧数和开关。 |
| `heartbeat_policy()` | 两端 | 读取当前心跳策略。 |
| `should_ping()` | 两端 | 判断距离上次发送是否超过心跳间隔。 |
| `heartbeat_expired()` | 两端 | 判断距离上次接收是否超过失联上限。 |
| `should_rekey()` | 两端 | 判断自动 rekey 的时间或帧数条件是否满足。 |
| `send(message)` | 两端 | Prost 编码、Noise 加密并发送一条 `SecureMessage`。 |
| `receive()` | 两端 | 等待下一条业务消息；内部处理 Ping/Pong 和 responder rekey。 |
| `ping(nonce)` | 两端 | 手动发送加密 Ping；通常无需直接调用。 |
| `request_rekey()` | Agent/initiator | 发起同步 rekey，等待 Ack 后切换双向 cipher state。 |
| `require_rekey()` | Server/responder | 通知 Agent 应发起 rekey，本身不立即切换密钥。 |
| `generation()` | 两端 | 返回当前会话 rekey 代数，初始为 0。 |
| `encrypted_frames()` | 两端 | 返回本代已处理的加密帧数，rekey 后归零。 |
| `request_agent_key_rotation(prepared)` | Agent | 发送 Agent 新静态公钥申请。 |
| `accept_agent_key_rotation(rotation_id)` | Server | 接受 Agent 静态公钥申请。 |
| `announce_server_key(prepared)` | Server | 宣布下一把 Server 静态公钥。 |
| `acknowledge_server_key(rotation_id, key_id)` | Agent | 确认已保存 Server 新公钥。 |

`HeartbeatPolicy::default()` 是 30 秒发送间隔、90 秒无入站消息超时。
`RekeyPolicy::default()` 是 1 小时或 `2^20` 个加密帧后自动 rekey，且 `automatic = true`。

`receive()` 的返回值含义：

| 返回值 | 含义 |
| --- | --- |
| `Ok(Some(message))` | 收到一条非会话控制的加密消息。 |
| `Ok(None)` | 对端正常关闭 gRPC 流。 |
| `Err(HeartbeatTimeout)` | 超过心跳失联上限。 |
| `Err(RekeyRequired)` | Server 要求 Agent 调用 `request_rekey()`。 |
| 其他 `Err` | gRPC、Noise、帧格式或远端协议错误。 |

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
| `mode` | `EnrollmentXxPsk3` 或 `AuthenticatedIk`。 |
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

- Axum HTTP 与 Tonic gRPC 共用端口；
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
