# Server WebSocket 参考实现

可以，下面给你一套更完整的“参考代码形态”。这不是要你直接复制全部，而是让你看清楚 server 侧 WS 应该怎么组织。

这份文档按三层来读：

1. 先看第 1 到第 4 节：二进制主链路的基础结构
2. 再看第 5 节：如果要兼容 `Text`，应该怎么补，不要怎么补
3. 最后看第 6 节：最终推荐实现和落地顺序

核心设计是：

1. `WebSocket` 进来后，每条连接创建一个 `AgentWsSession`
2. `AgentWsSession` 持有协议状态，包括负载模式、明文、握手中、加密中
3. `socket.split()` 后：
   - `reader` 只读 agent 发来的消息
   - `writer_task` 只负责把已经编码好的 `Message` 发出去
   - session owner loop 负责协议解析、加密、解密、业务分发
4. `server_rx` 是 server 内部下发队列，其他模块往这里发 `ServerFrame`
5. secure 加密状态必须留在 owner loop，不能放 writer task 里

关键约束：

- 一条连接一旦锁定为 `BinaryWire` 或 `TextFrame`，后续就不要混用
- `secure_psk` 只跑在 `BinaryWire` 上；文本模式默认只依赖 `WSS/TLS`
- 第三方兼容层只负责 `decode/encode -> 转成自有 frame`，不要直接写业务状态
- `writer_task` 只负责发 WS 消息，不持有 `snow::TransportState`
- 真正的协议入口不是 `Message::Binary` 或 `Message::Text`，而是“先判负载模式，再解自有 frame 或 adapter frame”
- 队列容量不要写死成 `const`，应从运行时配置读取，并在连接建立时快照

---

## 1. `service/agent/connection.rs`

```rust
//! agent 连接注册表。
//!
//! 这里只保存 server 到 agent 的下发入口，不处理协议解析。

use std::{collections::HashMap, sync::Arc};

use smalux_protocol::ServerFrame;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

/// 一条在线 agent 连接的可下发句柄。
#[derive(Clone)]
pub struct AgentConnectionHandle {
    /// 本次 WS 连接 ID，每次重连都会变化。
    pub connection_id: Uuid,
    /// agent 业务 ID，第一次收到 ClientFrame 后才能确认。
    pub agent_id: Option<String>,
    /// server -> agent 下发队列。
    pub server_tx: mpsc::Sender<ServerFrame>,
}

/// 在线连接注册表。
#[derive(Default)]
pub struct AgentConnectionRegistry {
    by_connection_id: RwLock<HashMap<Uuid, AgentConnectionHandle>>,
    by_agent_id: RwLock<HashMap<String, Uuid>>,
}

impl AgentConnectionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 注册一条新连接。
    pub async fn register(&self, handle: AgentConnectionHandle) {
        if let Some(agent_id) = handle.agent_id.clone() {
            self.by_agent_id.write().await.insert(agent_id, handle.connection_id);
        }

        self.by_connection_id
            .write()
            .await
            .insert(handle.connection_id, handle);
    }

    /// 连接拿到 agent_id 后更新索引。
    pub async fn bind_agent_id(&self, connection_id: Uuid, agent_id: String) {
        let mut connections = self.by_connection_id.write().await;

        if let Some(handle) = connections.get_mut(&connection_id) {
            handle.agent_id = Some(agent_id.clone());
            self.by_agent_id.write().await.insert(agent_id, connection_id);
        }
    }

    /// 根据 connection_id 移除连接。
    pub async fn unregister(&self, connection_id: Uuid) {
        let handle = self.by_connection_id.write().await.remove(&connection_id);

        if let Some(handle) = handle {
            if let Some(agent_id) = handle.agent_id {
                self.by_agent_id.write().await.remove(&agent_id);
            }
        }
    }

    /// 给指定 agent 下发 ServerFrame。
    pub async fn send_to_agent(&self, agent_id: &str, frame: ServerFrame) -> anyhow::Result<()> {
        let connection_id = {
            let index = self.by_agent_id.read().await;
            *index
                .get(agent_id)
                .ok_or_else(|| anyhow::anyhow!("agent is not online: {agent_id}"))?
        };

        let server_tx = {
            let connections = self.by_connection_id.read().await;
            connections
                .get(&connection_id)
                .map(|handle| handle.server_tx.clone())
                .ok_or_else(|| anyhow::anyhow!("agent connection is missing: {agent_id}"))?
        };

        server_tx.send(frame).await?;
        Ok(())
    }
}
```

---

## 2. `state.rs`

```rust
//! server 共享状态。

use std::sync::Arc;

use sea_orm::DatabaseConnection;

use crate::{
    config::model::FrontendConfig,
    service::agent::connection::AgentConnectionRegistry,
};

#[derive(Debug, Clone)]
pub struct AgentWsRuntimeConfig {
    /// writer task 队列容量。
    pub writer_queue_capacity: usize,
    /// server -> agent 下发队列容量。
    pub server_frame_queue_capacity: usize,
    /// writer 队列满时是否直接关闭连接。
    pub close_on_writer_backpressure: bool,
    /// server 下发队列满时是否优先丢弃旧消息。
    pub drop_oldest_on_server_queue_full: bool,
    /// Ping 间隔；0 表示禁用 server 主动 ping。
    pub ping_interval: std::time::Duration,
    /// Pong 超时；超过后视为连接失活。
    pub pong_timeout: std::time::Duration,
}

impl Default for AgentWsRuntimeConfig {
    fn default() -> Self {
        Self {
            writer_queue_capacity: 64,
            server_frame_queue_capacity: 128,
            close_on_writer_backpressure: true,
            drop_oldest_on_server_queue_full: false,
            ping_interval: std::time::Duration::from_secs(30),
            pong_timeout: std::time::Duration::from_secs(90),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    /// 数据库连接。
    pub database: DatabaseConnection,
    /// 前端托管配置。
    pub frontend: FrontendConfig,
    /// 在线 agent 连接注册表。
    pub agent_connections: Arc<AgentConnectionRegistry>,
    /// agent WebSocket 运行时配置。
    pub agent_ws: AgentWsRuntimeConfig,
}

impl AppState {
    pub fn new(database: DatabaseConnection, frontend: FrontendConfig) -> Self {
        Self {
            database,
            frontend,
            agent_connections: AgentConnectionRegistry::new(),
            agent_ws: AgentWsRuntimeConfig::default(),
        }
    }
}
```

---

## 3. `http/agent/ws.rs`

```rust
//! agent 主连接 WebSocket upgrade。

pub(crate) mod session;

use axum::{
    extract::{State, WebSocketUpgrade},
    response::Response,
};

use crate::state::AppState;

/// agent WS upgrade 入口。
pub async fn upgrade_agent_ws(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| async move {
        let session = session::AgentWsSession::new(socket, state);
        session.run().await;
    })
}
```

---

## 4. `http/agent/ws/session.rs`（BinaryWire 主链路）

```rust
//! agent WebSocket session。
//!
//! 每条 WS 连接创建一个 session。session owner loop 独占协议状态，
//! writer task 只负责发送已经编码好的 WS Message。

use std::time::Instant;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use smalux_protocol::{
    ClientFrame, ClientPayload, ServerFrame,
    secure::{
        build_noise_responder, decode_secure_hello, decrypt_payload, encrypt_payload,
        read_handshake_message, write_handshake_message,
    },
    wire::{WirePacket, WirePacketKind, decode_wire_packet, encode_wire_packet},
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    service::agent::connection::AgentConnectionHandle,
    state::AppState,
};

/// 当前连接的安全状态。
enum AgentConnectionSecurity {
    /// 明文模式，只用于开发、调试或明确允许的部署。
    Plain,
    /// secure_psk 握手中。
    Handshaking {
        key_id: String,
        handshake: snow::HandshakeState,
        session_id: [u8; 16],
    },
    /// secure_psk 已完成，后续业务 payload 都要加密解密。
    Secure {
        key_id: String,
        transport: snow::TransportState,
        session_id: [u8; 16],
    },
}

/// 一条 agent WS 连接的上下文。
struct AgentConnectionContext {
    /// 本次连接 ID，每次重连都会变化。
    connection_id: Uuid,
    /// agent 业务 ID，从 ClientFrame.agent_id 得到。
    agent_id: Option<String>,
    /// 当前安全状态。
    security: AgentConnectionSecurity,
    /// 最近一次收到 agent 消息的时间。
    last_seen_at: Instant,
    /// 最近收到的 client frame sequence。
    last_client_sequence: u64,
    /// server 下发 wire sequence。
    next_wire_sequence: u64,
}

impl AgentConnectionContext {
    fn new() -> Self {
        Self {
            connection_id: Uuid::new_v4(),
            agent_id: None,
            security: AgentConnectionSecurity::Plain,
            last_seen_at: Instant::now(),
            last_client_sequence: 0,
            next_wire_sequence: 1,
        }
    }

    fn next_sequence(&mut self) -> u64 {
        let sequence = self.next_wire_sequence;
        self.next_wire_sequence += 1;
        sequence
    }
}

/// agent WS session。
pub(crate) struct AgentWsSession {
    socket: WebSocket,
    state: AppState,
    context: AgentConnectionContext,
    server_tx: mpsc::Sender<ServerFrame>,
    server_rx: mpsc::Receiver<ServerFrame>,
}

impl AgentWsSession {
    pub fn new(socket: WebSocket, state: AppState) -> Self {
        let (server_tx, server_rx) = mpsc::channel(state.agent_ws.server_frame_queue_capacity);

        Self {
            socket,
            state,
            context: AgentConnectionContext::new(),
            server_tx,
            server_rx,
        }
    }

    /// 运行一条 WS 连接。
    pub async fn run(mut self) {
        let connection_id = self.context.connection_id;

        self.state
            .agent_connections
            .register(AgentConnectionHandle {
                connection_id,
                agent_id: None,
                server_tx: self.server_tx.clone(),
            })
            .await;

        tracing::info!(%connection_id, "agent websocket session started");

        let (mut ws_writer, mut ws_reader) = self.socket.split();
        let (writer_tx, mut writer_rx) =
            mpsc::channel::<Message>(self.state.agent_ws.writer_queue_capacity);

        let writer_task = tokio::spawn(async move {
            while let Some(message) = writer_rx.recv().await {
                if let Err(error) = ws_writer.send(message).await {
                    tracing::warn!(error = %error, "agent websocket writer failed");
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                maybe_message = ws_reader.next() => {
                    let Some(message) = maybe_message else {
                        tracing::info!(%connection_id, "agent websocket reader ended");
                        break;
                    };

                    match message {
                        Ok(Message::Binary(bytes)) => {
                            if let Err(error) = self.handle_binary(bytes.as_ref(), &writer_tx).await {
                                tracing::warn!(%connection_id, error = %error, "agent binary message failed");
                                break;
                            }
                        }
                        Ok(Message::Text(text)) => {
                            tracing::debug!(%connection_id, bytes = text.len(), "agent text message ignored");
                        }
                        Ok(Message::Ping(data)) => {
                            if writer_tx.send(Message::Pong(data)).await.is_err() {
                                tracing::warn!(%connection_id, "agent pong queue closed");
                                break;
                            }
                        }
                        Ok(Message::Pong(_)) => {
                            self.context.last_seen_at = Instant::now();
                            tracing::trace!(%connection_id, "agent pong received");
                        }
                        Ok(Message::Close(close)) => {
                            tracing::info!(%connection_id, ?close, "agent websocket close received");
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(%connection_id, error = %error, "agent websocket read failed");
                            break;
                        }
                    }
                }

                maybe_frame = self.server_rx.recv() => {
                    let Some(frame) = maybe_frame else {
                        tracing::info!(%connection_id, "agent server frame channel closed");
                        break;
                    };

                    if let Err(error) = self.send_server_frame(frame, &writer_tx).await {
                        tracing::warn!(%connection_id, error = %error, "send server frame failed");
                        break;
                    }
                }
            }
        }

        self.state.agent_connections.unregister(connection_id).await;
        drop(writer_tx);

        if let Err(error) = writer_task.await {
            tracing::warn!(%connection_id, error = %error, "agent websocket writer task join failed");
        }

        tracing::info!(%connection_id, "agent websocket session stopped");
    }

    /// 处理 agent 发来的 Binary 消息。
    async fn handle_binary(
        &mut self,
        bytes: &[u8],
        writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let packet = decode_wire_packet(bytes)?;

        self.context.last_seen_at = Instant::now();

        tracing::trace!(
            connection_id = %self.context.connection_id,
            wire_kind = packet.kind.as_str(),
            wire_sequence = packet.sequence,
            payload_bytes = packet.payload.len(),
            "agent wire packet received"
        );

        match packet.kind {
            WirePacketKind::PlainData => {
                let frame = smalux_protocol::decode_client_frame_bytes(&packet.payload)?;
                self.handle_client_frame(frame).await?;
            }
            WirePacketKind::Hello => {
                self.handle_secure_hello(packet, writer_tx).await?;
            }
            WirePacketKind::Handshake => {
                self.handle_secure_handshake(packet, writer_tx).await?;
            }
            WirePacketKind::SecureData => {
                let payload = self.decrypt_secure_payload(&packet.payload)?;
                let frame = smalux_protocol::decode_client_frame_bytes(&payload)?;
                self.handle_client_frame(frame).await?;
            }
            WirePacketKind::Close => {
                tracing::info!(
                    connection_id = %self.context.connection_id,
                    "agent wire close received"
                );
            }
        }

        Ok(())
    }

    /// 处理 secure hello。
    async fn handle_secure_hello(
        &mut self,
        packet: WirePacket,
        writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let hello = decode_secure_hello(&packet.payload)?;

        tracing::debug!(
            connection_id = %self.context.connection_id,
            key_id = %hello.key_id,
            "agent secure hello received"
        );

        // TODO: 从数据库或缓存根据 key_id 查 secret。
        // server 端不要保存 agent 传来的完整 token，只保存 key_id 对应的 secret。
        let psk = self.lookup_secure_psk(&hello.key_id).await?;

        let mut handshake = build_noise_responder(&psk)?;
        let response = write_handshake_message(&mut handshake, b"")?;

        self.context.security = AgentConnectionSecurity::Handshaking {
            key_id: hello.key_id,
            handshake,
            session_id: packet.session_id,
        };

        let response_packet = WirePacket::new(
            WirePacketKind::Handshake,
            packet.session_id,
            self.context.next_sequence(),
            response,
        );

        let bytes = encode_wire_packet(&response_packet)?;
        writer_tx.send(Message::Binary(bytes.into())).await?;

        Ok(())
    }

    /// 处理 secure handshake 第二阶段。
    async fn handle_secure_handshake(
        &mut self,
        packet: WirePacket,
        _writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let current = std::mem::replace(&mut self.context.security, AgentConnectionSecurity::Plain);

        let AgentConnectionSecurity::Handshaking {
            key_id,
            mut handshake,
            session_id,
        } = current
        else {
            anyhow::bail!("secure handshake received in invalid state");
        };

        if packet.session_id != session_id {
            anyhow::bail!("secure handshake session id mismatch");
        }

        read_handshake_message(&mut handshake, &packet.payload)?;
        let transport = handshake.into_transport_mode()?;

        self.context.security = AgentConnectionSecurity::Secure {
            key_id,
            transport,
            session_id,
        };

        tracing::info!(
            connection_id = %self.context.connection_id,
            "agent secure handshake completed"
        );

        Ok(())
    }

    /// 解密 secure data。
    fn decrypt_secure_payload(&mut self, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        let AgentConnectionSecurity::Secure { transport, .. } = &mut self.context.security else {
            anyhow::bail!("secure data received before secure handshake completed");
        };

        decrypt_payload(transport, payload)
    }

    /// 处理已经解出来的 ClientFrame。
    async fn handle_client_frame(&mut self, frame: ClientFrame) -> anyhow::Result<()> {
        self.context.last_client_sequence = frame.sequence;

        if self.context.agent_id.as_deref() != Some(frame.agent_id.as_str()) {
            self.context.agent_id = Some(frame.agent_id.clone());

            self.state
                .agent_connections
                .bind_agent_id(self.context.connection_id, frame.agent_id.clone())
                .await;
        }

        match frame.payload {
            ClientPayload::Heartbeat { heartbeat } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    sequence = frame.sequence,
                    last_report_sequence = ?heartbeat.last_report_sequence,
                    "agent heartbeat received"
                );
            }
            ClientPayload::Snapshot { snapshot } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    sequence = frame.sequence,
                    "agent snapshot received"
                );

                // TODO: 写入最新完整快照。
                let _ = snapshot;
            }
            ClientPayload::Delta { delta } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    sequence = frame.sequence,
                    "agent delta received"
                );

                // TODO: 合并增量并写入最新状态。
                let _ = delta;
            }
            ClientPayload::JobResult { result } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    sequence = frame.sequence,
                    "agent job result received"
                );

                // TODO: 保存 probe/job 结果。
                let _ = result;
            }
            ClientPayload::RemoteTaskResult { result } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    task_id = %result.task_id,
                    status = ?result.status,
                    "agent remote task result received"
                );
            }
            ClientPayload::ShellStream { event } => {
                tracing::trace!(
                    agent_id = %frame.agent_id,
                    sequence = frame.sequence,
                    "agent shell stream event received"
                );

                // TODO: 转发给前端终端 WS。
                let _ = event;
            }
            ClientPayload::Ack { ack } => {
                tracing::debug!(
                    agent_id = %frame.agent_id,
                    ack_sequence = ack.sequence,
                    "agent ack received"
                );
            }
            ClientPayload::Error { error } => {
                tracing::warn!(
                    agent_id = %frame.agent_id,
                    error_code = %error.code,
                    error_sequence = ?error.sequence,
                    "agent protocol error received"
                );
            }
        }

        Ok(())
    }

    /// 下发 ServerFrame。
    async fn send_server_frame(
        &mut self,
        frame: ServerFrame,
        writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let payload = smalux_protocol::encode_server_frame_bytes(&frame)?;

        let packet = match &mut self.context.security {
            AgentConnectionSecurity::Plain => {
                WirePacket::plain_data([0u8; 16], self.context.next_sequence(), payload)
            }
            AgentConnectionSecurity::Secure {
                transport,
                session_id,
                ..
            } => {
                let encrypted = encrypt_payload(transport, &payload)?;
                WirePacket::secure_data(*session_id, self.context.next_sequence(), encrypted)
            }
            AgentConnectionSecurity::Handshaking { .. } => {
                anyhow::bail!("cannot send server frame during secure handshake");
            }
        };

        let bytes = encode_wire_packet(&packet)?;
        writer_tx.send(Message::Binary(bytes.into())).await?;

        tracing::trace!(
            connection_id = %self.context.connection_id,
            wire_kind = packet.kind.as_str(),
            "server frame sent to agent"
        );

        Ok(())
    }

    /// 根据 key_id 查 server 保存的 PSK。
    async fn lookup_secure_psk(&self, key_id: &str) -> anyhow::Result<[u8; smalux_protocol::secure::PSK_LEN]> {
        // TODO: 这里应该从 DB 查询 key_id 对应的 secret，然后用 protocol 里的相同 HKDF 派生。
        // 当前只是占位，不能用于生产。
        let _ = key_id;
        anyhow::bail!("secure key lookup is not implemented")
    }
}
```

---

这里最重要的是这几个点：

- `Message::Binary(bytes)` 收到的永远是 wire packet，不是 `ClientFrame`
- 明文流程是：`Binary -> WirePacket::PlainData -> ClientFrame`
- 加密流程是：`Binary -> WirePacket::SecureData -> decrypt -> ClientFrame`
- server 下发流程是：`ServerFrame -> JSON bytes -> encrypt 可选 -> WirePacket -> Binary`
- `_server_tx` 要变成 `server_tx` 字段，并注册到 `AgentConnectionRegistry`
- writer task 不能自己加密，因为 Noise `TransportState` 有顺序状态，必须由 session owner loop 独占
- 队列容量应该来自 `AppState.agent_ws`，不要在 session 文件里写死常量

你现在可以先只实现 `PlainData`，把 `Hello/Handshake/SecureData` 留 TODO。等明文链路跑通，再接 secure。

---

## 5. Text 模式补全版

上面的主示例把 `Message::Text(...)` 暂时忽略了。如果你要把文本模式也补齐，建议不要直接“文本和二进制混着收发”而不做约束，而是给每条连接增加一个“负载模式锁”。

原因有两个：

1. server 下发时必须知道对端希望收到 `Message::Binary` 还是 `Message::Text`
2. secure_psk 只适合跑在二进制 wire packet 上，文本模式一般只走 JSON + WSS/TLS

推荐再加一个模式枚举：

```rust
/// 当前连接实际采用的负载模式。
enum AgentWsPayloadMode {
    /// 连接刚建立，还没看到第一条业务消息。
    Unknown,
    /// 自有协议二进制模式：WS Binary -> WirePacket -> ClientFrame。
    BinaryWire,
    /// 自有协议文本模式：WS Text -> JSON ClientFrame。
    TextFrame,
}
```

然后把它挂到 `AgentConnectionContext`：

```rust
struct AgentConnectionContext {
    connection_id: Uuid,
    agent_id: Option<String>,
    payload_mode: AgentWsPayloadMode,
    security: AgentConnectionSecurity,
    last_seen_at: Instant,
    last_client_sequence: u64,
    next_wire_sequence: u64,
}

impl AgentConnectionContext {
    fn new() -> Self {
        Self {
            connection_id: Uuid::new_v4(),
            agent_id: None,
            payload_mode: AgentWsPayloadMode::Unknown,
            security: AgentConnectionSecurity::Plain,
            last_seen_at: Instant::now(),
            last_client_sequence: 0,
            next_wire_sequence: 1,
        }
    }
}
```

再加两个辅助方法，避免一会儿收到 text、一会儿又收到 binary：

```rust
impl AgentWsSession {
    fn lock_binary_mode(&mut self) -> anyhow::Result<()> {
        match self.context.payload_mode {
            AgentWsPayloadMode::Unknown => {
                self.context.payload_mode = AgentWsPayloadMode::BinaryWire;
                Ok(())
            }
            AgentWsPayloadMode::BinaryWire => Ok(()),
            AgentWsPayloadMode::TextFrame => {
                anyhow::bail!("binary message received after text mode was locked")
            }
        }
    }

    fn lock_text_mode(&mut self) -> anyhow::Result<()> {
        match self.context.payload_mode {
            AgentWsPayloadMode::Unknown => {
                self.context.payload_mode = AgentWsPayloadMode::TextFrame;
                Ok(())
            }
            AgentWsPayloadMode::TextFrame => Ok(()),
            AgentWsPayloadMode::BinaryWire => {
                anyhow::bail!("text message received after binary mode was locked")
            }
        }
    }
}
```

### `run()` 里的 `Message::Text(...)`

把原来忽略 text 的分支改成这样：

```rust
match message {
    Ok(Message::Binary(bytes)) => {
        if let Err(error) = self.handle_binary(bytes.as_ref(), &writer_tx).await {
            tracing::warn!(%connection_id, error = %error, "agent binary message failed");
            break;
        }
    }
    Ok(Message::Text(text)) => {
        if let Err(error) = self.handle_text(text.as_str(), &writer_tx).await {
            tracing::warn!(%connection_id, error = %error, "agent text message failed");
            break;
        }
    }
    Ok(Message::Ping(data)) => {
        if writer_tx.send(Message::Pong(data)).await.is_err() {
            tracing::warn!(%connection_id, "agent pong queue closed");
            break;
        }
    }
    Ok(Message::Pong(_)) => {
        self.context.last_seen_at = Instant::now();
        tracing::trace!(%connection_id, "agent pong received");
    }
    Ok(Message::Close(close)) => {
        tracing::info!(%connection_id, ?close, "agent websocket close received");
        break;
    }
    Err(error) => {
        tracing::warn!(%connection_id, error = %error, "agent websocket read failed");
        break;
    }
}
```

### `handle_text()`

文本模式推荐按这个顺序处理：

1. 刷新 `last_seen_at`
2. 锁定连接为 `TextFrame`
3. 尝试按自有协议 JSON `ClientFrame` 解码
4. 如果你要兼容第三方文本协议，再走 adapter 分支

```rust
impl AgentWsSession {
    async fn handle_text(
        &mut self,
        text: &str,
        writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        self.context.last_seen_at = Instant::now();
        self.lock_text_mode()?;

        if text.trim().is_empty() {
            anyhow::bail!("empty text frame is not allowed");
        }

        tracing::trace!(
            connection_id = %self.context.connection_id,
            text_bytes = text.len(),
            "agent text frame received"
        );

        if let Ok(frame) = smalux_protocol::decode_client_frame(text) {
            self.handle_client_frame(frame).await?;
            return Ok(());
        }

        self.handle_text_adapter_message(text, writer_tx).await
    }
}
```

### 第三方文本协议 adapter 钩子

如果以后要兼容 `komari` 这类文本协议，不要把兼容逻辑直接塞进 `handle_text()`，而是留一个转换入口：

```rust
impl AgentWsSession {
    async fn handle_text_adapter_message(
        &mut self,
        text: &str,
        writer_tx: &mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let _ = writer_tx;

        // 这里推荐做成：
        // 1. 尝试 komari text/json -> 转成一个或多个 ClientFrame
        // 2. 逐个调用 self.handle_client_frame(frame).await
        // 3. 如果对方需要文本响应，也在这里返回 Message::Text
        //
        // 例如：
        // let frames = crate::service::agent::adapter::komari::decode_text_message(text)?;
        // for frame in frames {
        //     self.handle_client_frame(frame).await?;
        // }

        anyhow::bail!("unsupported text message format")
    }
}
```

这样你的层次就是：

- 自有文本协议：`Text -> decode_client_frame -> handle_client_frame`
- 第三方文本协议：`Text -> adapter decode -> ClientFrame -> handle_client_frame`

第三方兼容层仍然只负责“翻译”，不直接碰你的核心业务处理。

### `handle_binary()` 前先锁模式

二进制分支也要加模式锁：

```rust
async fn handle_binary(
    &mut self,
    bytes: &[u8],
    writer_tx: &mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    self.context.last_seen_at = Instant::now();
    self.lock_binary_mode()?;

    let packet = decode_wire_packet(bytes)?;

    tracing::trace!(
        connection_id = %self.context.connection_id,
        wire_kind = packet.kind.as_str(),
        wire_sequence = packet.sequence,
        payload_bytes = packet.payload.len(),
        "agent wire packet received"
    );

    match packet.kind {
        WirePacketKind::PlainData => {
            let frame = smalux_protocol::decode_client_frame_bytes(&packet.payload)?;
            self.handle_client_frame(frame).await?;
        }
        WirePacketKind::Hello => {
            self.handle_secure_hello(packet, writer_tx).await?;
        }
        WirePacketKind::Handshake => {
            self.handle_secure_handshake(packet, writer_tx).await?;
        }
        WirePacketKind::SecureData => {
            let payload = self.decrypt_secure_payload(&packet.payload)?;
            let frame = smalux_protocol::decode_client_frame_bytes(&payload)?;
            self.handle_client_frame(frame).await?;
        }
        WirePacketKind::Close => {
            tracing::info!(
                connection_id = %self.context.connection_id,
                "agent wire close received"
            );
        }
    }

    Ok(())
}
```

### 文本模式下如何下发 `ServerFrame`

如果连接已经锁成 `TextFrame`，下发就不能再走 `WirePacket`，而要直接发 JSON 文本：

```rust
async fn send_server_frame(
    &mut self,
    frame: ServerFrame,
    writer_tx: &mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    match self.context.payload_mode {
        AgentWsPayloadMode::Unknown => {
            anyhow::bail!("cannot send server frame before payload mode is known");
        }
        AgentWsPayloadMode::TextFrame => {
            let text = smalux_protocol::encode_server_frame(&frame)?;
            writer_tx.send(Message::Text(text.into())).await?;

            tracing::trace!(
                connection_id = %self.context.connection_id,
                frame_sequence = frame.sequence,
                "server text frame sent to agent"
            );

            Ok(())
        }
        AgentWsPayloadMode::BinaryWire => {
            let payload = smalux_protocol::encode_server_frame_bytes(&frame)?;

            let packet = match &mut self.context.security {
                AgentConnectionSecurity::Plain => {
                    WirePacket::plain_data([0u8; 16], self.context.next_sequence(), payload)
                }
                AgentConnectionSecurity::Secure {
                    transport,
                    session_id,
                    ..
                } => {
                    let encrypted = encrypt_payload(transport, &payload)?;
                    WirePacket::secure_data(*session_id, self.context.next_sequence(), encrypted)
                }
                AgentConnectionSecurity::Handshaking { .. } => {
                    anyhow::bail!("cannot send server frame during secure handshake");
                }
            };

            let bytes = encode_wire_packet(&packet)?;
            writer_tx.send(Message::Binary(bytes.into())).await?;

            tracing::trace!(
                connection_id = %self.context.connection_id,
                wire_kind = packet.kind.as_str(),
                "server binary frame sent to agent"
            );

            Ok(())
        }
    }
}
```

### 队列容量如何做成后台动态配置

`writer_queue_capacity` 和 `server_frame_queue_capacity` 这两个值，本质上属于 server 的运行时背压策略，不应该写死在源码里。

推荐做法：

1. 把容量放到 server 可修改的运行时配置里，例如 `AppState.agent_ws`
2. 每次建立新连接时，从当前运行时配置读取一次
3. 该值在当前 session 生命周期内保持不变

也就是说，“后台动态设置”更准确的语义应该是：

- 修改后对新连接立即生效
- 已建立连接默认不在线热改

原因是 `tokio::sync::mpsc::channel(capacity)` 的容量在创建后就是固定的，不能原地扩容。

如果你一定要让旧连接也同步新容量，不建议直接硬改 channel，而应当走“重建队列”的方案：

1. 创建新队列
2. 切换 `server_tx` 持有的发送端引用
3. 明确旧队列里未发送消息怎么处理
4. 处理切换窗口里的并发发送

这套复杂度明显更高，所以第一版建议只做“新连接吃新配置”。如果后台改完后你希望所有连接统一生效，最稳的是 server 主动关闭旧连接，让 agent 重连。

### 哪些参数适合后台动态设置

如果你希望“尽量不重启就能改”，不要把所有字段都粗暴塞进一个普通结构体里。更稳的做法是先按**生效机制**分层，再决定后台怎么改。

推荐分成四类：

#### 1. 可立即热更新，并影响现有连接

这类参数不依赖重建 channel，也不依赖重建 `snow::TransportState`，最适合后台直接修改后立即广播到活跃 session：

- `close_on_writer_backpressure`
- `drop_oldest_on_server_queue_full`
- `ping_interval`
- `pong_timeout`
- `idle_timeout`
- `max_frame_bytes`
- `close_on_protocol_error`
- `close_on_decode_error`

这类参数的共同点是：

1. session loop 每轮都可能会读到它们
2. 修改后不需要重建 writer/read loop
3. 对现有连接立即生效是合理且可预期的

推荐做法：

- 后台写入一份共享运行时配置
- 用 `watch` 或共享只读快照把变更推给所有活跃 session
- session 在 `select!` 循环里按需刷新本地 live view

#### 2. 后台可改，但只影响新连接

这类参数应该允许后台修改，但默认只让**新建连接**吃到新值：

- `writer_queue_capacity`
- `server_frame_queue_capacity`
- `max_pending_shell_frames`
- `max_pending_task_frames`
- 其他任何会参与 `mpsc::channel(capacity)` 初始化的容量字段

原因很直接：

1. `tokio::sync::mpsc::channel(capacity)` 创建后容量固定
2. 旧 session 已经持有发送端和接收端
3. 原地替换队列要处理切换窗口、未发送消息迁移、并发 send 冲突

所以这类字段的后台修改语义应该明确写成：

- 保存后立即生效于新连接
- 现有连接保持原值
- 如需统一生效，server 主动踢旧连接或等其自然重连

#### 3. 后台可改，但需要显式“重连/重建会话”才能生效

这类参数不是完全不能后台改，而是不适合对活跃连接立即热切换：

- 是否允许 `TextFrame`
- 是否允许某个 adapter
- 是否要求 secure 握手
- secure 握手版本或握手策略
- payload mode 锁定策略

它们更准确的语义不是“不可改”，而是：

- 后台可以改配置源
- 新连接立即按新策略工作
- 旧连接不强制热切换
- 如要统一切换，走“通知 + 主动断开重连”流程

#### 4. 不建议作为普通后台开关

这类字段往往属于部署级、兼容级或运维级策略，不建议放进普通后台页面给人频繁点：

- protocol / adapter 路由策略
- 某条路由是否接收第三方兼容协议
- 默认 secure_psk 提供器选择
- WS 路径级接入模式

这些更适合：

- 启动参数
- 环境变量
- 专门的管理接口
- 或需要更高权限的运维配置页

### 推荐的运行时配置模型

如果你后面打算把“后台可改参数”做完整，建议把配置拆成四层，而不是只保留一个 `AppState.agent_ws`：

```rust
#[derive(Debug, Clone)]
pub struct AgentWsRuntimeConfig {
    pub writer_queue_capacity: usize,
    pub server_frame_queue_capacity: usize,
    pub close_on_writer_backpressure: bool,
    pub drop_oldest_on_server_queue_full: bool,
    pub ping_interval: std::time::Duration,
    pub pong_timeout: std::time::Duration,
    pub idle_timeout: std::time::Duration,
    pub max_frame_bytes: usize,
    pub close_on_protocol_error: bool,
    pub close_on_decode_error: bool,
}

#[derive(Debug, Clone)]
pub struct AgentWsSessionConfigSnapshot {
    pub writer_queue_capacity: usize,
    pub server_frame_queue_capacity: usize,
    pub max_pending_shell_frames: usize,
    pub max_pending_task_frames: usize,
}

#[derive(Debug, Clone)]
pub struct AgentWsLiveRuntimeView {
    pub close_on_writer_backpressure: bool,
    pub drop_oldest_on_server_queue_full: bool,
    pub ping_interval: std::time::Duration,
    pub pong_timeout: std::time::Duration,
    pub idle_timeout: std::time::Duration,
    pub max_frame_bytes: usize,
    pub close_on_protocol_error: bool,
    pub close_on_decode_error: bool,
}

#[derive(Debug, Default, Clone)]
pub struct AgentWsRuntimeConfigPatch {
    pub writer_queue_capacity: Option<usize>,
    pub server_frame_queue_capacity: Option<usize>,
    pub close_on_writer_backpressure: Option<bool>,
    pub drop_oldest_on_server_queue_full: Option<bool>,
    pub ping_interval: Option<std::time::Duration>,
    pub pong_timeout: Option<std::time::Duration>,
    pub idle_timeout: Option<std::time::Duration>,
    pub max_frame_bytes: Option<usize>,
    pub close_on_protocol_error: Option<bool>,
    pub close_on_decode_error: Option<bool>,
}
```

再配两层共享持有：

```rust
pub type SharedAgentWsRuntimeConfig =
    std::sync::Arc<tokio::sync::RwLock<AgentWsRuntimeConfig>>;

pub type AgentWsLiveConfigTx =
    tokio::sync::watch::Sender<std::sync::Arc<AgentWsLiveRuntimeView>>;

pub type AgentWsLiveConfigRx =
    tokio::sync::watch::Receiver<std::sync::Arc<AgentWsLiveRuntimeView>>;
```

推荐职责如下：

1. `AgentWsRuntimeConfig`
   - 保存“后台当前完整值”
   - 适合落库、落缓存、做后台展示

2. `AgentWsRuntimeConfigPatch`
   - 后台 PATCH 请求的载体
   - 只传有变更的字段
   - 方便做局部更新，不会误覆盖未提交字段

3. `AgentWsSessionConfigSnapshot`
   - 在 `AgentWsSession::new()` 时生成
   - 专门放那些“只初始化一次”的字段

4. `AgentWsLiveRuntimeView`
   - 给活跃 session 热更新读取
   - 不放 queue capacity 这种不能原地改的值

### 后台修改时推荐的处理流程

不要让后台直接改 `AppState` 里的裸结构体。推荐统一走一个“校验 -> 合并 -> 持久化 -> 广播”的流程：

```text
后台 PATCH 请求
  -> 反序列化 AgentWsRuntimeConfigPatch
  -> 读取当前 AgentWsRuntimeConfig
  -> 校验 patch 合法性
  -> merge 到新配置
  -> 持久化到 DB / 配置存储
  -> 生成新的 AgentWsLiveRuntimeView
  -> watch::Sender 广播给活跃 session
  -> 返回“哪些字段立即生效，哪些字段仅新连接生效”
```

这里最重要的是最后一步。后台响应里最好明确带出两组字段：

- `applied_to_live_sessions`
- `applied_on_next_connection`

不然使用者会误以为改完就一定立即影响全部连接。

### 推荐的热更机制拆分

如果你希望“类似参数尽量都能后台设置”，推荐按下面这个机制做，不要混：

#### 1. snapshot on connect

典型字段：

- `writer_queue_capacity`
- `server_frame_queue_capacity`
- `max_pending_shell_frames`
- `max_pending_task_frames`

特点：

- 后台能改
- 新连接吃新值
- 活跃连接不重建内部队列

#### 2. watch-driven runtime refresh

典型字段：

- `ping_interval`
- `pong_timeout`
- `idle_timeout`
- `close_on_writer_backpressure`
- `drop_oldest_on_server_queue_full`
- `close_on_protocol_error`
- `close_on_decode_error`
- `max_frame_bytes`

特点：

- 后台改完即可广播
- 活跃 session 不需要重连
- loop 内根据最新 live view 决定行为

#### 3. requires reconnect

典型字段：

- `allow_text_frame`
- `require_secure_handshake`
- `adapter_enable_komari`
- `payload_mode_lock_policy`

特点：

- 配置源可以后台改
- 但旧连接默认不热切
- 需要显式断线重连或等自然重连

#### 4. not runtime mutable

典型字段：

- `/api/v1/agents/connect` 是否启用某兼容模式
- 某条路径绑定哪个 adapter
- 某类 secure provider 的装配方式

这类字段本质更接近“服务装配”，不应混进普通的 WS 运行参数面板。

### 为什么不建议所有字段都做成“立刻影响旧连接”

你想要的是“不重启就能改”，这没问题，但不要进一步变成“所有字段都强行热切到所有旧连接”。

原因有三个：

1. 会让 session 内部状态切换过于复杂
2. 会增加竞态条件，特别是队列和握手状态
3. 后台用户也很难预测改动到底何时、以什么方式生效

第一版最稳的边界应该是：

- 能热更的字段就真正热更
- 不能热更的字段就明确写成“新连接生效”
- 需要统一切换时，由 server 控制旧连接重连

这个边界既可维护，也方便以后继续扩展。

### 后台配置接口建议

如果你后面准备把这套运行时参数真正交给后台修改，建议不要做成“写数据库后等重启生效”，而是直接定义清楚运行时管理接口。

推荐最小接口集：

1. `GET /api/v1/web/runtime/agent-ws`
   - 返回当前完整配置
   - 返回字段默认值、当前值、生效范围

2. `PATCH /api/v1/web/runtime/agent-ws`
   - 接收局部 patch
   - 修改当前运行时配置
   - 广播可热更字段
   - 返回本次实际生效结果

3. `POST /api/v1/web/runtime/agent-ws/reconnect-all`
   - 可选
   - 在你希望“让只影响新连接的字段也统一生效”时使用
   - 本质是 server 主动关闭活跃 agent WS，让 agent 自动重连

4. `GET /api/v1/web/runtime/agent-ws/schema`
   - 可选
   - 给后台页面或管理端拉取字段说明、取值范围、默认值、是否热更

如果你不想把 schema 单独做成接口，也可以把 schema 一起塞进 `GET /runtime/agent-ws` 的响应里。

### `GET /api/v1/web/runtime/agent-ws` 返回建议

```jsonc
{
  "config": {
    "writer_queue_capacity": 64,
    "server_frame_queue_capacity": 128,
    "max_pending_shell_frames": 128,
    "max_pending_task_frames": 128,
    "close_on_writer_backpressure": true,
    "drop_oldest_on_server_queue_full": false,
    "ping_interval_ms": 30000,
    "pong_timeout_ms": 90000,
    "idle_timeout_ms": 300000,
    "max_frame_bytes": 1048576,
    "close_on_protocol_error": true,
    "close_on_decode_error": true,
    "allow_text_frame": false,
    "require_secure_handshake": true,
    "adapter_enable_komari": false,
    "payload_mode_lock_policy": "first-frame"
  },
  "meta": {
    "version": 17,
    "updated_at": "2026-06-28T12:00:00Z",
    "updated_by": "admin"
  },
  "apply_rules": {
    "live_fields": [
      "close_on_writer_backpressure",
      "drop_oldest_on_server_queue_full",
      "ping_interval_ms",
      "pong_timeout_ms",
      "idle_timeout_ms",
      "max_frame_bytes",
      "close_on_protocol_error",
      "close_on_decode_error"
    ],
    "next_connection_fields": [
      "writer_queue_capacity",
      "server_frame_queue_capacity",
      "max_pending_shell_frames",
      "max_pending_task_frames"
    ],
    "reconnect_required_fields": [
      "allow_text_frame",
      "require_secure_handshake",
      "adapter_enable_komari",
      "payload_mode_lock_policy"
    ]
  }
}
```

这里建议：

- 时间统一返回 UTC RFC3339
- interval / timeout 统一用毫秒字段，不要一部分秒、一部分字符串 duration
- `apply_rules` 直接告诉后台每个字段属于哪一类

### `PATCH /api/v1/web/runtime/agent-ws` 请求建议

只允许局部更新，不要要求前端每次提交全量配置。

```jsonc
{
  "writer_queue_capacity": 128,
  "ping_interval_ms": 15000,
  "pong_timeout_ms": 45000,
  "drop_oldest_on_server_queue_full": true
}
```

建议规则：

1. 缺失字段表示“不修改”
2. 显式传 `null` 不建议支持，除非你真有“恢复默认值”的语义
3. 所有数值字段先做范围校验，再 merge
4. 合并后要再次做交叉校验

### `PATCH` 响应建议

后台改配置后，不要只回一个 `200 ok`。要明确告诉调用方：

1. 哪些字段已更新
2. 哪些字段立即影响现有连接
3. 哪些字段只会在新连接生效
4. 是否建议执行 reconnect-all

```jsonc
{
  "config": {
    "writer_queue_capacity": 128,
    "server_frame_queue_capacity": 128,
    "max_pending_shell_frames": 128,
    "max_pending_task_frames": 128,
    "close_on_writer_backpressure": true,
    "drop_oldest_on_server_queue_full": true,
    "ping_interval_ms": 15000,
    "pong_timeout_ms": 45000,
    "idle_timeout_ms": 300000,
    "max_frame_bytes": 1048576,
    "close_on_protocol_error": true,
    "close_on_decode_error": true,
    "allow_text_frame": false,
    "require_secure_handshake": true,
    "adapter_enable_komari": false,
    "payload_mode_lock_policy": "first-frame"
  },
  "applied": {
    "updated_fields": [
      "writer_queue_capacity",
      "drop_oldest_on_server_queue_full",
      "ping_interval_ms",
      "pong_timeout_ms"
    ],
    "applied_to_live_sessions": [
      "drop_oldest_on_server_queue_full",
      "ping_interval_ms",
      "pong_timeout_ms"
    ],
    "applied_on_next_connection": [
      "writer_queue_capacity"
    ],
    "requires_reconnect": [],
    "reconnect_recommended": true
  },
  "meta": {
    "version": 18,
    "updated_at": "2026-06-28T12:03:00Z",
    "updated_by": "admin"
  }
}
```

### 字段级规格矩阵建议

下面这张表建议直接当成 server 实现时的字段规格来源。字段不一定要一次性全部开放给后台，但只要开放，就应该把语义固定下来。

| 字段 | 类型 | 默认值 | 后台可改 | 生效类别 | 建议范围 / 约束 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| `writer_queue_capacity` | `usize` | `64` | 是 | `next_connection` | `>= 1` | writer task 内部发送队列容量，只影响新连接 |
| `server_frame_queue_capacity` | `usize` | `128` | 是 | `next_connection` | `>= 1` | server -> session 下发队列容量，只影响新连接 |
| `max_pending_shell_frames` | `usize` | `128` | 是 | `next_connection` | `>= 1` | 单连接 shell 输出待发送上限，避免远程终端占满队列 |
| `max_pending_task_frames` | `usize` | `128` | 是 | `next_connection` | `>= 1` | 单连接 task 输出待发送上限 |
| `close_on_writer_backpressure` | `bool` | `true` | 是 | `live` | 无 | writer 队列持续拥塞时是否直接关闭连接 |
| `drop_oldest_on_server_queue_full` | `bool` | `false` | 是 | `live` | 无 | server 下发队列满时是否优先丢弃最旧消息 |
| `ping_interval_ms` | `u64` | `30000` | 是 | `live` | `0` 或 `>= 1000` | `0` 表示禁用 server 主动 ping |
| `pong_timeout_ms` | `u64` | `90000` | 是 | `live` | `>= ping_interval_ms`，`ping_interval_ms = 0` 时仍建议 `>= 1000` | 超时后将连接视为失活 |
| `idle_timeout_ms` | `u64` | `300000` | 是 | `live` | `0` 或 `>= pong_timeout_ms` | `0` 表示禁用 idle close |
| `max_frame_bytes` | `usize` | `1048576` | 是 | `live` | `>= 1024` | 单帧大小上限，建议不要无限大 |
| `close_on_protocol_error` | `bool` | `true` | 是 | `live` | 无 | 协议层错误是否直接断开连接 |
| `close_on_decode_error` | `bool` | `true` | 是 | `live` | 无 | 解码错误是否直接断开连接 |
| `allow_text_frame` | `bool` | `false` | 是 | `reconnect_required` | 无 | 是否允许该入口接收 Text 模式 |
| `require_secure_handshake` | `bool` | `true` | 是 | `reconnect_required` | 无 | 是否要求 binary wire 必须完成 secure 握手 |
| `adapter_enable_komari` | `bool` | `false` | 是 | `reconnect_required` | 无 | 是否允许当前入口启用 Komari 适配 |
| `payload_mode_lock_policy` | `string` | `first-frame` | 是 | `reconnect_required` | `first-frame` / `binary-only` / `text-only` | 连接建立后如何锁定负载模式 |

这里建议把 `生效类别` 固定成三种字符串：

- `live`
- `next_connection`
- `reconnect_required`

后台页面和接口响应都直接复用这三个字面量，不要前后端各自再发明一套名字。

### 字段级返回元数据建议

如果你希望后台页面能自动渲染参数面板，而不是把字段说明硬编码到前端，可以让 `GET /api/v1/web/runtime/agent-ws` 额外返回字段元数据。

例如：

```jsonc
{
  "fields": {
    "ping_interval_ms": {
      "type": "u64",
      "default": 30000,
      "current": 30000,
      "mutable": true,
      "apply_mode": "live",
      "minimum": 0,
      "unit": "ms",
      "description": "server 主动 ping 间隔，0 表示禁用"
    },
    "writer_queue_capacity": {
      "type": "usize",
      "default": 64,
      "current": 64,
      "mutable": true,
      "apply_mode": "next_connection",
      "minimum": 1,
      "unit": "count",
      "description": "writer task 队列容量，仅影响新连接"
    }
  }
}
```

这样后台页面的好处是：

1. 字段说明不用写死两份
2. 后端改默认值或约束后，前端自动同步
3. 以后新增字段时，前端更容易做通用配置页

### 字段校验建议

后台 PATCH 最容易出问题的不是存储，而是“用户改出一组互相矛盾的值”。建议至少做下面这些校验：

#### 数值范围校验

- `writer_queue_capacity >= 1`
- `server_frame_queue_capacity >= 1`
- `max_pending_shell_frames >= 1`
- `max_pending_task_frames >= 1`
- `ping_interval_ms == 0` 表示禁用主动 ping；否则建议 `>= 1000`
- `pong_timeout_ms >= ping_interval_ms`，除非 `ping_interval_ms == 0`
- `idle_timeout_ms == 0` 表示禁用 idle close；否则建议 `>= pong_timeout_ms`
- `max_frame_bytes >= 1024`

#### 交叉语义校验

- 如果 `require_secure_handshake = true`，就不应允许某些仅明文的 adapter 挂进同一路由
- 如果 `allow_text_frame = false`，就不应把文本 adapter 标为当前连接可用
- 如果 `drop_oldest_on_server_queue_full = false` 且 `close_on_writer_backpressure = false`，要明确你允许的是“阻塞等待”还是“直接返回队列满错误”

这一条很关键。不是所有布尔字段组合都天然有意义，建议把“非法组合”在 PATCH 时就拦掉。

### 活跃连接收到配置更新后的行为建议

只要你决定支持后台热更，就应该提前约定“活跃 session 什么时候读新值”，不然实现时很容易变成每个字段各写各的。

推荐统一语义：

1. `ping_interval_ms`
   - 从下一个 ping 调度周期开始使用新值
   - 不需要重置整条连接

2. `pong_timeout_ms`
   - 从下一次超时判断开始使用新值
   - 不需要回溯修改已经记录的历史时间戳

3. `idle_timeout_ms`
   - 从下一次 idle 检查开始使用新值

4. `max_frame_bytes`
   - 对之后收到的 frame 生效
   - 不需要追溯处理已经进入解码流程的数据

5. `close_on_protocol_error` / `close_on_decode_error`
   - 对之后发生的错误生效

6. `drop_oldest_on_server_queue_full`
   - 从下一次队列满时开始生效

7. `close_on_writer_backpressure`
   - 从下一次 writer 拥塞判断开始生效

也就是说，`live` 字段的语义不应该是“改完立刻回滚当前状态”，而应该是：

- 改完后，后续决策点按新配置执行

这个定义更简单，也最不容易出竞态问题。

### 后台修改失败场景建议

`PATCH /api/v1/web/runtime/agent-ws` 最好把失败类型也提前固定，不然以后前端只能靠字符串猜。

推荐至少区分这几类：

#### 1. 字段值非法

例如：

- `writer_queue_capacity = 0`
- `pong_timeout_ms < ping_interval_ms`
- `payload_mode_lock_policy = "random"`

建议响应：

```jsonc
{
  "error": {
    "code": "runtime_config_validation_failed",
    "message": "agent ws runtime config patch is invalid",
    "details": [
      {
        "field": "pong_timeout_ms",
        "reason": "must be greater than or equal to ping_interval_ms"
      }
    ]
  }
}
```

#### 2. 版本冲突

这个前面已经写了，`expected_version` 对不上时直接拒绝，不做隐式覆盖。

#### 3. 试图修改不允许后台改的字段

例如后面你把某些字段降级成“只能启动时配置”，那 PATCH 命中这些字段时应明确报错：

```jsonc
{
  "error": {
    "code": "runtime_config_field_not_mutable",
    "message": "field is not runtime mutable",
    "details": [
      {
        "field": "secure_provider",
        "reason": "restart required"
      }
    ]
  }
}
```

#### 4. 当前状态下不允许应用该组合

例如：

- 当前路由正在承载 Komari 兼容连接，但你尝试关闭 `allow_text_frame`
- 当前 secure provider 尚未加载，但你直接要求 `require_secure_handshake = true`

这种不是“值本身格式错”，而是“当前运行状态不允许”。建议单独用一个错误码，不要混进普通校验失败：

```jsonc
{
  "error": {
    "code": "runtime_config_apply_rejected",
    "message": "runtime config patch cannot be applied in current state",
    "details": [
      {
        "field": "allow_text_frame",
        "reason": "text adapter is still bound to current route"
      }
    ]
  }
}
```

### 后台页面展示建议

如果你后面会做管理页面，建议把每个字段直接打上状态标签，而不是只给一个输入框：

- `live`
- `next connection`
- `reconnect required`

再补两个提示：

1. 修改后立即生效
2. 修改后仅新连接生效，旧连接需重连

这样后台使用者不会误判。

### 建议增加“恢复默认值”接口

如果后台以后开放给人频繁改，建议再留一个恢复入口：

- `POST /api/v1/web/runtime/agent-ws/reset`

请求可以是：

```jsonc
{
  "fields": [
    "ping_interval_ms",
    "pong_timeout_ms"
  ]
}
```

或者：

```jsonc
{
  "reset_all": true
}
```

这样做的好处是：

1. 不需要让前端自己记默认值
2. 不会因为前端版本落后而写回错误默认值
3. 以后你改默认值策略时，后台接口还能保持兼容

### 建议保留“版本号 + 乐观并发”

如果以后后台不止一个管理入口，或者会有人同时改配置，建议 PATCH 支持版本控制。

最简单的做法是：

```jsonc
{
  "expected_version": 17,
  "patch": {
    "ping_interval_ms": 15000
  }
}
```

如果服务端发现当前版本已经不是 `17`，直接返回冲突，例如：

```jsonc
{
  "error": {
    "code": "runtime_config_version_conflict",
    "message": "agent ws runtime config has been updated by another request",
    "current_version": 18
  }
}
```

这能避免两个后台页面互相覆盖。

### 实现时推荐的内部职责

如果后面落代码，建议内部至少拆这几层：

1. `RuntimeConfigRepository`
   - 负责读写 DB / 本地持久化

2. `RuntimeConfigService`
   - 负责 merge patch
   - 负责校验
   - 负责生成 live view
   - 负责 watch 广播

3. `AgentWsSession`
   - 建连时读取 snapshot
   - 运行中订阅 live view
   - 不直接关心配置持久化

这样职责最清楚：

- web handler 只收请求
- service 只管配置规则
- session 只消费运行时结果

### 配置变更完整时序建议

如果你后面要把这套 runtime config 真正落成后台动态修改，建议脑子里固定下面这条时序。这样实现时不会把“持久化”“广播”“新连接读取”“旧连接热更”混在一起。

#### 1. 修改 `live` 字段时

```text
管理员
  -> PATCH /api/v1/web/runtime/agent-ws
Web Handler
  -> RuntimeConfigService::patch()
RuntimeConfigService
  -> 读取当前版本
  -> 校验 patch
  -> merge 新配置
  -> 持久化完整配置
  -> 生成新的 AgentWsLiveRuntimeView
  -> watch::Sender 广播 live view
  -> 返回 applied_to_live_sessions / applied_on_next_connection
活跃 AgentWsSession
  -> 收到 watch 变更
  -> 刷新本地 live config
  -> 从后续决策点开始按新值执行
```

这里最关键的边界是：

- `RuntimeConfigService` 负责“发布新配置”
- `AgentWsSession` 负责“消费新配置”
- session 不负责决定配置是否合法

#### 2. 修改 `next_connection` 字段时

```text
管理员
  -> PATCH /api/v1/web/runtime/agent-ws
RuntimeConfigService
  -> 校验 patch
  -> merge 新配置
  -> 持久化完整配置
  -> 不需要广播 queue capacity
  -> 返回 applied_on_next_connection
新建 AgentWsSession
  -> 在 AgentWsSession::new() 时读取 snapshot
  -> 使用新 queue capacity 创建内部队列
旧 AgentWsSession
  -> 保持原值不变
```

也就是说，这类字段的修改是“配置源改变了”，不是“活跃连接内部对象被重建了”。

#### 3. 修改 `reconnect_required` 字段时

```text
管理员
  -> PATCH /api/v1/web/runtime/agent-ws
RuntimeConfigService
  -> 校验 patch
  -> merge 新配置
  -> 持久化完整配置
  -> 返回 requires_reconnect
管理员 / 后台页面
  -> 决定是否调用 reconnect-all
POST /api/v1/web/runtime/agent-ws/reconnect-all
ConnectionRegistry / SessionManager
  -> 主动关闭现有 agent WS 连接
Agent
  -> 自动重连
新建 AgentWsSession
  -> 使用新策略建连
```

推荐不要在 PATCH 成功后自动断所有连接，除非你未来明确要做“强制立即应用”。第一版更稳的做法还是：

- PATCH 只改配置
- reconnect-all 单独显式调用

### 为什么 WS 基础层配置和远程功能层配置必须分开

后面你肯定还会加：

- remote shell
- remote task
- remote probe
- 甚至以后还有 backup / file transfer / stream

这些功能的参数如果直接和 `agent-ws runtime` 混在一起，最后会变成一个非常难维护的大配置对象。建议从现在就分层。

推荐拆成两层：

#### 1. WS 基础层配置

这层只关心“连接本身怎么活着、怎么收发、怎么限流、怎么保活、怎么断开”。

只应该包含这类字段：

- `writer_queue_capacity`
- `server_frame_queue_capacity`
- `max_pending_shell_frames`
- `max_pending_task_frames`
- `close_on_writer_backpressure`
- `drop_oldest_on_server_queue_full`
- `ping_interval_ms`
- `pong_timeout_ms`
- `idle_timeout_ms`
- `max_frame_bytes`
- `allow_text_frame`
- `require_secure_handshake`
- `payload_mode_lock_policy`

它们的共同点是：

1. 不关心具体业务内容
2. 只决定 transport/session 行为
3. 所有依赖 WS 的业务都会共享这些规则

#### 2. 远程功能层配置

这层只关心“某个业务功能是否允许、怎样执行、结果怎样限制”。

例如：

- remote shell 是否允许
- remote task 是否允许
- remote probe 是否允许
- shell 默认编码
- task 最大运行时长
- probe 默认超时
- probe 默认并发
- 某些功能是否只允许白名单 agent

这类字段不应该放进 `agent-ws runtime`，原因很直接：

1. 它们不是连接级行为
2. 它们不应该跟 ping/pong、frame 大小、queue capacity 混在一起
3. 它们未来很可能单独需要权限控制、审计和页面分组

### 推荐的配置模型分层

如果你后面继续扩展，建议配置模型至少分成下面三块：

```text
ServerRuntimeConfig
  ├─ agent_ws
  │   └─ AgentWsRuntimeConfig
  ├─ remote_command
  │   └─ RemoteCommandRuntimeConfig
  └─ remote_probe
      └─ RemoteProbeRuntimeConfig
```

对应语义：

1. `agent_ws`
   - 管连接
   - 管协议
   - 管保活
   - 管背压

2. `remote_command`
   - 管 shell / task 之类的远程执行能力
   - 管是否允许
   - 管超时、输出限制、并发限制

3. `remote_probe`
   - 管探测任务能力
   - 管默认超时、默认频率、默认结果限制

如果你后面再加 backup，也建议平行新增：

```text
ServerRuntimeConfig
  ├─ agent_ws
  ├─ remote_command
  ├─ remote_probe
  └─ backup
```

而不是继续往 `agent_ws` 里塞字段。

### remote shell / task / probe 与 WS 的正确关系

建议把它们和 WS 的关系固定成下面这句：

> WS 是承载通道，shell/task/probe 是通过通道传输的业务能力。

所以设计上应该是：

```text
WS session
  -> 收到 ServerFrame
  -> frame 类型可能是 shell/task/probe 命令
  -> 转给对应业务模块执行
  -> 业务模块产出结果
  -> 封装成 ServerFrame / ClientFrame
  -> 再经 WS session 发回
```

也就是说：

- WS runtime config 不决定“是否允许 shell”
- remote command config 才决定“shell/task 是否允许、怎样限制”
- WS runtime config 只决定“消息怎么传、队列怎么限、异常怎么断”

### 后台接口也应该分层

既然配置语义分层，后台接口最好也同步分层，不要所有东西都 PATCH 到一个接口。

推荐这样拆：

- `GET /api/v1/web/runtime/agent-ws`
- `PATCH /api/v1/web/runtime/agent-ws`
- `GET /api/v1/web/runtime/remote-command`
- `PATCH /api/v1/web/runtime/remote-command`
- `GET /api/v1/web/runtime/remote-probe`
- `PATCH /api/v1/web/runtime/remote-probe`

这样做的好处：

1. 页面分组更自然
2. 权限控制更容易做
3. 字段校验不会跨领域混乱
4. 以后新增 backup 不会影响现有 WS runtime 接口

### 第一版实现边界建议

如果你后面准备真正开始写 server，第一版建议只把 `plan2.md` 里这几件事做实：

1. 先落 `agent-ws runtime` 接口
2. 只实现 `live / next_connection / reconnect_required` 三种生效类别
3. 先不把 remote shell / task / probe 的业务参数混进来
4. 等 WS runtime 稳定后，再各自给 `remote-command`、`remote-probe` 单独开配置接口

这样你会得到一个清晰边界：

- 连接层先稳定
- 业务层后扩展

这个顺序更稳，也更方便你以后继续加功能。

### `remote-command` runtime 配置建议

既然前面已经决定把 WS 基础层和远程功能层拆开，那 `remote-command` 最好也按和 `agent-ws` 一样的规格来定义，而不是临时想到什么加什么。

这里的 `remote-command` 指：

- remote shell
- remote task
- 未来其他“通过 agent 远程执行”的命令能力

推荐职责边界：

- `remote-command` 决定“这类功能是否允许、如何限制、如何审计”
- `agent-ws` 只负责“这些命令通过什么通道传输”

#### 推荐字段矩阵

| 字段 | 类型 | 默认值 | 后台可改 | 生效类别 | 建议范围 / 约束 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| `enabled` | `bool` | `false` | 是 | `live` | 无 | 是否全局允许 remote command |
| `allow_shell` | `bool` | `false` | 是 | `live` | 无 | 是否允许交互式 shell |
| `allow_task` | `bool` | `true` | 是 | `live` | 无 | 是否允许一次性 task 执行 |
| `default_task_timeout_ms` | `u64` | `300000` | 是 | `live` | `>= 1000` | task 默认超时 |
| `max_task_timeout_ms` | `u64` | `3600000` | 是 | `live` | `>= default_task_timeout_ms` | task 最大允许超时 |
| `max_shell_idle_timeout_ms` | `u64` | `1800000` | 是 | `live` | `0` 或 `>= 1000` | shell 空闲超时，`0` 表示不因空闲关闭 |
| `max_command_bytes` | `usize` | `16384` | 是 | `live` | `>= 1` | 单条命令最大字节数 |
| `max_stdout_bytes` | `usize` | `1048576` | 是 | `live` | `>= 1024` | 单次 task / shell 输出累计上限 |
| `max_stderr_bytes` | `usize` | `524288` | 是 | `live` | `>= 1024` | stderr 累计上限 |
| `max_concurrent_sessions_per_agent` | `u32` | `1` | 是 | `live` | `>= 1` | 每个 agent 允许同时存在的远程 shell 会话数 |
| `max_concurrent_tasks_per_agent` | `u32` | `2` | 是 | `live` | `>= 1` | 每个 agent 同时运行 task 上限 |
| `audit_log_enabled` | `bool` | `true` | 是 | `live` | 无 | 是否记录 remote command 审计日志 |
| `command_whitelist_enabled` | `bool` | `false` | 是 | `live` | 无 | 是否启用命令白名单 |
| `command_whitelist_profile` | `string` | `default` | 是 | `live` | 非空字符串 | 使用哪个白名单策略 |

这里建议大部分都做成 `live`，原因是：

1. 它们本质是业务执行策略
2. 不依赖重建 WS 内部队列
3. 改完后对“后续新命令”生效即可

但有一个边界要写死：

- `live` 只影响**新发起的 shell/task**
- 已经在运行中的 task，不建议因为配置变更强制中断，除非你以后专门设计“强制终止策略”

也就是说：

- 改 `allow_task = false` 后，已启动 task 可以跑完
- 改 `max_task_timeout_ms` 后，只影响后续新启动 task

#### 推荐接口

- `GET /api/v1/web/runtime/remote-command`
- `PATCH /api/v1/web/runtime/remote-command`
- `POST /api/v1/web/runtime/remote-command/reset`

推荐不要给 `remote-command` 增加 `reconnect-all` 这类接口，因为它不是连接级配置。

#### 推荐 PATCH 响应语义

`remote-command` 的响应建议比 `agent-ws` 少一类，因为它通常不需要 `next_connection`：

```jsonc
{
  "applied": {
    "updated_fields": [
      "allow_shell",
      "max_task_timeout_ms"
    ],
    "applied_to_new_commands": [
      "allow_shell",
      "max_task_timeout_ms"
    ],
    "running_commands_unchanged": true
  }
}
```

这个 `running_commands_unchanged` 很有必要。它能防止后台误以为“改完后正在运行中的 shell/task 也被立刻套用了新限制”。

### `remote-probe` runtime 配置建议

`remote-probe` 和 `remote-command` 一样，也不应该混进 `agent-ws runtime`。它属于“通过 agent 执行探测任务”的业务层配置。

推荐职责边界：

- `remote-probe` 决定探测任务是否允许、如何调度、如何限频、如何限制结果
- `agent-ws` 只负责这些探测消息怎么传

#### 推荐字段矩阵

| 字段 | 类型 | 默认值 | 后台可改 | 生效类别 | 建议范围 / 约束 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| `enabled` | `bool` | `true` | 是 | `live` | 无 | 是否全局允许 probe 功能 |
| `allow_ping` | `bool` | `true` | 是 | `live` | 无 | 是否允许 ping 探测 |
| `allow_http` | `bool` | `true` | 是 | `live` | 无 | 是否允许 HTTP 探测 |
| `allow_tcp` | `bool` | `true` | 是 | `live` | 无 | 是否允许 TCP 探测 |
| `default_probe_timeout_ms` | `u64` | `5000` | 是 | `live` | `>= 100` | probe 默认超时 |
| `max_probe_timeout_ms` | `u64` | `60000` | 是 | `live` | `>= default_probe_timeout_ms` | probe 最大允许超时 |
| `default_probe_interval_ms` | `u64` | `60000` | 是 | `live` | `>= 1000` | 周期 probe 默认频率 |
| `min_probe_interval_ms` | `u64` | `5000` | 是 | `live` | `>= 1000` | 周期 probe 最小允许频率 |
| `max_concurrent_probes_per_agent` | `u32` | `4` | 是 | `live` | `>= 1` | 每个 agent 同时执行的 probe 上限 |
| `max_targets_per_job` | `u32` | `32` | 是 | `live` | `>= 1` | 单个 probe job 允许的目标数 |
| `max_result_bytes` | `usize` | `262144` | 是 | `live` | `>= 1024` | 单次 probe 结果最大字节数 |
| `audit_log_enabled` | `bool` | `true` | 是 | `live` | 无 | 是否记录 probe 调度与结果审计日志 |

这里也建议绝大部分都是 `live`，但语义同样要写清楚：

- 改配置后，对后续新调度的 probe 生效
- 已经开始运行的一轮 probe，不建议中途硬切

#### 推荐接口

- `GET /api/v1/web/runtime/remote-probe`
- `PATCH /api/v1/web/runtime/remote-probe`
- `POST /api/v1/web/runtime/remote-probe/reset`

#### 推荐 PATCH 响应语义

```jsonc
{
  "applied": {
    "updated_fields": [
      "default_probe_timeout_ms",
      "max_concurrent_probes_per_agent"
    ],
    "applied_to_new_probes": [
      "default_probe_timeout_ms",
      "max_concurrent_probes_per_agent"
    ],
    "running_probes_unchanged": true
  }
}
```

### 三类 runtime 配置的职责对照

建议把这张对照关系固定在文档里，后面你再加功能时就很容易判断字段应该放哪。

| 配置块 | 负责什么 | 不负责什么 |
| --- | --- | --- |
| `agent-ws` | 连接、协议、保活、背压、frame、payload mode | shell 是否允许、task 超时、probe 频率 |
| `remote-command` | shell/task 的启用、超时、输出限制、并发、审计 | ping/pong、queue capacity、wire 握手 |
| `remote-probe` | 探测类型、频率、超时、目标限制、并发、审计 | WS 连接生命周期、payload 编码 |

### agent WS 生命周期时序建议

除了 runtime config，另一块很容易后面写乱的是“连接整个生命周期怎么流转”。建议也在这里先固定。

#### 1. 正常建连生命周期

```text
Agent
  -> GET /api/v1/agents/connect (upgrade)
Axum Router
  -> upgrade_agent_ws()
AgentWsSession::new()
  -> 读取 AgentWsSessionConfigSnapshot
  -> 创建 server_tx / server_rx
  -> 订阅 AgentWsLiveConfigRx
AgentWsSession::run()
  -> 注册到 AgentConnectionRegistry
  -> split socket
  -> 启动 writer task
  -> reader loop / server frame loop / live config loop 并行 select
  -> 如果启用 secure
      -> 处理 hello
      -> 处理 handshake
      -> 进入 secure transport
  -> 收到首个业务 frame
      -> 绑定 agent_id
      -> 开始正常收发
```

#### 2. 运行期生命周期

```text
运行中 AgentWsSession
  -> 读取 agent 上行消息
  -> 解码为 ClientFrame / adapter frame
  -> 交给业务层处理
  -> 接收业务层下发的 ServerFrame
  -> 编码后交给 writer task
  -> 定期检查 ping / pong / idle timeout
  -> 监听 AgentWsLiveConfigRx
  -> 在后续决策点应用新的 live config
```

这里建议始终坚持一个原则：

- 配置更新只改变后续决策
- 不回滚已经完成的协议状态

例如：

- secure 已经建好，不因为 `require_secure_handshake` 修改就回退
- payload mode 已经锁定，不因为配置更新就中途改模式

#### 3. reconnect-all 生命周期

```text
管理员
  -> POST /api/v1/web/runtime/agent-ws/reconnect-all
Web Handler
  -> 调用 ConnectionRegistry / SessionManager
SessionManager
  -> 枚举活跃连接
  -> 发送 close frame 或直接关闭连接
AgentWsSession
  -> 退出 select loop
  -> 注销 registry
  -> 停止 writer task
Agent
  -> 自动重连
AgentWsSession::new()
  -> 读取最新 snapshot / live config
  -> 按新策略建连
```

#### 4. 异常关闭生命周期

```text
异常场景
  -> decode error / protocol error / writer failed / pong timeout / idle timeout
AgentWsSession
  -> 根据 live config 判断是否关闭
  -> 退出 run loop
  -> unregister(connection_id)
  -> drop writer_tx
  -> await writer task
  -> 记录 close reason
Agent
  -> 自行重连
```

这里建议在文档里默认规定：

- `close reason` 要进入日志
- 最好也进入连接事件审计

不然后面排查“为什么 agent 总重连”会很痛苦。

### 文档级最终边界

到这里为止，`plan2.md` 里的 runtime 设计边界建议固定为：

1. `agent-ws` 只管通道与连接
2. `remote-command` 只管 shell/task 业务策略
3. `remote-probe` 只管探测业务策略
4. 运行时配置更新分三类：
   - `live`
   - `next_connection`
   - `reconnect_required`
5. 业务层配置的 `live`，默认只影响后续新业务，不强制中断正在运行的任务

这个边界现在就定下来，后面你再加 backup、file transfer、stream，都能按同样方式平行扩展。

### 统一错误码 / 事件码 / close reason 建议

如果你后面要自己写 server，这一块最好现在就统一，不要等实现到一半再临时命名。否则后面会出现：

- HTTP 接口有一套错误码
- WS 日志有一套自由文本
- close reason 又是另一套字符串
- 审计事件里再来一套名字

最后排障会很痛苦。

推荐把这几层分开：

1. **HTTP 错误码**
   - 给后台接口返回
   - 稳定、机器可读

2. **连接事件码**
   - 给日志、审计、连接事件流使用
   - 关注“发生了什么”

3. **close reason**
   - 给 WS 会话关闭时记录
   - 关注“为什么断开”

4. **协议错误码**
   - 放进 `ClientFrame::ProtocolError` / `ServerFrame::ProtocolError` 之类的业务负载
   - 关注“协议层哪里不满足”

### HTTP 错误码建议

后台 runtime 配置接口建议统一用 `snake_case` 的稳定错误码，不要直接把英文报错文本暴露成契约。

推荐最小集合：

| 错误码 | 用途 |
| --- | --- |
| `runtime_config_validation_failed` | patch 字段值非法 |
| `runtime_config_version_conflict` | 乐观并发版本冲突 |
| `runtime_config_field_not_mutable` | 命中了不可运行时修改字段 |
| `runtime_config_apply_rejected` | 当前运行状态不允许应用这组配置 |
| `agent_connection_not_found` | 指定 agent 当前不在线 |
| `agent_connection_send_failed` | server 往 agent 下发失败 |
| `agent_runtime_not_supported` | 当前 agent 或当前 adapter 不支持该能力 |
| `remote_command_rejected` | shell/task 因策略被拒绝 |
| `remote_probe_rejected` | probe 因策略被拒绝 |

推荐约定：

- `code` 稳定
- `message` 给人看
- `details` 给字段级排障

例如：

```jsonc
{
  "error": {
    "code": "agent_connection_send_failed",
    "message": "failed to send frame to agent connection",
    "details": [
      {
        "field": "agent_id",
        "reason": "connection writer queue is closed"
      }
    ]
  }
}
```

### 连接事件码建议

连接事件码建议给日志和审计统一使用，最好也是稳定字面量，不要日志里今天写一个 tomorrow 又改一个。

推荐最小集合：

| 事件码 | 说明 |
| --- | --- |
| `agent_ws_connected` | WS 已完成 upgrade，并创建 session |
| `agent_ws_registered` | session 已注册到连接注册表 |
| `agent_ws_agent_bound` | session 已绑定 agent_id |
| `agent_ws_secure_hello_received` | 收到 secure hello |
| `agent_ws_secure_handshake_completed` | secure 握手完成 |
| `agent_ws_live_config_updated` | session 收到新的 live runtime config |
| `agent_ws_server_frame_sent` | server frame 已成功编码并发送 |
| `agent_ws_client_frame_received` | client frame 已成功解码 |
| `agent_ws_close_requested` | server 主动请求关闭连接 |
| `agent_ws_closed` | 连接已关闭并完成清理 |

建议这些事件至少进入：

- trace/debug/info 日志
- 可选的连接事件审计表

### close reason 建议

close reason 不建议直接写任意自由文本。最好也固定成可枚举值，然后再带人类可读描述。

推荐最小集合：

| close reason | 说明 |
| --- | --- |
| `normal_shutdown` | 正常关闭 |
| `server_reconnect_requested` | 后台触发 reconnect-all |
| `client_closed` | agent 主动发起关闭 |
| `reader_ended` | WS reader 提前结束 |
| `writer_failed` | writer task 发送失败 |
| `protocol_error` | 协议层错误 |
| `decode_error` | 解码失败 |
| `unsupported_payload_mode` | 负载模式不被允许 |
| `secure_handshake_failed` | secure 握手失败 |
| `secure_state_invalid` | secure 状态机非法 |
| `pong_timeout` | pong 超时 |
| `idle_timeout` | 空闲超时 |
| `server_frame_channel_closed` | server -> session 队列已关闭 |
| `internal_error` | 未归类内部错误 |

建议日志里至少写成这种结构：

```text
event=agent_ws_closed connection_id=... agent_id=... close_reason=pong_timeout message="agent websocket session stopped"
```

这样后面检索非常直接。

### 协议错误码建议

如果以后 `ServerFrame` / `ClientFrame` 里要带协议错误，不建议直接传一段自由文本。推荐把“错误码 + 可读说明 + 可选字段上下文”分开。

推荐最小集合：

| 协议错误码 | 说明 |
| --- | --- |
| `unsupported_message_type` | 当前 payload mode 不支持该消息 |
| `invalid_wire_packet` | wire packet 非法 |
| `invalid_secure_hello` | secure hello 非法 |
| `invalid_secure_handshake` | secure handshake 非法 |
| `secure_session_mismatch` | secure session_id 不匹配 |
| `secure_not_ready` | 在 secure 建立前发送了 secure data |
| `invalid_client_frame` | client frame 结构不合法 |
| `invalid_server_frame` | server frame 结构不合法 |
| `adapter_decode_failed` | adapter 解码失败 |
| `adapter_encode_failed` | adapter 编码失败 |
| `feature_not_enabled` | 功能未启用 |
| `command_rejected` | 远程命令被策略拒绝 |
| `probe_rejected` | 探测任务被策略拒绝 |

### 推荐的错误结构

无论是 HTTP 响应、协议错误负载，还是审计事件，建议都尽量复用一个相似结构：

```jsonc
{
  "code": "protocol_error",
  "message": "secure handshake session id mismatch",
  "details": [
    {
      "field": "session_id",
      "reason": "packet session id does not match current handshake state"
    }
  ]
}
```

这样你的 server 内部可以很自然地做映射：

- 内部错误类型
  -> HTTP 错误响应
  -> ProtocolError frame
  -> close reason
  -> audit event

### 错误层级映射建议

同一个问题，不同层看到的名字可以不同，但应该能互相映射。

例如 `pong timeout`：

| 层级 | 建议值 |
| --- | --- |
| 连接事件码 | `agent_ws_closed` |
| close reason | `pong_timeout` |
| 日志 message | `agent websocket session stopped due to pong timeout` |
| HTTP 错误码 | 通常没有，因为这是连接运行期事件 |

例如 `payload_mode` 非法：

| 层级 | 建议值 |
| --- | --- |
| 协议错误码 | `unsupported_message_type` |
| close reason | `unsupported_payload_mode` |
| 连接事件码 | `agent_ws_closed` |
| 日志 message | `payload mode is not allowed for current route` |

这个映射关系建议未来也写进实现注释里，不然 시간이一长很容易漂。

### ServerFrame / ClientFrame / transport / adapter 职责边界

这一块是协议设计里最容易越写越乱的地方。建议先把四层边界钉死：

#### 1. transport 层

transport 层只负责：

- `Message::Binary` / `Message::Text`
- `Ping` / `Pong` / `Close`
- `WirePacket`
- 可选 secure 加解密
- payload mode 锁定

transport 层不应该负责：

- agent 业务状态
- shell/task/probe 语义
- 第三方协议业务兼容逻辑

也就是说，transport 关心的是“字节怎么进出”，不是“业务是什么意思”。

#### 2. adapter 层

adapter 层只负责：

- 第三方协议 `<->` 自有协议 的翻译
- 文本 / 二进制第三方消息解码
- 把第三方响应重新编码回去

adapter 层不应该负责：

- 修改连接注册表
- 落库
- 直接操作业务 service
- 决定 shell/task/probe 是否允许

也就是说，adapter 只做“翻译”，不做“业务执行”。

#### 3. `ClientFrame` / `ServerFrame` 层

这层是**自有协议边界**。它们应该只承载：

- 自有业务事件
- 自有业务命令
- 自有业务结果
- 协议级错误和控制信号

它们不应该直接带：

- 第三方协议原始字段
- 第三方特有命名
- transport 私有状态

如果某个字段只有 Komari 才有意义，那它默认不应该直接进核心 `ClientFrame` / `ServerFrame`。

#### 4. 业务层

业务层只负责：

- 收到 `ClientFrame` 后做什么
- 生成哪个 `ServerFrame`
- 是否写库、是否审计、是否调其他 service
- 是否允许 remote shell / task / probe

业务层不应该负责：

- 手写 JSON 编码
- 直接处理 `Message::Binary`
- 直接处理 secure handshake

### 推荐的数据流方向

入站：

```text
WebSocket Message
  -> transport decode
  -> optional adapter decode
  -> ClientFrame
  -> business handler
```

出站：

```text
business handler
  -> ServerFrame
  -> optional adapter encode
  -> transport encode
  -> WebSocket Message
```

这里最关键的纪律是：

- 业务层永远只认 `ClientFrame` / `ServerFrame`
- 第三方协议永远只在 adapter 边界翻译
- secure 永远只在 transport 边界处理

### 哪些字段不该进核心 frame

下面这些东西，原则上都不应该直接塞进核心 `ClientFrame` / `ServerFrame`：

- Komari 专用字段名
- 第三方原始 query 参数
- 某个 adapter 特有的中间状态
- WS close code
- `session_id`、`wire sequence` 这类 transport 状态

这些信息如果确实需要保留：

1. transport 状态留在 session context
2. 第三方原始字段留在 adapter 内部上下文
3. 真正提炼后仍有业务意义的，才提升成核心 frame 字段

### 哪些字段应该进核心 frame

下面这些才更适合进入自有核心 frame：

- `agent_id`
- `timestamp`
- `sequence`
- `payload`
- `command_id`
- `probe_job_id`
- `shell_session_id`
- `task_id`
- `error.code`
- `error.message`

也就是说，核心 frame 只保留**跨协议、跨实现都稳定的业务语义**。

### 文档级 frame 设计原则

到这里建议把 frame 设计原则写死成三句：

1. 自有业务只认 `ClientFrame` / `ServerFrame`
2. 第三方兼容只在 adapter 边界翻译
3. transport 只处理字节、secure、wire，不承载业务语义

这个原则一旦守住，你后面再接 Komari 以外的兼容协议，也不会把核心协议模型弄脏。

### agent 认证与 secure 交互建议

前面已经把 runtime、frame、adapter、生命周期都拆清了，接下来最容易写歪的就是“认证到底在哪一层做”“token / key_id / session_id / agent_id 各自是什么意思”。

建议先把这几个概念完全分开：

| 字段 / 概念 | 用途 | 所属层 |
| --- | --- | --- |
| `token` | agent 接入 server 的凭证，证明“允许这台 agent 建连” | 接入认证层 |
| `key_id` | 用于定位某把 secure key / PSK 的标识 | secure 握手层 |
| `session_id` | 一次 secure 握手 / wire 会话的瞬时标识 | transport / secure 层 |
| `agent_id` | 业务层稳定标识这台 agent 是谁 | 业务层 |
| `connection_id` | 这一次 WS 连接的实例标识，每次重连都会变化 | server 连接层 |

一定不要把它们混成一个东西用，不然后面你会很难区分：

- “这台 agent 是谁”
- “这次连接是谁”
- “这次 secure 会话是谁”
- “这台 agent 是否允许接入”

### 推荐的认证层次

推荐按两层看：

#### 1. 接入认证

目的：

- 这台 agent 是否允许连进来
- 它连接的是哪个租户 / 哪个节点 / 哪个 agent 记录

推荐凭证：

- `token`

这个 `token` 的职责只应该是：

- 鉴权
- 查到 agent 记录
- 查到可用的 secure key / policy

它不应该直接承担：

- wire 加密
- 业务层 session 标识
- 第三方 adapter 的协议状态

#### 2. secure 通道认证

目的：

- 建立这一条 WS 二进制通道是否属于可信 agent
- 后续业务 payload 是否需要加密保护

推荐依赖：

- `key_id`
- `PSK` / secure secret
- `session_id`

这一层的职责是：

- 建立受保护 transport
- 保证后续 `SecureData` 能正确加解密

它不负责最终判定业务上的 `agent_id`。

### token / key_id / agent_id 的推荐关系

推荐关系如下：

```text
token
  -> 查到 agent 记录
  -> 查到 agent_id
  -> 查到 secure policy
  -> 查到一个或多个可用 key_id
key_id
  -> 查到对应 PSK / 派生材料
agent_id
  -> 作为业务层稳定标识
```

也就是说：

- `token` 是接入凭证
- `key_id` 是 secure 材料索引
- `agent_id` 是业务身份

这三者不要互相替代。

### 推荐的建连顺序

如果你以后走自有二进制 secure 协议，推荐建连顺序固定成下面这样：

#### 1. HTTP/WS upgrade 前置认证

```text
Agent
  -> GET /api/v1/agents/connect?token=...
Server
  -> 校验 token
  -> 查到 agent 记录
  -> 生成/确认本次 connection context
  -> 允许 upgrade
```

这一步解决的是“能不能接入”。

推荐这里就把这些信息挂到连接上下文里：

- `authorized_agent_id`
- `tenant_id`
- `secure_policy`
- `allowed_key_ids`
- `connection_id`

#### 2. secure hello

```text
Agent
  -> Binary WirePacket::Hello
  -> payload 含 key_id 等 hello 信息
Server
  -> 校验 key_id 是否属于当前 token / agent 可用范围
  -> 查到对应 PSK
  -> 生成 responder handshake
  -> 返回 WirePacket::Handshake
```

这一步解决的是“这条二进制 secure 通道是不是用允许的密钥材料来握手”。

#### 3. secure handshake 完成

```text
Agent
  -> Binary WirePacket::Handshake
Server
  -> 校验 session_id
  -> 完成 handshake
  -> 进入 Secure transport 状态
```

这一步之后，后续的业务 payload 才可以走：

- `WirePacket::SecureData`

#### 4. 首个业务 frame

```text
Agent
  -> SecureData / PlainData
  -> decode -> ClientFrame
Server
  -> 读取 frame.agent_id
  -> 与 authorized_agent_id 做一致性校验
  -> 首次通过后 bind 到 AgentConnectionRegistry
```

这一步解决的是：

- 协议里报出来的 `agent_id`，是否和认证层认定的是同一个 agent

推荐规则：

- 如果 `frame.agent_id != authorized_agent_id`，直接按协议错误处理
- 不要简单地“谁先发什么 id 就信谁”

### 推荐的一致性校验

为了防止身份串线，建议最少做这几层一致性校验：

1. `token -> agent_id`
   - 在 upgrade 前就确定

2. `token -> allowed_key_ids`
   - 当前 token 允许使用哪些 `key_id`

3. `hello.key_id in allowed_key_ids`
   - 不允许 agent 拿别人的 key_id 来握手

4. `frame.agent_id == authorized_agent_id`
   - 不允许握手完后业务层再冒充别的 agent

5. `session_id` 必须匹配当前握手上下文
   - 防止串包和错误复用

### `session_id` 的职责建议

`session_id` 只应该承担：

- 标识这一次 secure wire 会话
- 让 hello / handshake / secure data 能和当前握手上下文对上

它不应该承担：

- agent 身份
- token 身份
- 数据库主键
- 长期可复用标识

也就是说，`session_id` 是瞬时 transport 标识，不是业务身份。

### 推荐的认证失败处理

不同阶段失败，建议区分处理，不要都混成一个 `unauthorized`。

#### 1. upgrade 前 token 校验失败

建议：

- 直接拒绝 HTTP/WS upgrade
- 返回 HTTP `401` / `403`
- 记录审计事件

推荐错误码：

- `agent_auth_token_invalid`
- `agent_auth_token_expired`
- `agent_auth_token_revoked`
- `agent_auth_agent_disabled`

#### 2. secure hello / handshake 失败

建议：

- 已 upgrade 的连接直接关闭
- 记录 `close_reason`
- 记录 secure 失败事件

推荐 close reason：

- `secure_handshake_failed`
- `secure_state_invalid`

推荐协议错误码：

- `invalid_secure_hello`
- `invalid_secure_handshake`
- `secure_session_mismatch`

#### 3. 首个业务 frame 身份不一致

建议：

- 直接按协议错误处理
- 关闭连接
- 记录高优先级审计

推荐错误码：

- `agent_identity_mismatch`

### 推荐的认证事件审计

建议把下面这些事件至少写到连接审计里：

| 事件码 | 说明 |
| --- | --- |
| `agent_auth_requested` | agent 发起接入 |
| `agent_auth_succeeded` | token 校验通过 |
| `agent_auth_failed` | token 校验失败 |
| `agent_secure_hello_received` | 收到 hello |
| `agent_secure_handshake_succeeded` | secure 握手成功 |
| `agent_secure_handshake_failed` | secure 握手失败 |
| `agent_identity_verified` | `frame.agent_id` 与授权身份一致 |
| `agent_identity_mismatch` | `frame.agent_id` 与授权身份不一致 |

这些事件后面在排障时非常有价值，尤其是：

- 为什么这个 agent 连不上
- 为什么这个 agent 一直握手失败
- 为什么看起来 token 是对的，但后面又被踢了

### server 侧建议的落库模型

你后面自己写 server，不需要一开始把所有表都做满，但建议先把模型边界想清楚。至少建议拆这几类：

#### 1. agent 主表

用途：

- 保存 agent 业务身份
- 保存接入状态
- 保存所属租户 / 分组 / 标签

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `agent_id` | 业务稳定标识 |
| `tenant_id` | 所属租户 |
| `name` | 人类可读名称 |
| `status` | enabled / disabled / revoked |
| `last_seen_at` | 最近在线时间 |
| `created_at` | 创建时间 |
| `updated_at` | 更新时间 |

#### 2. agent credential 表

用途：

- 保存 token 元数据
- 保存 secure key 元数据
- 不建议明文保存完整 token

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `agent_id` | 关联 agent |
| `token_hash` | token 哈希或摘要 |
| `token_hint` | 方便后台识别的简短提示 |
| `key_id` | secure key 标识 |
| `key_material_ref` | secure 材料引用，不一定直接存原文 |
| `status` | active / rotated / revoked |
| `expires_at` | 过期时间 |
| `created_at` | 创建时间 |
| `updated_at` | 更新时间 |

建议原则：

- token 不直接明文落库，至少存 hash / digest
- `key_id` 可以公开索引
- `PSK` / secret 最好走受控存储或单独加密列

#### 3. connection session 表

用途：

- 保存一次 WS 连接实例
- 便于追踪重连、断开原因、握手状态

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `connection_id` | 本次连接实例 ID |
| `agent_id` | 关联 agent |
| `connected_at` | 建连时间 |
| `closed_at` | 关闭时间 |
| `close_reason` | 关闭原因 |
| `remote_addr` | 来源地址 |
| `transport_mode` | binary / text |
| `secure_mode` | plain / handshaking / secure |
| `secure_key_id` | 本次连接使用的 key_id |
| `session_id` | 本次 secure session_id |

#### 4. connection event 表

用途：

- 保存连接生命周期事件
- 方便按连接回放问题

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `connection_id` | 关联 connection session |
| `agent_id` | 关联 agent |
| `event_code` | 如 `agent_ws_connected` |
| `message` | 可读说明 |
| `payload_json` | 可选附加上下文 |
| `created_at` | 事件时间 |

#### 5. runtime config 表

用途：

- 保存当前生效配置版本
- 保存变更历史

建议至少拆两层：

1. 当前快照
2. 历史版本

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `scope` | `agent_ws` / `remote_command` / `remote_probe` |
| `version` | 配置版本 |
| `config_json` | 完整配置快照 |
| `updated_by` | 谁改的 |
| `updated_at` | 修改时间 |

如果要保留历史，再加：

| 字段 | 用途 |
| --- | --- |
| `previous_version` | 上一版本 |
| `change_note` | 修改说明 |
| `patch_json` | 本次 patch |

#### 6. remote command audit 表

用途：

- 保存 shell/task 的调度和结果审计

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `agent_id` | 目标 agent |
| `connection_id` | 关联连接 |
| `command_id` | 命令业务 ID |
| `command_type` | shell / task |
| `requested_by` | 谁发起 |
| `status` | queued / running / finished / failed / rejected |
| `exit_code` | 可选退出码 |
| `stdout_size` | stdout 字节数 |
| `stderr_size` | stderr 字节数 |
| `started_at` | 开始时间 |
| `finished_at` | 结束时间 |

如果你以后要更严格审计，可以再拆输出明细表，不建议一开始把大块 stdout/stderr 都直接塞主表。

#### 7. remote probe audit 表

用途：

- 保存 probe 下发、执行和结果摘要

建议字段：

| 字段 | 用途 |
| --- | --- |
| `id` | 内部主键 |
| `agent_id` | 执行 agent |
| `connection_id` | 关联连接 |
| `probe_job_id` | 任务业务 ID |
| `probe_type` | ping / http / tcp |
| `target` | 目标 |
| `status` | queued / running / finished / failed / rejected |
| `latency_ms` | 可选延迟 |
| `result_summary_json` | 摘要结果 |
| `started_at` | 开始时间 |
| `finished_at` | 结束时间 |

### 建议的表之间关系

推荐最小关系：

```text
agent
  ├─ agent_credential (1:n)
  ├─ connection_session (1:n)
  ├─ remote_command_audit (1:n)
  └─ remote_probe_audit (1:n)

connection_session
  └─ connection_event (1:n)
```

这样做的好处：

1. agent 维度容易查
2. 单次连接维度容易回放
3. 审计数据和配置数据不会混在一起

### 第一版落库建议

如果你不想一开始就把 DB 设计做太重，建议第一版至少落这几张：

1. `agent`
2. `agent_credential`
3. `connection_session`
4. `runtime_config`

然后日志先打完整，审计表第二阶段再补：

5. `connection_event`
6. `remote_command_audit`
7. `remote_probe_audit`

这样你一开始也能满足：

- 认证
- 建连
- 重连排障
- runtime config 持久化

不会一下把数据面做得过于重。

### 文档级认证与落库原则

到这里建议再把两条原则写死：

1. `token` 是接入凭证，不等于 `agent_id`
2. `session_id` 是一次 secure transport 会话标识，不等于业务身份

再加一条数据层原则：

3. 审计表和当前状态表分开，不要混成一张万能表

这三条守住，后面 server 设计会稳很多。

### server 主动下发命令的交互协议建议

前面已经把 runtime、认证、落库、frame 边界都定了。接下来真正写 server 时，最容易再临时设计的就是：

- server 怎么下发 shell
- agent 怎么回 shell 输出
- task 怎么开始 / 结束 / 失败
- probe 怎么下发 / 回结果 / 回错误

这里建议提前把帧语义固定下来。

### remote shell 交互建议

推荐把 remote shell 拆成 5 类业务语义：

1. `open`
2. `input`
3. `resize`
4. `output`
5. `close`

这样无论最后是 JSON frame、二进制 frame，还是以后换 transport，都不影响业务层理解。

#### server -> agent

推荐下发语义：

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `shell_open` | 请求 agent 打开一个交互式 shell 会话 | `shell_session_id` `command_id` `program` `args` `env` `cwd` |
| `shell_input` | 向已打开的 shell 会话写入输入 | `shell_session_id` `data` `encoding` |
| `shell_resize` | 调整终端大小 | `shell_session_id` `cols` `rows` |
| `shell_close` | 请求关闭 shell 会话 | `shell_session_id` `reason` |

#### agent -> server

推荐回传语义：

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `shell_opened` | shell 会话已创建成功 | `shell_session_id` `pid` |
| `shell_output` | shell 输出数据 | `shell_session_id` `stream` `data` `encoding` |
| `shell_closed` | shell 会话已结束 | `shell_session_id` `exit_code` `reason` |
| `shell_error` | shell 相关错误 | `shell_session_id` `error.code` `error.message` |

#### 推荐交互时序

```text
Server
  -> ServerFrame::ShellOpen
Agent
  -> 创建 shell
  -> ClientFrame::ShellOpened
Server
  -> ServerFrame::ShellInput
Agent
  -> 写入 stdin
  -> ClientFrame::ShellOutput
Server
  -> ServerFrame::ShellResize
Agent
  -> 调整 pty 尺寸
Server
  -> ServerFrame::ShellClose
Agent
  -> 关闭 shell
  -> ClientFrame::ShellClosed
```

建议注意两条：

1. `shell_session_id` 由 server 生成，agent 原样回传
2. `command_id` 也保留，方便把 shell 这类会话型命令和统一命令审计关联起来

### remote task 交互建议

task 和 shell 不一样，task 更适合做成“一次请求，一次结束”的有限状态流。

推荐拆成 4 类语义：

1. `run`
2. `stdout/stderr`
3. `finished`
4. `error`

#### server -> agent

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `task_run` | 请求执行一个非交互命令 | `task_id` `command_id` `program` `args` `env` `cwd` `timeout_ms` |
| `task_cancel` | 请求取消正在运行的 task | `task_id` `reason` |

#### agent -> server

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `task_started` | task 已开始执行 | `task_id` `pid` |
| `task_stdout` | stdout 数据块 | `task_id` `data` `encoding` |
| `task_stderr` | stderr 数据块 | `task_id` `data` `encoding` |
| `task_finished` | task 完成 | `task_id` `exit_code` `timed_out` |
| `task_error` | task 错误 | `task_id` `error.code` `error.message` |

#### 推荐交互时序

```text
Server
  -> ServerFrame::TaskRun
Agent
  -> 启动进程
  -> ClientFrame::TaskStarted
  -> ClientFrame::TaskStdout / TaskStderr ...
  -> ClientFrame::TaskFinished
```

如果中途取消：

```text
Server
  -> ServerFrame::TaskCancel
Agent
  -> 尝试终止进程
  -> ClientFrame::TaskFinished(timed_out=false, reason="cancelled")
```

建议：

- `task_id` 由 server 生成
- agent 不要自己重新生成另一个业务 task id
- 如果 agent 内部需要本地 pid / handle，可单独放结果字段里

### remote probe 交互建议

probe 比 shell/task 更偏“调度型任务”，建议用 job 语义。

推荐拆成 5 类语义：

1. `run`
2. `accepted`
3. `progress`
4. `result`
5. `finished/error`

#### server -> agent

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `probe_run` | 执行一次探测任务 | `probe_job_id` `probe_type` `target` `timeout_ms` |
| `probe_schedule_patch` | 修改周期探测任务 | `probe_job_id` `interval_ms` `targets` `enabled` |
| `probe_cancel` | 取消正在运行或已调度的任务 | `probe_job_id` `reason` |

#### agent -> server

| 操作 | 作用 | 关键字段 |
| --- | --- | --- |
| `probe_accepted` | job 已接收 | `probe_job_id` |
| `probe_progress` | 可选中间进度 | `probe_job_id` `message` |
| `probe_result` | 单个目标或单次探测结果 | `probe_job_id` `probe_type` `target` `status` `latency_ms` `result` |
| `probe_finished` | 整个 probe job 完成 | `probe_job_id` `success_count` `failure_count` |
| `probe_error` | probe 任务错误 | `probe_job_id` `error.code` `error.message` |

#### 推荐交互时序

```text
Server
  -> ServerFrame::ProbeRun
Agent
  -> ClientFrame::ProbeAccepted
  -> ClientFrame::ProbeResult ...
  -> ClientFrame::ProbeFinished
```

如果是周期任务：

```text
Server
  -> ServerFrame::ProbeSchedulePatch
Agent
  -> 更新本地 probe job 配置
  -> ClientFrame::ProbeAccepted
  -> 后续周期性上报 ProbeResult / ProbeFinished
```

### 三类主动下发命令的统一字段建议

为了让 server 侧审计、重试、去重更稳定，建议 shell/task/probe 尽量共享一批字段语义：

| 字段 | 用途 |
| --- | --- |
| `agent_id` | 目标 agent |
| `command_id` | 统一命令请求 ID |
| `issued_at` | server 下发时间 |
| `requested_by` | 谁发起 |
| `trace_id` | 跨链路追踪 |
| `timeout_ms` | 本次执行超时 |
| `metadata` | 可选扩展上下文 |

再按业务附加：

- shell 用 `shell_session_id`
- task 用 `task_id`
- probe 用 `probe_job_id`

建议规则：

- `command_id` 作为统一父级 ID
- `shell_session_id` / `task_id` / `probe_job_id` 作为具体业务实例 ID

这样查询时你可以同时支持：

- 按命令看一次完整请求
- 按具体 shell/task/probe 实例看细节

### 推荐的错误帧与结束帧语义

建议不要把“失败”和“结束”混成一个帧。

推荐规则：

1. `error` 表示这次业务请求无法正常继续或执行过程中出现异常
2. `finished/closed` 表示这次业务生命周期结束

例如：

- shell 可以先 `shell_error`，最后再 `shell_closed`
- task 可以直接 `task_error` 后没有 `task_started`
- probe 可以 `probe_error`，也可以部分 `probe_result` 后再 `probe_finished`

这样语义更稳定：

- `error` 是发生了什么问题
- `finished` 是生命周期有没有结束

### 审计日志字段规范建议

前面已经拆了 `connection_event`、`remote_command_audit`、`remote_probe_audit`。这里建议把四类日志的公共字段也统一下来。

#### 1. 配置变更审计

推荐字段：

| 字段 | 用途 |
| --- | --- |
| `scope` | `agent_ws` / `remote_command` / `remote_probe` |
| `version` | 配置版本 |
| `updated_by` | 谁改的 |
| `source` | web / api / cli / system |
| `patch_json` | 本次 patch |
| `effective_mode` | `live` / `next_connection` / `reconnect_required` |
| `created_at` | 时间 |

#### 2. 连接事件审计

推荐字段：

| 字段 | 用途 |
| --- | --- |
| `connection_id` | 连接实例 |
| `agent_id` | 业务 agent |
| `event_code` | 事件码 |
| `close_reason` | 可选关闭原因 |
| `transport_mode` | binary / text |
| `secure_mode` | plain / handshaking / secure |
| `remote_addr` | 来源地址 |
| `trace_id` | 追踪 ID |
| `payload_json` | 扩展上下文 |
| `created_at` | 时间 |

#### 3. 命令执行审计

推荐字段：

| 字段 | 用途 |
| --- | --- |
| `command_id` | 统一命令 ID |
| `command_type` | shell / task |
| `shell_session_id` | 可选 shell 会话 ID |
| `task_id` | 可选 task ID |
| `agent_id` | 目标 agent |
| `connection_id` | 所属连接 |
| `requested_by` | 发起人 |
| `status` | queued / running / finished / failed / rejected |
| `error_code` | 可选错误码 |
| `exit_code` | 可选退出码 |
| `stdout_size` | 输出统计 |
| `stderr_size` | 错误输出统计 |
| `trace_id` | 追踪 ID |
| `started_at` | 开始时间 |
| `finished_at` | 结束时间 |

#### 4. probe 审计

推荐字段：

| 字段 | 用途 |
| --- | --- |
| `command_id` | 可选统一命令 ID |
| `probe_job_id` | probe 实例 ID |
| `probe_type` | ping / http / tcp |
| `target` | 目标 |
| `agent_id` | 执行 agent |
| `connection_id` | 所属连接 |
| `requested_by` | 发起人 |
| `status` | queued / running / finished / failed / rejected |
| `error_code` | 可选错误码 |
| `latency_ms` | 延迟 |
| `result_summary_json` | 摘要结果 |
| `trace_id` | 追踪 ID |
| `started_at` | 开始时间 |
| `finished_at` | 结束时间 |

### 四类审计的统一查询维度建议

建议无论哪类审计，尽量都保留下面这些查询维度：

- `agent_id`
- `connection_id`
- `command_id`
- `trace_id`
- `requested_by`
- `created_at` / `started_at` / `finished_at`

这样后面你做后台时，查询路径会很稳定：

1. 按 agent 看所有连接和命令
2. 按 connection 看一次会话内发生了什么
3. 按 command 看一次请求链路
4. 按 trace_id 看跨模块关联

### 建议的审计日志原则

建议再把下面几条写死：

1. 所有 server 主动下发命令都必须有 `command_id`
2. 所有长生命周期命令都必须有自己的实例 ID
3. 错误码和状态字段优先结构化存储，不要只存自由文本
4. 大块输出内容不直接塞主审计表，只存统计和摘要

这样数据库不会很快被 stdout/stderr 或 probe 结果撑爆。

### 文档级主动命令与审计原则

到这里建议再固定三条：

1. shell/task/probe 都走统一的 `ServerFrame -> ClientFrame` 业务流
2. `command_id` 是统一请求 ID，业务实例 ID 负责细分具体会话
3. 审计优先记录结构化状态和关联 ID，原始大数据单独处理

这三条定下来，后面无论你接 REST 下发、Web 管理台下发，还是脚本自动下发，模型都不会乱。

### `ServerFrame` / `ClientFrame` JSON 样例建议

前面已经把语义、字段、ID、审计都定了。这里建议直接给出一组“可以拿来照着实现”的 JSON 样例，方便你后面写 server 和 agent 时保持一致。

下面这些样例只表达**业务层 frame**，不包含：

- `WirePacket`
- secure 加解密包装
- WS `Binary/Text` 外层

也就是说，这里展示的是：

- `ServerFrame`
- `ClientFrame`

### 通用 envelope 建议

建议所有自有 frame 都至少带这层外壳：

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:00:00Z",
  "sequence": 42,
  "payload": {
    "type": "..."
  }
}
```

推荐字段语义：

| 字段 | 说明 |
| --- | --- |
| `agent_id` | 业务层目标或来源 agent |
| `timestamp` | frame 生成时间 |
| `sequence` | 当前方向上的业务序号 |
| `payload.type` | 负载类型 |

### shell 相关样例

#### `ServerFrame::ShellOpen`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:00:00Z",
  "sequence": 1001,
  "payload": {
    "type": "shell_open",
    "command_id": "cmd_01JZ1234AAAA",
    "shell_session_id": "sh_01JZ1234BBBB",
    "program": "/bin/bash",
    "args": ["-l"],
    "cwd": "/opt/app",
    "env": {
      "TERM": "xterm-256color"
    },
    "metadata": {
      "requested_by": "admin"
    }
  }
}
```

#### `ClientFrame::ShellOpened`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:00:01Z",
  "sequence": 2001,
  "payload": {
    "type": "shell_opened",
    "command_id": "cmd_01JZ1234AAAA",
    "shell_session_id": "sh_01JZ1234BBBB",
    "pid": 4567
  }
}
```

#### `ClientFrame::ShellOutput`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:00:02Z",
  "sequence": 2002,
  "payload": {
    "type": "shell_output",
    "command_id": "cmd_01JZ1234AAAA",
    "shell_session_id": "sh_01JZ1234BBBB",
    "stream": "stdout",
    "encoding": "base64",
    "data": "bHMgLWxhCg=="
  }
}
```

#### `ServerFrame::ShellInput`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:00:03Z",
  "sequence": 1002,
  "payload": {
    "type": "shell_input",
    "command_id": "cmd_01JZ1234AAAA",
    "shell_session_id": "sh_01JZ1234BBBB",
    "encoding": "utf8",
    "data": "ls -la\n"
  }
}
```

#### `ClientFrame::ShellClosed`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:01:00Z",
  "sequence": 2009,
  "payload": {
    "type": "shell_closed",
    "command_id": "cmd_01JZ1234AAAA",
    "shell_session_id": "sh_01JZ1234BBBB",
    "exit_code": 0,
    "reason": "normal_exit"
  }
}
```

### task 相关样例

#### `ServerFrame::TaskRun`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:10:00Z",
  "sequence": 1101,
  "payload": {
    "type": "task_run",
    "command_id": "cmd_01JZ5678AAAA",
    "task_id": "task_01JZ5678BBBB",
    "program": "powershell",
    "args": ["-NoProfile", "-Command", "Get-Process"],
    "cwd": "C:/",
    "env": {},
    "timeout_ms": 300000
  }
}
```

#### `ClientFrame::TaskStarted`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:10:00Z",
  "sequence": 2101,
  "payload": {
    "type": "task_started",
    "command_id": "cmd_01JZ5678AAAA",
    "task_id": "task_01JZ5678BBBB",
    "pid": 7890
  }
}
```

#### `ClientFrame::TaskStdout`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:10:01Z",
  "sequence": 2102,
  "payload": {
    "type": "task_stdout",
    "command_id": "cmd_01JZ5678AAAA",
    "task_id": "task_01JZ5678BBBB",
    "encoding": "utf8",
    "data": "powershell output line 1\n"
  }
}
```

#### `ClientFrame::TaskFinished`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:10:05Z",
  "sequence": 2108,
  "payload": {
    "type": "task_finished",
    "command_id": "cmd_01JZ5678AAAA",
    "task_id": "task_01JZ5678BBBB",
    "exit_code": 0,
    "timed_out": false
  }
}
```

### probe 相关样例

#### `ServerFrame::ProbeRun`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:20:00Z",
  "sequence": 1201,
  "payload": {
    "type": "probe_run",
    "command_id": "cmd_01JZ9AAA1111",
    "probe_job_id": "probe_01JZ9AAA2222",
    "probe_type": "http",
    "target": "https://example.com/health",
    "timeout_ms": 5000
  }
}
```

#### `ClientFrame::ProbeAccepted`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:20:00Z",
  "sequence": 2201,
  "payload": {
    "type": "probe_accepted",
    "command_id": "cmd_01JZ9AAA1111",
    "probe_job_id": "probe_01JZ9AAA2222"
  }
}
```

#### `ClientFrame::ProbeResult`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:20:01Z",
  "sequence": 2202,
  "payload": {
    "type": "probe_result",
    "command_id": "cmd_01JZ9AAA1111",
    "probe_job_id": "probe_01JZ9AAA2222",
    "probe_type": "http",
    "target": "https://example.com/health",
    "status": "ok",
    "latency_ms": 86,
    "result": {
      "status_code": 200
    }
  }
}
```

#### `ClientFrame::ProbeFinished`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:20:02Z",
  "sequence": 2203,
  "payload": {
    "type": "probe_finished",
    "command_id": "cmd_01JZ9AAA1111",
    "probe_job_id": "probe_01JZ9AAA2222",
    "success_count": 1,
    "failure_count": 0
  }
}
```

### 错误样例

#### `ClientFrame::TaskError`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:10:00Z",
  "sequence": 2100,
  "payload": {
    "type": "task_error",
    "command_id": "cmd_01JZ5678AAAA",
    "task_id": "task_01JZ5678BBBB",
    "error": {
      "code": "feature_not_enabled",
      "message": "remote task is disabled by policy"
    }
  }
}
```

#### `ClientFrame::ProtocolError`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:30:00Z",
  "sequence": 2301,
  "payload": {
    "type": "protocol_error",
    "error": {
      "code": "secure_session_mismatch",
      "message": "packet session id does not match current handshake state"
    }
  }
}
```

### 配置更新相关样例

#### `ServerFrame::ConfigRefreshRequested`

如果以后你想让 server 主动要求 agent 重新回报某些能力或状态，可以预留这种语义：

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:40:00Z",
  "sequence": 1301,
  "payload": {
    "type": "config_refresh_requested",
    "reason": "server_runtime_updated"
  }
}
```

#### `ClientFrame::ConfigRefreshAck`

```jsonc
{
  "agent_id": "agent-001",
  "timestamp": "2026-06-28T12:40:01Z",
  "sequence": 2302,
  "payload": {
    "type": "config_refresh_ack",
    "applied": true
  }
}
```

### JSON 样例使用原则

建议把这几条也写死：

1. 所有样例都按同一 envelope 写
2. `payload.type` 作为判别字段
3. 统一使用结构化 `error`
4. 长生命周期命令都带 `command_id` 和实例 ID

这样以后你扩新能力时，frame 风格不会漂。

### server 内部模块调用链建议

如果你后面自己写 server，除了协议本身，最容易再写乱的是“哪个模块该调哪个模块”。建议先把调用链也固定下来。

### 推荐的核心模块

推荐至少拆这些模块：

1. `web handler`
2. `agent ws gateway`
3. `runtime config service`
4. `agent connection registry`
5. `command service`
6. `probe service`
7. `audit service`
8. `storage / repository`

### 推荐职责边界

#### 1. `web handler`

负责：

- HTTP 路由入口
- 参数解析
- 返回 HTTP 响应

不负责：

- 业务决策
- 直接操作 WS session 内部状态
- 手写审计落库逻辑

#### 2. `agent ws gateway`

负责：

- WS upgrade
- session 生命周期
- transport 编解码
- secure 握手
- 把 `ClientFrame` 交给业务层
- 把 `ServerFrame` 发给 agent

不负责：

- runtime config 持久化
- remote command 策略判断
- probe 调度策略

#### 3. `runtime config service`

负责：

- 配置 patch 校验
- merge 配置
- 发布 live config
- 写配置版本历史

不负责：

- 直接发送业务命令
- 直接处理业务 frame

#### 4. `agent connection registry`

负责：

- 保存在线连接句柄
- 按 `agent_id` 找连接
- 下发 `ServerFrame`

不负责：

- 编码
- 审计业务语义
- 策略判断

#### 5. `command service`

负责：

- 创建 shell/task 业务命令
- 生成 `command_id` / `shell_session_id` / `task_id`
- 检查 remote command 策略
- 调 registry 下发 `ServerFrame`
- 接收 shell/task 的 `ClientFrame` 结果并更新状态

不负责：

- 自己做 WS 编码
- 直接操作 secure transport

#### 6. `probe service`

负责：

- 创建 probe job
- 生成 `probe_job_id`
- 检查 remote probe 策略
- 调 registry 下发 probe frame
- 接收 probe 结果并聚合

不负责：

- 自己做 transport 编码
- 自己维护连接表

#### 7. `audit service`

负责：

- 记录连接事件
- 记录配置变更
- 记录 command/probe 审计

不负责：

- 改业务状态
- 控制连接行为

#### 8. `storage / repository`

负责：

- 数据持久化
- 查询和更新

不负责：

- 校验业务语义
- 拼接协议 frame

### 推荐调用链

#### 1. 下发 remote task

```text
Web Handler
  -> CommandService::run_task()
CommandService
  -> RemoteCommandPolicy check
  -> create command_id / task_id
  -> AuditService::record_command_requested()
  -> AgentConnectionRegistry::send_to_agent(ServerFrame::TaskRun)
AgentWsSession
  -> send_server_frame()
Agent
  -> 执行任务
AgentWsSession
  -> handle_client_frame(TaskStarted/Stdout/Finished)
CommandService
  -> update command state
AuditService
  -> record task result
Repository
  -> persist state / audit
```

#### 2. 修改 runtime config

```text
Web Handler
  -> RuntimeConfigService::patch()
RuntimeConfigService
  -> validate patch
  -> merge config
  -> Repository::save_runtime_config()
  -> publish live config
AuditService
  -> record config change
```

#### 3. agent 上报 probe 结果

```text
AgentWsSession
  -> decode ClientFrame::ProbeResult
ProbeService
  -> update probe job aggregate
AuditService
  -> record probe result summary
Repository
  -> persist probe audit
```

### 推荐的反向调用限制

建议把下面几条限制也固定下来：

1. `Repository` 不反向调用 `Service`
2. `AuditService` 不直接调 `AgentWsSession`
3. `AgentConnectionRegistry` 不知道业务 payload 细节
4. `Web Handler` 不直接落库复杂业务状态

这几条守住以后，代码扩展会轻松很多。

### 文档级模块原则

到这里建议把 server 内部模块原则写成四句：

1. `Web Handler` 只接入，不做复杂业务
2. `Service` 决定业务规则和状态变更
3. `Registry / Gateway` 只负责连接与投递
4. `Repository` 只负责持久化，不承载业务策略

这样以后你自己落目录结构时，模块职责也不会飘。

### 状态机设计建议

到这里为止，文档已经有了：

- runtime config
- 认证
- secure
- frame
- 主动命令
- 模块调用链

接下来最值得再固定的是“状态机”。因为一旦状态机没定清，后面代码里就很容易出现：

- 多处分支各自维护状态
- 一些非法状态转换没有提前拦住
- shell/task/probe 的结束和错误语义不统一

推荐至少固定四类状态机：

1. 连接状态机
2. secure 握手状态机
3. remote shell 状态机
4. remote task / remote probe 状态机

### 连接状态机建议

#### 推荐状态

| 状态 | 说明 |
| --- | --- |
| `accepted` | HTTP/WS upgrade 已接受，但 session 尚未完成注册 |
| `registered` | session 已注册到 `AgentConnectionRegistry` |
| `auth_bound` | upgrade 前 token 已校验通过，已拿到授权身份 |
| `active` | 已能正常收发业务 frame |
| `closing` | 已进入关闭流程 |
| `closed` | 连接已完成清理 |

#### 推荐状态流转

```text
accepted
  -> registered
  -> auth_bound
  -> active
  -> closing
  -> closed
```

注意：

- `registered` 和 `auth_bound` 可以在实现上合并，但语义上最好区分
- `active` 表示不仅建连了，而且已经具备正常业务收发能力

#### 不允许的状态流转

- `closed -> active`
- `closing -> active`
- `accepted -> closed` 之外再继续收发业务 frame

建议代码里一旦发生这类跳转，直接当内部错误处理并记日志。

### secure 握手状态机建议

#### 推荐状态

| 状态 | 说明 |
| --- | --- |
| `plain` | 当前未启用 secure，或尚未进入 secure 流程 |
| `hello_received` | 已收到 `WirePacket::Hello` |
| `handshaking` | server 已创建 responder handshake |
| `secure_ready` | 已完成 handshake，可处理 `SecureData` |
| `secure_failed` | secure 失败，等待关闭连接 |

#### 推荐状态流转

```text
plain
  -> hello_received
  -> handshaking
  -> secure_ready
```

失败分支：

```text
plain / hello_received / handshaking
  -> secure_failed
  -> closing
```

#### 推荐约束

1. `SecureData` 只能在 `secure_ready` 接收
2. `Handshake` 只能在 `handshaking` 接收
3. `Hello` 不应在 `secure_ready` 之后再次出现
4. `session_id` 必须始终绑定当前这一次 secure 状态机

建议把 `secure_state_invalid` 当成明确的 close reason，而不是普通 warn 一下继续跑。

### remote shell 状态机建议

shell 是长生命周期交互式会话，建议状态机要比 task 更细。

#### 推荐状态

| 状态 | 说明 |
| --- | --- |
| `requested` | server 已创建 shell 请求，但还未收到 agent 确认 |
| `opened` | agent 已回 `shell_opened` |
| `streaming` | 正在持续收发 output / input |
| `closing` | server 或 agent 已发起关闭 |
| `closed` | shell 已结束 |
| `failed` | shell 建立或运行失败 |

#### 推荐状态流转

```text
requested
  -> opened
  -> streaming
  -> closing
  -> closed
```

失败分支：

```text
requested / opened / streaming
  -> failed
  -> closed
```

#### 推荐约束

1. `shell_input` 只能发给 `opened` / `streaming`
2. `shell_resize` 只能发给 `opened` / `streaming`
3. `shell_output` 只能从 `opened` / `streaming` 收到
4. `shell_closed` 之后不能再接受新的 `shell_output`

### remote task 状态机建议

task 是一次性执行，更适合有限状态。

#### 推荐状态

| 状态 | 说明 |
| --- | --- |
| `queued` | server 已创建 task 请求 |
| `started` | agent 已确认启动 |
| `streaming` | 正在接收 stdout/stderr |
| `finished` | task 已正常结束 |
| `failed` | task 执行失败或被拒绝 |
| `cancelled` | task 被取消 |

#### 推荐状态流转

```text
queued
  -> started
  -> streaming
  -> finished
```

失败分支：

```text
queued / started / streaming
  -> failed
```

取消分支：

```text
queued / started / streaming
  -> cancelled
```

#### 推荐约束

1. `task_started` 只能出现一次
2. `task_finished`、`task_error`、`task_cancelled` 只能三选一作为终态
3. 终态之后不再接收新的 stdout/stderr

### remote probe 状态机建议

probe 有“单次”和“周期”两类，但建议先抽象成统一 job 状态。

#### 推荐状态

| 状态 | 说明 |
| --- | --- |
| `queued` | server 已创建 probe job |
| `accepted` | agent 已接收 job |
| `running` | 正在执行单次或周期探测 |
| `reporting` | 正在持续回传结果 |
| `finished` | 本轮或整个 job 结束 |
| `failed` | 执行失败 |
| `cancelled` | 被取消 |

#### 推荐状态流转

单次 probe：

```text
queued
  -> accepted
  -> running
  -> reporting
  -> finished
```

周期 probe：

```text
queued
  -> accepted
  -> running
  -> reporting
  -> running
  -> reporting
  -> ...
  -> cancelled / failed / finished
```

#### 推荐约束

1. `probe_result` 只能在 `running` / `reporting` 收到
2. `probe_finished` 是一个完整轮次或 job 的结束信号，语义要提前定清
3. `probe_schedule_patch` 只允许作用在 `accepted` / `running` / `reporting`

### 状态机的统一原则

建议把下面几条写死：

1. 终态不能回到非终态
2. 非法状态流转必须记日志
3. 终态之后的重复 frame 默认忽略并审计
4. 是否“忽略”还是“直接断开连接”，要按 frame 类型提前定好

建议做法：

- transport 层非法状态偏向断开连接
- 业务层重复终态偏向忽略并审计

### 分阶段实现路线图建议

如果你后面要按阶段推进，而不是一次把整套 server 写完，建议直接按下面几期落。

### 阶段 1：最小可连通版本

目标：

- server 能接受 agent WS
- 能识别 `PlainData`
- 能完成 `ClientFrame` / `ServerFrame` 基础收发

范围：

1. `AgentConnectionRegistry`
2. `AgentWsSession`
3. `payload_mode`
4. `PlainData` 解码
5. `send_server_frame()` 明文链路
6. 基础日志

完成标准：

- agent 能连上
- server 能收一个最小 `ClientFrame`
- server 能回一个最小 `ServerFrame`
- 连接注册、解绑正常

### 阶段 2：runtime config 与连接管理

目标：

- runtime config 可读可改
- live / next_connection / reconnect_required 生效语义打通

范围：

1. `RuntimeConfigService`
2. `GET/PATCH/reset/reconnect-all`
3. `watch` 广播 live config
4. reconnect-all
5. runtime config 落库

完成标准：

- 能通过接口改 `ping_interval_ms`
- 活跃连接收到 live config
- 改 queue capacity 仅对新连接生效
- reconnect-all 后新策略生效

### 阶段 3：认证与 secure

目标：

- token 接入认证打通
- secure hello / handshake / secure data 打通

范围：

1. token 校验
2. `allowed_key_ids`
3. `lookup_secure_psk`
4. `Hello / Handshake / SecureData`
5. 身份一致性校验

完成标准：

- 非法 token 不能 upgrade
- 非法 key_id 无法握手
- 合法 secure data 能正常解码
- `frame.agent_id` 不一致会触发拒绝

### 阶段 4：remote task

目标：

- server 能下发一次性 task
- 能接收 stdout/stderr/finished/error

范围：

1. `CommandService::run_task`
2. task 状态机
3. task 审计
4. task 取消

完成标准：

- 可成功执行一个 task
- 可收 stdout/stderr
- 可正确结束和审计
- 取消语义明确

### 阶段 5：remote shell

目标：

- server 能建立交互式 shell
- 能输入、输出、resize、关闭

范围：

1. shell 状态机
2. shell session 管理
3. shell 审计
4. shell 权限策略

完成标准：

- shell 可成功建立
- 输入输出稳定
- resize 生效
- close 语义一致

### 阶段 6：remote probe

目标：

- server 能下发 probe
- 能收结果并审计

范围：

1. `ProbeService`
2. probe job 状态机
3. probe 结果聚合
4. probe 审计

完成标准：

- 单次 probe 可跑通
- 周期 probe 可更新配置
- 结果可聚合和查询

### 阶段 7：adapter 与兼容层

目标：

- 第三方协议接入不污染核心协议

范围：

1. `handle_text_adapter_message()`
2. `handle_binary_adapter_message()`
3. Komari adapter
4. adapter 错误与审计

完成标准：

- 第三方协议可转换成自有 `ClientFrame`
- 自有 `ServerFrame` 可回编码为第三方格式
- 核心 `ClientFrame / ServerFrame` 不出现第三方专有字段

### 阶段 8：审计与排障增强

目标：

- 能快速排障
- 能看清连接、命令、probe、配置变更链路

范围：

1. `connection_event`
2. `remote_command_audit`
3. `remote_probe_audit`
4. 查询接口
5. trace_id 串联

完成标准：

- 能按 `agent_id` 查连接和命令
- 能按 `command_id` 回放一次命令链路
- 能按 `trace_id` 串起跨模块事件

### 每阶段实现原则

建议每个阶段都遵守这几条：

1. 先打通最小主链路，再补边角
2. 每阶段都要有清晰完成标准
3. 每阶段结束后都能独立验证
4. 不要为了兼容层推翻核心模型

### 推荐的开发顺序总结

如果只用一句话概括，建议顺序是：

```text
连接主链路
  -> runtime config
  -> auth + secure
  -> task
  -> shell
  -> probe
  -> adapter
  -> 审计增强
```

这个顺序的原因很简单：

- 先把底座打稳
- 再加业务能力
- 最后再做兼容层和深度运维能力

这样你后面每往上叠一层，都不会反过来推翻前面的设计。

### 术语表与命名字典建议

到这里文档里已经出现了很多 ID、状态、frame 名称。为了避免后面实现时命名慢慢漂掉，建议把最关键的术语直接定死。

#### 核心身份与连接相关术语

| 名称 | 建议含义 | 不要混成什么 |
| --- | --- | --- |
| `agent_id` | 业务层稳定标识一台 agent 的身份 | token、connection_id |
| `connection_id` | 一次 WS 连接实例 ID，每次重连都会变 | agent_id、session_id |
| `session_id` | 一次 secure transport 握手/会话标识 | agent_id、数据库主键 |
| `token` | 接入认证凭证 | agent_id、key_id |
| `key_id` | secure key / PSK 的索引标识 | token、session_id |
| `tenant_id` | 多租户场景下的租户标识 | agent_id |

#### 主动命令相关术语

| 名称 | 建议含义 | 不要混成什么 |
| --- | --- | --- |
| `command_id` | 一次 server 主动下发业务命令的统一请求 ID | shell_session_id、task_id、probe_job_id |
| `shell_session_id` | 一个交互式 shell 会话实例 ID | command_id、task_id |
| `task_id` | 一个一次性 task 执行实例 ID | command_id、shell_session_id |
| `probe_job_id` | 一个 probe job 实例 ID | command_id、task_id |
| `trace_id` | 跨模块追踪一次请求链路的 ID | command_id、connection_id |
| `requested_by` | 发起人或发起系统标识 | agent_id |

#### 运行时配置相关术语

| 名称 | 建议含义 | 不要混成什么 |
| --- | --- | --- |
| `live` | 配置变更对活跃连接或后续决策点立即生效 | `next_connection` |
| `next_connection` | 改完后只影响新连接 | `live` |
| `reconnect_required` | 改完配置源后，需要重连才统一生效 | `live` |
| `snapshot` | 建连或初始化时读取的一次性配置快照 | live runtime view |
| `live runtime view` | 通过 `watch` 热更新给活跃 session 的运行视图 | snapshot |

#### 协议边界相关术语

| 名称 | 建议含义 | 不要混成什么 |
| --- | --- | --- |
| `transport` | WS message、wire packet、secure、payload mode 这一层 | business layer |
| `adapter` | 第三方协议与自有协议之间的翻译层 | business handler |
| `ClientFrame` | agent -> server 的自有业务 frame | WirePacket |
| `ServerFrame` | server -> agent 的自有业务 frame | WS Message |
| `payload.type` | 业务 payload 判别字段 | close reason |
| `close_reason` | 连接关闭原因 | 协议错误码 |

### 命名约束建议

建议把下面几条直接当成命名约束：

1. 统一用 `snake_case`
2. ID 字段统一以 `_id` 结尾
3. 时间戳统一以 `_at` 结尾
4. 超时和间隔统一以 `_ms` 结尾
5. 布尔开关优先用 `allow_`、`enable_`、`require_`、`close_on_`

例如：

- `command_id`
- `connected_at`
- `ping_interval_ms`
- `allow_text_frame`
- `require_secure_handshake`

不要混出这种风格漂移：

- `commandId`
- `pingInterval`
- `secureRequired`
- `shellsid`

### 不建议混用的名字

下面这些概念在文档里已经有明确边界，建议实现时也不要再起近义词：

| 已确定名称 | 不建议再起的近义词 |
| --- | --- |
| `command_id` | `request_id`、`job_id`（除非就是 probe job） |
| `connection_id` | `socket_id`、`client_id` |
| `agent_id` | `node_id`、`client_id` |
| `session_id` | `conn_session_id`、`crypto_session_id` |
| `probe_job_id` | `probe_id`（如果语义其实是 job） |

建议原则是：

- 文档里定了一个名，代码里尽量就只用这个名

### 分阶段测试矩阵建议

如果这份文档后面真的要指导实现，那测试范围也应该提前固定。否则开发很容易出现“功能堆出来了，但不知道什么算完成”。

推荐按阶段定义测试矩阵。

### 阶段 1 测试矩阵：最小可连通版本

#### 正常路径

1. agent 成功 upgrade 到 WS
2. server 成功注册 `connection_id`
3. `PlainData` 成功解码成最小 `ClientFrame`
4. server 成功回一个最小 `ServerFrame`

#### 异常路径

1. reader 提前结束
2. writer task 发送失败
3. 非法最小 JSON / 非法二进制 payload

#### 回归点

1. 注册后一定能解绑
2. 关闭连接后 registry 不残留句柄
3. 明文链路不会误走 secure 分支

### 阶段 2 测试矩阵：runtime config 与连接管理

#### 正常路径

1. `GET /runtime/agent-ws` 返回完整配置
2. `PATCH` 修改 `live` 字段后活跃 session 感知到变化
3. `PATCH` 修改 `next_connection` 字段后仅新连接生效
4. `reconnect-all` 后旧连接被关闭并重连

#### 异常路径

1. patch 非法值
2. patch 版本冲突
3. patch 命中不可变字段

#### 回归点

1. queue capacity 不会错误影响旧连接
2. live config 广播不会造成 session 崩溃
3. reset 接口能恢复默认值

### 阶段 3 测试矩阵：认证与 secure

#### 正常路径

1. 合法 token 成功 upgrade
2. 合法 `key_id` 成功 hello / handshake
3. `SecureData` 成功解密并还原 `ClientFrame`
4. `frame.agent_id == authorized_agent_id`

#### 异常路径

1. 非法 token
2. 过期或撤销 token
3. 不在 `allowed_key_ids` 内的 `key_id`
4. `session_id` 不匹配
5. secure 未 ready 就收到 `SecureData`
6. `frame.agent_id` 冒充其他 agent

#### 回归点

1. token 只负责接入认证，不被错误当成业务身份
2. secure 失败时一定进入明确 close reason
3. secure 成功后 plain / secure 状态不串线

### 阶段 4 测试矩阵：remote task

#### 正常路径

1. `task_run` 成功下发
2. agent 回 `task_started`
3. agent 回 stdout/stderr
4. agent 回 `task_finished`

#### 异常路径

1. task 被策略拒绝
2. task 超时
3. task 启动失败
4. `task_cancel` 后任务终止

#### 回归点

1. `task_started` 只出现一次
2. 终态之后不再接受 stdout/stderr
3. `task_id` 与 `command_id` 关联稳定

### 阶段 5 测试矩阵：remote shell

#### 正常路径

1. `shell_open` 成功
2. `shell_input` -> `shell_output`
3. `shell_resize` 生效
4. `shell_close` -> `shell_closed`

#### 异常路径

1. shell 被策略拒绝
2. shell 打开失败
3. shell 已关闭后继续收到 output
4. 非法 session 上收到 input/resize

#### 回归点

1. `shell_session_id` 生命周期完整
2. shell 错误与 shell 关闭语义不混
3. output 编码字段稳定

### 阶段 6 测试矩阵：remote probe

#### 正常路径

1. `probe_run` 成功
2. agent 回 `probe_accepted`
3. agent 回一条或多条 `probe_result`
4. agent 回 `probe_finished`
5. 周期任务 `probe_schedule_patch` 生效

#### 异常路径

1. probe 被策略拒绝
2. probe 超时
3. probe target 非法
4. 周期任务取消失败

#### 回归点

1. `probe_job_id` 生命周期稳定
2. 单次和周期 probe 状态机不串
3. 结果聚合与原始结果摘要一致

### 阶段 7 测试矩阵：adapter 与兼容层

#### 正常路径

1. 第三方文本消息能 decode 成自有 `ClientFrame`
2. 自有 `ServerFrame` 能 encode 成第三方格式
3. Komari 兼容入口不影响自有入口

#### 异常路径

1. adapter decode 失败
2. adapter encode 失败
3. 第三方入口收到不支持 payload mode

#### 回归点

1. 核心 `ClientFrame / ServerFrame` 不出现第三方专有字段
2. adapter 错误不会污染 transport 状态

### 阶段 8 测试矩阵：审计与排障增强

#### 正常路径

1. 连接事件被完整记录
2. 配置变更被完整记录
3. command/probe 审计可按主键查询
4. `trace_id` 能串起链路

#### 异常路径

1. audit 写入失败
2. audit 部分字段缺失
3. 查询条件组合异常

#### 回归点

1. 审计失败不应反向打崩主链路
2. 结构化字段优先于自由文本
3. 大字段不直接撑爆主表

### 推荐的测试层次

建议测试不要只写一种，至少分三层：

1. 单元测试
   - patch 校验
   - 状态机流转
   - frame 编解码

2. 集成测试
   - WS 连接
   - secure hello / handshake
   - runtime config PATCH
   - shell/task/probe 业务流

3. 回归测试
   - 针对你修过的 bug 固定复现样例
   - 防止后续改 adapter、secure、runtime config 时把主链路带坏

### 文档级测试原则

最后建议再固定四条测试原则：

1. 每阶段必须同时覆盖正常路径和异常路径
2. 每个状态机至少要覆盖一条非法状态流转
3. 每个新增 ID 都要验证关联关系是否稳定
4. 每次修 bug 都尽量补一条回归用例

这样这份文档就不只是“怎么设计”，还包含了“怎么证明它是对的”。

### 什么时候不建议支持文本模式

如果这个 `/agent/v1/connect` 入口以后只给自有 agent 用，而且你已经明确“自有协议统一走 binary wire + secure_psk”，那最干净的做法其实是：

- 自有 agent 主连接只接受 `Binary`
- `Text` 一律返回协议错误或直接断开
- 第三方兼容单独走别的入口，例如 `/agent/v1/komari/connect`

这样更简单，也更不容易在一条连接里混入两套协议。

但如果你现在就是想保留一个统一入口，同时兼容“自有 JSON 文本调试模式”或“第三方文本协议”，那上面这套 `payload_mode` 锁定方案是比较稳的。

### 最后落地建议

最实用的顺序是：

1. 先做 `BinaryWire` 完整链路
2. 再补 `TextFrame -> decode_client_frame -> handle_client_frame`
3. 最后才加 `handle_text_adapter_message()`，把第三方文本协议翻译成你的 `ClientFrame`

这样不会把核心协议和兼容层搅在一起。

---

## 6. 最终推荐实现

如果你现在只想确定“最后应该写成什么样”，推荐按下面这一版收敛，不要在 `run()` 里长期堆很多 if/else。

### 6.1 推荐的上下文模型

```rust
enum AgentWsPayloadMode {
    Unknown,
    BinaryWire,
    TextFrame,
}

enum AgentConnectionSecurity {
    Plain,
    Handshaking {
        key_id: String,
        handshake: snow::HandshakeState,
        session_id: [u8; 16],
    },
    Secure {
        key_id: String,
        transport: snow::TransportState,
        session_id: [u8; 16],
    },
}

struct AgentConnectionContext {
    connection_id: Uuid,
    agent_id: Option<String>,
    payload_mode: AgentWsPayloadMode,
    security: AgentConnectionSecurity,
    last_seen_at: Instant,
    last_client_sequence: u64,
    next_wire_sequence: u64,
}
```

这里的职责要分清：

- `payload_mode` 决定“这条连接收发 Text 还是 Binary”
- `security` 决定“BinaryWire 下的业务 payload 是否需要先解密/加密”
- `agent_id` 决定“这条连接对应哪个业务 agent”
- `state.agent_ws` 决定“这条连接初始化时使用什么队列容量”
- `state.agent_ws` 里某些字段可以热更影响当前连接，某些字段只能影响新连接

### 6.2 推荐的 session 结构

```rust
pub(crate) struct AgentWsSession {
    socket: WebSocket,
    state: AppState,
    context: AgentConnectionContext,
    server_tx: mpsc::Sender<ServerFrame>,
    server_rx: mpsc::Receiver<ServerFrame>,
}
```

你后面如果还要扩展，也建议只往这几个方向加字段：

- `auth`：例如连接级认证结果
- `metrics`：例如收发统计
- `shutdown`：例如主动踢连接

不要把 repository、service、adapter 实例直接堆进 `context`，它只放连接状态。

### 6.3 推荐的收包总流程

统一理解成两层：

1. WebSocket 层：
   - `Message::Binary(bytes)`
   - `Message::Text(text)`
   - `Ping/Pong/Close`
2. 协议层：
   - `BinaryWire -> WirePacket -> ClientFrame`
   - `TextFrame -> ClientFrame`
   - `TextFrame -> 第三方 adapter -> ClientFrame`

完整流程建议是：

```text
WS Upgrade
  -> AgentWsSession::run()
  -> reader.next()
  -> 按 Message 类型分发
  -> lock payload mode
  -> decode 自有协议 / 第三方协议
  -> handle_client_frame()
  -> 写入状态 / 持久化 / 转发结果
```

也就是：

- `run()` 只负责 I/O 分发
- `handle_binary()` / `handle_text()` 只负责协议解包
- `handle_client_frame()` 才负责业务落地
- session 初始化时顺手读取一次 `state.agent_ws.*_capacity`
- `run()` 里如果实现 server 主动 ping 和背压策略，可以按需读取共享运行时配置

### 6.4 推荐的下发总流程

server 下发也统一理解成三层：

1. 业务层生成 `ServerFrame`
2. session 根据 `payload_mode` 选择编码方式
3. writer task 只负责发 `Message`

对应流程：

```text
业务模块
  -> AgentConnectionRegistry::send_to_agent()
  -> server_tx.send(ServerFrame)
  -> AgentWsSession::send_server_frame()
  -> TextFrame: encode_server_frame() -> Message::Text
  -> BinaryWire: encode_server_frame_bytes() -> 可选加密 -> WirePacket -> Message::Binary
```

这里最重要的一点是：

- registry 不做编码
- writer task 不做编码
- owner loop 才做编码和可选加密

### 6.5 第三方兼容的正确位置

如果以后要兼容 `komari` 或别的 agent，不建议这样做：

- 在 `handle_client_frame()` 里判断是不是 `komari`
- 在核心 `ServerFrame` / `ClientFrame` 结构上加第三方字段

推荐这样做：

```text
第三方 Text/Binary 消息
  -> adapter decode
  -> 转成自有 ClientFrame
  -> handle_client_frame()

自有 ServerFrame
  -> adapter encode
  -> 第三方 Text/Binary 消息
```

也就是说：

- 核心业务只认自有 `ClientFrame` / `ServerFrame`
- 第三方协议只在 transport/adapter 边界翻译

### 6.6 路由层的推荐边界

如果你后面确定“自有 agent 只跑二进制 secure 协议”，推荐把入口拆开：

- `/agent/v1/connect`
  只给自有 agent，用 `BinaryWire + secure_psk`
- `/agent/v1/compat/komari`
  只给 `komari` 这类第三方兼容协议，用 `TextFrame`

如果你暂时想保留统一入口，也可以先放一起，但要保留这两个边界：

- 统一入口只负责接入，不负责业务兼容判断
- 真正的兼容判断放到 `handle_text_adapter_message()` / `handle_binary_adapter_message()`

### 6.7 最终推荐的实现顺序

最稳的顺序是：

1. 先实现 `BinaryWire + PlainData`
2. 打通 `ClientFrame -> handle_client_frame -> registry/send_server_frame`
3. 再补 `secure_psk` 的 `Hello / Handshake / SecureData`
4. 再补 `TextFrame -> decode_client_frame`
5. 最后补第三方 adapter

原因很简单：

- 第 1 到第 3 步打通的是你的主协议
- 第 4 步只是加一种轻量传输形态
- 第 5 步才是兼容层，不该反过来主导你的结构

### 6.8 不建议的写法

下面这些写法后面都会变难维护：

- `Message::Binary` 里直接写业务落库逻辑
- `writer_task` 里做加密
- 一条连接同时接受 `Text` 和 `Binary`
- 在 `ClientFrame` / `ServerFrame` 里直接塞第三方专用字段
- registry 里直接存 `WebSocket` 或 writer sink

### 6.9 你现在最值得先写的版本

如果按“先能跑，再扩展”的目标，最值得先实现的是：

1. `payload_mode`
2. `AgentConnectionRegistry`
3. `AgentWsRuntimeConfig`
4. `handle_binary()` 的 `PlainData`
5. `handle_text()` 的自有 JSON `ClientFrame`
6. `send_server_frame()` 的 `TextFrame` / `BinaryWire` 分流

等这版跑通后，再把 `secure_psk` 和第三方 adapter 接上，结构不会推翻。
