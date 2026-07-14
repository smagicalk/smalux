# smalux-protocol

`smalux-protocol` 定义 Smalux 的通用 Client/Server 会话协议。它提供 Protobuf 模型、帧编解码、协议协商、会话状态校验、可插拔安全接口，以及内置的 Noise XX/IK 实现，但不建立网络连接。

## 职责边界

```text
Transport              WSS、gRPC、TCP 等网络读写，由调用方实现
    ↓ bytes
FrameCodec             ClientFrame / ServerFrame 编解码和大小限制
    ↓ frame
ProtocolSession        Hello、握手、Ready、Close 和 sequence 状态校验
    ↓ protected bytes
SecuritySession        Noise 或调用方提供的算法负责握手、加密和解密
    ↓ SessionContent
Application            根据 google.protobuf.Any 分发具体业务消息
```

本 crate 当前不包含：

- WSS、gRPC、HTTP 或 TCP 实现。
- Agent 遥测、任务、终端等业务消息。
- TLS、token、注册授权或证书生命周期管理。
- ACK、自动重传、持久化队列和应用层压缩。

## Wire 格式

`.proto` 文件是协议格式的唯一事实来源：

```text
proto/smalux/protocol/v1/core.proto
```

构建时使用内置的 `protoc` 生成 Rust 类型，生成文件位于 Cargo 的 `OUT_DIR`，不提交到 Git。

最外层严格区分消息方向：

```text
ClientFrame                         ServerFrame
├── ClientHello                     ├── ServerHello
├── SecurityHandshake               ├── SecurityHandshake
├── SessionContent                  ├── SessionContent
└── ProtectedPayload                └── ProtectedPayload
```

Frame 使用 Protobuf `oneof`，因此一条 Frame 最多只能包含一个 body。Protobuf 仍允许 body 完全为空，所以 `FrameCodec` 会额外拒绝空 Frame。

完成握手后，双方交换 `SessionContent`：

```text
SessionContent
├── ApplicationEnvelope
├── Ping
├── Pong
├── ProtocolError
└── Close
```

业务消息放入 `ApplicationEnvelope`：

| 字段 | 约束 | 作用 |
| --- | --- | --- |
| `message_id` | 16 字节 | 跨连接追踪一条业务消息 |
| `correlation_id` | 可选 16 字节 | 将异步响应关联到原请求 |
| `sequence` | 每个方向从 1 连续递增 | 检测重复、回退和跳号 |
| `sent_at_unix_ms` | `uint64` | 日志与延迟观测，不参与安全判断 |
| `payload` | `google.protobuf.Any` | 承载具体业务 Protobuf 消息 |

Client 和 Server 各自维护独立 sequence。sequence 只用于协议校验，不是加密 nonce，也不提供 ACK 或重传。

## 会话流程

```text
Client                                      Server
  |                                           |
  |------------ ClientHello ----------------->|
  |<----------- ServerHello ------------------|
  |                                           |
  |------ SecurityHandshake(step = 0) -------->|
  |<----- SecurityHandshake(step = 1) ---------|
  |------ SecurityHandshake(step = 2) -------->|
  |                                           |
  |      complete_security(&security)          |
  |                 Ready                     |
  |                                           |
  |----------- ProtectedPayload -------------->|
  |<---------- ProtectedPayload ---------------|
  |--------------- Ping ---------------------->|
  |<-------------- Pong -----------------------|
  |--------------- Close --------------------->|
  |<-------------- Close ----------------------|
  |                Closed                      |
```

`ProtocolSession` 的状态变化：

```text
AwaitingClientHello
    -> AwaitingServerHello
    -> NegotiatingSecurity
    -> Ready
    -> Closing
    -> Closed

任意不可恢复的格式、顺序或协商错误 -> Failed
```

每条成功发送或接收的 Frame 都应按方向交给状态机一次。收到受保护帧时，外层 Frame 先通过状态校验，解密出的 `SessionContent` 再调用 `on_decrypted_client_content` 或 `on_decrypted_server_content`。

## Hello 协商

ClientHello 声明：

- 每个 Major 支持的 Minor 连续范围。
- 支持和强制要求的 capability。
- 支持的安全方案。
- 本地最大 Frame 大小。
- 32 字节随机 nonce。

Server 使用 `NegotiationPolicy` 按以下规则选择：

1. 选择双方最高的公共 Major，再选择该 Major 最高的公共 Minor。
2. capability 取交集，任一方要求的 capability 缺失则拒绝。
3. 按 Server 本地安全方案顺序选择第一个公共方案。
4. 最大 Frame 取双方本地限制的较小值。
5. Server 提供 16 字节 session ID 和 32 字节 nonce。

协商结果被编码为 `NegotiationTranscript`。具体安全实现必须认证这些字节，防止版本、能力、安全方案或帧限制被篡改。

## 基本编解码示例

```rust
use smalux_protocol::{
    ClientFrame, ClientHello, CodecLimits, FrameCodec, VersionRange, client_frame,
};

let codec = FrameCodec::new(CodecLimits::default())?;
let frame = ClientFrame {
    body: Some(client_frame::Body::Hello(ClientHello {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: vec!["smalux.core.ping.v1".into()],
        required_capabilities: vec![],
        supported_security_schemes: vec![
            "smalux.security.noise.xx.25519.chachapoly.blake2s.v1".into(),
        ],
        max_frame_bytes: 1024 * 1024,
        nonce: vec![0; 32],
    })),
};

let bytes = codec.encode_client(&frame)?;
let decoded = codec.decode_client(&bytes)?;

match decoded.body {
    Some(client_frame::Body::Hello(hello)) => {
        println!("versions: {}", hello.supported_versions.len());
    }
    Some(_) => println!("another client frame"),
    None => unreachable!("FrameCodec rejects empty frames"),
}
# Ok::<(), smalux_protocol::Error>(())
```

WSS 和 gRPC 已经提供消息边界，应使用 `encode_client`、`decode_client`、`encode_server`、`decode_server`。

TCP 等连续字节流应使用 varint 长度前缀接口：

```rust
use bytes::BytesMut;
use smalux_protocol::{CodecLimits, FrameCodec};

let codec = FrameCodec::new(CodecLimits::default())?;
let mut receive_buffer = BytesMut::new();

// 每次网络读取后追加数据，再循环取出所有完整 Frame。
receive_buffer.extend_from_slice(network_chunk);
while let Some(frame) = codec.decode_client_delimited(&mut receive_buffer)? {
    handle_client_frame(frame);
}
# Ok::<(), smalux_protocol::Error>(())
```

数据不完整时返回 `Ok(None)` 且不消费缓冲区。畸形、非规范或超限长度前缀立即返回错误。

## 业务 Any 示例

任何由 `prost` 生成且实现 `prost::Name` 的消息都可以包装：

```rust
use smalux_protocol::{Ping, pack_any, unpack_any};

let input = Ping {
    nonce: 42,
    sent_at_unix_ms: 1000,
};
let payload = pack_any(&input);
let output: Ping = unpack_any(&payload)?;
assert_eq!(input, output);
# Ok::<(), smalux_protocol::Error>(())
```

`unpack_any` 会严格校验 type URL。未知业务类型不应由核心协议直接丢弃，调用方可以保留 Any、返回业务级不支持错误或交给兼容适配器。

## 加密与解密流程

本 crate 定义 `SecurityProvider` 和 `SecuritySession`，并内置基于 `snow` 的 Noise XX/IK 实现。调用方也可以提供其他安全实现。所有实现都通过 `NegotiatedParameters::security_context()` 取得只读的 `SecurityContext`，再创建单连接安全会话。协商结果和 transcript 不提供可变字段，避免握手完成后被意外修改。

Noise 的模式选择、身份责任、握手时序、record 格式、分片、rekey 和失败处理详见 [NOISE.md](NOISE.md)。

发送受保护消息：

```text
业务消息
  -> pack_any
  -> ApplicationEnvelope
  -> SessionContent.encode
  -> SecuritySession.protect
  -> ProtectedPayload
  -> ClientFrame / ServerFrame
  -> FrameCodec.encode
  -> Transport.send
```

接收受保护消息：

```text
Transport.receive
  -> FrameCodec.decode
  -> ProtocolSession.on_*_frame
  -> SecuritySession.unprotect
  -> SessionContent.decode
  -> ProtocolSession.on_decrypted_*_content
  -> unpack_any / 业务分发
```

调用 `ProtocolSession::complete_security(&security)` 时，状态机会检查安全方案名称一致且 `SecuritySession::state() == SecurityState::Ready`。随后状态机会固化 `protects_content()` 的结果：返回 `true` 时自动拒绝 Ready 状态下的 `plaintext_content`；返回 `false` 时自动拒绝 `protected_payload`，链路安全由该安全实现和 Transport 共同保证。

自定义加密算法必须自行管理 nonce、密钥、重放保护和敏感数据清理。内置 Noise 实现由 `TransportState` 管理 AEAD nonce，并在每个方向处理 1,048,576 个 record 后 rekey。`message_id`、`sequence`、Hello nonce 都不能直接替代加密 nonce。

## 资源限制

| 限制 | 默认/范围 |
| --- | --- |
| 默认最大 Frame | 1 MiB |
| 可配置范围 | 4 KiB 到 16 MiB |
| capability 数量 | 最多 128 |
| security scheme 数量 | 最多 32 |
| 名称长度 | 最多 128 UTF-8 字节 |
| session ID | 固定 16 字节 |
| Hello nonce | 固定 32 字节 |
| message/correlation ID | 固定 16 字节 |

远端协商只能降低 Frame 限制，不能提高本地 `CodecLimits`。应用层还应对业务 Any 内容设置更细的数量和长度限制。

## 扩展协议

以后增加业务协议时继续放在本 crate，但按协议族拆分 schema，例如：

```text
proto/smalux/protocol/v1/core.proto
proto/smalux/agent/v1/telemetry.proto
proto/smalux/agent/v1/control.proto
```

业务消息通过 Any 接入，不向 `ClientFrame` 或 `ServerFrame` 的核心 oneof 持续添加业务字段。新增字段必须使用新的 Protobuf 字段编号，已经发布的字段编号禁止复用。

## 运行 Echo 示例

模块提供三个本地 Echo 示例。它们都在同一进程中启动 Server，Client 从控制台读取文本，并完成 Hello 协商、业务消息往返和 Close 流程。

| 示例 | 运行命令 | Frame 承载方式 |
| --- | --- | --- |
| TCP | `cargo run -p smalux-protocol --example echo` | Protobuf varint length-delimited |
| WSS | `cargo run -p smalux-protocol --example wss_echo` | WSS Binary 中的 raw Protobuf，业务内容使用 Noise XX |
| gRPC | `cargo run -p smalux-protocol --example grpc_echo` | Tonic 双向流中的 Protobuf Frame，业务内容使用 Noise XX |

WSS 示例会动态生成仅对 `127.0.0.1` 有效的临时证书，Server 使用该证书建立 TLS，Client 将该证书加入本次运行的临时信任根，因此实际连接地址是 `wss://127.0.0.1:<随机端口>`。

gRPC 示例通过以下双向流方法传输核心 Frame：

```protobuf
rpc OpenSession(stream smalux.protocol.v1.ClientFrame)
    returns (stream smalux.protocol.v1.ServerFrame);
```

该 service 只属于 example，定义在 `proto/smalux/example/grpc/v1/transport.proto`，不会进入 `smalux-protocol` 的公开 Rust API。

输入任意文本后，Server 会通过 `ApplicationEnvelope` 原样返回；输入 `exit` 或发送 EOF 结束：

```text
[client] -> ClientHello
[server] <- ClientHello
[server] -> ServerHello
[client] <- ServerHello
echo> hello
[client] -> EchoMessage sequence=1
[server] <- EchoMessage sequence=1 text="hello"
[server] -> EchoMessage sequence=1
[client] <- EchoMessage sequence=1 text="hello"
server returned: hello
```

输入 `exit` 或发送 EOF 即可结束。TCP 示例保留 `smalux.security.example-plaintext.v1` 作为明文状态机对照；WSS 和 gRPC 示例会生成临时 Noise 静态密钥，互相 Pin 公钥，执行 XX 三步握手，并只用 `ProtectedPayload` 传输业务内容和 Close。WSS 还具有 TLS 传输加密，gRPC 示例的 HTTP/2 Transport 仅监听本机明文端口。临时密钥和本机监听都只适合示例，禁止直接用于生产环境。
