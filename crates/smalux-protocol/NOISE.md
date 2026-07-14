# Noise 安全层

本文定义 `smalux-protocol` 内置 Noise 实现的使用约束。Protobuf Frame 仍由核心协议定义；Noise 只负责认证协商上下文、建立双向流量密钥，以及保护 `SessionContent`。

## 固定算法

当前版本固定使用以下算法组合，不允许运行时分别替换其中某一项：

```text
DH       X25519
Cipher   ChaChaPoly
Hash     BLAKE2s
```

稳定协商名称：

| 模式 | scheme |
| --- | --- |
| XX | `smalux.security.noise.xx.25519.chachapoly.blake2s.v1` |
| IK | `smalux.security.noise.ik.25519.chachapoly.blake2s.v1` |

算法或 wire 行为发生不兼容变更时必须使用新的 scheme，不能静默改变现有名称的含义。

## XX 与 IK

### XX

XX 不要求 Client 在连接前知道 Server 静态公钥。双方在三步握手中交换静态身份：

```text
Client                                      Server
  |---- SecurityHandshake(step=0, e) -------->|
  |<--- SecurityHandshake(step=1, e, ee, s) --|
  |---- SecurityHandshake(step=2, s, se) ---->|
  |             verify remote key             |
  |              TransportState               |
```

XX 适合首次注册或通过其他可信来源取得公钥的流程，但“握手得到公钥”不等于“信任该公钥”。生产代码仍必须通过 `PinnedRemoteKey` 或 `CallbackRemoteKeyVerifier` 校验身份。`AllowUnknownRemoteKey` 只适合受限注册流程和测试。

### IK

IK 要求 Client 在握手前已经可信地取得 Server 静态公钥。它使用两步握手，适合已注册设备的后续连接：

```text
Client                                      Server
  |---- SecurityHandshake(step=0, e, es, s) -->|
  |<--- SecurityHandshake(step=1, e, ee, se) ---|
  |             verify remote key              |
  |              TransportState                |
```

创建 IK Client Provider 时，`enable_ik(Some(server_public_key))` 必须传入 Server 公钥；IK Server 使用 `enable_ik(None)`。

## 身份与授权边界

`NoiseKeypair` 是一方的长期 X25519 静态身份。公钥可以公开和持久化，私钥必须由 Agent 或 Server 的安全存储管理。`smalux-protocol` 不负责：

- 注册码、token 或用户登录；
- 公钥与 agent ID、租户、权限的数据库绑定；
- 私钥文件格式、权限、轮换、撤销和恢复；
- 连接断开后的业务会话恢复。

`NoiseRemoteKeyVerifier` 只做同步身份判定。回调处于握手关键路径，必须快速且不可阻塞，禁止直接执行网络请求或慢数据库查询。Server 应在创建 Provider 前准备好内存中的信任信息，或先异步查询再通过固定校验器启动握手。

## 协商绑定

Hello 完成后，`NegotiatedParameters` 生成规范化 `NegotiationTranscript`，其中包含：

- 最终协议版本；
- 最终 capability 集合；
- 最终安全 scheme；
- 最终 Frame 上限；
- session ID；
- Client 和 Server nonce。

Noise 使用以下 prologue：

```text
"smalux/noise/v1\0" || NegotiationTranscript
```

任一字段在链路中被修改都会导致双方 transcript 不同，Noise 握手失败。`channel_binding()` 在握手完成后返回 Noise handshake hash，可供上层审计或绑定后续认证，但不能替代远端静态公钥校验。

## API 流程

Hello 完成后创建安全会话：

```rust
use smalux_protocol::{SecurityProvider, SecurityRole};

let context = protocol_session
    .negotiated()
    .expect("Hello must complete first")
    .security_context(SecurityRole::Client);
let security = noise_provider.start(context)?;
# Ok::<(), smalux_protocol::Error>(())
```

握手期间循环调用：

1. 当前写入方调用 `next_handshake()`。
2. 将结果包装为当前方向的 `SecurityHandshake` Frame。
3. Frame 先交给 `ProtocolSession::on_client_frame` 或 `on_server_frame` 校验 step。
4. 对端将字段转换为 `HandshakeMessage` 并调用 `receive_handshake()`。
5. 双方 `SecurityState::Ready` 后调用 `ProtocolSession::complete_security()`。

Noise 会话声明 `protects_content() == true`。进入 Ready 后，明文 `plaintext_content` 会被状态机拒绝。

发送业务内容：

```text
SessionContent
  -> protect_session_content
  -> ProtectedPayload
  -> ClientFrame / ServerFrame
  -> Transport
```

接收业务内容：

```text
Transport
  -> ClientFrame / ServerFrame
  -> ProtocolSession.on_*_frame
  -> unprotect_session_content
  -> ProtocolSession.on_decrypted_*_content
  -> SessionContent
```

方向 Frame 校验和解密后内容校验都必须执行。只解密而不调用 `on_decrypted_*_content` 会绕过 sequence、Close 和错误状态检查。

## ProtectedPayload record

单个 Noise 消息的硬上限为 65,535 字节。为支持协议最大 Frame，`protect()` 会透明分片，并把多个 Noise 消息封装为一个 `ProtectedPayload.ciphertext`：

```text
offset  size       value
0       4          ASCII "SNR1"
4       2          第一个密文 record 长度，u16 big-endian
6       n          第一个 Noise 密文 record
...     2          下一个密文 record 长度
...     n          下一个 Noise 密文 record
```

规则：

- 每个明文分片最多 60 KiB；
- 每个密文 record 包含 16 字节 AEAD tag；
- 空明文仍编码为一个认证 record；
- 容器必须至少包含一个 record；
- 长度截断、未知 header、超限或认证失败都会使当前安全会话进入 `Failed`；
- 最大明文根据协商后的 `max_frame_bytes` 动态计算，确保外层 Protobuf 编码后不超过 Frame 限制。

该 record 容器是 Noise 安全层内部格式，不是 Transport 分帧。WSS Binary、gRPC message 或 TCP length-delimited Frame 仍负责最外层消息边界。

## Rekey

发送和接收方向分别维护 record 计数。每处理 `1_048_576` 个 record 后调用：

```text
发送方向  TransportState::rekey_outgoing()
接收方向  TransportState::rekey_incoming()
```

分片后的每个 record 都单独计数，而不是每个 `ProtectedPayload` 计数。双方必须可靠、有序且不重复地传输 Frame，否则 Noise nonce 和 rekey 状态会失步并导致认证失败。

## 失败与关闭

以下情况会 fail closed，使会话进入 `SecurityState::Failed`：

- 握手 step 错误或在错误轮次收包；
- transcript 不一致；
- 远端静态公钥校验失败；
- record 格式、长度或认证失败；
- 重放旧密文；
- 计数器溢出；
- 输出超过协商 Frame 上限；
- 内部互斥锁中毒。

进入 `Failed` 后不能恢复或降级为明文，调用方必须关闭底层连接并重新建立完整会话。正常协议 Close 交换完成后调用 `SecuritySession::close()`，实现会丢弃 TransportState、远端公钥副本和 channel binding。

`NoiseKeypair` 使用 `zeroize` 清理 Provider 持有的静态私钥，解密临时缓冲区和 channel binding 也会主动清理。`snow` 内部状态的具体内存清理能力受其实现约束，因此进程崩溃转储、交换文件和宿主机权限仍需由部署环境保护。

## Transport 前提

当前实现只支持可靠、有序、无重复的字节传输，例如：

- WebSocket Binary；
- gRPC 双向流；
- TCP 上的 length-delimited Frame。

不能直接用于 UDP、乱序消息队列或会自动重放旧消息的 Transport。若未来支持这类链路，需要单独设计 nonce、窗口、乱序重组和重放策略，并使用新的安全 scheme。
