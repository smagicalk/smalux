---
title: Protocol 概览
description: gRPC、Proto、Noise 和业务授权的分层边界。
---

# Protocol 概览

`smalux-protocol` 是 Agent 与 Server 共用的版本化协议 crate。它同时提供 `.proto`、生成的 Tonic
类型、Noise 状态机和高层 Session API。

## 服务与路径

Proto package 为 `smalux.agent.v1`，gRPC service 为 `AgentTransport`：

| RPC | 类型 | 用途 |
| --- | --- | --- |
| `HealthCheck` | Unary | 检查 gRPC 服务可达性，不建立 Noise Session。 |
| `OpenSession` | 双向 Stream | 完成 Noise 握手并持续收发加密业务消息。 |

如果 Server 把 gRPC Router 放在 `/api/v1/grpc` 下，完整方法路径为：

```text
/api/v1/grpc/smalux.agent.v1.AgentTransport/OpenSession
```

前缀由 Axum 或反向代理决定；service 和 method 部分由 Proto package 决定。Client 使用
`set_grpc_prefix("/api/v1/grpc")` 配置相同前缀，不手写完整 RPC 方法路径。

## 外层 ProtocolFrame

`OpenSession` 流传输 `ProtocolFrame`，只允许：

- `NoiseHandshake`：XXpsk3 或 IK 的握手消息；
- `ciphertext`：握手完成后的 Noise AEAD 密文；
- `ProtocolError`：尚未建立加密通道时可安全公开的通用错误。

Token、Job、TaskReport、授权错误和业务负载不应直接放在外层 Frame。握手完成后，ciphertext 解密为
`SecureMessage`，再根据 `oneof body` 分类为注册、通用消息、Job、TaskReport、Session 控制或换钥消息。

## 为什么同时使用 gRPC 和 Noise

gRPC 提供 HTTP/2 流、多语言代码生成、反压和代理生态；Noise 提供与 TLS 终止位置无关的端到端业务
加密和静态身份认证。

```text
Agent
  gRPC framing
    Noise ciphertext
      SecureMessage
        TaskReport / JobCommand
          |
          v
Cloudflare / Nginx      只能终止外层 TLS
          |
          v
Server                  解密 Noise 业务消息
```

使用公开 HTTPS 时，TLS 仍然必要：它隐藏更多 HTTP 元数据、兼容浏览器和代理，并提供标准传输保护。
Noise 是额外的端到端层，不是鼓励在公网裸跑 h2c 的理由。

## 公开 API 层级

一般应用使用：

```text
AgentProtocolClient
ServerSessionAcceptor
TonicNoiseSession
SessionDriver / SessionHandle
```

只有需要把 Noise 接入非 Tonic 传输时，才直接使用 `ClientXxHandshake`、`ServerXxHandshake`、
`ClientIkHandshake`、`ServerIkHandshake` 和 `SecureSession`。同一个会话不能同时由底层手动状态机和
高层 Driver 驱动，因为 AEAD nonce 必须严格按帧顺序推进。

## crate 不负责什么

Protocol 不负责：

- HTTP 监听、Axum Router 和 TLS 证书加载；
- Token 生成、注册表、数据库和租户授权；
- Agent Job 持久化和 TaskReport 离线队列；
- RPC 自动重连和业务消息 ACK；
- 私钥文件权限、KMS/HSM 和多实例密钥同步。

它会返回需要持久化的 identity、public key、transaction ID 和 rotation snapshot，但保存位置和事务
边界由应用决定。
