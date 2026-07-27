---
title: 安全与密钥生命周期
description: TLS、Noise、Token、rekey 和静态密钥轮换的安全边界。
---

# 安全与密钥生命周期

## TLS 与 Noise 的职责

| 能力 | TLS | Noise |
| --- | --- | --- |
| 浏览器、CDN、标准代理兼容 | 是 | 否 |
| 保护 Agent 到 TLS 终止点 | 是 | 否 |
| 穿过 CDN 后仍保持业务密文 | 否 | 是 |
| 首次无 Server 公钥注册 | 依赖公开 CA/自签配置 | XXpsk3 + 注册 PSK |
| 后续固定 Agent/Server 静态身份 | 可用 mTLS，但代理场景复杂 | IK |

公网部署推荐 HTTPS + Noise。开发机或可信内网可以使用 h2c + Noise，但 HTTP/2 路径、连接元数据和流量
特征仍是明文可观察的。

## 注册 Token 与 PSK

当前 Example 把固定 64 位十六进制 Token 解码为 32 字节 XXpsk3 PSK，并把 Token 放在 Noise 密文内
提交给业务注册逻辑。生产 Token 应满足：

- 使用密码学安全随机数；
- 短期有效、单次使用、可撤销；
- 绑定预期 Agent、租户或注册策略；
- 错误尝试限流并写审计；
- 不放在 URL、普通日志或未加密 gRPC metadata 中。

如果同时存在多组注册 PSK，Server 需要在握手开始前安全选择正确 PSK。不能先用任意 PSK 完成握手，
再根据密文 Token 决定原本应该使用哪个 PSK。

## 连接 rekey

连接 rekey 更新当前 Session 的对称 cipher key，不改变长期身份，也不重建 TCP/TLS/gRPC 流。

```text
Agent request_rekey
  -> RekeyRequest(next generation)
Server 切换 incoming
  -> 使用旧 outgoing 发送 RekeyAck
Server 切换 outgoing
Agent 验证 Ack 后切换 incoming/outgoing
```

默认策略是一小时或 `2^20` 个加密帧触发。等待 Ack 时提前到达的业务消息会按序缓存，完成后继续交付。

## Agent 静态密钥轮换

1. `AgentKeySet::prepare_rotation()` 生成 pending identity 和 snapshot。
2. Agent **先持久化**包含 pending 私钥的 snapshot。
3. 通过已认证 Session 发送新公钥请求。
4. Server 验证并保存 pending Agent 公钥。
5. Server 发送接受消息。
6. Agent `promote_pending()`。
7. 稳定窗口结束后双方退休 previous 公钥。

网络消息中只出现新公钥和轮换 ID，私钥只存在于本地 snapshot。

## Server 静态密钥轮换

1. `ServerKeyRing::prepare_rotation()` 生成 next identity、公告和 snapshot。
2. Server 先保存包含 next 私钥的 snapshot。
3. 通过旧的已认证 Session 公告 next 公钥。
4. Agent `PinnedServerKeys::stage()` 验证并保存 pending 公钥。
5. Agent 发送 acknowledgement。
6. Server `promote_next()`，Agent `promote_pending()`。
7. 稳定窗口后 `retire_previous()`。

轮换窗口内 Agent 可用 `connect_with_candidates()` 按 `pending/current/previous` 顺序尝试。每次尝试是新的
RPC 和 Noise 状态，失败 Session 不能复用。

## 存储要求

- `NoiseIdentity` 故意不实现 `Debug`，但调用方仍必须保护导出的私钥字节。
- snapshot 应原子写入，避免 current 已推进而磁盘仍只有旧状态。
- 多实例 Server 必须同步 keyring 和注册表，否则同一 Agent 连到不同实例会随机认证失败。
- 日志只记录 key ID、rotation ID 和安全错误分类，不记录 PSK、Token、私钥或完整业务载荷。

## 威胁边界

Noise 能防止中间代理读取或修改业务消息，但不能修复终端被入侵、弱 Token、错误授权、私钥泄露、流量分析、
拒绝服务或应用把秘密主动写入日志的问题。安全设计必须同时覆盖端点、存储、权限、审计和限流。
