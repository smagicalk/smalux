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
4. Server 验证并保存注册尝试中的 Agent 公钥；此时还没有 active Agent 授权记录。
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

正式 Server 不直接让 gRPC handler 修改 `ServerKeyRing`。`ServerKeyRingManager` 负责运行时
协调：首次启动使用 `keyring_id = _server` 的唯一键原子创建，轮换先在候选快照上计算，再
使用数据库 `revision` 做 CAS；CAS 成功后才替换当前握手句柄。多实例进程按固定周期读取
更高 revision 并替换本地句柄，因此同一个数据库上的实例最终收敛，旧 revision 不会覆盖新密钥。

## 存储要求

- `NoiseIdentity` 故意不实现 `Debug`，但调用方仍必须保护导出的私钥字节。
- snapshot 应原子写入，避免 current 已推进而磁盘仍只有旧状态。
- 多实例 Server 必须共享同一 keyring 持久化源；当前实现通过 revision 轮询同步 keyring，注册表
  仍需使用同样的共享数据库/一致性策略，否则同一 Agent 连到不同实例会随机认证失败。
- 日志只记录 key ID、rotation ID 和安全错误分类，不记录 PSK、Token、私钥或完整业务载荷。

建议把持久化接口按业务事务拆开，而不是让 Protocol 直接依赖数据库：

| 方法语义 | 必须原子保存的内容 |
| --- | --- |
| 保存注册 pending | 注册公钥、Server Token 绑定的展示名称、预分配 Agent ID、Token ID 和 transaction ID；此时不创建 Agent 授权记录。 |
| 提交注册 | pending 状态转换为 committed，并记录注册 ID。 |
| 保存 Agent 轮换 | current、pending、previous identity 和 rotation ID snapshot。 |
| 保存 Server 轮换 | current、next、previous identity 和确认进度。 |
| 保存固定 Server key | current、pending、previous 公钥和 rotation ID。 |

每次都应先保存“下一状态”再发送会让对端推进的确认消息。进程崩溃后从 snapshot 恢复状态机，而不是仅凭
日志推断密钥位置。

## 故障与恢复

| 故障 | 允许的恢复方式 | 禁止做法 |
| --- | --- | --- |
| 私钥文件损坏或丢失 | 从受保护备份恢复，或吊销旧身份后重新注册。 | 伪造同一公钥对应的新私钥。 |
| 轮换中断 | 加载 snapshot，使用 current/pending/previous 候选继续。 | 只保留最新公钥并立即删除 previous。 |
| 注册 Token 泄露 | 立即撤销并审计使用记录，签发新 Token。 | 继续复用泄露 Token。 |
| Server key 不匹配 | 检查受信任轮换公告或重新注册流程。 | 自动接受网络上任意新公钥。 |
| Noise 解密失败 | 关闭整个 Session，重新 IK。 | 复用已经推进 nonce 的加密状态。 |

## 威胁边界

Noise 能防止中间代理读取或修改业务消息，但不能修复终端被入侵、弱 Token、错误授权、私钥泄露、流量分析、
拒绝服务或应用把秘密主动写入日志的问题。安全设计必须同时覆盖端点、存储、权限、审计和限流。

Noise 也不替代 Server 业务授权。一次 IK 成功只证明对端持有登记公钥；Server 每次建立会话仍要检查该
Agent 是否 active、是否被吊销、属于哪个租户，以及当前允许接收哪些 Job。
