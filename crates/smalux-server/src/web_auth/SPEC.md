---
id: server-web-login
type: module-design
title: Web 登录与会话边界
status: active
---

# Web 登录基础

本模块提供本地首管理员初始化、同源登录/恢复/退出、meta，以及 JSON-RPC `session.info`、`metrics.latest`、Agent/Job 只读查询、Report/Event 摘要查询与 metrics WebSocket。CPU/内存投影由 `web_metrics` 共享服务提供；Agent/Job/Report/Event 查询复用 `AdminService` 与数据库已有目录、报告和事件记录。用户管理、改密、Job 写入/Operation、raw report payload 和完整资源级授权仍未开放；能力目录不得将未实现方法标为可用。HTTP 处理与密码验证位于模块入口，`store` 负责认证持久化查询及事务；不调用 CLI 子进程，不暴露 IPC。

Web 默认关闭，启用要求回环 listener 与精确规范 origin。生产通过同机 HTTPS 反代访问；仅显式开发模式允许回环 HTTP。认证写入必须带可信 Origin、JSON 类型和 `X-Smalux-Client: web`，有效会话退出额外检查会话绑定 CSRF。读取检查提供的 Origin/Fetch Metadata。所有认证响应禁止缓存；不信任代理来源头，不开放跨域 CORS。

管理员密码使用随机盐 Argon2id，CLI 仅隐藏交互输入，无默认密码或公开 bootstrap。独立迁移新增用户、会话、脱敏安全事件及并发 bootstrap claim；唯一 claim 与用户创建同事务。会话 Cookie 为 32 字节随机值的规范十六进制编码，host-only、HttpOnly、SameSite=Strict、Path=/api/v1，生产 Secure；数据库仅存 SHA-256 摘要。CSRF 使用域分离摘要派生，不持久化原始 Cookie。

每次会话校验都查询用户 enabled、到期与吊销状态；续期不延长绝对期限，条件更新不复活已吊销/到期会话。成功登录、退出和对应事件原子提交；数据库失败不能返回模拟成功。事件不保存输入密码、Cookie、CSRF 或原始 IP。启动与登录时清理已过期/吊销超过 7 天的会话和超过 30 天的事件。

登录限流是每进程全局滑动窗口，默认每分钟 30 次，最多 2 个 Argon2 校验并发；不存在账号也验证启动时生成的 dummy hash。哈希在阻塞线程执行，许可随哈希任务持有。此限流不保证跨进程或重启后连续性，不表示完成生产容量验证。
## Agent/Job/Report/Event 只读 RPC

这些方法只在 Web 显式启用且通过 Cookie 会话认证后提供：

- `agent.list` / `agent.get`：复用 `AdminService` 的 Agent 目录查询及当前进程 SessionRegistry；响应不包含 Noise 公钥、region、labels、note 等当前存储中不存在/不应公开的字段。在线状态不是跨重启持久状态。
- `job.list` / `job.get`：只返回 Server 权威 Job catalog 的受限 summary。每个 Agent 最多 1024 个目录项、总编码定义最大 1 MiB；不存在目录等价于空目录 revision `0`。当前过渡 DTO 不提供编辑用完整 Task config，也不暴露原始 Proto bytes。
- `report.list` / `event.list`：admin/operator 可读，不含原始 payload。Agent 必须存在；可以按 Job ID 和 `[fromMs,toMs)` 查询。Report 时间基准是 `received_at`，Event 是 `emitted_at`；排序分别为时间 DESC + 稳定记录 ID DESC，游标用于获取严格更旧的下一页。毫秒入参在数据库侧换算为微秒。

当前是单 Server 管理域，并无 Agent 级细粒度 Web ACL：已登录主体可读 Agent/Job 目录；报告和事件额外要求 admin/operator。此过渡授权规则不能称为完整 RBAC/资源级隔离。查询响应不包含 clientMutationId、操作记录或应用 ACK；Job 的配置提交、操作可观测状态及原始 Report 获取仍未实现。没有新增 Agent/Job/Report/Event migration。


## metrics WebSocket 生命周期

`ws` 在 `/api/v1/ws` 校验同源 Origin 与会话 Cookie，不要求自定义认证头。只接受 subscribe/unsubscribe/ping；订阅快照和 HTTP metrics.latest 共用投影，不开放写方法。admin/operator/viewer 可读配置范围内有效 Agent 的 CPU/内存，每次订阅、推送及独立 1 秒守卫重查授权。logout 持久吊销成功后通过 session 摘要取消对应连接；到期、用户禁用、Agent 吊销或 Server shutdown 结束连接与子任务。自动快照、WS 控制及心跳不续 idle TTL。

全进程 32 连接，每连接 16 订阅和 100 个去重 Agent；入站 64 KiB、出站 1 MiB、有界队列 32，读库/发送超时 5 秒。文本控制 120 次/分钟，最近 128 个请求 id 防重复。默认 2 秒比较权威投影，仅在变化时推送；无跨连接事件日志，新订阅/重连总是新 epoch 和完整快照。cursor 请求触发 resyncRequired；队列满或发送超时关闭并要求客户端重建快照，不隐藏丢帧。

验收位于 `ws_tests.rs`（真实 TCP/tokio-tungstenite）与 `ws::tests`，数据质量与来源选择在 `web_metrics/tests.rs`。本批不接前端、网络/历史/operation topic 或绑定管理 API，不宣称完成生产容量验证。
