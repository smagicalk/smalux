---
title: 实现状态
description: 区分当前已实现、示例实现和待完成的能力。
---

# 实现状态

项目尚未发布稳定版本，下面按当前仓库代码区分能力成熟度。

| 层级 | 含义 | 是否可直接生产部署 |
| --- | --- | --- |
| 已实现并有测试 | crate 内存在实现和自动测试。 | 仍需结合正式应用、存储和运维能力评估。 |
| Example 中实现 | 可运行演示完整调用顺序。 | 否；使用固定 Token、文件状态或简化错误处理。 |
| 仍需完成 | 设计已识别，但主应用尚未形成闭环。 | 否。 |

## 已实现并有测试

- Agent Scheduler：触发、队列、并发、重试、取消和生命周期；
- 固定采集 Task：主机、CPU、内存、负载、磁盘、网络、IP；
- 进程和 Socket 的 SUMMARY/BASIC/DETAILED 模式；
- ICMP、TCP Connect、UDP Request、HTTP 多节点探测；
- Proto JobDefinition、JobCommand、TaskDefinition、TaskReport；
- RemoteJobController 命令幂等、目录 revision 和远程所有权；
- Noise XXpsk3、IK、加密 Session、心跳和同步 rekey；
- Agent/Server 静态密钥轮换状态与 snapshot；
- Tonic Client/Server 适配和 SessionDriver；
- 注册四阶段状态机与错误/超时测试。
- `smalux-server` library 的 Axum 启动链、`/api/v1/health` 和 `/api/v1/grpc` 路由装配；
- Server Noise PSK resolver 的异步接口与握手级超时。
- Server 数据库注册中心：一次性 Token 查询、幂等 prepare、原子 commit、Agent 授权与吊销查询。
- Agent 正式入口：状态恢复、XXpsk3/IK 自动选择、首次连接及断线退避重连、Scheduler 与 RemoteJobController 装配；
- Agent Server 公钥轮换：校验公告、先持久化新旧公钥候选，再通过当前加密会话确认。
- Agent 能力同步：连接后主动上报版本、稳定 Task kind 和 Probe 协议，Server 可查询并校验快照；
- Agent 运行配置：CLI/环境变量覆盖 Scheduler 容量、TaskReport 内存队列和关闭 drain 超时；
- Agent 优雅关闭：Ctrl+C 与 Unix SIGTERM 停止 Scheduler、限时补发内存队列并关闭会话；
- Agent 离线窗口：默认断线 30 分钟后清空远程 Job，重连后由 Server 重新同步权威目录；
- Agent 身份文件权限：Unix `0600` 与 Windows 保护 ACL，读取时检查不安全权限。

## Example 中实现

- Axum REST、WebSocket、gRPC 单端口；
- h2c 与可选 Server TLS；
- 固定注册 Token；
- 文件形式 pending/committed 注册表；
- Client/Server manual 和 driver 模式；
- Server 控制台查询 Token、Agent 和吊销。

Example 用于说明调用流程，不具备生产数据库、审计、限流和密钥安全存储。

正式入口的当前状态也需要单独说明：`smalux-agent` 已组装 Client、Scheduler、RemoteJobController、
自动心跳和重连循环，并可作为 library 嵌入其他进程；`smalux-server` 已能启动 Axum listener，
装配健康检查、Agent gRPC/Noise 入口和数据库注册中心。Server 已在认证后查询并确认 Agent 本地 Job
策略，通过可替换 Provider 预留权威 Job 目录来源，并能接收 JobCommandResult 与 TaskReport；默认
Provider 仍不下发 Job，接收结果也尚未落库。因此“认证连接已实现”仍不
等于“端到端监控系统已经可部署”。

## 仍需完成

- TaskReport 本地持久化、跨 Session ACK、去重和重放；
- Server 端 Job Provider 的真实数据源，以及 JobCommandResult 与 TaskReport 持久化；
- Server Job/指标数据库模型与迁移，以及 Agent/Token 管理 API；
- Server 基于能力快照过滤 Job，以及跨版本能力兼容策略；
- 管理 REST API、Web 管理端和用户授权；
- 安装包、系统服务、容器镜像、升级和回滚；
- `smalux-plus-rustic` 实际业务实现；
- 多实例部署下的迁移互斥、事件通知和端到端并发验证；
- 生产监控、审计、速率限制和容量测试。

## 阅读原则

网站描述应与当前状态一致。如果某页使用“推荐”“生产应当”等措辞，表示设计要求，不等于仓库已经实现。
判断实际可用性时依次查看 Proto、公开 Rust API、测试和 Example。

每次把 Example 能力接入正式入口后，应把对应条目从“Example 中实现”移动到“已实现并有测试”，同时补齐
配置入口、持久化、关闭恢复和集成测试。仅复制路由或调用函数而没有生产状态管理，不算完成迁移。
