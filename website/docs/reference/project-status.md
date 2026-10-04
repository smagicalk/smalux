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
- Server Job/runtime 控制面可选 CAS、目录和 runtime 独立通知；
- Server 保留 Job 历史 revision，用于校验延迟 TaskReport/JobEvent 的 Agent 归属；
- 重连 `AgentReconcileSummary` 摘要对账，以及新的 Agent 进程实例检测；
- JobEvent 的进程实例序号、重复 payload 拒绝和序号缺口记录；
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
策略、能力和插件 inventory，数据库 Provider 会按 Agent 当前兼容能力返回权威完整 Job 目录；
JobCommandResult、成功 TaskReport 与异常 JobEvent 都会持久化。Server 的本地 CLI 已可替换/查询/
清空单 Agent Job catalog，更新/查询/清空 Plus runtime，并查询报告与事件。插件私有参数由
schema.pb 动态编码，Server 不安装插件二进制。

## 仍需完成

- 可选的 TaskReport/JobEvent Agent 本地持久化、跨 Session 业务 ACK 和崩溃后重放（当前轻量设计
  使用有界内存队列，重启后由 Server 对账重新下发）；
- Job 模板、批量 Agent 分配、管理 HTTP API 与浏览器表单；
- 跨版本能力兼容策略和生产级结果归档；
- 管理 REST API、Web 管理端和用户授权；
- 安装包、系统服务、容器镜像、升级和回滚；
- `smalux-plus-rustic` 实际业务实现；
- 多实例部署下的迁移互斥、跨进程实时事件通知和端到端并发验证；
- 生产监控、审计、速率限制和容量测试。

## 阅读原则

网站描述应与当前状态一致。如果某页使用“推荐”“生产应当”等措辞，表示设计要求，不等于仓库已经实现。
判断实际可用性时依次查看 Proto、公开 Rust API、测试和 Example。

每次把 Example 能力接入正式入口后，应把对应条目从“Example 中实现”移动到“已实现并有测试”，同时补齐
配置入口、持久化、关闭恢复和集成测试。仅复制路由或调用函数而没有生产状态管理，不算完成迁移。
