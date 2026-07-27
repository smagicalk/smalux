---
title: 实现状态
description: 区分当前已实现、示例实现和待完成的能力。
---

# 实现状态

项目尚未发布稳定版本，下面按当前仓库代码区分能力成熟度。

## 已实现并有测试

- Agent Scheduler：触发、队列、并发、重试、取消和生命周期；
- 固定采集 Task：主机、CPU、内存、负载、磁盘、网络、IP；
- 进程和 Socket 的 SUMMARY/BASIC/DETAILED 模式；
- ICMP、TCP Connect、HTTP 多节点探测；
- Proto JobDefinition、JobCommand、TaskDefinition、TaskReport；
- JobController 命令幂等、目录 revision 和远程所有权；
- Noise XXpsk3、IK、加密 Session、心跳和同步 rekey；
- Agent/Server 静态密钥轮换状态与 snapshot；
- Tonic Client/Server 适配和 SessionDriver；
- 注册四阶段状态机与错误/超时测试。

## Example 中实现

- Axum REST、WebSocket、gRPC 单端口；
- h2c 与可选 Server TLS；
- 固定注册 Token；
- 文件形式 pending/committed 注册表；
- Client/Server manual 和 driver 模式；
- Server 控制台查询 Token、Agent 和吊销。

Example 用于说明调用流程，不具备生产数据库、审计、限流和密钥安全存储。

## 仍需完成

- Agent 与 Server 正式连接生命周期和自动重连；
- TaskReport 本地持久化、跨 Session ACK、去重和重放；
- Server 正式 Agent/Token/Job/指标数据库模型与迁移；
- 能力协商和协议版本策略；
- 管理 REST API、Web 管理端和用户授权；
- 安装包、系统服务、容器镜像、升级和回滚；
- `smalux-plus-rustic` 实际业务实现；
- 多实例密钥与注册状态同步；
- 生产监控、审计、速率限制和容量测试。

## 阅读原则

网站描述应与当前状态一致。如果某页使用“推荐”“生产应当”等措辞，表示设计要求，不等于仓库已经实现。
判断实际可用性时依次查看 Proto、公开 Rust API、测试和 Example。
