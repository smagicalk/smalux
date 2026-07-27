---
title: Server 安装边界
description: 当前 Server 骨架、数据库依赖和部署前必须补齐的能力。
---

# Server 安装边界

`smalux-server` 当前提供 Axum、Tower、SeaORM 和 Protocol 依赖构成的服务端骨架。它不是已经完成的
监控平台发行物，正式 Agent 注册表、Job 管理 API、指标存储和 Web 管理端仍需要继续实现。

:::warning 当前运行状态

正式 Server 当前固定使用 `ServerConfig::default()`，监听 `127.0.0.1:8080`，没有 CLI、配置文件或
环境变量覆盖入口。`build_app_router()` 返回空 Router，因此监听成功不表示已经提供 REST 或 gRPC API。

:::

## 构建

```powershell
cargo build -p smalux-server --release
```

Server manifest 已准备 SQLite、PostgreSQL 和 MySQL 的 SeaORM 驱动，并保留 `frontend-embed` feature：

```powershell
cargo build -p smalux-server --release --features frontend-embed
```

只有在前端静态资源构建和嵌入逻辑完成后，该 feature 才能形成完整的单文件部署体验。

## 运行当前骨架

```powershell
cargo run -p smalux-server
```

成功时输出：

```text
[server] listening on http://127.0.0.1:8080
```

端口已被占用时会返回操作系统 bind 错误。当前没有 `--port` 参数；需要换端口必须先完成配置入口，
而不是把正式部署建立在修改源码常量上。

停止前台进程使用 `Ctrl+C`。当前启动函数尚未接入 shutdown signal 和优雅 drain，因此正式实现时需要
让 HTTP Server、gRPC Session、数据库连接池和后台任务共享取消信号。

## 生产部署前必须确定

1. 数据库类型、连接池、迁移和备份恢复。
2. 注册 Token 的生成、TTL、单次消费、审计和限流。
3. Agent 公钥、吊销状态、租户和业务权限模型。
4. Server Noise 密钥的安全存储、轮换和多实例同步。
5. TaskReport 的幂等、确认水位、保留时间和批量写入。
6. REST、WebSocket 和 gRPC 的公开路径及反向代理规则。
7. 日志、指标、追踪、健康检查和优雅关闭。

## 不应直接复用 Example 的部分

Protocol Example 的固定 Token、目录注册表和控制台命令只用于展示交互顺序。它们缺少数据库事务、
并发控制、访问审计、密钥保护和管理 API，不能直接作为生产 Server 的认证模块。

可以复用的是协议调用顺序：`accept_incoming` 分类 XXpsk3/IK，业务层完成验证和落库后，再调用
`prepare`、`complete` 或 `authorize`。

## 从空 Router 到正式 Server

推荐按以下顺序增加能力：

1. 配置解析、日志和关闭信号；
2. `/health/live` 与 `/health/ready`，分别表达进程存活和依赖就绪；
3. 数据库连接、迁移和 repository；
4. Agent 注册 Token 与公钥授权；
5. Tonic `AgentTransport` 路由和 Session registry；
6. Job 管理 API、TaskReport ingest 和幂等存储；
7. WebSocket/REST 管理接口；
8. 身份认证、租户授权、限流、审计和管理端。

每一步应先有独立健康状态和测试，再对外开放路由。数据库已连接不等于迁移完成，HTTP 能监听也不等于
Agent 会话已经就绪。
