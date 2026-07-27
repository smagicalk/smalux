---
title: Server 安装边界
description: 当前 Server 骨架、数据库依赖和部署前必须补齐的能力。
---

# Server 安装边界

`smalux-server` 当前提供 Axum、Tower、SeaORM 和 Protocol 依赖构成的服务端骨架。它不是已经完成的
监控平台发行物，正式 Agent 注册表、Job 管理 API、指标存储和 Web 管理端仍需要继续实现。

## 构建

```powershell
cargo build -p smalux-server --release
```

Server manifest 已准备 SQLite、PostgreSQL 和 MySQL 的 SeaORM 驱动，并保留 `frontend-embed` feature：

```powershell
cargo build -p smalux-server --release --features frontend-embed
```

只有在前端静态资源构建和嵌入逻辑完成后，该 feature 才能形成完整的单文件部署体验。

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
