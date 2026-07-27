---
title: 部署拓扑
description: Agent、Server、TLS、Noise 和存储组件的部署关系。
---

# 部署拓扑

## 最小开发拓扑

```text
Agent Client
    |
    | h2c + gRPC + Noise
    v
127.0.0.1:8080
    Axum Router
      /api/v1/health
      /api/v1/status
      /api/v1/ws
      /api/v1/grpc/*
```

本地 h2c 适合 Example 和开发调试。Noise 仍然加密业务消息，但 HTTP 路径、连接元数据和流量特征没有 TLS
保护，不应直接暴露到公网。

## Server 直接 TLS

```text
Agent -- HTTPS/h2 + Noise --> Smalux Server
                              |- Axum/Tonic
                              |- TLS certificate
                              |- Database
                              `- Noise key store
```

Server 自己加载公开证书链和私钥。Agent 使用系统根证书验证 HTTPS，同时在 gRPC 流内继续执行 Noise。

## CDN 或反向代理

```text
Agent
  HTTPS/h2 + Noise ciphertext
        |
        v
Cloudflare / Nginx
  终止 TLS，保留 gRPC HTTP/2 语义
        |
        v
Smalux Server
  h2c 或内部 TLS
  解密 Noise 业务消息
```

代理能看到域名、路径、时间和密文长度，但不能读取 Noise 内的 Token、Job 或 TaskReport。源站链路是否继续
TLS 取决于网络边界；跨不可信网络时仍应使用 TLS。

## 生产组件

正式部署通常还需要：

- 数据库：Agent、注册事务、Job、上报和审计；
- Server Noise key store：支持 current/next/previous 和原子 snapshot；
- Agent 本地状态：identity、固定 Server 公钥、远程 Job 和离线队列；
- Token 服务：随机生成、TTL、单次消费、限流；
- 监控：Server 自身指标、日志、健康检查和告警；
- 备份：数据库和密钥材料必须一起设计恢复流程。

## 启动与就绪顺序

1. 加载并校验 TLS 证书、Server Noise keyring 和数据库连接。
2. 恢复未完成的注册与密钥轮换事务。
3. 启动 REST/gRPC Router，但 readiness 暂时返回未就绪。
4. 完成 Job 目录、Token 服务和后台写入队列初始化。
5. readiness 变为就绪，代理才开始转发新 Agent 会话。

关闭时顺序相反：先从负载均衡摘除实例，停止接受新会话，再通知现有 Driver 关闭并刷新持久化队列，最后
关闭数据库。只收到进程信号就立刻退出会增加 pending 注册、迟到 TaskReport 和轮换 snapshot 不一致风险。

## Agent 本地持久化

Agent 至少需要分别持久化：Noise identity 与固定 Server key、注册状态、远程 Job 目录版本、离线上报
队列。密钥材料和普通缓存的权限不同，实际部署时不应把所有文件放在可被普通用户读取的同一目录。

远程 Job 在断线后默认仍能按 Scheduler 继续执行。应用层可配置离线运行窗口；窗口到期后暂停远程 Job，
但本地安全或维护 Job 可继续。重新连接后先完成目录 revision 对账，再恢复远程调度和上报。

## 多实例 Server

负载均衡多个 Server 前，必须共享或同步：

- Agent 公钥和吊销状态；
- 注册 pending/committed 事务；
- Token 状态；
- Server Noise keyring；
- Job catalog revision 和命令幂等状态。

如果各实例使用不同 Noise current key，Agent 必须知道候选公钥并能重试；更简单的初期方案是让实例共享
受保护的 Server identity，并保证私钥分发和轮换审计。

多实例还要保证同一 Agent 的并发会话策略明确：允许多个活跃会话时，Job 命令和上报 ACK 必须具有全局
幂等语义；只允许一个会话时，需要用数据库租约或一致性路由替换旧会话，不能依赖单进程内 HashMap。
