---
title: 反向代理与单端口
description: REST、WebSocket 和 gRPC 共用 Axum 端口的路由与代理要求。
---

# 反向代理与单端口

Protocol Example 使用 Tonic `Routes::into_axum_router()` 把生成的 gRPC service 转成 Axum Router，再与
REST 和 WebSocket 合并：

```rust
let grpc_router = Routes::new(agent_transport_service).into_axum_router();

let app = Router::new()
    .route("/api/v1/health", get(health))
    .route("/api/v1/status", get(status))
    .route("/api/v1/ws", get(websocket))
    .nest("/api/v1/grpc", grpc_router);
```

同一个 TCP 端口可以同时处理普通 HTTP/1 请求、WebSocket Upgrade 和 gRPC HTTP/2。协议由 HTTP 版本、
headers 和路由共同区分，不需要为 gRPC 单独监听端口。

## Client 前缀

```rust
let mut client = AgentProtocolClient::new("https://agent.example.com");
client.set_grpc_prefix("/api/v1/grpc");
```

最终 OpenSession 路径为：

```text
/api/v1/grpc/smalux.agent.v1.AgentTransport/OpenSession
```

不要把完整方法路径传给 `new()`；endpoint 只包含 scheme 和 authority。

## Nginx 要求

代理 gRPC 时需要保留 HTTP/2/gRPC 语义，不能把请求当普通 HTTP/1 upstream。配置时重点确认：

- `/api/v1/grpc/` 使用 gRPC upstream；
- `/api/v1/ws` 正确转发 Upgrade/Connection headers；
- REST 路径使用普通 HTTP upstream；
- 长流 read timeout 足够长，心跳负责检测失联；
- 请求体和缓冲策略不会阻塞双向流；
- 源站使用 h2c 还是 TLS 与 upstream 配置一致。

具体 Nginx 指令依版本和源站 TLS 策略不同，应以部署环境的官方文档和实际握手测试为准。

源站使用 h2c 时，最小路由形态如下，域名和 upstream 名称需要按环境替换：

```nginx
location /api/v1/grpc/ {
    grpc_pass grpc://smalux_upstream;
    grpc_read_timeout 5m;
    grpc_send_timeout 5m;
}

location = /api/v1/ws {
    proxy_pass http://smalux_upstream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}

location /api/v1/ {
    proxy_pass http://smalux_upstream;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
}
```

`grpc_read_timeout` 是两次上游读取之间允许的空闲时间，不是整个长流的总寿命；Nginx 当前默认值为 60 秒。
它应大于协议心跳间隔与可接受网络抖动，否则健康 Session 会被代理提前关闭。源站使用 TLS 时把
`grpc://` 改为 `grpcs://`，并单独配置源站证书验证，不能只改 scheme 后跳过验证。

以上指令依据 [Nginx gRPC 模块官方文档](https://nginx.org/en/docs/http/ngx_http_grpc_module.html)。

## Cloudflare 要求

使用 Cloudflare 时，公开入口通常是 HTTPS。应确认所用产品和计划对 gRPC、HTTP/2 长流、超时和请求大小
的当前限制。Noise 可以穿过 TLS 终止继续保护业务内容，但不能绕过 CDN 的连接时长、速率和流量限制。

Cloudflare 当前要求公开 gRPC endpoint 监听 443、支持 TLS 与 HTTP/2，并通过 ALPN 宣告 HTTP/2；域名
必须处于代理状态，SSL/TLS 模式至少为 Full，同时需要在 Network 页面开启 gRPC。Cloudflare Access
不会替代这条 gRPC 流的业务认证，因此 Smalux 仍要执行 Noise 和 Server 授权。详见
[Cloudflare gRPC 官方要求](https://developers.cloudflare.com/network/grpc-connections/)。

上线前不要只用浏览器打开 URL 判断成功。浏览器不会执行原生 gRPC 双向流，应使用 Example Client 或
grpcurl 先测 `HealthCheck`，再建立真实 `OpenSession` 并观察心跳跨过代理空闲超时。

## 健康检查

- `/api/v1/health`：普通 HTTP 健康检查，适合负载均衡器。
- gRPC `HealthCheck`：验证 gRPC service 可达，但不建立 Noise Session。
- `OpenSession`：业务长流，不能被当成高频健康探测反复打开。

## 排查顺序

1. 直接访问 `/api/v1/health`，确认域名、TLS 和普通路由。
2. 调用 gRPC `HealthCheck`，确认 HTTP/2、路径前缀和代理规则。
3. 建立 XXpsk3/IK，确认 Noise 身份和 Token。
4. 最后检查长期心跳、rekey 和业务消息。

这样能区分 TLS、路由、gRPC、Noise 和业务授权错误，避免把所有失败都归因于“连接不上”。

常见现象与定位：

| 现象 | 优先检查 |
| --- | --- |
| REST 正常，gRPC 返回 404 | `/api/v1/grpc` 前缀和 `grpc_pass` location 是否一致。 |
| gRPC 返回 415 | `Content-Type: application/grpc` 是否被代理改写。 |
| 约 60 秒固定断流 | Nginx `grpc_read_timeout` 与心跳是否匹配。 |
| 直连正常，Cloudflare 后失败 | gRPC 开关、443、Full 模式、ALPN/HTTP2。 |
| HealthCheck 正常，OpenSession 失败 | Noise PSK、固定 Server key、Agent 授权或注册事务。 |
