---
title: 网络探测
description: 配置多个 ICMP、TCP Connect 和 HTTP 探测节点。
---

# 网络探测

`ProbeTaskConfig` 在一次 Task 运行中并发探测多个节点。每个节点可以独立选择协议、尝试次数、超时和
尝试间隔，Task 级 `concurrency` 限制同时工作的节点数量。

## 通用节点字段

| 字段 | 说明 |
| --- | --- |
| `name` | Server 分配的人类可读名称，结果中原样返回。 |
| `host` | ICMP/TCP 使用的域名或 IP；HTTP 中用于统一展示。 |
| `attempts` | 本次 Task 内的尝试次数，必须大于零。 |
| `timeout` | 每次尝试的独立超时。 |
| `interval` | 相邻尝试之间的等待时长，可以为零。 |
| `target` | `icmp_echo`、`tcp_connect` 或 `http` 之一。 |

配置转换阶段会拒绝缺失 target、零 attempts、零 timeout、非法端口、非法 URL，以及 HTTP 成功状态范围
颠倒等错误。应在 Job 应用时失败，而不是等到每个调度周期都重复产生相同运行错误。

## ICMP Echo

ICMP 用于判断网络层可达性并统计往返延迟。配置目标没有额外字段：

```text
name: gateway
host: 192.0.2.1
attempts: 4
timeout: 1s
interval: 250ms
target: icmp_echo
```

ICMP 失败不一定表示业务不可用：中间设备可能禁止 Echo，但 TCP/HTTP 仍正常。Linux 环境还可能受到
ping socket 组范围或 capability 约束，权限错误会作为节点尝试错误返回。

## TCP Connect

TCP 探测尝试完成连接建立，成功后立即关闭连接：

```text
name: database
host: db.internal.example
port: 5432
attempts: 3
timeout: 2s
target: tcp_connect
```

它能证明目标端口接受连接，但不能证明数据库认证、查询或业务逻辑正常。

## HTTP

HTTP 使用完整 URL，并通过状态码范围定义成功：

```text
name: public-api
host: api.example.com
url: https://api.example.com/health
expected_status_min: 200
expected_status_max: 299
follow_redirects: false
```

关闭重定向更容易发现入口配置错误；确实希望检查最终页面时才开启。健康端点应轻量、无副作用，并避免
返回敏感数据。

## 结果解释

`ProbeSnapshot` 返回节点总数、健康节点数和每个节点的明细。节点明细包括：

- 实际协议、解析后的 IP、尝试数和成功数；
- 失败率；
- 成功样本的最小、平均、最大延迟；
- 每次尝试的耗时、HTTP 状态码或错误文本。

只要至少一次尝试成功且满足协议条件，节点就计入 `healthy_nodes`。告警系统仍应结合失败率和连续多次
Task 结果，避免单次偶发成功掩盖高丢包。

例如一次 4 次尝试的结果可以这样解释：

| 成功数 | 节点健康 | 失败率 | 延迟统计 |
| --- | --- | --- | --- |
| 4 | 是 | 0% | 使用 4 个成功样本。 |
| 1 | 是 | 75% | 只使用 1 个成功样本，但应触发高丢包判断。 |
| 0 | 否 | 100% | 没有有效的 min/avg/max 延迟。 |

每次尝试的错误文本用于诊断，不适合作为稳定机器枚举。Server 应主要依据结构化成功数、状态码和耗时
做统计，并把错误文本作为附加上下文。

## 并发与负载

总并发近似受三层共同限制：Scheduler 的 Job 并发、Probe Task 的节点并发，以及节点内部尝试时序。
节点很多时优先限制 Task `concurrency`，不要通过极短 timeout 制造大量快速失败和 DNS 请求。

单节点最坏耗时可近似估算为：

```text
attempts * timeout + (attempts - 1) * interval
```

节点受 Task 并发限制，因此整个 Task 的最坏耗时还要乘以节点批次数。Scheduler 的执行超时必须覆盖这个
预算并留出 DNS、系统调度和结果组装余量；否则 Task 会在自身重试尚未完成前被外层超时取消。

## 如何选择协议

| 目标 | 优先协议 | 限制 |
| --- | --- | --- |
| 判断基础网络可达与延迟 | ICMP | 可能被防火墙丢弃，也可能需要系统权限。 |
| 判断端口是否接受连接 | TCP Connect | 不验证应用认证和业务响应。 |
| 判断真实服务健康 | HTTP | 成本更高，但能验证 TLS、路由和状态码。 |

生产监控常同时配置 TCP 与 HTTP：TCP 用于定位连接层故障，HTTP 用于判断业务入口。两者结果不同并不
矛盾，例如 TCP 成功而 HTTP 失败通常说明服务已监听，但路由、TLS 或应用逻辑异常。
