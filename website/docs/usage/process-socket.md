---
title: 进程与 Socket
description: 按采集成本选择进程和 TCP/UDP Socket 统计或明细。
---

# 进程与 Socket

进程和 Socket 都可能产生大量数据，并触发权限敏感或昂贵的平台查询，因此使用统一的
`CollectionMode` 明确成本。

## CollectionMode

| 模式 | 进程 | Socket | 适用场景 |
| --- | --- | --- | --- |
| `SUMMARY` | 总数和状态聚合，不返回条目。 | TCP/UDP 总数和 TCP 状态聚合。 | 高频趋势监控。 |
| `BASIC` | PID、父 PID、名称、状态、启动时间。 | 地址、端口、协议、地址族和 TCP 状态。 | 常规清单与诊断。 |
| `DETAILED` | 增加 CPU、内存、I/O、路径和命令行。 | 尝试关联 PID。 | 低频深入排障。 |

Proto 零值 `UNSPECIFIED` 必须拒绝，调用方需要主动决定成本。

## 进程筛选

`ProcessSelection` 支持：

- `include_pids`：精确 PID 白名单；
- `include_names`：精确进程名白名单；
- `exclude_names`：精确进程名黑名单。

任一 include 非空时启用白名单，exclude 最终优先。`ranking` 决定截断前的排序：稳定 PID、CPU 使用率
从高到低，或物理内存从高到低。`max_entries` 只限制返回条目，不改变 `matched_processes` 统计。

DETAILED 模式读取完整命令行时应考虑敏感信息：某些程序会把 Token、URL 凭据或密钥路径放在命令行。
Server 存储和 UI 展示前应增加脱敏和访问控制。

进程 CPU 使用率依赖前后样本。`cpu_warmed_up = false` 表示尚无可靠前一样本，不能把零值直接解释为
进程没有 CPU 消耗。

## Socket 筛选

Socket Task 可以选择：

- TCP、UDP 或两者；
- IPv4、IPv6 或两者；
- 最多返回的条目数。

SUMMARY 模式仍返回 TCP 状态分布，例如 LISTEN、ESTABLISHED、TIME_WAIT。BASIC 模式返回地址和端口；
DETAILED 模式额外尝试关联 PID。

`pids_included = false` 表示当前模式或平台没有尝试查询 PID，此时空 `associated_pids` 不能解释为
“没有进程拥有该 Socket”。平台 API、容器隔离或权限不支持时，结果通过 `SocketCollectionStatus`
标记 `UNAVAILABLE` 并给出说明，而不是伪造空数据。

## 推荐周期

- SUMMARY：可用于较高频趋势，但仍需在目标规模上压测。
- BASIC：建议中低频，并设置 `max_entries`。
- DETAILED：按需或低频运行，避免持续扫描命令行和 PID 关联。

当连接或进程数量超过返回上限时，`truncated = true`。Server 必须保留这个标志，不能把前 N 条误当成
完整清单。
