---
title: 系统采集任务
description: CPU、内存、磁盘、网络、IP 和主机信息采集能力。
---

# 系统采集任务

Agent 中 `tasks/collect/collectors/` 负责读取原始平台数据，`tasks/collect/` 中的固定 Task 负责应用
Proto 配置并返回 `TaskResult`。所有采样结果携带 `SampleMetadata`：采样开始时间，以及存在上一样本时
的采样间隔。

## 支持类型

| Task | 配置重点 | 结果 |
| --- | --- | --- |
| System | 组合开关和筛选 | 主机、CPU、内存、负载、I/O、IP、Socket、进程摘要。 |
| CPU | 采样行为 | 拓扑、频率、总使用率和逻辑 CPU 使用率。 |
| Memory | 无复杂筛选 | 物理内存和交换区容量与使用量。 |
| Load | 平台支持情况 | 1、5、15 分钟平均负载。 |
| Host | 主机静态属性 | 主机名、系统、内核、架构、启动时间。 |
| Disk I/O | 设备名、挂载点 include/exclude | 容量、累计计数、增量和速率。 |
| Network I/O | 网卡 include/exclude | 字节、数据包、错误计数、增量和速率。 |
| Local IP | 网卡筛选 | 本机接口 IPv4/IPv6 地址。 |
| Public IP | IPv4/IPv6 族选择 | 外部服务解析得到的公网地址。 |

## 选择规则

网卡和磁盘都采用相同优先级：

1. 任一 include 非空时进入白名单模式，只保留明确包含项。
2. 再应用 exclude；同一项同时匹配 include 和 exclude 时，exclude 最终优先。
3. include 全部为空时默认选择全部，再应用 exclude。

网卡必须使用完整接口名，例如 `Ethernet`、`eth0`；磁盘可按完整设备名或完整挂载点选择。使用完整匹配
可以避免模糊名称意外采集其他设备。

## 增量与速率

磁盘和网络的速率不是单次系统调用直接返回，而是当前累计计数减去同一 Task 上次样本，再除以实际
采样间隔。因此：

- 首次采样通常只有累计值，没有可靠增量速率；
- Task 实例重建后需要重新预热；
- 系统计数器回绕或设备重建时实现必须避免产生负增量；
- Scheduler 延迟会改变实际采样间隔，不能只使用配置周期计算速率。

## System Task 的取舍

System Task 适合低频主机概览，但一次组合多个 Collector，成本和结果体积都更高。高频监控建议拆成
独立 Job：CPU/内存可以较高频，磁盘容量和 Host 信息可以较低频，进程与 Socket 详情按需运行。

## Public IP

公网 IP Task 会访问外部解析服务，可选择：

- 同时查询 IPv4 和 IPv6；
- 仅 IPv4；
- 仅 IPv6。

它与 Local IP Task 不同，会引入外部网络依赖、DNS、代理和服务可用性风险。生产环境应配置超时、
重试和允许访问的服务，并避免把临时失败解释为主机没有公网地址。
