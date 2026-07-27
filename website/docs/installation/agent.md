---
title: Agent 安装边界
description: 当前 Agent 的构建、权限和运行准备事项。
---

# Agent 安装边界

`smalux-agent` 是部署在被观测主机上的探针。目前仓库提供源码和可执行 crate，尚未提供正式安装包、
systemd unit、Windows Service 包装或自动升级器。

:::warning 当前运行状态

正式 Agent 的 `main()` 当前为空，只声明 `cli`、`job_control`、`scheduler` 和 `tasks` 模块后退出。
采集器、Scheduler 和 JobController 已经可以在测试或后续应用装配中使用，但尚未组成常驻 Agent 进程。

:::

## 构建

```powershell
cargo build -p smalux-agent --release
```

Windows 产物通常位于 `target/release/smalux-agent.exe`，Linux 产物位于
`target/release/smalux-agent`。

直接运行当前产物会立即正常退出且不采集数据：

```powershell
target/release/smalux-agent.exe
```

这不是崩溃。正式启动流程仍需装配：

```text
读取本地配置和身份
  -> 启动 SchedulerRuntime
  -> 恢复本地/远程 Job
  -> 建立 Protocol Session
  -> 启动 SessionDriver
  -> 接收 JobCommand
  -> 将 TaskReport 写入本地队列或长流
  -> 处理关闭与重连
```

## 运行前考虑

### 权限

不同采集器对权限的要求不同：

- CPU、内存、负载和常规系统信息通常可由普通用户读取；
- 进程详情可能受到目标进程所有者或系统保护策略限制；
- Socket 详情在不同操作系统上可见范围不同；
- ICMP 在 Linux 上可能依赖 ping socket、组范围或 capability；
- 备份、脚本和其他有副作用的 Plus Task 需要额外的本地授权策略。

不要为了方便直接长期使用管理员/root 运行。应先确定启用的 Task，再授予最小权限。

### 长期状态

生产 Agent 至少需要持久化：

- Agent Noise 静态私钥与公钥；
- 首次注册学到的 Server Noise 公钥；
- `agent_id`、`registration_id` 和 committed 状态；
- 已安装的远程 Job 目录及 revision；
- 需要可靠上报时的本地 TaskReport 队列；
- 密钥轮换的 pending/current/previous snapshot。

Protocol Example 使用普通文件展示这些字段，但不是生产安全存储实现。私钥文件应限制访问权限，
必要时接入系统密钥库、TPM、HSM 或加密数据库。

建议把状态分成三个事务边界：

| 状态组 | 内容 | 更新要求 |
| --- | --- | --- |
| 身份 | Agent 私钥、Server 公钥、注册事务 | commit 前先原子保存 pending。 |
| Job catalog | Proto `JobDefinition` 和 catalog revision | Scheduler 应用成功后才推进版本。 |
| 上报队列 | TaskReport、重试次数、过期时间 | 先落盘再发送，ACK 后再删除。 |

不要把三组状态写进一个不断整体覆盖的大 JSON 文件；高频上报会放大写入，身份和 Job 也更难独立恢复。

## 断网行为

当前 `JobController` 不会因为 Session 断开自动删除远程 Job，因此 Agent 可以在网络抖动期间继续运行。
生产实现应增加离线窗口：超过允许时长后，是继续采集、暂停远程 Job，还是仅保留本地 Job，需要明确策略。

TaskReport 当前没有跨重连 ACK。要求不丢数据时，应先写本地有界队列，再由连接层发送；队列必须配置容量、
过期时间、磁盘上限和丢弃策略，防止 Server 长期不可用拖垮 Agent。

## 后续正式安装应包含

- 独立运行用户和数据目录；
- 数据目录权限检查和私钥安全写入；
- Windows Service 或 systemd 服务定义；
- 环境变量/配置文件 schema 与启动校验；
- 结构化日志和退出码；
- 优雅关闭、升级前 drain 和回滚；
- 卸载时“保留身份”与“彻底清除身份”的显式选项。
