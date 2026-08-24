---
title: Agent 安装边界
description: 当前 Agent 的构建、权限和运行准备事项。
---

# Agent 安装边界

`smalux-agent` 是部署在被观测主机上的探针。目前仓库提供源码和可执行 crate，尚未提供正式安装包、
systemd unit、Windows Service 包装或自动升级器。

:::note 当前运行状态

正式 Agent 已经组装 `SmaluxClient`、`RemoteJobController`、Scheduler 和进程内结果 outbox。
启动后会恢复或创建本地身份，自动选择 XXpsk3/IK，接收远程 Job 并在断线后退避重连。

:::

## 构建

```powershell
cargo build -p smalux-agent --release
```

## 下载 Release 产物

仓库的 Release workflow 目前只允许在 GitHub Actions 页面手动触发，并创建 Draft Release。
Agent 和 Server 使用同一个版本号，但会分别打包。版本来源是两个 crate 的 `Cargo.toml`；触发
workflow 时输入的 Tag 必须带 `v` 前缀，并与 Cargo 版本一致：

```text
Cargo version: 0.1.0
Release tag:   v0.1.0
```

Agent 产物命名为：

```text
smalux-agent-v0.1.0-x86_64-pc-windows-msvc.zip
smalux-agent-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
smalux-agent-v0.1.0-x86_64-unknown-linux-musl.tar.gz
smalux-agent-v0.1.0-x86_64-apple-darwin.tar.gz
smalux-agent-v0.1.0-aarch64-apple-darwin.tar.gz
```

`x86_64-unknown-linux-gnu` 适用于 Ubuntu、Debian、Rocky Linux 等 glibc 系统；
`x86_64-unknown-linux-musl` 适用于 Alpine Linux 等 musl 系统。macOS 产物分别对应 Intel 和
Apple Silicon。每个归档都包含 Agent 可执行文件、本 crate 的 README 和仓库 LICENSE。

下载后应先校验汇总文件：

```bash
sha256sum -c SHA256SUMS --ignore-missing
```

Windows PowerShell 可以使用：

```powershell
Get-FileHash .\smalux-agent-v0.1.0-x86_64-pc-windows-msvc.zip -Algorithm SHA256
```

当前 Release 不包含代码签名、系统服务定义或自动升级器。Draft Release 经人工确认后再公开；
生产部署仍需自行配置 systemd/Windows Service、权限和升级回滚策略。

Windows 产物通常位于 `target/release/smalux-agent.exe`，Linux 产物位于
`target/release/smalux-agent`。

配置 Server 地址和首次注册 Token 后运行（Agent 展示名称由 Server 签发 Token 时绑定）：

```powershell
target/release/smalux-agent.exe run --server-endpoint http://127.0.0.1:12345 --token <TOKEN>
```

当前启动流程是：

```text
读取本地配置和身份
  -> 合并 CLI、环境变量和默认配置
  -> 恢复本地连接身份
  -> 建立 Protocol Session
  -> 启动 SessionDriver
  -> 启动 SchedulerRuntime
  -> 上报 Job 策略和 Agent 能力快照
  -> 接收 JobCommand
  -> 将 TaskReport 写入长流，并在断线期间使用有界内存队列暂存
  -> Ctrl+C/SIGTERM: 停 Scheduler -> 限时 drain -> 断开 Session -> 停本地 IPC
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

正式 Agent 文件存储在 Unix 使用 `0600`，在 Windows 使用仅 SYSTEM、Administrators 和当前所有者
可访问的保护 ACL；读取已有文件时也会检查并警告不安全权限。更高安全等级仍可通过
`AgentStateStore` 接入系统密钥库、TPM、HSM 或加密数据库。

建议把状态分成三个事务边界：

| 状态组 | 内容 | 更新要求 |
| --- | --- | --- |
| 身份 | Agent 私钥、Server 公钥、注册事务 | commit 前先原子保存 pending。 |
| Job catalog | Proto `JobDefinition` 和 catalog revision | Scheduler 应用成功后才推进版本。 |
| 上报队列 | TaskReport、重试次数、过期时间 | 当前只有有界内存队列；持久化与 ACK 尚未实现。 |

不要把三组状态写进一个不断整体覆盖的大 JSON 文件；高频上报会放大写入，身份和 Job 也更难独立恢复。

## 断网行为

Agent 在短暂网络抖动期间继续运行已有远程 Job。从首次断线开始持续超过
`--offline-job-timeout`（默认 `30m`，也可使用 `SMALUX_OFFLINE_JOB_TIMEOUT`）后，Agent 会取消并
清空全部远程 Job、重置内存目录 revision，本地 Job 不受影响。期限内重连会取消计时器；期限后重连
则由 Agent 上报策略和能力，Server 重新下发权威 `ReplaceAllJobs` 快照。

TaskReport 当前使用可配置容量的内存队列，临时断线不会让 Scheduler 重跑已完成 Task，满载时丢弃
最旧报告并告警；重连和优雅关闭时按 FIFO 补发。它没有跨重启持久化或 Server 业务 ACK。要求不丢
数据时仍需实现磁盘队列、过期时间、磁盘上限、幂等键和确认水位。

## 后续正式安装应包含

- 独立运行用户和数据目录；
- 数据目录权限检查和私钥安全写入；
- Windows Service 或 systemd 服务定义；
- 环境变量/配置文件 schema 与启动校验；
- 结构化日志和退出码；
- 优雅关闭、升级前 drain 和回滚；
- 卸载时“保留身份”与“彻底清除身份”的显式选项。
