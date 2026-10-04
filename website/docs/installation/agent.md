---
title: Agent 安装边界
description: 当前 Agent 的构建、权限和运行准备事项。
---

# Agent 安装边界

`smalux-agent` 是部署在被观测主机上的探针。仓库提供源码和可执行 crate，Release workflow 也会手动生成 Draft
跨平台归档；但归档不是安装器，仍未提供 systemd unit、Windows Service 包装或自动升级器。

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

Agent 的本地持久化不只包含身份，且不同状态使用独立文件或目录：

| 状态组 | 默认位置 | 内容与边界 |
| --- | --- | --- |
| 身份 | `<data_dir>/agent/connection-state.json` | Agent Noise 密钥、Server 公钥候选、`agent_id`、注册事务和 committed 状态。 |
| 远程 Job policy | `<data_dir>/agent/job-policy.json` | 只由 Agent 本地 IPC/CLI 修改的 `deny_all` 与逐项 Task 拒绝；每次认证以会话快照上报 Server，不是 Server 持久化的策略副本。 |
| Plus 业务数据 | `<data_dir>/plus/<plugin_id>/<plugin_version>/` | 插件可自行持久化业务幂等数据；Agent 只管理 Worker 生命周期、超时、并发和结果上报。 |
| Agent 运行态 | 仅当前进程 | 当前远程 Job 目录、Scheduler 执行/队列、TaskReport、JobEvent 和 JobCommandResult outbox。 |

Server 数据库持久化 Agent 的权威 Job/runtime 目录、能力与插件 inventory、成功 TaskReport 和 JobEvent；
Agent 重启后生成新的进程实例标识并上报摘要，Server 再从权威目录按当前策略和能力过滤后下发。运行态
和 outbox 不跨 Agent 进程恢复，不承诺跨重启的报告重放。

正式 Agent 文件存储在 Unix 使用 `0600`，在 Windows 使用仅 SYSTEM、Administrators 和当前所有者
可访问的保护 ACL；读取已有文件时也会检查并警告不安全权限。更高安全等级仍可通过
`AgentStateStore` 接入系统密钥库、TPM、HSM 或加密数据库。

不要把身份、策略和高频运行态写进一个不断整体覆盖的大 JSON 文件；独立事务边界可以减少写放大，
也便于在策略损坏时单独备份并使用 `jobs policy repair --reset` 修复。
修复前必须停止 Agent，否则命令会拒绝离线修改。

## 断网行为

Agent 在短暂网络抖动期间继续运行已有远程 Job。从首次断线开始持续超过
`--offline-job-timeout`（默认 `30m`，也可使用 `SMALUX_OFFLINE_JOB_TIMEOUT`）后，Agent 会取消并
清空全部远程 Job、重置内存目录 revision，本地 Job 不受影响。期限内重连会取消计时器；期限后重连
则由 Agent 上报策略和能力，Server 重新下发权威 `ReplaceAllJobs` 快照。

TaskReport 当前使用可配置容量的内存队列，临时断线不会让 Scheduler 重跑已完成 Task，满载时丢弃
最旧报告并告警；重连和优雅关闭时按 FIFO 补发。它没有跨重启持久化或 Server 业务 ACK，这是当前
轻量 Agent 的明确边界。未来若要求不丢数据，再增加磁盘队列、过期时间、磁盘上限、幂等键和确认水位。

## 后续正式安装应包含

- 独立运行用户和数据目录；
- 数据目录权限检查和私钥安全写入；
- Windows Service 或 systemd 服务定义；
- 环境变量/配置文件 schema 与启动校验；
- 结构化日志和退出码；
- 优雅关闭、升级前 drain 和回滚；
- 卸载时“保留身份”与“彻底清除身份”的显式选项。
