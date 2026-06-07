# smalux-core

`smalux-core` 是 workspace 的共享核心库，放 agent 和 server 都需要复用的模型、单位换算、通用工具和日志初始化能力。

## 当前职责

- 定义内部监控模型，例如 `SystemInfo`、`CpuInfo`、`MemoryInfo`、`DiskInfo`、`NetworkInfo`、`ProcessInfo`、`SocketInfo`。
- 定义 agent 上报模型 `AgentReport`。
- 定义 Komari 兼容协议模型。
- 提供统一 tracing 初始化，支持测试环境控制台输出和运行环境文件滚动 + 控制台输出。
- 提供容量/流量单位转换工具。

## 目录结构

```text
src/
  lib.rs          # crate 入口
  log.rs          # tracing 初始化
  flow.rs         # 单位换算
  model.rs        # 内部领域模型入口
  model/info.rs   # 监控模型聚合入口
  model/info/     # CPU / memory / disk / network / process / socket 子模型
  protocol.rs     # 外部协议模型入口
  protocol/       # Komari 等协议模型
  utils.rs        # 通用工具
```

## 上报模型

当前上报数据使用 `AgentReport`，`meta.schema_version = 5`，包含：

- `meta`: schema version、agent version、report timestamp
- `identity`: agent id、hostname、public IP status、local IPs
- `system`: 静态系统信息
- `core`: CPU、memory、load average，可选采样组
- `disk`: disk capacity、disk speed、warmed up，可选采样组
- `network`: network speed、total traffic、warmed up，可选采样组
- `processes`: process count、`count/light/details` 级别、采集状态，可选采样组
- `sockets`: TCP/UDP socket count、`count/light/details` 级别、采集状态、统计来源和准确性，可选采样组

`identity.public_ip` 是带状态的必填对象，不用 `null` 或占位 IP 表示失败：

- `ready`: 获取成功，带 `ip`、`source`、`sampled_at`。
- `failed`: 获取失败且没有旧 IP，带 `last_attempt_at` 和 `error`。
- `stale`: 最近一次刷新失败，但保留旧 `ip`。
- `disabled`: 配置关闭公网 IP 采集。
- `pending`: 尚未完成首次尝试。

进程明细、连接明细、温度、备份、Docker、GPU 等后续功能建议继续在 `model/info` 下新增独立模型，然后再挂到新的上报分组。

## 常用命令

```powershell
cargo check -p smalux-core
cargo test -p smalux-core
```
