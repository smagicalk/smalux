# smalux-core

`smalux-core` 是 workspace 的共享核心库，放 agent 和 server 都需要复用的模型、单位换算、通用工具和日志初始化能力。

## 当前职责

- 定义内部监控模型，例如 `SystemInfo`、`CpuInfo`、`MemoryInfo`、`DiskInfo`、`NetworkInfo`、`ProcessInfo`、`SocketInfo`。
- 定义 agent 上报模型 `AgentReport`。
- 定义 Komari 兼容协议模型。
- 提供统一 tracing 初始化，支持测试环境控制台输出和运行环境文件滚动 + 控制台输出。
- 提供容量/流量单位转换工具。

## 不负责的内容

- 不定义自有 `ClientFrame` / `ServerFrame`；这些在 `smalux-protocol`。
- 不处理 WebSocket、HTTP、wire packet 或 `secure_psk`。
- 不做 agent 采样调度或 server 持久化。
- 不保存运行时连接状态、命令状态或任务状态。

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

## 公共模型边界

`smalux-core` 的模型分三层：

1. `model/info/*`
   - 监控领域模型。
   - 例如 CPU、内存、磁盘、网络、进程、socket、身份信息。
2. `model::info::AgentReport`
   - agent 最新完整状态快照。
   - `smalux-protocol` 的 `snapshot` 最终承载这个结构。
3. `protocol/*`
   - 第三方兼容协议模型，目前主要是 Komari。
   - 这些不是自有协议稳定面，只是兼容层复用的数据结构。

## 公共工具

`smalux-core` 当前最值得复用的公共能力有三类：

- 日志初始化
  - `log.rs`
  - agent 和 server 都直接复用，避免两边日志滚动、级别和格式再各写一套。
- 脱敏工具
  - `utils.rs`
  - server 记录请求体、URL、token、header、命令输出时应直接复用这里的规则。
- 单位换算
  - `flow.rs`
  - 用于流量、速率和容量展示，不要在 UI、agent、server 各自重复换算逻辑。

## 脱敏规则

当前脱敏工具主要面向日志，不改变业务数据本身：

- JSON 文本：按字段名递归脱敏。
- 普通文本：按常见 `key=value` / `key: value` 形式脱敏。
- URL：按敏感 query 参数名脱敏。
- 字节流：能识别成 JSON 时先按 JSON 规则处理，否则按文本或 base64 预览。

典型敏感字段：

- `token`
- `access_token`
- `authorization`
- `password`
- `secret`
- `api_key`
- `private_key`
- `psk`
- `command`
- `stdout`
- `stderr`

推荐规则：

- server 记录 REST body、agent 帧、命令结果、认证信息时，不要自己写一套脱敏逻辑。
- 对外返回的 API JSON 不应依赖脱敏工具；脱敏是日志层行为，不是业务层字段裁剪。

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

## 与协议的关系

`smalux-core` 和 `smalux-protocol` 的关系要保持清晰：

- `smalux-core`
  - 定义“监控数据长什么样”。
- `smalux-protocol`
  - 定义“这些数据如何放进 frame、如何通过 wire 发送”。

当前对应关系：

- `AgentReport`
  - 对应 `ClientFrame(type=snapshot).report`
- `DeltaReport`
  - 定义在 `smalux-protocol`，但内部字段结构引用 `smalux-core` 的监控模型分组
- Komari report/basic info/task result/ping result
  - 兼容层会复用 `smalux-core` 模型做映射

## 常用命令

```powershell
cargo check -p smalux-core
cargo test -p smalux-core
```
