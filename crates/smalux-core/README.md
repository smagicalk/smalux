# smalux-core

`smalux-core` 是 agent 和 server 共享的基础库，只放双方都需要的模型和通用工具。

## 职责

- 定义监控数据模型，例如 `AgentReport`、CPU、内存、磁盘、网络、进程和 socket。
- 定义第三方兼容会复用的基础模型。
- 提供日志初始化能力：测试环境输出控制台，运行环境输出控制台 + 滚动文件。
- 提供日志脱敏工具，避免 token、password、secret、命令输出等敏感内容直接写入日志。
- 提供容量、流量、速率等通用换算工具。

不负责：

- 不定义 `ClientFrame` / `ServerFrame`，这些属于 `smalux-protocol`。
- 不处理 WebSocket、HTTP、binary wire 或 `secure_psk`。
- 不做 agent 采样调度、server 持久化、连接状态或命令状态管理。

## 目录

```text
src/
  lib.rs          # crate 入口
  log.rs          # tracing 初始化
  flow.rs         # 容量/流量单位换算
  model.rs        # 监控模型入口
  model/info.rs   # AgentReport 和监控分组入口
  model/info/     # CPU / memory / disk / network / process / socket 等子模型
  protocol.rs     # 第三方兼容基础模型入口
  protocol/       # Komari 等兼容模型
  utils.rs        # 通用工具和脱敏方法
```

## 上报模型

核心完整快照是 `AgentReport`，当前包含：

- `meta`: schema version、agent version、report timestamp。
- `identity`: agent id、hostname、公网 IP 状态、本地 IP。
- `system`: 操作系统、架构、启动时间等静态信息。
- `core`: CPU、内存、swap、load average。
- `disk`: 容量、挂载点、文件系统、读写累计和速度。
- `network`: 网卡、IP、MAC、MTU、收发累计和网速。
- `processes`: 进程数量、采集级别、可选明细。
- `sockets`: TCP/UDP 数量、采集级别、统计来源、可选明细。

`identity.public_ip` 使用状态对象表达，不用空字符串或占位 IP 表示失败：

- `ready`: 获取成功。
- `failed`: 获取失败且没有旧值。
- `stale`: 最近刷新失败，但保留旧值。
- `disabled`: 配置关闭。
- `pending`: 尚未完成首次尝试。

## 脱敏工具

日志中建议直接复用 `utils.rs` 的脱敏方法：

- JSON 按字段名递归脱敏。
- URL 按敏感 query 参数名脱敏。
- 普通文本按常见 `key=value` / `key: value` 形式脱敏。
- 字节流优先识别 JSON，否则生成安全预览。

典型敏感字段：`token`、`authorization`、`password`、`secret`、`api_key`、`private_key`、`psk`、`command`、`stdout`、`stderr`。

复用建议：

- agent/server 记录 URL、header、body、frame、命令结果时优先调用这里的脱敏工具。
- 脱敏只用于日志，不用于业务响应裁剪。
- 新增敏感字段名时在 core 集中维护，避免 agent/server 各写一份规则。

## 与协议 crate 的关系

- `smalux-core` 定义“监控数据长什么样”。
- `smalux-protocol` 定义“监控数据如何放进 frame、wire 和 secure channel”。

对应关系：

- `AgentReport` -> `ClientFrame(type=snapshot).report`
- `DeltaReport` -> 定义在 `smalux-protocol`，内部字段引用 `smalux-core` 的监控分组
- Komari 兼容 -> 由 agent adapter 使用 core 模型做转换

## 常用命令

```powershell
cargo check -p smalux-core
cargo test -p smalux-core
```
