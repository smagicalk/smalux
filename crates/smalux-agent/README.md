# smalux-agent

`smalux-agent` 运行在被监控主机上，负责采集本机指标、生成上报事件、连接 server，并执行已授权的远程能力。

## 当前能力

- 采集：identity、公网 IP、CPU、内存、磁盘、网络、进程、socket。
- 采样调度：不同采集组可配置不同频率，同一时刻到期的采样组合并成一个 update。
- 上报：完整 `snapshot`、可选 `delta`、业务级 `heartbeat`。
- 导出：自有 `smalux_json`、Komari 兼容格式。
- transport：WebSocket、HTTP POST、binary wire、`secure_psk`。
- 控制：`config_patch`、`snapshot_request`、一次性进程/socket 采集。
- 远程能力：remote task、remote job probe、remote shell。
- 安全边界：所有远程命令能力共享同一个 CLI 启用开关，当前覆盖 remote shell / remote task，server 运行时不能动态打开。

暂未接入：温度、GPU、电池、Docker、备份、gRPC transport。

## 快速定位

| 需求 | 入口 |
| --- | --- |
| 看启动参数 | `src/config/cli/args.rs` |
| 看默认配置 | `src/config/defaults.rs`、`src/config/model.rs` |
| 看采集逻辑 | `src/service/collector.rs`、`src/collect.rs`、`src/collect/*` |
| 看上报合并策略 | `src/service/reporter.rs`、`src/telemetry/*` |
| 看导出和重连 | `src/service/export.rs`、`src/export/*` |
| 看 server 控制消息 | `src/service/message/*` |
| 看远程能力 | `src/service/remote/*` |
| 看 Komari 兼容 | `src/export/komari.rs`、`src/export/komari/*` |

## 目录

```text
src/
  main.rs              # 启动入口
  config.rs
  config/
    defaults.rs
    model.rs           # AgentConfig / AgentConfigPatch
    manager.rs         # watch 动态配置
    cli.rs
    cli/               # args / startup / value
  collect.rs           # 本机采集入口
  collect/             # CPU / memory / disk / network / process / socket
  telemetry.rs
  telemetry/           # LatestTelemetry / update / aggregator / report event
  service.rs           # 启动 export、collector、reporter、control
  service/
    collector.rs       # 采集调度
    reporter.rs        # update -> snapshot/delta/heartbeat
    export.rs          # 出站监管、pending、重连恢复
    message.rs
    message/           # handler / inbound / outbound
    remote.rs
    remote/            # task / job / probe / shell
  export.rs
  export/              # adapter、transport、WebSocket、HTTP、Komari、rustls
  export/komari/       # Komari 兼容消息和模型
```

## 运行流程

```text
main
  -> parse CLI
  -> build AgentConfig + ServiceOptions
  -> init_tracing(RUST_LOG + log file)
  -> ConfigManager::new()
  -> service::run()
      -> export_supervisor()
      -> collector_loop()
      -> identity_refresh_loop()
      -> reporter_loop()
      -> control dispatcher
```

数据路径：

```text
collector_loop
  -> TelemetryUpdate
  -> reporter_loop 持有 LatestTelemetry
  -> TelemetryAggregator 生成 snapshot/delta/heartbeat
  -> ClientEvent
  -> export_supervisor
  -> ProtocolAdapter
  -> TransportHub
  -> WebSocket / HTTP
```

控制路径：

```text
server / Komari
  -> transport inbound
  -> 协议 adapter
  -> InboundCommand
  -> ControlDispatcher
  -> ConfigManager / diagnostic / remote task / remote job / shell
  -> ack/error/result
  -> 出站队列
```

## 配置边界

agent 不使用配置文件。配置来源固定为：

```text
默认值
  -> CLI 启动参数
  -> server 下发 config_patch
```

静态能力只允许 CLI 开启：

- remote command（一个开关同时控制 remote shell 和 remote task）
- process/socket 详细采集最高授权级别

动态配置允许 server patch：

- 采样频率
- 上报策略
- export 参数
- remote task/shell/probe 的运行限制
- remote probe enabled 和持续 job

日志配置不支持 server patch；日志级别统一读 `RUST_LOG`。

## 上报策略

当前上报由采集事件驱动：

- 某个采集组到期，采集后产生 `TelemetryUpdate`。
- 同一调度点到期的采集组合并。
- reporter 根据最新状态决定发送 `snapshot`、`delta`、`heartbeat` 或跳过。
- server 可以用 `snapshot_request` 请求完整快照。

示例：

```text
disk = 2s
network = 3s

第 2 秒 -> disk update
第 3 秒 -> network update
第 4 秒 -> disk update
第 6 秒 -> disk + network update
```

这避免了单一 reporter interval 把不同频率采样强行绑在一起。

## Export 边界

- `ProtocolAdapter` 只做“内部语义 -> 外部格式”。
- `TransportHub` 只管理 transport 生命周期和投递。
- `export_supervisor` 负责 pending、最新 snapshot、重连恢复和即时事件。
- 新增协议格式时，优先新增 adapter。
- 新增 transport 时，再接入 `TransportHub` 和 worker。

当前格式：

- `smalux_json`: 自有协议。
- `komari`: Komari 兼容。

## 自有协议

agent 发往 server：

- `snapshot`
- `delta`
- `heartbeat`
- `ack`
- `error`
- `remote_task_result`
- `job_result`

server 发往 agent：

- `snapshot_request`
- `config_patch`
- `collect_processes_once`
- `collect_sockets_once`
- `remote_task_run`
- `job_apply`
- `remote_shell_open`

完整字段见：

- [../smalux-protocol/README.md](../smalux-protocol/README.md)
- [../smalux-server/plan.md](../smalux-server/plan.md)

## 远程能力

remote task：

- 非交互命令。
- 通过 `remote_task_run` 下发。
- 结果通过 `remote_task_result` 回传。
- 启用开关复用 `--remote-command-enabled`。

remote job：

- 通用任务模型。
- 当前稳定 kind 是 `probe`。
- `operation=once` 立即运行，不修改本地 job 表。
- `operation=replace` 整组替换持续 job。
- `operation=patch` 增量 upsert/remove。

remote shell：

- 通过 `remote_shell_open` 打开。
- 主控制通道只负责打开会话和 ack/error。
- 真实 PTY 输入输出走独立临时 WebSocket stream。
- 自有 stream 可继续走 binary wire / `secure_psk`；Komari stream 保持第三方兼容。
- 启用开关复用 `--remote-command-enabled`。

## Komari 兼容

Komari 相关代码集中在 `export/komari*`：

- Komari inbound：`terminal`、`exec`、`ping`。
- Komari outbound：实时 report、basic info、exec result、ping result。
- Komari ping 会被转换成内部 `job_apply(kind=probe)`。
- 内部 `job_result(kind=probe)` 会转换回 Komari `ping_result`。

启动示例：

```powershell
cargo run -p smalux-agent -- -f komari -s https://monitor.smagical.de -t GAxOsu0Yrj6RBSPt7yRAdQ -x true --remote-probe-enabled true
```

## 扩展建议

新增采集项：

- `collect/*`
- `config/model.rs`
- `telemetry/*`
- `smalux-core::model::info`
- 文档和测试

新增控制能力：

- 先扩展 `smalux-protocol`
- 再翻译成 `InboundCommand`
- 最后新增独立 manager
- 结果统一走出站队列

新增第三方兼容：

- 放在独立 adapter 目录。
- 外部消息必须先转换成 `InboundCommand` 或内部上报语义。
- 不要让兼容字段进入自有协议模型。

## 常用命令

```powershell
cargo fmt --all --check
cargo check -p smalux-agent
cargo test -p smalux-agent
cargo run -p smalux-agent -- --help
```

调试单个方向时优先跑对应模块测试，例如：

```powershell
cargo test -p smalux-agent service::remote::job
cargo test -p smalux-agent export::komari
```
