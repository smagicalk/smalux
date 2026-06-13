# smalux-agent

`smalux-agent` 是运行在被监控主机上的采集与上报进程。

## 手册导航

如果是按问题查阅，建议直接跳到对应章节：

- 想看功能范围：`当前已完成功能`
- 想看代码入口：`目录结构`、`代码阅读路线`
- 想看扩展边界：`扩展边界`
- 想看导出与 wire：`导出协议`
- 想看 server 如何对接：`Server 自实现对接流程`
- 想看远程能力：`远程能力`
- 想看 patch 规则：`Server Patch`、`更新语义汇总`
- 想看完整 JSON：`数据格式`、`Komari 格式`

## 当前已完成功能

- 启动参数解析：所有参数都有默认值，支持短参数，启动时不依赖配置文件。
- 动态配置：启动配置来自默认值和 CLI；连接 server 后可接收 `config_patch` 热更新运行中采集和上报参数。
- 日志：读取 `RUST_LOG`；测试默认 `debug` 输出到控制台，正式运行输出到控制台和滚动文件。
- 日志滚动：支持通过 CLI 设置日志路径、保留文件数和单文件大小上限。
- 系统身份采集：采集 `agent_id`、hostname、本地 IP、公网 IP 状态和基础系统信息。
- 公网 IP：可开关、可配置首次上报是否等待、可低频刷新、失败时上报状态而不是阻塞。
- 核心指标采集：CPU、单核 CPU、内存、swap、load average。
- 磁盘采集：容量、可用空间、文件系统、挂载点、只读/可移动状态、读写增量、累计 IO、读写速度。
- 网络采集：网卡明细、MAC、IP、MTU、收发字节、包、错误、累计流量、实时网速。
- 网络筛选：支持 include / exclude 网卡；默认统计全部网卡，include 优先于 exclude。
- 进程与连接：支持 `count` / `light` / `details` 三个级别；默认只采集进程总数、TCP socket 总数、UDP socket 总数，server 远程触发的最高级别由启动参数 `--allow-process-level` / `--allow-socket-level` 控制。
- 独立采样频率：core、disk、network、processes、sockets、public_ip、snapshot/heartbeat 和 outbound delivery 可以分别配置频率。
- Telemetry 状态：`collector_loop` 按采集调度点提交 `TelemetryUpdate`；同一调度点到期的采样组会合并成一个 update，`reporter_loop` 独占 `LatestTelemetry` 最新缓存并组装 snapshot、delta 或业务级 heartbeat。
- 上报策略：默认周期完整快照；可选启用业务级 heartbeat 和 delta 增量上报；server 可通过 `snapshot_request` 按需请求完整快照；reporter、control ack/error、remote task 结果和 remote job 结果通过统一有界出站队列进入 export，避免无界堆积。
- 导出抽象：service 通过 `ExportRouter` 和 `TransportHub` 投递数据，具体格式由 `ProtocolAdapter` 实现；transport 真实发送由 `export/worker.rs` 后台执行并回传 `TransportEvent`，当前支持 `smalux_json` 与 `komari`，后续可扩展 gRPC 等 transport。
- WebSocket：支持 `ws` / `wss`、额外 query、query token、bearer token、ping heartbeat、断线重连、server close 清理、Smalux binary wire 和 `secure_psk`；wire packet 与安全通道实现来自 `smalux-protocol`，agent/server 共用。
- HTTP：支持 JSON POST、请求超时、TLS 跳过校验开关；当前用于 Komari basic info 和 exec task result。
- 协议层：`export.format` 当前支持 `smalux_json` 和 `komari`；`smalux_json` 可编码 `snapshot`、`delta`、业务级 `heartbeat`、控制层 `ack/error`、`remote_task_result` 和 `job_result`，通过 WebSocket binary wire 发送；`komari` 兼容实时 report、basic info、terminal、exec task result 和 ping result。
- 控制消息：`smalux_json` 自有协议通过 `ServerFrame` 下发 `snapshot_request`、`config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_task_run`、`job_apply` 和 `remote_shell_open`；带 `sequence` 的命令调度后会收到控制层 `ack/error`，`target_agent_id` 不匹配时会被直接丢弃。
- 远程 shell：通过 CLI-only 参数启用；`smalux_json` 控制通道接收 `remote_shell_open`，每个会话使用独立临时 WebSocket stream 转发原始 PTY 输入、输出和 resize。
- 远程 task：通过 CLI-only 参数启用；`remote_task_run` 执行非交互命令，受并发、超时和输出大小限制，结果通过主出站队列回传 `remote_task_result`。
- 远程 probe：默认关闭，可通过启动参数给初始值，也可由 server `config_patch.remote_probe.enabled=true` 动态开启；支持 TCP / HTTP 探测，ICMP 当前会回 `status=failed`，所有探测受全局和同目标频率保护。
- 测试：覆盖配置、采集、telemetry、WebSocket、Komari 本地 mock、动态配置、reporter、公网 IP 状态等核心路径。

## 暂未接入功能

- 温度、GPU、电池等采集项暂未接入。
- 业务级 heartbeat 和 delta 已实现，但默认关闭，避免破坏只支持完整快照的服务端。
- gRPC transport 暂未实现；HTTP transport 当前只承载 JSON POST。
- 日志配置只支持启动时设置；server patch 不支持日志字段。

## 目录结构

```text
src/
  main.rs          # 二进制入口：日志、CLI、ConfigManager、service
  config.rs        # 配置模块入口
  config/
    defaults.rs    # 默认值
    model.rs       # AgentConfig / AgentConfigPatch
    cli.rs         # CLI 模块入口和测试
    cli/           # args / startup / value：参数定义、启动转换和 CLI 枚举转换
    manager.rs     # watch 动态配置管理
  collect.rs       # 本机采集入口，持有 sysinfo 长生命周期对象
  collect/         # CPU / memory / disk / network / process / socket 具体映射逻辑
  telemetry.rs     # telemetry latest 缓存、内部 update 事件和聚合策略入口
  telemetry/       # LatestTelemetry、TelemetryUpdate、ReportEvent、TelemetryAggregator
  service.rs       # bootstrap、导出监管、采集循环、上报循环和控制消息
  service/         # service 子模块：按消息流和远程能力聚合，避免文件过散
    message.rs     # message 子模块入口
    message/       # handler / inbound / outbound：协议 handler、控制分发、出站事件
    remote.rs      # remote 子模块入口
    remote/        # shell / task / job / probe：远程 shell、一次性任务、通用 job 和探测执行器
  export.rs        # 导出抽象
  export/          # WebSocket、HTTP、Komari、rustls 和 transport worker 适配
  export/komari/   # Komari model、URL、server message、terminal/exec 消息解析
```

## 代码阅读路线

第一次阅读 agent 代码时建议按“入口 -> 数据生成 -> 数据发送 -> 入站控制 -> 远程能力”的顺序看，不要从某个长文件随机跳入：

1. `src/main.rs`
   - 只做启动装配：CLI、日志、`ConfigManager` 和 `service::run()`。
   - 如果启动参数行为不符合预期，先看 `src/config/cli/args.rs` 和 `src/config/cli/startup.rs`。
2. `src/service.rs`
   - 看 `run()` 如何创建队列、启动 `export_supervisor()`、`collector_loop()`、`collector::identity::identity_refresh_loop()` 和 `reporter_loop()`。
   - 这里只看生命周期，具体业务细节继续进入子模块。
3. `src/service/collector.rs` + `src/collect.rs`
   - `collector_loop()` 决定什么时候采样。
   - `LocalCollector` 和 `collect/*` 决定采样内容如何映射成内部 sample。
4. `src/service/reporter.rs` + `src/telemetry/*`
   - `reporter_loop()` 是 latest telemetry 的唯一拥有者。
   - `TelemetryAggregator` 决定本次发送 `snapshot`、`delta`、`heartbeat`，还是跳过。
5. `src/service/export.rs` + `src/export/*`
   - `export_supervisor()` 消费出站事件，维护最新 report、pending 即时事件和重连恢复。
   - `ProtocolAdapter` 只做格式转换，`TransportHub` 只做 transport 投递。
6. `src/service/message/*`
   - `SmaluxControlHandler` 把自有 `ServerFrame` 转成 `InboundCommand`。
   - `ControlDispatcher` 执行配置更新、一次性诊断、远程 task/job/shell，并回 `ack/error`。
7. `src/service/remote/*`
   - `task.rs` 是非交互命令执行。
   - `job.rs` 是通用远程 job 外壳，当前把 `kind=probe` 委派给 probe 执行器。
   - `probe.rs` 是远程网络探测执行器。
   - `shell/*` 是交互式 PTY 和临时 stream 桥接。
8. `src/export/komari/*`
   - Komari 兼容集中放这里；删除或替换第三方兼容时优先从这个目录和 `export/komari.rs` 入手。

排查运行问题时可以先按日志里的关键词定位：`collector` 看采样，`reporter` 看是否生成 report，`export` 看是否投递，`websocket wire` 看二进制封包和 `secure_psk`，`control` 看 server 下发命令调度。

## 扩展边界

这一节不是描述“当前实现细节”，而是约束后续扩展时应该把代码放在哪里、哪些扩展点允许扩、哪些耦合暂时保留。

### 导出格式边界

- `ExportFormat` / `ProtocolAdapter` 只负责把内部语义转换成外部消息格式：
  - 输入是 `OutboundReport`、`remote_task_result`、`job_result`、控制层 `ack/error`
  - 输出是一个或多个 `TransportRequest`
- `ProtocolAdapter` 不负责：
  - 采集系统数据
  - 决定调度频率
  - 管理重连
  - 管理 transport 生命周期
- 新增导出格式时，优先修改：
  - `src/export/adapter.rs`
  - `src/export/plan.rs`
  - 必要时新增 `src/export/<format>.rs`
- 不要让某个格式 adapter 直接操作 `LatestTelemetry`、`ConfigManager` 或远程能力执行器。

### TransportHub 边界

- `TransportHub` 只负责 transport 生命周期和投递：
  - 创建 transport worker
  - 连接长连接 transport
  - 为主实时通道绑定 inbound handler
  - 把 `TransportRequest` 投递给对应 worker
- `TransportHub` 不负责：
  - 业务级重试策略
  - pending 缓存
  - snapshot / delta / heartbeat 选择
  - 远程任务结果编码
- 这些逻辑固定放在：
  - `src/service/export.rs`
  - `src/service/export/delivery.rs`
  - `src/service/export/pending.rs`
  - `src/service/export/pipeline.rs`

### InboundCommand 边界

- `InboundCommand` 是 agent 内部唯一的控制语言。
- 所有外部协议都必须先翻译成 `InboundCommand`，再进入 `ControlDispatcher`：
  - Smalux server 控制消息：`src/service/message/handler.rs`
  - Komari 兼容消息：`src/export/komari/message.rs`
- 协议解析层不要直接调用：
  - `RemoteShellManager`
  - `RemoteTaskManager`
  - `RemoteProbeManager`
  - `ConfigManager`
- 新增控制协议或新兼容层时，优先新增“协议消息 -> InboundCommand”的翻译代码，而不是在协议层复制一套业务逻辑。

### CLI-only 与运行时配置边界

- `ServiceOptions` 保存 CLI-only 静态能力开关：
  - `remote_shell.enabled`
  - `remote_task.enabled`
  - 诊断级 details 授权
- 这些字段不进入 `AgentConfigPatch`，server 运行时不能开启它们。
- `AgentConfig` 保存运行时可热更新参数：
  - 采样频率
  - 上报策略
  - export 参数
  - remote shell / task / probe 的运行限制
- 新增配置项时，先判断它属于哪一类：
  - “是否允许执行某能力”一般属于 `ServiceOptions`
  - “已启用能力的限制和频率”一般属于 `AgentConfig`

### CLI 模块边界

- `src/config/cli.rs` 只作为 CLI 模块入口和测试承载文件，避免重新堆回一个超长文件。
- `src/config/cli/args.rs` 只放 clap 参数结构、参数解析器和 CLI 原始输入类型：
  - 新增启动参数时先放这里
  - 只做字符串到基础类型的解析，不做业务配置校验
- `src/config/cli/startup.rs` 只负责把 CLI 输入转换成启动期结果：
  - `AgentConfig::default()` + CLI patch
  - `ServiceOptions::default()` + CLI-only 静态能力
  - `validate_config()` 和 `ServiceOptions::validate()`
- `src/config/cli/value.rs` 只负责 CLI enum 和运行时 enum 的映射：
  - CLI 可读值保持 snake_case，例如 `smalux_json`、`secure_psk`
  - 不在这里处理 transport、adapter 或配置校验
- CLI 测试继续放在 `src/config/cli.rs`，因为它们验证的是完整启动参数行为，而不是某个子模块的内部实现。

### Remote Shell Stream 边界

- `RemoteShellManager` 只认识统一语义：`RemoteShellInput`、`RemoteShellStreamEvent` 和 `RemoteShellFrame`。
- 外部 stream 协议通过 `RemoteShellStreamCodec` adapter 转换：
  - `SmaluxShellCodec` 位于 `src/service/remote/shell/stream.rs`，入站解析 Smalux shell JSON command，出站生成 `SmaluxWire` payload；WebSocket transport 再按 `binary_plain` 或 `secure_psk` 处理。
  - `KomariTerminalCodec` 位于 `src/export/komari/terminal.rs`，启用 WebSocket raw binary frame；PTY 输出直接发 raw binary，Komari text/binary 输入先转换成统一 `RemoteShellInput`。
- 加密不在 codec 内实现：自有 Smalux stream 的加密由 WebSocket wire 层负责；Komari stream 只依赖 `wss://` 的 TLS，保持第三方协议兼容。
- 新增 shell stream 兼容格式时，优先新增一个 codec adapter，并由对应协议 handler 在 `InboundCommand::RemoteShellOpen` 中传入；不要在 `RemoteShellManager` 内按协议名分支。
- 后续如果出现 gRPC shell、direct shell / relay shell 并存或多 backend 生命周期，再考虑继续拆 `manager.rs`；当前 codec seam 已经能隔离格式差异，不建议为了行数继续硬拆。

### 新功能接入建议

- 新增采集项：
  - 一般会同时修改 `collect`、`config/model`、`telemetry/state`、`telemetry/aggregator` 和文档
  - 这是正常 spread，不代表架构有问题
- 新增导出格式：
  - 优先走 `ProtocolAdapter` 扩展点
  - 如有新 transport，再补 `TransportHub` / `worker` 接入
- 新增远程能力：
  - 先增加 `InboundCommand`
  - 再补 handler 翻译
  - 最后加独立 manager，并通过出站队列回传结果
- 新增 server 控制消息：
  - 先决定是否需要 `ack/error`
  - 需要时复用现有控制响应链路，不新造第二套回包机制

### 什么时候再拆文件

当前不建议为了行数继续硬拆。出现下面情况时再拆更合适：

- `config/cli/args.rs`
  - 新增 2 到 3 组参数，并且参数分组开始明显挤压阅读
- `config/cli/startup.rs`
  - 出现第二个独立转换目标，不再只是 `CLI -> config/service_options/config_patch`
- `service/remote/shell/manager.rs`
  - 增加权限、审计、文件传输、多 backend、direct mode 等新生命周期
- `export/plan.rs`
  - 导出 delivery 增长到明显超过当前几类，周期 delivery 和即时结果 delivery 开始互相挤压

## 配置来源

配置优先级固定为：

```text
AgentConfig::default() + ServiceOptions::default()
  -> CliArgs::parse().into_startup()
  -> CLI 覆盖动态配置和服务静态选项
  -> server 下发 config_patch 只更新动态配置
```

当前不使用配置文件。所有参数都有默认值；server 未下发时一直使用启动时配置，server 下发后通过 `ConfigManager` 的 `tokio::sync::watch` 通知运行中任务。remote shell / remote task 的启用开关和远程 details 诊断授权属于 CLI-only 服务静态选项，不进入 `AgentConfigPatch`，server patch 不能开启这些高权限能力；remote shell / remote task 的运行限制属于动态 `AgentConfig`，可由 CLI 设置初始值，也可由 server patch 调整。remote probe 不执行本地命令，默认关闭但属于动态配置，server 可以按需开启，并受本地频率保护。

启动阶段顺序：

```text
CliArgs::parse()
  -> into_startup()
     -> ServiceOptions::default()
     -> CLI 覆盖服务静态选项
     -> ServiceOptions::validate()
     -> AgentConfig::default()
     -> CLI 覆盖启动期日志配置
     -> CLI patch 覆盖动态配置
     -> validate_config()
  -> init_tracing(initial_config.log_file, initial_config.log_retention_files, initial_config.log_max_size_mb)
  -> ConfigManager::new(initial_config)
  -> service::run(config_manager, service_options)
```

参数格式约定：

- 所有启动参数都是可选参数；未传时使用默认值，server 未下发 patch 时持续使用启动时配置。
- 时间参数使用人类可读格式，例如 `500ms`、`1s`、`5m`、`24h`。
- 布尔参数当前需要显式传值，例如 `--network-enabled false`、`--unsafe-cert true`。
- `--query KEY=VALUE` 可以重复传入；重复 key 使用最后一次传入的值。
- server patch 字段均为可选字段，未出现的字段保持当前值。

基础参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `agent_id` | `SMALUX_AGENT_ID` 或启动时生成 UUID v4 | `--agent-id` / `-i` | 不支持 | agent 实例标识；未显式配置时重启会变化，运行中不允许由 server 改身份 |
| `log_file` | `logs/smalux-agent.log` | `--log-file` / `-l` | 不支持 | 日志文件路径，启动时初始化 tracing |
| `log_retention_files` | `14` | `--log-retention-files` / `-L` | 不支持 | 保留最近 N 个滚动日志文件，必须大于 0 |
| `log_max_size_mb` | `64` | `--log-max-size-mb` | 不支持 | 单个日志文件最大大小，单位 MB，必须大于 0 |
| `log_payload` | `false` | `--log-payload true|false` | 不支持 | 是否允许 `trace` 日志打印截断、脱敏后的实际导出 payload 预览；仍可能包含非敏感上报数据 |
| `log_payload_max_bytes` | `4096` | `--log-payload-max-bytes` | 不支持 | 实际 payload 日志预览最大原始字节数，必须大于 0 |

导出连接参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `export.base_url` | `http://127.0.0.1:9000` | `--server` / `-s` | `export.base_url` | server 根地址；只允许 `http` / `https`，不能带 path/query/fragment，具体 endpoint 由 adapter 派生 |
| `export.format` | `smalux_json` | `--format` / `-f` | `export.format` | 导出数据编码格式；当前支持 `smalux_json` / `komari` |
| `export.wire_mode` | `binary_plain` | `--wire-mode` | `export.wire_mode` | Smalux 自有 wire 模式；`binary_plain` 明文 JSON bytes，`secure_psk` 使用 Noise PSK 加密 |
| `export.secure_required` | `false` | `--secure-required true|false` | `export.secure_required` | 为 `true` 时必须使用 `smalux_json + secure_psk`；当前配置一旦为 `true`，server patch 不能关闭它 |
| `export.auth_mode` | `none` | `--auth` / `-a` | `export.auth_mode` | `none` / `query` / `bearer` |
| `export.token` | 空 | `--token` / `-t` | `export.token` | `query` / `bearer` 模式作为传输认证 token；`secure_psk` 模式必须是 `smx1.<key_id>.<secret_base64url>`，且不会明文发送 |
| `export.query_token_param` | `token` | `--query-token-param` / `-k` | `export.query_token_param` | query token 参数名 |
| `export.query` | 空 | `--query KEY=VALUE` / `-q KEY=VALUE` | `export.query` | 额外 query 参数集合；CLI 可重复，patch 是整体替换 |
| `export.unsafe_cert` | `false` | `--unsafe-cert true|false` / `-u true|false` | 不支持 | 是否跳过 WebSocket/HTTP TLS 证书校验；安全敏感，只允许启动时设置 |
| `export.heartbeat` | `30s` | `--heartbeat` / `-H` | `export.heartbeat` | WebSocket ping 间隔，`0` 表示禁用；非 0 值最低 `100ms`；HTTP 模式不使用 |
| `export.reconnect_interval` | `5s` | `--reconnect-interval` / `-r` | `export.reconnect_interval` | 连接失败或重连等待时间 |

采集参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `core.enabled` | `true` | `--core-enabled true|false` | `core.enabled` | 是否采集 CPU / 内存 / load |
| `core.interval` | `1s` | `--core-interval` / `-c` | `core.interval` | 核心指标采样频率 |
| `disk.enabled` | `true` | `--disk-enabled true|false` | `disk.enabled` | 是否采集磁盘 |
| `disk.interval` | `5s` | `--disk-interval` / `-d` | `disk.interval` | 磁盘采样频率 |
| `disk.include_per_device` | `true` | `--disk-per-device true|false` | `disk.include_per_device` | 是否上报单磁盘明细 |
| `network.enabled` | `true` | `--network-enabled true|false` | `network.enabled` | 是否采集网络 |
| `network.interval` | `5s` | `--network-interval` / `-n` | `network.interval` | 网络采样频率 |
| `network.include_per_interface` | `true` | `--network-per-interface true|false` | `network.include_per_interface` | 是否上报单网卡明细 |
| `network.include_interfaces` | `[]` | `--network-interface NAME` / `-I NAME` | `network.include_interfaces` | 只统计指定网卡；空列表表示全部网卡 |
| `network.exclude_interfaces` | `[]` | `--network-exclude-interface NAME` | `network.exclude_interfaces` | 排除指定网卡；`include_interfaces` 非空时忽略 |
| `processes.enabled` | `true` | `--processes-enabled true|false` | `processes.enabled` | 是否采集进程总数 |
| `processes.interval` | `60s` | `--processes-interval` | `processes.interval` | 进程总数采样频率 |
| `processes.level` | `count` | `--processes-level count|light|details` | `processes.level` | 进程采集级别；`count` 只上报总数，`light` 上报 top 列表，`details` 上报完整明细；server patch/request 不能超过 `--allow-process-level` |
| `processes.limit` | `50` | `--processes-limit` | `processes.limit` | 进程 light/details 返回条数上限，范围 `1..=500` |
| `sockets.enabled` | `true` | `--sockets-enabled true|false` | `sockets.enabled` | 是否采集 TCP/UDP socket 总数 |
| `sockets.interval` | `60s` | `--sockets-interval` | `sockets.interval` | socket 总数采样频率 |
| `sockets.level` | `count` | `--sockets-level count|light|details` | `sockets.level` | Socket 采集级别；`count` 只上报总数，`light` 上报 TCP 状态聚合，`details` 上报 socket 列表；server patch/request 不能超过 `--allow-socket-level` |
| `sockets.limit` | `200` | `--sockets-limit` | `sockets.limit` | socket details 返回条数上限，范围 `1..=2000` |

`processes` / `sockets` 的 interval 还有级别保护：`count` 只受全局最小间隔 `100ms` 限制，`light` 至少 `1s`，`details` 至少 `10s`。这些限制同时作用于启动参数和 server patch，避免误把高成本诊断跑成高频任务。

`processes.count` 和 `sockets.tcp` / `sockets.udp` 始终会在对应采样组启用时上报，`level=light` / `details` 只是在总数之外附加更多明细，不会把总数拿掉；只有 `enabled=false` 时整个采样组才会从 JSON 中省略。

公网 IP 参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `public_ip.enabled` | `true` | `--public-ip-enabled true|false` | `public_ip.enabled` | 是否采集公网 IP |
| `public_ip.required_for_first_report` | `false` | `--public-ip-required true|false` | 不支持 | 第一包是否必须等到公网 IP `ready`；启动门禁字段 |
| `public_ip.prefer_interface_candidate` | `true` | `--public-ip-prefer-interface true|false` | `public_ip.prefer_interface_candidate` | 是否优先使用网卡公网候选地址 |
| `public_ip.verify_interface_candidate` | `true` | `--public-ip-verify-interface true|false` | `public_ip.verify_interface_candidate` | 是否用外部服务校验网卡公网候选地址 |
| `public_ip.lookup_timeout` | `3s` | `--public-ip-lookup-timeout` | `public_ip.lookup_timeout` | 单轮外部公网 IP 探测超时时间 |
| `public_ip.retry_interval` | `30s` | `--public-ip-retry-interval` | 不支持 | 第一包门禁开启时，公网 IP 获取失败后的启动重试间隔 |
| `public_ip.refresh_interval` | `24h` | `--public-ip-refresh-interval` / `-p` | `public_ip.refresh_interval` | 公网 IP 成功后的低频刷新间隔 |
| `public_ip.max_concurrency` | `2` | `--public-ip-max-concurrency` | `public_ip.max_concurrency` | 公网 IP 外部服务最大并发数，必须大于 0 |

公网 IP 默认是可选采集：启动时会尝试获取，但获取失败不会阻塞第一包上报。`identity.public_ip.status` 表示当前状态：

- `ready`：获取成功，包含 `ip`、`source`、`sampled_at`，外部校验成功时包含 `verified_at`。
- `failed`：最近一次获取失败，包含 `last_attempt_at` 和 `error`，没有可用旧 IP。
- `stale`：最近一次刷新失败，但保留了上一次可用 `ip`。
- `disabled`：`public_ip.enabled=false`，不会尝试公网 IP 探测。
- `pending`：尚未完成首次尝试，正常启动上报前通常不会出现。

上报参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `report.enabled` | `true` | `--report-enabled true|false` | `report.enabled` | 是否启用上报 |
| `report.interval` | `5s` | `--report-interval` / `-R` | `report.interval` | 定时 snapshot/heartbeat 检查频率；采集 update 也会触发 reporter 立即尝试生成上报 |
| `report.heartbeat_enabled` | `false` | `--report-heartbeat-enabled true|false` | `report.heartbeat_enabled` | 无变化时是否发送业务级 heartbeat |
| `report.heartbeat_interval` | `30s` | `--report-heartbeat-interval` | `report.heartbeat_interval` | 业务级 heartbeat 最小间隔 |
| `report.delta_enabled` | `false` | `--report-delta-enabled true|false` | `report.delta_enabled` | 是否启用 delta 增量上报 |
| `report.snapshot_interval` | `5m` | `--report-snapshot-interval` | `report.snapshot_interval` | 启用 delta 后，强制定期发送完整 snapshot 的间隔 |
| `report.force_snapshot_min_interval` | `10s` | `--report-force-snapshot-min-interval` | `report.force_snapshot_min_interval` | server `snapshot_request` 触发完整 snapshot 的最小响应间隔；过快重复请求会合并 |

出站 delivery 参数：

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `outbound.realtime_report.enabled` | `true` | `--realtime-report-enabled true|false` | `outbound.realtime_report.enabled` | 是否启用实时上报 delivery |
| `outbound.realtime_report.send_on_start` | `true` | `--realtime-report-send-on-start true|false` | `outbound.realtime_report.send_on_start` | 拿到第一份 report 后是否立即发送一次 |
| `outbound.basic_info.enabled` | `true` | `--basic-info-enabled true|false` | `outbound.basic_info.enabled` | 是否启用 basic info 事件；当前用于 Komari `uploadBasicInfo` |
| `outbound.basic_info.refresh_interval` | `5m` | `--basic-info-refresh-interval` | `outbound.basic_info.refresh_interval` | basic info 低频事件生成间隔 |
| `outbound.basic_info.send_on_start` | `true` | `--basic-info-send-on-start true|false` | `outbound.basic_info.send_on_start` | 第一份 telemetry ready 后是否立即生成一次 basic info |

采集 update 会驱动 reporter 立即尝试生成 `OutboundReport`；`report.interval` 仍用于定时 snapshot/heartbeat 检查，避免长时间没有采集变化时完全静默。`outbound.realtime_report` 控制 realtime delivery 是否启用以及是否在第一份 report ready 后立即运行；realtime report 使用 `OnLatestReport` 触发，没有独立 interval 字段。`outbound.basic_info` 由 reporter 使用：在 Komari 模式下，reporter 会按 `send_on_start` 和 `refresh_interval` 生成 `OutboundEvent::BasicInfo`，export 层只负责编码和发送，不再主动定时拉取 latest report。

远程能力运行参数：

这些参数写入动态 `AgentConfig`。CLI 只提供启动初始值；remote shell / remote task 的执行能力仍由 CLI-only 开关控制，server 只能调整运行限制。remote probe 不执行命令，默认关闭，可由 server patch 动态开启或关闭。

| 配置字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `remote_shell.max_sessions` | `1` | `--remote-shell-max-sessions` / `-M` | `remote_shell.max_sessions` | 最大远程 shell 并发会话数，必须大于 0 |
| `remote_shell.idle_timeout` | `10m` | `--remote-shell-idle-timeout` | `remote_shell.idle_timeout` | 远程 shell 无输入输出后的空闲超时 |
| `remote_shell.session_timeout` | `1h` | `--remote-shell-session-timeout` | `remote_shell.session_timeout` | 单个远程 shell 会话最长运行时间 |
| `remote_shell.program` | 空 | `--remote-shell-program` / `-P` | `remote_shell.program` | 自定义 shell 程序；未传或 patch 为 `null` 时按平台选择默认 shell，Windows 默认 `powershell.exe`，其他平台默认 `/bin/sh` |
| `remote_task.max_concurrent` | `1` | `--remote-task-max-concurrent` / `-C` | `remote_task.max_concurrent` | 最大远程任务并发数，必须大于 0 |
| `remote_task.timeout` | `30s` | `--remote-task-timeout` | `remote_task.timeout` | 单个远程任务默认最大运行时间；server 单次请求的 `timeout` 不能超过此值 |
| `remote_task.max_stdout_bytes` | `65536` | `--remote-task-max-stdout-bytes` | `remote_task.max_stdout_bytes` | stdout 最大回传字节数，超过后截断但继续 drain 子进程输出 |
| `remote_task.max_stderr_bytes` | `65536` | `--remote-task-max-stderr-bytes` | `remote_task.max_stderr_bytes` | stderr 最大回传字节数，超过后截断但继续 drain 子进程输出 |
| `remote_probe.enabled` | `false` | `--remote-probe-enabled true|false` | `remote_probe.enabled` | 是否允许 server 触发远程网络探测；可动态开启 |
| `remote_probe.timeout` | `3s` | `--remote-probe-timeout` | `remote_probe.timeout` | 单次探测超时，范围 `100ms..=10s` |
| `remote_probe.global_min_interval` | `500ms` | `--remote-probe-global-min-interval` | `remote_probe.global_min_interval` | 任意两个探测启动之间的最小间隔，范围 `200ms..=1h` |
| `remote_probe.target_min_interval` | `10s` | `--remote-probe-target-min-interval` | `remote_probe.target_min_interval` | 同一 `probe_type + target` 重复探测的最小间隔，范围 `1s..=24h` |

服务静态参数：

这些参数只在启动时解析到 `ServiceOptions`，不会写入 `AgentConfig`，也不会接受 server patch 动态修改。

| 服务字段 | 默认值 | CLI 参数 | server patch | 说明 |
| --- | --- | --- | --- | --- |
| `diagnostics.process_permission` | `count` | `--allow-process-level none|count|light|details` | 不支持 | server 通过 patch 或一次性请求触发进程采集时允许的最高级别 |
| `diagnostics.socket_permission` | `count` | `--allow-socket-level none|count|light|details` | 不支持 | server 通过 patch 或一次性请求触发 socket 采集时允许的最高级别 |
| `remote_shell.enabled` | `false` | `--remote-shell-enabled true|false` / `-S true|false` | 不支持 | 是否启用远程交互式 shell 能力 |
| `remote_task.enabled` | `false` | `--remote-task-enabled true|false` / `-T true|false` | 不支持 | 是否启用远程非交互任务能力 |

完整运行配置字段如下。当前 agent 不读取配置文件，这个 JSONC 只用于说明 `AgentConfig` 的完整形状；启动时由默认值和 CLI 参数生成，连接 server 后可以由 `config_patch` 局部覆盖动态字段。server patch 中所有字段都可省略，省略表示保持当前值；日志字段只在启动阶段生效，不属于 server patch。

```jsonc
{
  "agent_id": "agent-1", // agent 实例 ID；未传时使用 SMALUX_AGENT_ID 或自动 UUID
  "log_file": "logs/smalux-agent.log", // 日志文件路径；仅启动初始化 tracing 时生效
  "log_retention_files": 14, // 保留最近 N 个滚动日志文件，必须大于 0
  "log_max_size_mb": 64, // 单个日志文件最大大小，单位 MB，必须大于 0
  "log_payload": false, // 是否允许 trace 日志打印截断、脱敏后的实际导出 payload 预览；仅启动时生效
  "log_payload_max_bytes": 4096, // 实际 payload 日志预览最大原始字节数，必须大于 0
  "core": {
    "enabled": true, // 是否采集 CPU / memory / load
    "interval": "1s" // 核心指标采样间隔
  },
  "disk": {
    "enabled": true, // 是否采集磁盘
    "interval": "5s", // 磁盘采样间隔
    "include_per_device": true // 是否上报单磁盘明细
  },
  "network": {
    "enabled": true, // 是否采集网络
    "interval": "5s", // 网络采样间隔
    "include_per_interface": true, // 是否上报单网卡明细
    "include_interfaces": [], // 只统计指定网卡；空数组表示全部网卡
    "exclude_interfaces": [] // 排除指定网卡；include_interfaces 非空时忽略
  },
  "processes": {
    "enabled": true, // 是否采集进程信息
    "interval": "60s", // 进程采样间隔
    "level": "count", // count | light | details
    "limit": 50 // light/details 返回条数上限，范围 1..=500
  },
  "sockets": {
    "enabled": true, // 是否采集 TCP/UDP socket 信息
    "interval": "60s", // socket 采样间隔
    "level": "count", // count | light | details
    "limit": 200 // details 返回条数上限，范围 1..=2000
  },
  "public_ip": {
    "enabled": true, // 是否采集公网 IP
    "required_for_first_report": false, // 第一包是否必须等到 public_ip.status=ready
    "prefer_interface_candidate": true, // 是否优先使用网卡公网候选地址
    "verify_interface_candidate": true, // 使用网卡候选地址后是否继续用外部服务校验
    "lookup_timeout": "3s", // 单轮外部公网 IP 探测超时
    "retry_interval": "30s", // 第一包门禁开启时，公网 IP 失败后的重试间隔
    "refresh_interval": "24h", // 运行中公网 IP 低频刷新间隔
    "max_concurrency": 2 // 外部公网 IP 服务最大并发数，必须大于 0
  },
  "report": {
    "enabled": true, // 是否启用定时上报
    "interval": "5s", // 上报检查间隔
    "heartbeat_enabled": false, // 无变化时是否发送业务级 heartbeat
    "heartbeat_interval": "30s", // 业务级 heartbeat 最小间隔
    "delta_enabled": false, // 是否启用 delta 增量上报
    "snapshot_interval": "5m", // 启用 delta 后，强制定期发送完整 snapshot 的间隔
    "force_snapshot_min_interval": "10s" // server 强制 snapshot 的最小响应间隔
  },
  "outbound": {
    "realtime_report": {
      "enabled": true, // 是否启用实时上报 delivery
      "send_on_start": true // 第一份 report ready 后是否立即发送一次
    },
    "basic_info": {
      "enabled": true, // 是否启用 basic info 事件；当前用于 Komari uploadBasicInfo
      "refresh_interval": "5m", // basic info 低频事件生成间隔
      "send_on_start": true // 第一份 telemetry ready 后是否立即生成一次
    }
  },
  "remote_shell": {
    "max_sessions": 1, // 最大并发 shell 会话数；启用开关不在 AgentConfig 中
    "idle_timeout": "10m", // 无输入输出后的空闲超时
    "session_timeout": "1h", // 单会话最长运行时间
    "program": null // 自定义 shell 程序；null 表示按平台选择默认 shell
  },
  "remote_task": {
    "max_concurrent": 1, // 最大并发任务数；启用开关不在 AgentConfig 中
    "timeout": "30s", // 单个任务默认最大运行时间
    "max_stdout_bytes": 65536, // stdout 最大回传字节数
    "max_stderr_bytes": 65536 // stderr 最大回传字节数
  },
  "remote_probe": {
    "enabled": false, // 是否允许 server 触发远程网络探测；默认关闭，可动态开启
    "timeout": "3s", // 单次探测超时，范围 100ms..=10s
    "global_min_interval": "500ms", // 任意两个探测启动之间的最小间隔
    "target_min_interval": "10s" // 同一 probe_type + target 重复探测的最小间隔
  },
  "export": {
    "base_url": "http://127.0.0.1:9000", // server 根地址；只能是 http/https 根地址，不能带 path/query/fragment
    "format": "smalux_json", // 导出数据编码格式；支持 smalux_json / komari
    "wire_mode": "binary_plain", // Smalux wire 模式；binary_plain | secure_psk
    "secure_required": false, // true 时要求 smalux_json + secure_psk；当前配置为 true 后 server patch 不能关闭
    "token": "REPLACE_WITH_TRANSPORT_TOKEN", // query/bearer 认证 token；secure_psk 时格式为 smx1.<key_id>.<secret_base64url>
    "auth_mode": "bearer", // none | query | bearer；secure_psk 时必须为 none，避免 token 明文泄露
    "query_token_param": "token", // query token 参数名，仅 auth_mode=query 时使用
    "query": {
      "agent_id": "agent-1",
      "region": "local"
    }, // 额外 query 参数；server patch 是整体替换语义
    "unsafe_cert": false, // 是否跳过 WebSocket/HTTP TLS 证书校验
    "heartbeat": "30s", // WebSocket ping 间隔；0 表示禁用，HTTP 模式不使用
    "reconnect_interval": "5s" // 连接失败或断线后的重连间隔
  }
}
```

CLI-only 服务静态选项不属于 `AgentConfig`，只在进程启动时由 CLI 生成：

```jsonc
{
  "diagnostics": {
    "process_permission": "count", // server 远程触发进程采集的最高权限；none | count | light | details
    "socket_permission": "count" // server 远程触发 socket 采集的最高权限；none | count | light | details
  },
  "remote_shell": {
    "enabled": false // 是否启用远程交互式 shell；server patch 不能开启
  },
  "remote_task": {
    "enabled": false // 是否启用远程非交互任务；server patch 不能开启
  }
}
```

完整 server patch 字段如下。这里列出的字段才是 server 运行期可以下发的完整 `AgentConfigPatch` 形状；所有字段都可以省略，省略表示不修改当前值；出现未知字段会被拒绝。`agent_id`、日志字段、`public_ip.required_for_first_report`、`public_ip.retry_interval`、`export.unsafe_cert` 和 CLI-only 服务静态选项不在这里。

```jsonc
{
  "type": "config_patch", // 运行时动态配置 patch
  "patch": {
    "core": {
      "enabled": true, // 可选；是否采集 CPU / memory / load
      "interval": "1s" // 可选；核心指标采样间隔
    },
    "disk": {
      "enabled": true, // 可选；是否采集磁盘
      "interval": "5s", // 可选；磁盘采样间隔
      "include_per_device": true // 可选；是否上报单磁盘明细
    },
    "network": {
      "enabled": true, // 可选；是否采集网络
      "interval": "5s", // 可选；网络采样间隔
      "include_per_interface": true, // 可选；是否上报单网卡明细
      "include_interfaces": ["Ethernet", "Wi-Fi"], // 可选；整体替换 include 网卡列表；空数组表示清空
      "exclude_interfaces": ["Loopback Pseudo-Interface 1"] // 可选；整体替换 exclude 网卡列表；include 非空时忽略
    },
    "processes": {
      "enabled": true, // 可选；是否采集进程信息
      "interval": "60s", // 可选；进程采样间隔
      "level": "count", // 可选；count | light | details；不能超过启动时 --allow-process-level
      "limit": 50 // 可选；light/details 返回条数上限，范围 1..=500
    },
    "sockets": {
      "enabled": true, // 可选；是否采集 TCP/UDP socket 信息
      "interval": "60s", // 可选；socket 采样间隔
      "level": "count", // 可选；count | light | details；不能超过启动时 --allow-socket-level
      "limit": 200 // 可选；details 返回条数上限，范围 1..=2000
    },
    "public_ip": {
      "enabled": true, // 可选；是否采集公网 IP
      "prefer_interface_candidate": true, // 可选；是否优先使用网卡公网候选地址
      "verify_interface_candidate": true, // 可选；使用网卡候选地址后是否继续用外部服务校验
      "lookup_timeout": "3s", // 可选；单轮外部公网 IP 探测超时
      "refresh_interval": "24h", // 可选；运行中公网 IP 低频刷新间隔
      "max_concurrency": 2 // 可选；外部公网 IP 服务最大并发数，必须大于 0
    },
    "report": {
      "enabled": true, // 可选；是否启用 reporter 上报
      "interval": "5s", // 可选；定时 snapshot/heartbeat 检查间隔
      "heartbeat_enabled": false, // 可选；是否启用业务级 heartbeat；Komari 格式必须为 false
      "heartbeat_interval": "30s", // 可选；业务级 heartbeat 最小发送间隔
      "delta_enabled": false, // 可选；是否启用 delta 增量上报；Komari 格式必须为 false
      "snapshot_interval": "5m", // 可选；启用 delta 后强制定期发送完整 snapshot 的间隔
      "force_snapshot_min_interval": "10s" // 可选；响应 server snapshot_request 的最小间隔
    },
    "outbound": {
      "realtime_report": {
        "enabled": true, // 可选；是否启用实时上报 delivery
        "send_on_start": true // 可选；第一份 report ready 后是否立即发送一次
      },
      "basic_info": {
        "enabled": true, // 可选；是否启用 basic info 事件；当前用于 Komari uploadBasicInfo
        "refresh_interval": "5m", // 可选；basic info 低频事件生成间隔
        "send_on_start": true // 可选；第一份 telemetry ready 后是否立即生成一次
      }
    },
    "remote_shell": {
      "max_sessions": 1, // 可选；最大并发 shell 会话数；不能开启 remote_shell.enabled
      "idle_timeout": "10m", // 可选；无输入输出后的空闲超时
      "session_timeout": "1h", // 可选；单会话最长运行时间
      "program": null // 可选；字符串表示覆盖 shell 程序，null 表示清空为平台默认 shell
    },
    "remote_task": {
      "max_concurrent": 1, // 可选；最大并发任务数；不能开启 remote_task.enabled
      "timeout": "30s", // 可选；单个任务默认最大运行时间
      "max_stdout_bytes": 65536, // 可选；stdout 最大回传字节数
      "max_stderr_bytes": 65536 // 可选；stderr 最大回传字节数
    },
    "remote_probe": {
      "enabled": false, // 可选；是否允许 server 触发远程网络探测
      "timeout": "3s", // 可选；单次探测超时，范围 100ms..=10s
      "global_min_interval": "500ms", // 可选；任意两个探测启动之间的最小间隔，范围 200ms..=1h
      "target_min_interval": "10s" // 可选；同一 probe_type + target 重复探测的最小间隔，范围 1s..=24h
    },
    "export": {
      "base_url": "https://example.com", // 可选；server 根地址；修改后 export supervisor 会重连并重新派生 endpoint
      "format": "smalux_json", // 可选；smalux_json | komari
      "wire_mode": "binary_plain", // 可选；binary_plain | secure_psk；Komari 不使用
      "secure_required": false, // 可选；true 后 server patch 不能再关闭
      "token": "REPLACE_WITH_SERVER_ISSUED_TOKEN", // 可选；认证 token；secure_psk 时格式为 smx1.<key_id>.<secret_base64url>
      "auth_mode": "bearer", // 可选；none | query | bearer
      "query_token_param": "token", // 可选；query token 参数名，仅 auth_mode=query 时使用
      "query": {
        "agent_id": "agent-1", // 可选；额外 query 参数；整个 query map 会被替换
        "region": "local" // 可选；额外 query 参数
      },
      "heartbeat": "30s", // 可选；WebSocket ping 间隔；0 表示禁用，HTTP 模式不使用
      "reconnect_interval": "5s" // 可选；连接失败或断线后的重连间隔
    }
  }
}
```

常用启动参数：

```powershell
cargo run -p smalux-agent -- `
  -i agent-1 `
  -l logs/agent-1.log `
  -L 14 `
  --log-max-size-mb 64 `
  -s http://127.0.0.1:9000 `
  -f smalux_json `
  --wire-mode binary_plain `
  -a bearer `
  -t REPLACE_WITH_TRANSPORT_TOKEN `
  -c 2s `
  -d 10s `
  -n 10s `
  -I Ethernet `
  -I Wi-Fi `
  --network-exclude-interface "Loopback Pseudo-Interface 1" `
  -R 5s `
  --report-force-snapshot-min-interval 10s `
  -p 24h `
  -r 5s
```

调试自有 server 时建议先用最小命令启动：

```powershell
cargo run -p smalux-agent -- `
  -i agent-dev-1 `
  -s http://127.0.0.1:9000 `
  -f smalux_json `
  --wire-mode binary_plain `
  -a none `
  --report-delta-enabled false `
  -R 5s
```

这个组合只要求 server 支持 WebSocket upgrade、Smalux binary wire 的 `PlainData` 和 `ClientFrame(type=snapshot)`。等 snapshot 跑通后，再逐步打开 `delta`、`secure_psk`、remote probe、remote task 和 remote shell。

如果要调试加密链路，先生成并保存同一份 secret：

```powershell
cargo run -p smalux-agent -- `
  -i agent-secure-1 `
  -s https://example.com `
  -f smalux_json `
  --wire-mode secure_psk `
  --secure-required true `
  -a none `
  -t smx1.agent-secure-1.REPLACE_WITH_BASE64URL_SECRET
```

server 侧只保存 `key_id=agent-secure-1` 对应的 secret；agent 不会把 secret 明文发出。加密链路调试时可以把日志级别设为 `RUST_LOG=smalux_agent=debug,smalux_protocol=debug`，日志只应出现 `key_id`、wire kind、sequence 和握手状态，不应出现完整 token 或 secret。

完整 CLI 参数示例：

```powershell
cargo run -p smalux-agent -- `
  -i agent-1 `
  -l logs/agent-1.log `
  -L 14 `
  --log-max-size-mb 64 `
  -s http://127.0.0.1:9000 `
  -f smalux_json `
  --wire-mode binary_plain `
  -t REPLACE_WITH_TRANSPORT_TOKEN `
  -a bearer `
  -k token `
  -q agent_id=agent-1 `
  -q region=local `
  -u false `
  -H 30s `
  -r 5s `
  --core-enabled true `
  -c 1s `
  --disk-enabled true `
  -d 5s `
  --disk-per-device true `
  --network-enabled true `
  -n 5s `
  --network-per-interface true `
  -I Ethernet `
  -I Wi-Fi `
  --network-exclude-interface "Loopback Pseudo-Interface 1" `
  --public-ip-enabled true `
  --public-ip-required false `
  --public-ip-prefer-interface true `
  --public-ip-verify-interface true `
  --public-ip-lookup-timeout 3s `
  --public-ip-retry-interval 30s `
  -p 24h `
  --public-ip-max-concurrency 2 `
  --report-enabled true `
  -R 5s `
  --report-heartbeat-enabled false `
  --report-heartbeat-interval 30s `
  --report-delta-enabled false `
  --report-snapshot-interval 5m `
  --report-force-snapshot-min-interval 10s `
  --realtime-report-enabled true `
  --realtime-report-send-on-start true `
  --basic-info-enabled true `
  --basic-info-refresh-interval 5m `
  --basic-info-send-on-start true `
  -S false `
  -M 1 `
  --remote-shell-idle-timeout 10m `
  --remote-shell-session-timeout 1h `
  -P powershell.exe `
  -T false `
  -C 1 `
  --remote-task-timeout 30s `
  --remote-task-max-stdout-bytes 65536 `
  --remote-task-max-stderr-bytes 65536 `
  --remote-probe-enabled false `
  --remote-probe-timeout 3s `
  --remote-probe-global-min-interval 500ms `
  --remote-probe-target-min-interval 10s
```

WebSocket 兼容第三方服务时可以追加多个 query：

```powershell
cargo run -p smalux-agent -- `
  -s "http://127.0.0.1:9000" `
  -q agent_id=agent-1 `
  -q region=local
```

如需 query token：

```powershell
cargo run -p smalux-agent -- -a query -t REPLACE_WITH_TRANSPORT_TOKEN -k access_token
```

Komari WebSocket 兼容示例：

```powershell
cargo run -p smalux-agent -- `
  -s "https://example.com" `
  -f komari `
  -a query `
  -t komari-token `
  -R 5s
```

Komari 兼容约束：

- `export.base_url` 必须是 `http://host[:port]` 或 `https://host[:port]` 根地址，不能带 path、query 或 fragment；Komari adapter 会自动派生所有固定 endpoint。
- token 必须通过 query 提供，推荐使用 `auth_mode=query` + `export.token`；也可以用 `export.query` 显式放入 token 参数。不要把 token 放到 URL 里。
- `https://host` 会自动派生为 `wss://host/api/clients/report`、`https://host/api/clients/uploadBasicInfo`、`https://host/api/clients/task/result` 和 `wss://host/api/clients/terminal`。
- 不支持 `auth_mode=bearer`，因为 Komari report / basic info 使用 query token。
- 不支持业务级 `heartbeat` 和 `delta`；Komari adapter 发送完整 snapshot 映射后的 report，把 `remote_task_result` 映射为 task/result HTTP 请求，把 `job_result(kind=probe)` 映射为 WebSocket `ping_result`。
- Komari `basic info` 是 reporter 产生的独立低频事件，第一份 telemetry ready 后按 `outbound.basic_info.send_on_start` 决定是否立即发送，之后按 `outbound.basic_info.refresh_interval` 发送，默认 5 分钟。
- Komari 实时 `report` 是独立 `realtime_report` delivery，跟随 reporter 产生的最新完整 snapshot 通过 WebSocket 发送；HTTP `basic_info` 由 `OutboundEvent::BasicInfo` 触发，经 Komari adapter 编码为 `POST /api/clients/uploadBasicInfo`。
- Komari report 要求 `report.interval <= 10s`，用于保证定时 snapshot 兜底，避免第三方服务长时间没有业务数据帧导致断开。
- WebSocket 模式收到 `{ "message": "terminal", "request_id": "..." }` 时，会打开 `/api/clients/terminal?id=...` 临时 stream 并复用 remote shell；需要启动时开启 `--remote-shell-enabled true`。
- WebSocket 模式收到 `{ "message": "exec", "task_id": "...", "command": "..." }` 时，会转换成内部 `remote_task`；需要启动时开启 `--remote-task-enabled true`。执行结果会通过 `POST /api/clients/task/result?token=...` 回传。
- WebSocket 模式收到 `{ "message": "ping", "ping_task_id": 123, "ping_type": "tcp", "ping_target": "host:443" }` 时，会转换成内部 `job_apply(operation=once, kind=probe)`。默认不发包并回内部 `status=rejected`，Komari 输出时仍会映射成 `value=-1`；server 可通过 `config_patch.remote_probe.enabled=true` 动态开启，频率受 `remote_probe.global_min_interval` 和 `remote_probe.target_min_interval` 限制。
- 重复敏感 query 参数会被拒绝，例如 `export.query` 已经包含 `token=...` 时不要再同时使用 `-a query -t ...`。

`log_file`、`log_retention_files`、`log_max_size_mb`、`log_payload` 和 `log_payload_max_bytes` 只在启动阶段生效，不接受 server patch。当前 `tracing` subscriber 初始化后不会热切换日志文件、保留数量或大小阈值；payload 日志也不会由 server 动态开启，避免远端把敏感内容写入本机日志。如果后续确实需要热切换日志，需要单独设计 reloadable writer。

日志文件使用 `tracing-rolling-file` 滚动，并通过 `tracing_appender::non_blocking` 后台线程写入，避免业务路径直接阻塞在磁盘 IO。滚动条件是“按天”或“当前文件达到 `log_max_size_mb` MB”，任一条件满足都会切换文件；当前文件使用 `log_file` 路径，历史文件使用 `log_file.1`、`log_file.2` 这种序号后缀。默认保留最近 `14` 个滚动文件，单文件默认 `64MB`。`log_retention_files` 和 `log_max_size_mb` 都必须大于 0。

默认日志不会打印实际导出请求体，只记录 `body_bytes`、delivery、transport、sequence 等元信息。需要排查协议内容时同时满足两个条件才会输出实际 payload 预览：

1. `RUST_LOG` 打开到对应模块的 `trace`，例如 `RUST_LOG=smalux_agent=trace`。
2. 启动参数显式传入 `--log-payload true`，并可用 `--log-payload-max-bytes 4096` 控制预览上限。

payload 预览会标出 `kind`、`encoding`、原始 `bytes`、`redacted`、`truncated` 和 `preview`。JSON 会递归按字段名脱敏，文本会按常见 `key=value` / `key: value` 关键词脱敏，UTF-8 二进制如果能解析为 JSON 也会先按 JSON 脱敏；无法识别的二进制仍只按 base64 预览。当前敏感字段包括 `token`、`access_token`、`authorization`、`password`、`secret`、`api_key`、`private_key`、`psk`、`command`、`stdout`、`stderr`，并会对 shell stream 的 `data` 和 Komari task result 的 `result` 做上下文脱敏。开启后仍可能包含非敏感 telemetry JSON 或第三方兼容 payload，生产环境建议只在短时间排障时开启。

安全检查重点：常规日志不应输出完整 token、Authorization header、secure_psk secret、远程命令、stdout/stderr 或 shell stream data；URL 日志必须先脱敏敏感 query；`unsafe_cert` 只允许启动时设置；远程 shell 和 remote task 只能通过 CLI 开启，不能由 server 动态开启。`--log-payload true` 只适合短时排障，即使脱敏后也可能包含非敏感但仍有业务价值的 telemetry 内容。

脱敏实现位于 `smalux-core::utils::redact`，并通过 `smalux_core::log` re-export 常用入口。server 需要记录请求或协议 payload 时可以直接复用 `smalux_core::log::redact_sensitive_json()`、`redact_sensitive_json_text()`、`redact_sensitive_json_bytes()` 和 `redact_sensitive_text()`，避免 agent/server 使用不同规则。

未通过 `--agent-id` 或 `SMALUX_AGENT_ID` 显式设置时，agent 会在本次进程启动时生成一个 UUID v4。当前没有本地持久化 ID，因此重启后会得到新的默认 ID；生产环境如果需要同一台机器长期稳定识别，应显式传入 `--agent-id` 或设置 `SMALUX_AGENT_ID`。

## 导出协议

service 层只依赖 `ExportRouter` 和 `TransportHub`，不直接依赖 `WebSocketClient`、`HttpClient` 或具体 adapter。具体数据格式由 `export::build_protocol_adapter()` 根据 `export.format` 选择 adapter，再交给 `ExportRouter` 统一编码和投递；adapter 根据 `export.base_url` 派生固定 endpoint 并生成 `TransportPlan`，由 `TransportHub` 启动并管理 transport。

当前已实现的组合：

- `smalux_json`：用户配置 `http` / `https` 根地址，adapter 派生主 WebSocket endpoint `/agent/v1/connect`；上报通过 binary wire 发送，server 控制消息由 `SmaluxControlHandler` 解析。
- `komari`：用户配置 `http` / `https` 根地址，adapter 派生 Komari 固定路径；实时 report 走 WebSocket `/api/clients/report`，basic info 走 HTTP `/api/clients/uploadBasicInfo`，exec 结果走 HTTP `/api/clients/task/result`，terminal 走 WebSocket `/api/clients/terminal`，ping result 走实时 WebSocket。

后续新增 gRPC 等协议时应增加 transport spec 和 driver；新增第三方兼容格式时应增加 adapter，而不是改 service 生命周期逻辑。

当前导出连接流程：

```text
ExportConfig
  -> build_protocol_adapter(export.format)
  -> ExportRouter
  -> router.transport_plan(export)
  -> TransportHub::from_plan()
  -> set_realtime_report_handler(Box<dyn InboundProtocolHandler>)
  -> connect_all()
  -> router.encode_report(OutboundReport)
  -> Vec<TransportRequest>
  -> TransportHub::enqueue()
  -> export::worker transport task
  -> TransportEvent::Sent | Failed
  -> close_all()
```

adapter 输出语义：

- 非空 `Vec<TransportRequest>`：当前事件被编码成功，交给对应 transport 发送。
- 空 `Vec`：当前格式明确跳过该事件，例如后续第三方格式不支持业务心跳时可以不发送。
- `Err`：编码失败，按导出错误处理并触发重连流程。

当前 transport ID：

- `RealtimeReport`：实时上报通道。`smalux_json` 使用 WebSocket binary wire；`komari` 使用 WebSocket text report。
- `AuxiliaryHttp`：辅助 HTTP 通道。当前 Komari 使用它发送 basic info 和 exec task result 的 JSON POST。

当前 WebSocket transport 能力：

- transport 接收 adapter 派生后的完整 endpoint；用户侧只配置 `export.base_url` 根地址。
- 额外 query 用 `--query KEY=VALUE` 或 `export.query` 配置追加，支持多个参数。
- 认证模式支持 `none`、`query`、`bearer`。
- `query` 认证会把 token 追加到 URL query，参数名由 `query_token_param` 控制。
- `bearer` 认证会写入 `Authorization: Bearer <token>`。
- URL 日志和 Debug 输出会脱敏敏感 query。
- 如果 `export.query` 已带 `token`，又配置 `auth=query` 追加同名 token，会拒绝连接，避免服务端解析凭证出现歧义。
- `smalux_json` binary payload 会由 WebSocket transport 按 `export.wire_mode` 封为 Smalux wire packet。
- `binary_plain` 使用 `PlainData` packet 承载未加密 JSON bytes，适合开发和内网联调。
- `secure_psk` 使用 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` 握手；`export.token` 只用于本地派生 PSK，完整 token 不会进入 URL 或 header。
- `secure_psk` 下 `export.auth_mode` 必须为 `none`；如果需要额外路由参数，用 `export.query` 放非敏感字段。

`smalux_json` 使用的 inbound handler 是 `SmaluxControlHandler`，只识别 `smalux_protocol::ServerFrame`。当前稳定下行命令包括 `snapshot_request`、`config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_task_run`、`job_apply` 和 `remote_shell_open`，handler 会保留 server `sequence` 并在调度后回传 `ack` 或 `error`；如果 `target_agent_id` 存在且不等于当前 agent ID，会记录日志并直接丢弃，不回 ack/error。解析失败或未知 `type` 同样只丢弃，不断开主连接。`komari` 当前使用 `KomariInboundHandler`，把 terminal 消息转换成远程 shell 入站命令，把 exec 消息转换成远程 task 入站命令，把 ping 消息转换成 `job_apply(operation=once, kind=probe)`，其它第三方 server 文本消息会安全忽略，避免把 Komari 的事件误解析成 smalux 控制消息。如果要兼容更多服务端控制消息，可以新增：

- 新的 `InboundProtocolHandler` 实现，解析第三方服务端控制消息。
- 新的 message adapter，把第三方消息转换为 `InboundCommand`。
- 新的 export adapter；格式选择放在 `export.rs`，具体实现放在 `export/` 子模块。
- 新的 export 配置字段，例如自定义 header、子协议、压缩、握手扩展等。

当前已经支持的连接和请求数据：

```text
base URL            来自 export.base_url / --server，只能是 http/https 根地址
adapter path        来自 export.format 内部固定路径，用户不配置 path
extra query         来自 export.query / --query
query token         来自 export.auth_mode=query + export.token
authorization       来自 export.auth_mode=bearer + export.token
wire mode           来自 export.wire_mode，仅 smalux_json WebSocket binary 使用
unsafe tls          来自 export.unsafe_cert，WebSocket 和 HTTP 都会使用
heartbeat interval  来自 export.heartbeat，仅 WebSocket 使用；0 禁用，非 0 值按 Duration 精度调度
```

### Smalux Wire 和 secure_psk

`smalux_json` 的 WebSocket 上报不再直接发送 text frame。adapter 先把 `OutboundReport` 编码为标准 `smalux_protocol::ClientFrame` JSON bytes，WebSocket transport 再根据 `export.wire_mode` 调用 `smalux_protocol::wire` 和 `smalux_protocol::secure` 生成 binary frame：

```text
OutboundReport
  -> smalux_protocol::ClientFrame JSON bytes
  -> WebSocket transport
     -> binary_plain: WirePacket(kind=PlainData, payload=json_bytes)
     -> secure_psk:  Noise encrypt(json_bytes) -> WirePacket(kind=SecureData, payload=ciphertext)
  -> WebSocket Binary frame
```

wire packet 固定头如下，所有整数都是 big-endian：

```text
magic       4 bytes   固定为 "SMX1"
version     1 byte    当前为 1
kind        1 byte    1 PlainData, 2 Hello, 3 Handshake, 4 SecureData, 5 Close
flags       2 bytes   当前保留，写 0
session_id 16 bytes   当前连接或 stream 的随机 session id
sequence    8 bytes   业务消息序号；Hello=0，首个 Handshake=1
payload_len 4 bytes   payload 长度
payload     N bytes   明文 JSON、Noise 握手消息或密文
```

`payload_len` 最大为 `1 MiB`，超过会被拒绝；`flags` 当前始终写 `0`，首版 server 可以把非 0 flags 视为不支持。`session_id` 在同一条 WebSocket 连接或 remote shell stream 内必须保持一致，server 回握手包时也必须沿用 agent Hello 的 `session_id`。

`secure_psk` token 格式：

```text
smx1.<key_id>.<secret_base64url>
```

- `key_id` 明文放在 Hello payload，server 用它查询本地保存的 secret。
- `secret_base64url` 必须解码出至少 32 字节；agent 使用 `HKDF-SHA256` 派生 32 字节 PSK。
- 完整 token 和 secret 不会发送给 server，也不会进入 URL、header 或日志。
- `secure_psk` 模式下 `export.auth_mode` 必须为 `none`；否则配置校验会拒绝。

PSK 派生精确参数：

```text
input secret      = base64url_decode(secret_base64url)  # 兼容带 padding 和不带 padding
secret min length = 32 bytes
HKDF hash         = SHA-256
HKDF salt         = "smalux secure psk v1 salt"
HKDF info         = "smalux secure psk v1 " + key_id
output length     = 32 bytes
Noise psk slot    = psk(0, derived_psk)
Noise pattern     = Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s
Noise payload      = empty bytes during both handshake messages
```

HKDF 测试向量，server 实现时建议做成单元测试：

```text
key_id                = "agent-key"
secret bytes          = 32 bytes of 0x07
secret_base64url      = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
token                 = "smx1.agent-key.BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
derived_psk_hex       = "a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2"
derived_psk_base64url = "plsq_xK2fp0l-ucJSyQkgTOgQ6Hy8roWFXJ5gGstYqI"
```

如果 server 用同样 `key_id` 和 secret 派生出的 PSK 不等于上面值，说明 HKDF salt、info 拼接、base64url 解码或 UTF-8 字节处理有误，不能继续写 Noise 握手。

`key_id` 会参与 HKDF info，因此不同 agent 即使误用同一个 secret，也会派生出不同 PSK。server 实现时必须使用 UTF-8 字节拼接 `info_prefix + key_id`，不要对 `key_id` 再做 JSON 转义、base64 或大小写转换。

Hello payload 是 UTF-8 JSON bytes：

```json
{
  "key_id": "agent-secure-1",
  "pattern": "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s"
}
```

agent 侧 secure 握手流程：

```text
WebSocket HTTP Upgrade
  -> agent sends WirePacket(Hello, payload={"key_id":"...","pattern":"Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s"})
  -> agent sends WirePacket(Handshake, payload=noise initiator msg1)
  -> server uses key_id lookup secret and creates Noise responder
  -> server sends WirePacket(Handshake, payload=noise responder msg2)
  -> agent verifies response with PSK and enters transport mode
  -> later business frames use WirePacket(SecureData, payload=noise ciphertext)
```

server 后续实现时不需要重复写 wire/secure 代码，直接依赖 `smalux-protocol`：先用 `wire::decode_wire_packet()` 读取 Hello，再用 `secure::decode_secure_hello()` 取 `key_id` 和 `pattern`，用 `secure::parse_secure_token()` 或同等 secret 查询结果得到 `SecurePskKey`，再用 `secure::build_noise_responder()` 构造 Noise responder。握手成功后，server 下发控制消息时也走同一条业务 payload 通道：把 `ServerFrame` JSON 编码成 UTF-8 bytes，`secure_psk` 模式用 `secure::encrypt_payload()` 加密后放入 `WirePacket::secure_data()`，`binary_plain` 模式放入 `WirePacket::plain_data()`。`ServerFrame.sequence` 是 agent 回控制层 `ack/error` 的关联 ID。

server 侧 secure_psk 最小实现步骤：

```text
1. 收到 WirePacket(kind=Hello, sequence=0)
   -> decode JSON
   -> 校验 pattern
   -> 保存 session_id，后续 Handshake/SecureData 必须匹配
   -> 用 key_id 查询本地 secret
   -> 按固定 HKDF 参数派生 32 字节 PSK
2. 收到 WirePacket(kind=Handshake, sequence=1)
   -> 校验 session_id 与 Hello 一致
   -> 创建 Noise responder，psk slot=0
   -> read initiator msg1，握手 payload 为空字节
   -> write responder msg2，握手 payload 为空字节
   -> 回发 WirePacket(kind=Handshake, same session_id, payload=msg2)
3. 握手完成后进入 transport mode
   -> agent 上报：校验 session_id 后，WirePacket(kind=SecureData).payload 先 Noise decrypt，再 decode ClientFrame JSON
   -> server 下发：ServerFrame JSON 先 Noise encrypt，再封 WirePacket(kind=SecureData, same session_id)
4. 任意 magic/version/session/pattern/PSK/AEAD 校验失败都关闭连接，不进入 ready 状态
```

### Server 对接索引

自己写 server 时，完整字段定义和最小闭环要求以 [crates/smalux-server/README.md](/abs/path/F:/code/rust/smalux/crates/smalux-server/README.md) 和 [crates/smalux-server/plan.md](/abs/path/F:/code/rust/smalux/crates/smalux-server/plan.md) 为准。

agent 这边只保留两个最关键的对接事实：

- 主连接入口是 `/agent/v1/connect`，业务流围绕 `snapshot` / `delta` / `heartbeat` / `ack` / `error` / `remote_task_result` / `job_result` 展开。
- 远程能力通过 `ServerFrame` 下发，`job_apply(kind=probe)` 是远程网络探测的统一入口，`remote_shell_open` 和 `remote_task_run` 仍然是独立能力。

### Server 对接注意事项

写自有 server 时，下面这些边界要按当前 agent 行为处理：

- `export.heartbeat` 是 WebSocket ping，不是业务 heartbeat；`0` 表示禁用，非 0 值按 `Duration` 精度调度，不会把 `500ms` 截断成 `0`；业务 heartbeat 由 `report.heartbeat_enabled` 控制，默认关闭。
- 采集 update 会触发 reporter 立即尝试生成 report；`report.interval` 负责定时 snapshot/heartbeat 兜底，不再是唯一 report 生成来源。
- `latest_report` 仍是 export supervisor 的最新状态语义；realtime delivery 收到新 report 会立即尝试投递，server 端仍应保存 latest state，而不是试图按每个 report 建无界队列。
- `ack/error` 只说明带 `sequence` 的 `ServerFrame` 是否被调度，不说明远程 task/probe 已完成。
- `error.code` 当前按 `{server_frame_type}_failed` 生成，例如 `snapshot_request_failed`、`job_apply_failed`、`remote_shell_open_failed`。server 逻辑判断只依赖 `error.code` 和 `error.sequence`，不要依赖自然语言 `message`。
- `config_patch` 调度成功会先回控制层 `ack`，但它只表示 patch 已通过校验并应用到运行配置；server 如果要确认采样节奏或导出连接实际变化，还应继续观察后续 snapshot/delta 或新连接。
- `secure_psk` 模式下，server 不能发送 WebSocket text 控制消息；agent 会直接拒绝连接路径。
- `remote_shell_open.stream_url` 是单个 shell 会话的临时 WebSocket 地址；自有 `smalux_json` server 仍要在这个 stream 上执行同样的 binary wire / `secure_psk` 规则，文档中的 shell JSON 只是 wire payload 里的业务 JSON。
- `remote_task_run` 有副作用，重连后不要盲目重发；用 `task_id` 做幂等。
- `job_apply(operation=once, kind=probe)` 被禁用或限频时，agent 不会发网络包，但仍会回 `job_result(kind=probe).result.status=rejected`，并带 `error` 说明原因。
- 进程和 socket 的 `level=details` 即使通过 server patch 请求，也需要 agent 启动时允许 details；否则会返回控制错误或拒绝一次性采集。
- `public_ip.status=failed/stale/disabled` 都是正常上报状态，server 不应因为公网 IP 不 ready 就拒绝整包。

## 远程能力

远程 shell 和 remote task 默认关闭，只能通过 CLI 启动参数开启，server patch 不能动态开启或关闭这些执行能力。remote probe 默认关闭，但不执行本地命令，可由 server patch 动态开启或关闭，并受本地频率保护。能力开启后的运行限制属于动态配置，server 可以按需调整。

- `remote_shell` 面向交互式终端。主上报 WebSocket 只接收 `remote_shell_open` 控制消息；每个 shell 会话会再打开一个临时 WebSocket stream 承载 PTY input / output / resize / close / exit。
- shell runner 使用 `portable-pty` 启动本地 PTY。PTY 输出是单一原始字节流，不区分 stdout / stderr；核心层会先把输出表示为 base64 `output` 事件，Smalux stream 直接发送该事件，Komari adapter 会解回 raw binary 后再发送给第三方 terminal stream。
- 会话受当前动态配置里的 `remote_shell.max_sessions`、`remote_shell.idle_timeout`、`remote_shell.session_timeout` 和 `remote_shell.program` 限制；会话结束或异常退出会释放并发名额。server patch 修改这些字段后，新打开的会话使用新配置，已经运行中的会话继续使用打开时的配置。
- stream WebSocket 会复用当前 `export` 的认证、额外 query、`unsafe_cert`、heartbeat 和 Smalux wire 设置；如果 stream URL 已自带 token，又配置 query token，重复敏感 query 会按现有 WebSocket 规则拒绝。
- `smalux_json` 自有 shell stream 使用 `SmaluxShellCodec`：server 和 agent 交换的 shell command/event 是 JSON 语义，但实际 WebSocket frame 走 Smalux binary wire；`secure_psk` 下会先完成 Noise PSK 握手，再把 shell JSON payload 放进 `SecureData`。
- `komari` terminal 使用 `KomariTerminalCodec`：主 report WebSocket 收到 Komari `terminal` 消息后，agent 派生 `/api/clients/terminal?id=REQUEST_ID&token=TOKEN` 临时连接；该连接启用 raw binary frame，PTY 输出直接作为 binary 发送，`opened/exit` 事件不会发给 Komari。入站 binary 会作为 PTY 输入；入站 text 会先尝试 Smalux shell JSON command，再尝试 Komari 兼容 `{ "type": "input", "input": "..." }`，最后作为原始输入转发。
- 部分 shell 会发送终端控制查询，例如 Windows PowerShell 可能输出 `ESC[6n` 查询光标位置；完整终端前端需要把终端模拟器产生的响应通过 `input` 回写给 agent。
- `remote_task` 面向非交互的一次性任务，例如后续备份、更新、脚本执行。主控制通道接收 `remote_task_run`，agent 直接执行指定 `program + args`，不会把参数拼成 shell 字符串。任务受 `remote_task.max_concurrent`、`remote_task.timeout`、`remote_task.max_stdout_bytes` 和 `remote_task.max_stderr_bytes` 限制；stdout/stderr 超过上限会截断但继续 drain，避免子进程因 pipe 堵塞卡死。
- `remote_task_result` 走主出站队列；`smalux_json` 编码为 WebSocket binary `ClientFrame(type=remote_task_result)`，`komari` 编码为 HTTP `POST /api/clients/task/result?token=...`。Komari 的 `command` 字符串会按平台转换为 shell 执行：Windows 使用 `powershell.exe -NoProfile -Command`，其它平台使用 `/bin/sh -c`。
- `remote_job` 是远程周期/一次性能力的统一外壳。主控制通道接收 `job_apply`；`operation=once` 用于立即运行一次，`operation=replace/patch` 用于同步持续 job 表。首版只实现 `kind=probe`，后续 backup、health_check 等能力应新增 job kind 和执行器，而不是新增 `remote_xxx_apply` 协议入口。
- `remote_probe` 面向远程网络连通性探测，是 `job_apply(kind=probe)` 当前唯一执行器。Komari `ping` 会转换成 `job_apply(operation=once, kind=probe)`。当前支持 `tcp` 和 `http`；`icmp` 先返回 `status=failed`，后续如果需要真实 ICMP 再单独接跨平台实现。
- `job_result(kind=probe)` 走主出站队列；`smalux_json` 编码为 WebSocket binary `ClientFrame(type=job_result)`，结果会带回 `point_id`、`request_id/job_id`、`probe_type` 和 `target`；`komari` 编码为 WebSocket text `{ "type": "ping_result", ... }`，Komari 格式本身只带 `task_id/ping_type/value`。
- `operation=once` 在 `remote_probe.enabled=false`、全局间隔未到或同目标间隔未到时，agent 不发网络包，立即回传 `status=rejected`。持续任务 `replace/patch` 会在 agent 本地启动或更新 worker，worker 按各自 `interval` 持续探测并持续上报 `job_result(kind=probe)`；持续任务命中限频时只跳过当次执行，不回 rejected 结果。
- server patch 不允许开启 `remote_shell.enabled` / `remote_task.enabled`，避免服务端在 agent 运行中扩大远程执行权限；运行限制可以调整，但只会在能力已由 CLI 开启时产生实际效果。
- Komari 的 terminal 消息会转换成同一套 `remote_shell` 管理逻辑，exec 消息会转换成同一套 `remote_task` 管理逻辑，ping 消息会转换成同一套 `remote_probe` 管理逻辑，而不是在 Komari adapter 里直接执行。

## Server Patch

server 控制通道使用 Smalux `ServerFrame`。所有自有下行命令都带 `protocol_version`、server 侧递增 `sequence`、`sent_at` 和顶层 `type`；命令调度成功后 agent 会回控制层 `ack`，失败或被拒绝会回 `error`。`target_agent_id` 是可选路由保护字段：为空表示当前连接上的 agent，不为空且不匹配当前 `agent_id` 时，agent 会直接丢弃该 frame，不回 ack/error。

本地开发时，`binary_plain` 模式可以用 WebSocket text 直接发送 `ServerFrame` JSON 方便调试；自有 server 正式实现仍推荐始终把 `ServerFrame` JSON bytes 放进 Smalux binary wire payload。`secure_psk` 模式只接受加密后的 binary wire payload，明文 text 会被拒绝。

`config_patch`：

```jsonc
{
  "protocol_version": 1, // Smalux 协议版本
  "sequence": 203, // server 侧递增序号；agent ack/error 会引用它
  "sent_at": 1710000000, // server 发送时间，Unix 秒
  "target_agent_id": "agent-1", // 可选；不写表示当前连接上的 agent
  "type": "config_patch",
  "patch": {
    "core": { "enabled": true, "interval": "2s" },
    "disk": { "interval": "10s", "include_per_device": false },
    "network": {
      "interval": "10s",
      "include_per_interface": false,
      "include_interfaces": ["Ethernet", "Wi-Fi"],
      "exclude_interfaces": ["Loopback Pseudo-Interface 1"]
    },
    "processes": { "interval": "120s", "level": "light", "limit": 25 },
    "sockets": { "enabled": true, "interval": "120s", "level": "details", "limit": 100 },
    "public_ip": { "refresh_interval": "12h", "max_concurrency": 4 },
    "remote_task": {
      "max_concurrent": 1,
      "timeout": "30s",
      "max_stdout_bytes": 65536,
      "max_stderr_bytes": 65536
    },
    "remote_probe": {
      "enabled": true,
      "timeout": "3s",
      "global_min_interval": "500ms",
      "target_min_interval": "10s"
    },
    "report": {
      "interval": "5s",
      "heartbeat_enabled": true,
      "heartbeat_interval": "30s",
      "delta_enabled": true,
      "snapshot_interval": "5m",
      "force_snapshot_min_interval": "10s"
    },
    "outbound": {
      "realtime_report": {
        "enabled": true,
        "send_on_start": true
      },
      "basic_info": {
        "enabled": true,
        "refresh_interval": "5m",
        "send_on_start": true
      }
    },
    "export": {
      "format": "smalux_json",
      "auth_mode": "bearer",
      "token": "REPLACE_WITH_SERVER_ISSUED_TOKEN",
      "reconnect_interval": "10s",
      "query": { "agent_id": "agent-1" }
    }
  }
}
```

patch 只更新传入字段。所有 `config_patch` 模型都会拒绝未知字段：字段不在 patch 模型中时会解析失败，并通过控制错误返回给 server。`export.query`、`network.include_interfaces` 和 `network.exclude_interfaces` 是整体替换语义；如果需要清空网卡筛选，server 可以下发空列表。`outbound.realtime_report.enabled`、`outbound.realtime_report.send_on_start`、`outbound.basic_info.*`、`remote_shell`、`remote_task` 和 `remote_probe` 是局部 patch，未出现的对象或字段保持当前值；realtime report 没有独立 interval，发送节奏来自采集 update 和 `report.*` 策略。如果下发值和当前配置完全相同，`ConfigManager` 会直接忽略，不通知运行任务重建。`export.secure_required=true` 是单向安全闸：它要求 `export.format=smalux_json` 且 `export.wire_mode=secure_psk`，并且当前配置一旦为 `true`，server patch 不能再把它改回 `false`。`export.unsafe_cert` 会放宽 TLS 证书校验，只允许启动时配置，不在 patch 模型中。`agent_id`、`public_ip.required_for_first_report` 和 `public_ip.retry_interval` 也是启动语义字段，不在 patch 模型中。`remote_shell.program` 有三态语义：字段缺省表示不修改，`null` 表示清空为平台默认 shell，字符串表示覆盖 shell 程序。网卡名称会在应用 patch 时做 trim、过滤空字符串并去重。网络筛选优先级为：`include_interfaces` 非空时只统计 include 列表，`exclude_interfaces` 不参与过滤；`include_interfaces` 为空时才应用 `exclude_interfaces`。server patch 涉及 `processes` / `sockets` 且最终采样组启用时，目标 `level` 不能超过启动时的 `--allow-process-level` / `--allow-socket-level`；否则控制消息会被拒绝。`diagnostics`、`remote_shell.enabled` 和 `remote_task.enabled` 不在 patch 模型中，不能通过 server 动态下发；`remote_probe.enabled` 在 patch 模型中，允许动态开启或关闭。

推荐配置组合：

- 本地开发：`wire_mode=binary_plain`、`auth_mode=none`、`report.delta_enabled=false`。server 先只实现 binary wire + snapshot，方便抓包和打印 JSON。
- 自有生产：`wire_mode=secure_psk`、`secure_required=true`、`auth_mode=none`、token 使用 `smx1.<key_id>.<secret_base64url>`。此时 token 只用于派生 PSK，不进入 URL 或 header。
- 低流量监控：开启 `report.delta_enabled=true`，把 `report.snapshot_interval` 设为 `5m` 或更长；如果 server 没实现 delta，就保持默认完整 snapshot。
- 低频公网 IP：保持 `public_ip.enabled=true`，把 `public_ip.refresh_interval` 设为 `24h` 或更长；失败时状态会随 report 上报，不阻塞主流程。
- 高成本诊断：默认 `processes.level=count`、`sockets.level=count`，server 远程权限默认也是 `count`；需要排查时启动 agent 时用 `--allow-process-level light|details` / `--allow-socket-level light|details` 放开对应最高级别。
- 远程执行：`remote_shell.enabled` 和 `remote_task.enabled` 只通过 CLI 打开；server 只能调整超时、并发和输出大小，不能在运行中扩大执行权限。

推荐 server patch 最小化原则：

- 只下发变化字段，不要每次都发送完整配置。
- 不要把日志字段放进 patch；日志只在启动阶段初始化。
- 修改 `export.base_url`、`export.format`、`export.wire_mode`、`export.auth_mode` 或 `export.token` 会触发导出连接重建，server 应避免高频下发这些字段；当前 `export.secure_required=true` 时不要下发任何降级组合，agent 会拒绝。`export.unsafe_cert` 不允许 server 动态下发。
- 修改采样 interval 会重建采集调度表，后续按新频率提交 telemetry update；reporter 已持有的 latest 缓存不会被清空。
- 如果 server 切换 `report.delta_enabled`，建议立即发送一次 `snapshot_request`，让双方重新建立 delta 基准。

server 想确认 patch 是否生效，可以按字段类型观察：

| patch 类型 | 生效观察方式 |
| --- | --- |
| 采样开关或 interval | 后续 snapshot/delta 中对应采样组出现、消失或 `sampled_at` 间隔变化 |
| `report.delta_enabled` | 后续上报从 `snapshot` 变为 `delta` / `heartbeat`，或关闭后恢复完整 `snapshot` |
| `outbound.basic_info` | reporter 生成 basic info 事件的开关、首次发送策略或发送节奏变化；realtime report 始终跟随最新 report 触发 |
| `network.include_interfaces` / `exclude_interfaces` | `network.value.networks` 和汇总值只包含筛选后的网卡 |
| `processes.level` / `sockets.level` | 总数字段始终存在，`light` / `details` 字段按级别出现 |
| `remote_probe.enabled` | 后续 `job_apply(operation=once, kind=probe)` 从 `status=rejected` 变为实际 TCP/HTTP 探测结果；持续任务也会开始本地调度 |
| `export.*` 连接字段 | agent 会按新配置重建导出连接，server 可能看到旧连接关闭和新连接建立 |

如果 patch 下发后没有变化，先看三点：字段是否属于 server patch 模型、值是否和当前配置相同、是否被 CLI-only 或 startup-only 限制挡住。完全相同的 patch 会被忽略，不会重建任务；`agent_id`、`remote_shell.enabled`、`remote_task.enabled`、`diagnostics.*`、`export.unsafe_cert`、`public_ip.required_for_first_report`、`public_ip.retry_interval` 和日志字段本来就不能动态修改。

按需完整快照请求使用同一条控制通道。它不修改配置，只请求 reporter 通过 `TelemetryAggregator` 立即生成完整 `snapshot`，同时刷新后续 delta 的基准状态。为了避免 server 连续刷完整包，agent 用 `report.force_snapshot_min_interval` 做最小响应间隔保护；间隔内的重复请求会合并，等到允许后只发送一次完整 snapshot。

```jsonc
{
  "protocol_version": 1,
  "sequence": 201,
  "sent_at": 1710000000,
  "target_agent_id": "agent-1", // 可选；不写表示当前连接上的 agent
  "type": "snapshot_request",
  "request": {
    "reason": "delta_base_missing" // 可选；server 记录触发原因，agent 只用于日志和诊断
  }
}
```

一次性诊断采集使用同一条控制通道下发，但执行位置在 `collector_loop()`，不会另开一个 `LocalCollector` 并发刷新 sysinfo 状态。请求会提交对应采样组的 `TelemetryUpdate` 给 reporter，reporter 更新内部 latest 缓存后立即尝试生成 `snapshot` 或 `delta` 上报；当前不会在控制通道立即返回 request/response。

```jsonc
{
  "protocol_version": 1,
  "sequence": 204,
  "sent_at": 1710000000,
  "target_agent_id": "agent-1", // 可选
  "type": "collect_processes_once",
  "request": {
    "level": "light", // 可选；缺省使用当前 processes.level
    "limit": 25 // 可选；缺省使用当前 processes.limit，范围 1..=500
  }
}
```

```jsonc
{
  "protocol_version": 1,
  "sequence": 205,
  "sent_at": 1710000000,
  "target_agent_id": "agent-1", // 可选
  "type": "collect_sockets_once",
  "request": {
    "level": "details", // 可选；不能超过启动时 --allow-socket-level
    "limit": 100 // 可选；缺省使用当前 sockets.limit，范围 1..=2000
  }
}
```

`collect_processes_once` 和 `collect_sockets_once` 进入有界命令队列，队列满时会直接拒绝，避免 server 突发下发导致 agent 堆积高成本诊断任务。一次性请求同样受 CLI-only 授权保护：请求级别不能超过启动时的 `--allow-process-level` / `--allow-socket-level`。

## 更新语义汇总

不是所有数据都支持字段级部分更新。当前规则按数据类别区分：

| 数据类别 | 是否支持部分更新 | 更新粒度 | 说明 |
| --- | --- | --- | --- |
| `config_patch` | 支持 | 配置字段级 | patch 中出现的字段覆盖当前值，未出现的字段保持不变；未知字段会被拒绝 |
| `export.query` | 支持，但出现时整体替换 | 整个 query map | patch 中带 `export.query` 时替换当前全部额外 query；不带则保持不变 |
| `network.include_interfaces` | 支持，但出现时整体替换 | 整个 include 列表 | 传空数组表示清空 include；不带字段表示保持当前 include |
| `network.exclude_interfaces` | 支持，但出现时整体替换 | 整个 exclude 列表 | 传空数组表示清空 exclude；不带字段表示保持当前 exclude |
| `outbound.realtime_report` | 支持部分字段 | delivery 字段级 | server 只能更新 `enabled` 和 `send_on_start`；发送节奏由 latest report 触发 |
| `outbound.basic_info` | 支持 | delivery 字段级 | 可以只更新 `enabled`、`refresh_interval` 或 `send_on_start` 其中一个字段；当前主要由 Komari adapter 使用 |
| `agent_id` / `public_ip.required_for_first_report` / `public_ip.retry_interval` / `export.unsafe_cert` | 不支持 server 部分更新 | startup-only 或安全敏感配置 | 只能通过 CLI 或默认值在启动时设置，不在 patch 模型中 |
| `log_file` / `log_retention_files` / `log_max_size_mb` / `log_payload` / `log_payload_max_bytes` | 不支持 server 部分更新 | startup-only 日志配置 | 只能通过 CLI 或默认值在启动时设置，不在 patch 模型中；payload 日志不会被 server 动态开启 |
| `diagnostics` / `remote_shell.enabled` / `remote_task.enabled` | 不支持 server 部分更新 | CLI-only 静态能力 | 只能启动时设置，server patch 不能打开或修改 |
| `remote_shell.max_sessions` / `remote_shell.idle_timeout` / `remote_shell.session_timeout` / `remote_shell.program` / `remote_task.*` | 支持 | 配置字段级 | CLI 可作为启动初始值，server patch 可动态调整运行限制；remote task 启用开关仍是 CLI-only |
| `remote_probe.*` | 支持 | 配置字段级 | 默认关闭；CLI 可作为启动初始值，server patch 可动态开启、关闭和调整频率保护 |
| `snapshot_request` | 不是配置更新 | 单次命令 | 请求 reporter 生成完整 snapshot；受 `report.force_snapshot_min_interval` 保护，重复请求会合并 |
| `collect_processes_once` / `collect_sockets_once` | 不是配置更新 | 单次命令 | 只触发一次采样，提交 `TelemetryUpdate`，不修改 `AgentConfig` |
| `remote_task_run` | 不是配置更新 | 单次命令 | 执行非交互命令，结果通过 `remote_task_result` 回传，不修改 `AgentConfig` |
| `job_apply` | 不是配置更新 | 通用远程 job 同步 | `operation=once` 立即运行一次 job；`operation=replace/patch` 同步持续 job 表。首版稳定 `kind=probe`，由 agent 本地 worker 按 interval 持续探测并通过 `job_result(kind=probe)` 回传 |
| `ack` | 不是配置更新 | 控制层确认 | 只对带 `sequence` 的 Smalux server frame 回传，表示命令已被接收并成功调度 |
| `error` | 不是配置更新 | 控制层错误 | 只对带 `sequence` 的 Smalux server frame 回传，表示命令被拒绝或调度失败 |
| `snapshot` 上报 | 不属于部分更新 | 完整当前状态 | 每次 snapshot 包含完整 `AgentReport`；禁用采样组会省略 |
| `delta` 上报 | 支持 | 采样组级 | 只发送变化的 `identity/core/disk/network/processes/sockets`；出现对象表示替换整个采样组；进程和连接采样组只要出现就仍包含总数 |
| `heartbeat` 上报 | 不包含监控数据 | 业务在线信号 | 只携带最近完整 snapshot 时间和序号，不更新指标 |
| `komari` report | 不支持 delta | 第三方实时 JSON + HTTP 辅助请求 | `export.format=komari` 发送 Komari 格式快照，把 remote task result 映射为 task/result，把 `job_result(kind=probe)` 映射为 ping_result；配置层会拒绝业务级 delta/heartbeat |

因此，监控数据的“部分更新”只到采样组级别，不到嵌套字段级别。例如 `network.value.networks[0].received` 变化时，delta 会发送整个 `network` 采样组；`processes.value.details.items[]` 变化时，delta 会发送整个 `processes` 采样组。server 应把 delta 中出现的采样组整体替换到 latest state 中，而不是尝试按内部字段 merge。

## 调用流程

```text
main()
  -> CliArgs::parse()
  -> args.into_startup()
     -> ServiceOptions::default()
     -> CLI static capability switches
     -> ServiceOptions::validate()
     -> AgentConfig::default()
     -> CLI startup-only log config
     -> AgentConfigPatch from CLI, including dynamic remote limits
     -> validate_config()
  -> init_tracing(initial_config.log_file, initial_config.log_retention_files, initial_config.log_max_size_mb)
  -> ConfigManager::new(initial_config)
  -> service::run(config_manager, service_options)
     -> bounded outbound event queue
     -> outbound sequence allocator
     -> collector command channel
     -> reporter command channel
     -> export_supervisor(outbound_rx)
        -> build_protocol_adapter()
        -> ExportLogOptions(config.log_payload, config.log_payload_max_bytes)
        -> ExportRouter(adapter, log_options)
        -> adapter.transport_plan()
        -> TransportPlan::apply_outbound_config(config.outbound)
        -> TransportHub
     -> SmaluxControlHandler(config patch / snapshot request / one-shot collect / remote shell open / remote task run / job apply)
     -> bootstrap_once()
     -> retry_identity_until_ready()
     -> initial LatestTelemetry moved into reporter_loop()
     -> collector_loop()
     -> collector::identity::identity_refresh_loop()
     -> reporter_loop()
        -> ReporterCommand::ForceSnapshot when server requests snapshot
        -> TelemetryUpdate from collector/identity
        -> LatestTelemetry::build_report()
        -> TelemetryAggregator
           -> OutboundReport::Snapshot | Delta | Heartbeat | skip
        -> OutboundEvent::Report bounded queue
        -> export_supervisor()
        -> active latest-report export deliveries: realtime_report
        -> ExportRouter::send_report()
           -> ProtocolAdapter::encode_report()
        -> outbound.basic_info timer in reporter_loop()
           -> OutboundEvent::BasicInfo bounded queue
           -> export_supervisor()
           -> ExportRouter::send_basic_info()
              -> ProtocolAdapter::encode_basic_info()
        -> Vec<TransportRequest>
        -> TransportHub::enqueue()
        -> export::worker transport task
        -> TransportEvent::Sent | Failed
     -> remote task manager
        -> remote_task_run
        -> execute program + args
        -> OutboundEvent::RemoteTaskResult bounded queue
        -> export_supervisor()
        -> ExportRouter::send_remote_task_result()
     -> remote job manager
        -> job_apply(kind=probe)
        -> delegate to remote probe manager
        -> check dynamic enabled and rate limits
        -> TCP / HTTP probe or immediate status=rejected
        -> OutboundEvent::RemoteJobResult bounded queue
        -> export_supervisor()
        -> ExportRouter::send_remote_job_result()
     -> ControlDispatcher
        -> framed server command success/failure
        -> OutboundEvent::ControlAck | ControlError bounded queue
        -> ExportRouter::send_control_ack() | send_control_error()
```

配置更新流程：

```text
ServerFrame JSON bytes
  -> SmaluxControlHandler::on_message()
  -> smalux_protocol::decode_server_frame()
     -> parse failed / unknown type: drop without ack/error
     -> target_agent_id mismatch: drop without ack/error
     -> success: InboundCommandEnvelope::with_response(command, sequence)
  -> InboundCommandQueue
  -> ControlDispatcher::dispatch()
  -> config_patch:
     -> details permission guard
     -> ConfigManager::apply_patch()
        -> clone current config
        -> patch.apply_to()
        -> validate_config()
        -> watch::Sender::send_replace()
     -> collector / reporter / export supervisor 按订阅处理变化
  -> snapshot_request:
     -> report.enabled guard
     -> try_send ReporterCommand::ForceSnapshot
     -> reporter_loop() 按 report.force_snapshot_min_interval 立即发送或合并延迟发送完整 snapshot
     -> collect_processes_once / collect_sockets_once:
     -> 解析 level / limit，校验上限和 details 权限
     -> try_send CollectorCommand
     -> collector_loop() 采样并提交 TelemetryUpdate
     -> reporter_loop() 更新 latest 缓存并立即尝试上报 snapshot 或 delta
  -> remote_task_run:
     -> 校验 CLI-only remote_task.enabled
     -> 校验并发、timeout、stdout/stderr 回传上限
     -> spawn 非交互任务
     -> 结果写入 OutboundEventQueue
     -> export_supervisor() 通过主上报 transport 回传 remote_task_result
     -> job_apply(kind=probe):
     -> operation=once:
     -> 校验 remote_probe.enabled
     -> 校验 global_min_interval / target_min_interval
     -> 未启用或限频：不发包，直接写入 status=rejected 结果
     -> 已启用且未限频：spawn TCP/HTTP 探测
     -> operation=replace/patch:
     -> 同步持续任务定义
     -> 启动或更新本地 job worker
     -> worker 按 interval 持续执行 TCP/HTTP 探测
     -> 结果写入 OutboundEventQueue
     -> export_supervisor() 通过主上报 transport 回传 job_result(kind=probe)
  -> if sequence meta exists:
     -> success: OutboundEvent::ControlAck
     -> failure: OutboundEvent::ControlError
     -> export_supervisor() 通过主上报 transport 回传 ack/error
```

远程 shell 打开流程：

```text
ServerFrame(type=remote_shell_open)
  -> SmaluxControlHandler::on_message()
  -> InboundCommand::RemoteShellOpen
  -> ControlDispatcher::dispatch()
  -> RemoteShellManager::open()
     -> 校验 CLI 是否启用、当前动态会话上限、session_id、stream_url
     -> 复用当前 export 认证/TLS/query/wire 设置构造 stream WebSocket
     -> spawn shell session
        -> connect stream_url
        -> portable-pty openpty + spawn 本地 shell
        -> stream input -> PTY writer thread
        -> stream resize -> MasterPty::resize()
        -> PTY reader thread -> stream output(base64)
        -> child exit/timeout/close -> stream exit
```

远程非交互任务流程：

```text
ServerFrame(type=remote_task_run)
  -> SmaluxControlHandler::on_message()
  -> InboundCommand::RemoteTaskRun
  -> ControlDispatcher::dispatch()
  -> RemoteTaskManager::start()
     -> CLI-only enabled guard
     -> running counter <= remote_task.max_concurrent
     -> effective timeout = min(request.timeout, config.remote_task.timeout)
     -> tokio::process::Command(program).args(args)
     -> stdout/stderr reader drains pipe and keeps only configured byte limit
     -> RemoteTaskResultEnvelope
     -> OutboundEvent::RemoteTaskResult
     -> ExportRouter::send_remote_task_result()
     -> smalux_json: ClientFrame(type=remote_task_result)
     -> komari: POST /api/clients/task/result?token=...

ServerFrame(type=job_apply) / Komari ping
  -> SmaluxControlHandler::on_message()
  -> InboundCommand::RemoteJobApply
  -> ControlDispatcher::dispatch()
  -> RemoteJobManager::apply()
     -> kind=probe delegate to RemoteProbeManager
     -> operation=once:
     -> dynamic enabled guard
     -> global_min_interval / target_min_interval rate guard
     -> TCP connect or HTTP GET
     -> success: latency_ms
     -> failure / timeout / disabled / limited: -1
     -> operation=replace/patch:
     -> sync jobs into in-memory scheduler
     -> per-job worker runs on interval
     -> RemoteJobResultEnvelope
     -> OutboundEvent::RemoteJobResult
     -> ExportRouter::send_remote_job_result()
     -> smalux_json: ClientFrame(type=job_result, kind=probe)
     -> komari: WebSocket text ping_result
```

## 采集流程

```text
LocalCollector
  -> sample_identity()  # hostname / local_ips / public_ip status
  -> sample_core()      # CPU / memory / load average
  -> sample_disk()      # disk capacity / disk speed
  -> sample_network()   # network speed / total traffic
  -> sample_processes() # count / light / details by config
  -> sample_sockets()   # count / light / details by config
  -> LatestTelemetry
  -> AgentReport
  -> TelemetryAggregator
  -> OutboundReport::Snapshot | Delta | Heartbeat
  -> OutboundEvent::Report
  -> bounded outbound event queue
  -> ExportRouter
  -> ProtocolAdapter 编码为 smalux_json 或 komari
  -> TransportHub::enqueue()
  -> export::worker transport task
  -> TransportEvent::Sent | Failed
```

`sample_disk()` 和 `sample_network()` 分别记录自己的上次采样时间，用真实时间差计算磁盘速度和网络速度。第一次采样只提供累计值，相关分组的 `warmed_up = false`。网络采样默认统计全部网卡；配置 `network.include_interfaces` 后，外层汇总和 `network.networks[]` 明细都只包含指定网卡；未配置 include 时可以用 `network.exclude_interfaces` 排除不需要统计的网卡。身份采样使用独立的网卡刷新对象，不受网速网卡筛选影响，也不会打断网络速度采样的 delta 周期。`sample_processes()` 按 `processes.level` 分为 `count` / `light` / `details`；`count` 只刷新进程数量，`light` 返回按内存排序的 top 进程，`details` 返回受 `processes.limit` 限制的进程明细。`sample_sockets()` 按 `sockets.level` 分为 `count` / `light` / `details`；Linux/Android 的 `count` 优先读取 `/proc/net/sockstat*` 快速计数，失败或非 Linux 平台回退到 socket table；`light` 返回 TCP 状态聚合，`details` 返回受 `sockets.limit` 限制的 socket 明细。socket 采集失败时会保留上一份可用值并把状态标记为 `stale`。

身份采样始终会生成 `IdentityInfo`。公网 IP 获取成功时写入 `ready`；外部服务不可用、超时或没有可用公网候选地址时写入 `failed`；低频刷新失败且已有旧 IP 时写入 `stale`。因此 `public_ip.required_for_first_report=false` 时，第一包可以携带公网 IP 失败状态正常上报。

service 启动后会把初始 `LatestTelemetry` 移交给 `reporter_loop()`，后续不再用共享锁连接采集和上报。运行期按下面几层流转：

- 采集层：`collector_loop()` 使用中心采集调度表按 core/disk/network/processes/sockets 的独立频率计算到期 group；同一调度点到期的 group 会合并成一个 `TelemetryUpdate::Batch`，一次性提交给 reporter；一次性诊断命令也会提交对应 group 的 `TelemetryUpdate`。
- 身份层：`collector::identity::identity_refresh_loop()` 属于采集模块里的低频身份刷新任务，按 `public_ip.refresh_interval` 刷新公网 IP，并提交 `TelemetryUpdate::IdentityRefresh`；这里配置字段仍叫 `public_ip`，因为当前低频身份刷新只包含公网 IP 状态。刷新失败且已有旧公网 IP 时，reporter 内部 latest 缓存会保留旧 IP 并标记为 `stale`。
- reporter 层：`reporter_loop()` 是 latest telemetry 缓存的唯一拥有者；收到 update 后先应用到 `LatestTelemetry`，再交给 `TelemetryAggregator` 决定发送完整 `snapshot`、`delta`、业务级 `heartbeat`，或在无变化且未到心跳间隔时跳过；`report.interval` 仍用于定时 snapshot/heartbeat 兜底。
- Komari basic info：Komari 模式下，reporter 会按 `outbound.basic_info.send_on_start` 和 `outbound.basic_info.refresh_interval` 从同一份 latest telemetry 构建 `OutboundEvent::BasicInfo`；basic info 是低频辅助事件，不做 pending，失败后等待下一次 refresh interval 自然重试。
- 入站控制：server 入站消息先由协议 handler 转换为 `InboundCommandEnvelope`，写入容量为 `128` 的有界入站命令队列，再由 `ControlDispatcher` 统一校验和执行；`snapshot_request` 会转成 `ReporterCommand::ForceSnapshot` 投递给 reporter。
- 出站队列：reporter 会把 `OutboundEvent::Report` 和 `OutboundEvent::BasicInfo` 写入容量为 `256` 的有界出站队列；remote task 完成后写入 `OutboundEvent::RemoteTaskResult`；remote job 完成、被禁用或被限频时写入 `OutboundEvent::RemoteJobResult`；带 `sequence` 的 server frame 执行后写入 `OutboundEvent::ControlAck` 或 `OutboundEvent::ControlError`。队列满时 reporter 会等待 export 端消费，remote task / remote job 结果和控制响应会按各自路径等待或返回错误，避免无界堆积。
- export 层：`export_supervisor()` 消费出站队列并维护 `latest_report`；`realtime_report` 跟随最新 report 触发，`basic_info` 由 `OutboundEvent::BasicInfo` 触发，即时任务和 job 结果通过 `ExportRouter::send_remote_task_result()` / `send_remote_job_result()` 投递，控制响应通过 `send_control_ack()` / `send_control_error()` 投递。
- transport 层：`TransportHub` 为每个 transport 启动 `export::worker` transport task，worker 发送队列默认容量为 `128`；投递满队列时返回错误，真实发送成功或失败会通过 `TransportEvent::Sent` / `TransportEvent::Failed` 回到 `export_supervisor`。
- pending 语义：`last_sent_sequence` 只在收到 `Sent` 后更新；`Failed` 按当前 delivery 的 failure policy 决定重连或只记录日志。远程任务结果、远程 job 结果和控制响应在收到 `Sent` 前会保留 pending 副本，重连后会重投；如果当前 adapter 不支持该事件，会记录为跳过并移除 pending。

身份低频刷新只响应身份相关配置变化：`public_ip.refresh_interval` 变化只重建定时器；`agent_id` 或 `public_ip.enabled/prefer_interface_candidate/verify_interface_candidate/lookup_timeout/max_concurrency` 变化会立刻重新采样一次身份信息；无关的 core/disk/network/processes/sockets 配置变化不会触发身份刷新。

默认兼容模式下 `report.delta_enabled=false` 且 `report.heartbeat_enabled=false`，采集 update 或 `report.interval` tick 都会生成完整 `snapshot`。启用 delta 后，第一包仍然发送完整 `snapshot`；后续只在 identity/core/disk/network/processes/sockets 有变化时发送 `delta`，并按 `report.snapshot_interval` 定期强制刷新完整 `snapshot`。server 也可以发送 `snapshot_request` 请求完整快照；agent 会先检查 `report.enabled`，再受 `report.force_snapshot_min_interval` 保护，间隔内重复请求会合并为下一次允许的完整 snapshot。启用业务级 heartbeat 后，无变化且达到 `report.heartbeat_interval` 时发送 `heartbeat`，用于让 server 确认 agent 业务状态仍在线。`report.snapshot_interval` 和 `report.heartbeat_interval` 使用 `Duration` 精度判断，亚秒值不会被截断成 0。`export.heartbeat` 是 WebSocket ping 间隔，属于传输层 keepalive，不是业务级 heartbeat。

如果启动阶段公网 IP 获取失败，并且 `public_ip.enabled=true` 且 `public_ip.required_for_first_report=true`，service 会按 `public_ip.retry_interval` 重试身份采集，直到 `identity.public_ip.status=ready`。`required_for_first_report` 和 `retry_interval` 是启动门禁字段，不允许 server patch 动态修改；如果不希望公网 IP 阻塞第一包，需要在启动参数中关闭该门禁。

## 数据格式

server 控制消息当前由 `smalux_json` 主 WebSocket 承载。Smalux 自有 server 应按当前 `export.wire_mode` 发送 binary wire payload；`binary_plain` 模式为了本地调试可以直接用 WebSocket text 发送 `ServerFrame` JSON，`secure_psk` 模式会拒绝明文 text。自有协议控制 payload 统一是 `ServerFrame`，当前支持 `snapshot_request`、`config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_task_run`、`job_apply` 和 `remote_shell_open`。`ServerFrame` 包含 server `sequence`，agent 调度后回 `ack/error`；可选 `target_agent_id` 不匹配时会直接丢弃，不回 ack/error。

字段定义和稳定协议边界以 [crates/smalux-protocol/README.md](/abs/path/F:/code/rust/smalux/crates/smalux-protocol/README.md) 为准；本节重点描述 agent 视角下的交互流程、运行约束和示例 payload。

下面所有 Smalux 自有控制示例都是解包或解密后的 `ServerFrame` JSON。真正通过 WebSocket 发送时还要按 wire mode 加一层封装：

| wire mode | WebSocket frame | payload 处理 | agent 接收要求 |
| --- | --- | --- | --- |
| `binary_plain` | binary | `WirePacket(kind=PlainData, payload=utf8_json_bytes)` | binary 必须是 `PlainData`；开发期 text 可以直接作为 `ServerFrame` JSON |
| `secure_psk` | binary | `Noise.encrypt(utf8_json_bytes)` 后放入 `WirePacket(kind=SecureData)` | 只接受 `SecureData`；text 会被拒绝 |

因此 server 实现时可以先把控制对象序列化为 UTF-8 JSON bytes，再交给统一的 `send_payload(bytes)`。`send_payload` 根据当前连接的 wire mode 决定直接封 `PlainData`，还是用 Noise transport 加密后封 `SecureData`。

`config_patch` 示例：

```jsonc
{
  "protocol_version": 1,
  "sequence": 203,
  "sent_at": 1710000000,
  "target_agent_id": "agent-1", // 可选
  "type": "config_patch",
  "patch": {
    "core": { "interval": "2s" }
  }
}
```

`AgentConfigPatch` 字段均为可选，未出现的字段保持当前值。时间字段使用人类可读字符串，例如 `500ms`、`2s`、`5m`、`24h`。

带 `sequence` 的 Smalux server frame 示例：

```jsonc
{
  "protocol_version": 1, // smalux-protocol 通信协议版本
  "sequence": 201, // server 侧命令序号；agent ack/error 会原样引用
  "sent_at": 1710000000, // server 发送时间，Unix 秒
  "type": "snapshot_request",
  "request": {
    "reason": "delta_base_missing" // 可选；例如 server 发现 delta base_sequence 不匹配
  }
}
```

agent 接收并成功调度带 `sequence` 的 server frame 后，会回传控制层 ack：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 30,
  "sent_at": 1710000001,
  "type": "ack",
  "ack": {
    "sequence": 201 // 被确认的 server sequence
  }
}
```

如果命令被拒绝或调度失败，会回传控制层 error：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 31,
  "sent_at": 1710000001,
  "type": "error",
  "error": {
    "sequence": 201, // 失败对应的 server sequence
    "code": "snapshot_request_failed",
    "message": "reporting is disabled"
  }
}
```

`ack` 只表示控制命令已被 agent 接收并成功调度，不等于后续 snapshot、remote task 或 remote job 结果已经送达 server。真正的完整快照仍以 `type=snapshot` 的业务 frame 上报；remote task 的执行结果仍以 `type=remote_task_result` 回传；remote job 的执行结果以 `type=job_result` 回传，当前 probe 探测结果放在 `kind=probe` 分支内。

远程 shell 打开请求：

```jsonc
{
  "protocol_version": 1, // Smalux 协议版本
  "sequence": 301, // server 侧递增序号；agent 会先回 ack/error
  "sent_at": 1710000200, // server 发送时间，Unix 秒
  "type": "remote_shell_open", // 只在 CLI 启用 remote_shell 后接受
  "request": {
    "session_id": "shell-1", // server 生成的会话 ID
    "stream_url": "wss://example.com/agent/shell/shell-1", // 临时 WebSocket stream 地址
    "cols": 120, // 可选，初始终端列数；缺省 80
    "rows": 30 // 可选，初始终端行数；缺省 24
  }
}
```

shell stream 上 server 发给 agent 的消息：

下面这些是 `smalux_json` 自有 shell stream 的业务 JSON。`binary_plain` 下它们是 `WirePacket(kind=PlainData)` 的 payload；`secure_psk` 下它们是解密后的 `WirePacket(kind=SecureData)` payload。只有本地调试或明文兼容路径才应直接用 WebSocket text。

```jsonc
{ "type": "input", "data": "echo hello\r\n" } // UTF-8 文本输入，encoding 缺省为 utf8
{ "type": "input", "encoding": "base64", "data": "AAEC" } // 原始字节输入
{ "type": "resize", "cols": 120, "rows": 30 } // 调整 PTY 尺寸
{ "type": "close" } // 请求关闭本次 shell 会话
{ "type": "heartbeat" } // 可选；stream 保活，不写入 PTY
```

shell stream 上 agent 发给 server 的消息：

这些事件同样是 Smalux wire payload 里的业务 JSON，不是 `secure_psk` 模式下的裸 text frame。

```jsonc
{ "type": "opened", "session_id": "shell-1" } // shell 已启动
{ "type": "output", "session_id": "shell-1", "encoding": "base64", "data": "aGVsbG8NCg==" } // PTY 原始输出
{ "type": "exit", "session_id": "shell-1", "code": 0 } // shell 退出；强制关闭时 code 可能为 null
{ "type": "error", "session_id": "shell-1", "message": "..." } // 会话错误
```

远程非交互任务请求：

```jsonc
{
  "protocol_version": 1,
  "sequence": 302,
  "sent_at": 1710000201,
  "target_agent_id": "agent-1", // 可选
  "type": "remote_task_run", // 只在 CLI 启用 remote_task 后执行；未启用会回传 rejected
  "request": {
    "task_id": "task-1", // server 生成的任务 ID，结果会原样带回
    "program": "powershell.exe", // 要执行的程序；agent 不做 shell 拼接
    "args": ["-NoProfile", "-Command", "Get-Date"], // 可选，直接传给 program
    "timeout": "30s" // 可选，实际值不会超过 remote_task.timeout
  }
}
```

`smalux_json` 远程任务结果会作为 `smalux_protocol::ClientFrame` 回传：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 10,
  "sent_at": 1710000100,
  "type": "remote_task_result",
  "result": {
    "task_id": "task-1",
    "status": "success", // success | failed | timed_out | rejected
    "exit_code": 0, // 启动失败、超时或 rejected 时省略
    "stdout": "2026-05-29T12:00:00Z",
    "stderr": "",
    "started_at": 1710000099,
    "finished_at": 1710000100,
    "duration_ms": 1000,
    "timed_out": false,
    "stdout_truncated": false,
    "stderr_truncated": false,
    "error": "..." // 可选；failed/timed_out/rejected 时可能出现
  }
}
```

任务执行说明：

- `program` 和 `args` 直接传给 `tokio::process::Command`，不会自动经过系统 shell；如果确实需要 shell 语义，server 必须显式把 `program` 设置为 `powershell.exe`、`cmd.exe`、`/bin/sh` 等，并自行传入参数。
- `timeout` 是本次请求期望值，agent 会取它和当前 `remote_task.timeout` 的较小值；server 不能通过单次请求突破 agent 配置上限。
- stdout/stderr 会按字节上限截断，但 reader 会继续 drain pipe，避免高输出命令阻塞子进程。
- `rejected` 表示 agent 没有执行命令，常见原因是 `remote_task.enabled=false` 或并发已满。

远程网络探测一次性请求：

```jsonc
{
  "protocol_version": 1,
  "sequence": 303,
  "sent_at": 1710000202,
  "target_agent_id": "agent-1", // 可选
  "type": "job_apply",
  "request": {
    "operation": "once",
    "runs": [
      {
        "kind": "probe",
        "request_id": 123, // server 生成的探测请求 ID，字符串或数字都可以，结果会原样带回
        "point_id": "point-main-api", // 可选；server 业务探测点 ID，结果会原样带回，推荐批量 ping 时填写
        "probe_type": "tcp", // tcp | http | icmp；icmp 当前返回 status=failed
        "target": "example.com:443", // tcp 使用 host:port 或 tcp://host:port；http 使用 URL 或 host
        "timeout": "3s" // 可选；不写时使用 agent 当前 remote_probe.timeout
      }
    ]
  }
}
```

远程网络探测持续任务同步：

```jsonc
{
  "protocol_version": 1,
  "sequence": 304,
  "sent_at": 1710000203,
  "target_agent_id": "agent-1", // 可选
  "type": "job_apply",
  "request": {
    "operation": "replace",
    "generation": 12, // server 维护的单调递增版本
    "jobs": [
      {
        "kind": "probe",
        "job_id": "main-api",
        "point_id": "point-main-api",
        "enabled": true,
        "probe_type": "tcp",
        "target": "example.com:443",
        "interval": "30s",
        "timeout": "3s"
      }
    ]
  }
}
```

`smalux_json` 远程 job 结果会作为 `smalux_protocol::ClientFrame` 回传；当前网络探测结果使用 `kind=probe` 分支：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 11,
  "sent_at": 1710000101,
  "type": "job_result",
  "result": {
    "kind": "probe",
    "result": {
      "run_id": "4c21e6f8-8ef3-4ee5-9c91-8f630f650b92", // agent 为本次探测运行生成的唯一 ID
      "source": "once", // once | job；once 表示一次性请求，job 表示持续探测任务
      "point_id": "point-main-api", // server 业务探测点 ID；下发时有就会原样带回
      "request_id": 123, // source=once 时存在，等于 server 下发 runs[].request_id
      "job_id": "main-api", // source=job 时存在，等于持续任务 job_id；source=once 时没有
      "probe_type": "tcp",
      "target": "example.com:443",
      "status": "success", // success | failed | rejected
      "latency_ms": 13, // 成功时存在；failed/rejected 时通常没有
      "started_at": 1710000100,
      "finished_at": 1710000101,
      "duration_ms": 13,
      "error": "remote probe is disabled" // 可选；失败、禁用、限频或未实现时出现
    }
  }
}
```

探测执行说明：

- `operation=once` 在 `remote_probe.enabled=false`、全局限频未过或同目标限频未过时，agent 不发任何网络包，立即回传 `status=rejected`。
- `operation=replace/patch` 只同步持续任务，不直接回业务结果；每个启用的 job 会在 agent 本地立即启动一次探测，然后按各自 `interval` 持续执行。
- TCP 探测只尝试建立 TCP 连接；HTTP 探测按 Komari 约定发送 `GET` 请求，但不读取响应 body。
- 当前不做目标 allowlist；是否允许探测由 server 自己控制，但 agent 本地会用 `global_min_interval` 和 `target_min_interval` 做硬保护。
- 不维护显式 `max_concurrent`；一次性探测和持续 job 共享同一套全局/同目标限频保护。

Komari exec 入站消息：

```jsonc
{
  "message": "exec", // Komari server 事件类型
  "task_id": "task-1", // Komari server 生成的任务 ID
  "command": "echo hello" // 命令字符串；agent 会按平台转换为 shell 执行
}
```

Komari exec 会复用同一套 `RemoteTaskManager`，因此仍然要求启动时开启 `--remote-task-enabled true`，并受 `remote_task.max_concurrent`、`remote_task.timeout`、`remote_task.max_stdout_bytes` 和 `remote_task.max_stderr_bytes` 限制。未启用或并发已满时不会执行命令，但会把 rejected 结果回传给 Komari。Komari 只有一个 `command` 字符串字段；兼容层会转换为：

- Windows：`powershell.exe -NoProfile -Command <command>`
- 其它平台：`/bin/sh -c <command>`

Komari exec 结果使用 HTTP 回传：

```text
POST /api/clients/task/result?token=TOKEN
```

请求体：

```jsonc
{
  "task_id": "task-1", // 原样回传 Komari task_id
  "result": "hello", // stdout/stderr/error 合并后的文本
  "exit_code": 0, // 有进程退出码时使用真实退出码；rejected/timed_out 等无退出码时为 -1
  "finished_at": "2026-05-29T12:00:00Z" // RFC3339 秒级时间
}
```

Komari ping 入站消息：

```jsonc
{
  "message": "ping", // Komari server 事件类型
  "ping_task_id": 123, // Komari server 生成的 ping 任务 ID，结果会保持原始 JSON 类型
  "ping_type": "tcp", // tcp | http | icmp
  "ping_target": "example.com:443" // 探测目标
}
```

Komari ping 会复用同一套 `RemoteProbeManager`，因此默认不会发包，除非当前动态配置里 `remote_probe.enabled=true`。内部自有协议使用 `status/latency_ms`；兼容 Komari 输出时仍按 Komari 约定映射为 `value`，成功时为耗时毫秒，未启用、限频、失败、超时或 `icmp` 未实现时为 `-1`。

Komari ping 结果通过 WebSocket report 通道回传：

```jsonc
{
  "type": "ping_result",
  "task_id": 123, // 原样回传 ping_task_id
  "ping_type": "tcp",
  "value": 13,
  "finished_at": "2026-05-29T12:00:00Z" // RFC3339 秒级时间
}
```

认证相关 JSON 示例：

```json
{
  "type": "config_patch",
  "patch": {
    "export": {
      "base_url": "https://example.com",
      "auth_mode": "query",
      "token": "REPLACE_WITH_TRANSPORT_TOKEN",
      "query_token_param": "access_token",
      "query": {
        "agent_id": "agent-1",
        "region": "local"
      },
      "heartbeat": "30s",
      "reconnect_interval": "5s"
    }
  }
}
```

secure_psk 配置示例：

```jsonc
{
  "type": "config_patch",
  "patch": {
    "export": {
      "base_url": "https://example.com",
      "format": "smalux_json",
      "wire_mode": "secure_psk",
      "secure_required": true,
      "auth_mode": "none",
      "token": "smx1.agent-key.BASE64URL_SECRET"
    }
  }
}
```

`export.format=smalux_json` 时，业务 payload 是 `smalux_protocol::ClientFrame` 标准 JSON。监控上报先表示为 `OutboundReport::Snapshot | Delta | Heartbeat`，远程任务结果表示为 `RemoteTaskResultEnvelope`，最后由 export adapter 编码成 `TransportRequest::WebSocketBinary { sequence, body }`，再交给 WebSocket transport 按 `export.wire_mode` 封为 `PlainData` 或 `SecureData`。兼容其他服务端时，可以把同一个内部事件编码成其他外层格式，或者对不支持的事件返回空请求列表跳过发送。当前监控上报使用 `type=snapshot` / `type=delta` / `type=heartbeat`，其中完整 `report` 字段由 reporter 内部 `LatestTelemetry::build_report()` 组装为 `smalux_core::model::info::AgentReport`。`protocol_version` 是通信协议版本，`report.meta.schema_version = 5` 是上报数据模型版本，两者不要混用。

```jsonc
{
  "protocol_version": 1, // smalux-protocol 通信协议版本
  "agent_id": "agent-1", // agent 实例 ID，冗余放在 frame 顶层便于 server 快速路由
  "sequence": 1, // agent 侧递增消息序号，用于发现跳号或后续 delta 基准
  "sent_at": 1710000000, // frame 发送时间，Unix 秒
  "type": "snapshot", // snapshot | delta | heartbeat | ack | error | remote_task_result | job_result
  "report": {
    // 完整 AgentReport，字段结构见下方
  }
}
```

业务级 heartbeat frame：

```jsonc
{
  "protocol_version": 1, // smalux-protocol 通信协议版本
  "agent_id": "agent-1", // agent 实例 ID
  "sequence": 2, // agent 侧递增消息序号
  "sent_at": 1710000030, // frame 发送时间，Unix 秒
  "type": "heartbeat", // 业务级 heartbeat，不是 WebSocket ping
  "heartbeat": {
    "last_report_at": 1710000000, // 最近一次完整 snapshot 发送时间，Unix 秒
    "last_report_sequence": 1 // 最近一次完整 snapshot 序号
  }
}
```

delta frame：

```jsonc
{
  "protocol_version": 1, // smalux-protocol 通信协议版本
  "agent_id": "agent-1", // agent 实例 ID
  "sequence": 3, // agent 侧递增消息序号
  "sent_at": 1710000005, // frame 发送时间，Unix 秒
  "type": "delta", // 增量上报
  "delta": {
    "base_sequence": 1, // 本次 delta 基于的上一条已发送业务消息序号
    "report_at": 1710000005, // 本次 delta 生成时间，Unix 秒
    "identity": {}, // 可选；身份信息变化时出现
    "core": {}, // 可选；核心指标变化时出现，null 表示该组被关闭
    "disk": {}, // 可选；磁盘指标变化时出现，null 表示该组被关闭
    "network": {}, // 可选；网络指标变化时出现，null 表示该组被关闭
    "processes": {}, // 可选；进程汇总变化时出现，null 表示该组被关闭
    "sockets": {} // 可选；socket 汇总变化时出现，null 表示该组被关闭
  }
}
```

delta 字段语义：

- 字段缺省：该字段相对上一份状态没有变化。
- `identity` 出现：替换 server 上保存的身份信息。
- `core` / `disk` / `network` / `processes` / `sockets` 出现对象：替换对应采样组最新值。
- `core` / `disk` / `network` / `processes` / `sockets` 出现 `null`：对应采样组被关闭，server 应清空该组。
- delta 不做采样组内部字段级 merge；出现哪个采样组，server 就用该采样组对象整体覆盖旧值。
- server 如果发现 `base_sequence` 对不上，应发送带 `sequence` 的 `snapshot_request`；agent 会回传 `ack/error` 并通过 reporter 生成完整 `snapshot`，后续 delta 会基于这次完整快照重新建立基准。

### Komari 格式

`export.format=komari` 时，周期监控只消费 `OutboundReport::Snapshot`。`delta` 和业务级 `heartbeat` 在配置校验阶段会被拒绝；`remote_task_result` 会映射到 Komari `task/result` HTTP 接口；`job_result(kind=probe)` 会映射到 Komari WebSocket `ping_result`；控制层 `ack/error` 当前不映射到 Komari 格式，会返回空请求列表并跳过发送。

Komari WebSocket report endpoint。配置 `export.base_url=https://host` 时，agent 会把它派生为下面的实际请求 URL；这个 URL 不是 CLI 或配置文件里直接填写的值：

```text
wss://host/api/clients/report?token=TOKEN
```

Komari basic info endpoint 会由同一个 `export.base_url` 派生：

```text
POST https://host/api/clients/uploadBasicInfo?token=TOKEN
```

Komari task result endpoint 同样由同一个 `export.base_url` 派生：

```text
POST https://host/api/clients/task/result?token=TOKEN
```

Komari terminal endpoint 同样由同一个 `export.base_url` 和 Komari 下发的 `request_id` 派生：

```text
wss://host/api/clients/terminal?id=REQUEST_ID&token=TOKEN
```

Komari 标准 agent 模式完整连接顺序：

```text
service bootstrap
  -> reporter builds OutboundEvent::BasicInfo when telemetry is ready
  -> Komari adapter encodes basic info
  -> POST /api/clients/uploadBasicInfo?token=TOKEN
  -> lazy connect WebSocket /api/clients/report?token=TOKEN
  -> send first report text JSON
  -> send report text JSON whenever reporter produces a new complete snapshot
  -> reporter repeats basic info every outbound.basic_info.refresh_interval, 5m by default
  -> when report WebSocket receives {"message":"terminal","request_id":"..."}
     -> open WebSocket /api/clients/terminal?id=REQUEST_ID&token=TOKEN
     -> bridge PTY output as raw binary frames until terminal closes
  -> when exec result is ready, POST /api/clients/task/result?token=TOKEN
  -> when ping result is ready, send WebSocket text ping_result
```

Komari terminal stream：

- 主 report WebSocket 收到 `{ "message": "terminal", "request_id": "..." }` 后，Komari adapter 会创建内部 `RemoteShellOpen` 命令；真正执行仍由 `RemoteShellManager` 完成。
- terminal stream 会清空 Smalux 认证配置，避免在已经带 `token` 的 Komari URL 上重复追加 query token。
- terminal stream 使用 WebSocket raw binary frame，不使用 Smalux `WirePacket`，也不执行 `secure_psk` Noise 握手；生产环境应使用 `wss://` 保护传输。
- agent -> Komari：PTY 输出直接发送 binary frame；内部 `opened` 和 `exit` 事件会跳过，避免第三方 terminal 把 JSON 当终端输出。
- Komari -> agent：binary frame 直接写入 PTY；text frame 会先尝试解析 Smalux shell JSON command，再尝试 `{ "type": "input", "input": "..." }`，解析失败时作为原始 UTF-8 输入写入 PTY。

实时 report JSON：

```jsonc
{
  "cpu": { "usage": 12.3 }, // CPU 使用率百分比
  "ram": { "total": 17179869184, "used": 8589934592 }, // 内存总量和已使用量，单位字节
  "swap": { "total": 2147483648, "used": 0 }, // swap 总量和已使用量，单位字节
  "load": { "load1": 0.1, "load5": 0.2, "load15": 0.3 }, // 系统负载
  "disk": { "total": 512000000000, "used": 256000000000 }, // 磁盘总量和已使用量，单位字节
  "network": {
    "up": 1024, // 当前上传速率，单位字节/秒
    "down": 2048, // 当前下载速率，单位字节/秒
    "totalUp": 123456789, // 累计上传流量，单位字节
    "totalDown": 987654321 // 累计下载流量，单位字节
  },
  "connections": { "tcp": 64, "udp": 16 }, // 当前 TCP/UDP socket 总数
  "uptime": 3600, // 系统运行时长，单位秒
  "process": 128, // 当前进程总数
  "message": "" // 预留消息字段
}
```

basic info JSON：

```jsonc
{
  "arch": "x86_64", // CPU 架构
  "cpu_cores": 8, // CPU 核心数
  "cpu_name": "Intel(R) Core(TM)", // CPU 名称
  "disk_total": 512000000000, // 磁盘总量，单位字节
  "gpu_name": "", // 当前暂未采集 GPU，固定为空字符串
  "ipv4": "203.0.113.10", // 公网 IPv4，获取不到时为空字符串
  "ipv6": "", // 公网 IPv6，获取不到时为空字符串
  "mem_total": 17179869184, // 内存总量，单位字节
  "os": "Windows 11 Pro", // 操作系统名称
  "kernel_version": "10.0.26100", // 内核版本
  "swap_total": 2147483648, // swap 总量，单位字节
  "version": "0.1.0", // agent 版本
  "virtualization": "" // 当前暂未采集虚拟化信息，固定为空字符串
}
```

下面是 `report` 字段的完整 JSONC 结构，注释仅用于文档说明，实际 WebSocket 发送的是不带注释的标准 JSON。为了把所有字段都标出来，示例会同时列出部分互斥或可选字段；真实 payload 会按配置、平台能力和 `skip_serializing_if` 省略不存在的字段。

```jsonc
{
  "meta": {
    "schema_version": 5, // 上报模型版本；当前固定为 5
    "agent_version": "0.1.0", // agent crate 版本
    "report_at": 1710000000 // 本次上报时间，Unix 秒
  },
  "identity": {
    "agent_id": "agent-1", // agent 实例 ID；默认 UUID，建议生产显式传入
    "hostname": "host-1", // 主机名
    "public_ip": {
      "status": "ready", // disabled | pending | ready | failed | stale
      "ip": "203.0.113.10", // 可选；ready/stale 时存在
      "source": "external_http", // 可选；external_http | interface_candidate
      "sampled_at": 1710000000, // 可选；公网 IP 成功采样时间，Unix 秒
      "verified_at": 1710000000, // 可选；外部服务校验时间，Unix 秒
      "last_attempt_at": 1710000000, // 可选；最近一次尝试时间，Unix 秒
      "error": "Public IP lookup timed out" // 可选；failed/stale 时记录失败原因
    },
    "local_ips": [
      {
        "ip": "192.168.1.10", // 本地网卡 IP
        "mask_len": 24 // 网络前缀长度
      }
    ]
  },
  "system": {
    "name": "Windows", // 系统名称
    "kernel_version": "10.0.26100", // 内核版本
    "kernel_long_version": "Windows 11 Pro", // 内核完整版本
    "os_version": "11", // 操作系统版本
    "long_os_version": "Windows 11 Pro", // 操作系统完整版本
    "hostname": "host-1", // 主机名
    "distribution_id": "windows", // 发行版标识
    "uptime": 86400, // 系统运行时长，单位秒
    "boot_time": 1709913600, // 系统启动时间，Unix 秒
    "supported": true, // 当前平台是否被 sysinfo 支持
    "core_num": 8, // 物理核心数
    "cpu_arch": "x86_64" // CPU 架构
  },
  "core": {
    // 可选；core.enabled=false 时整个 core 字段不存在
    "sampled_at": 1710000000, // 核心指标采样时间，Unix 秒
    "value": {
      "cpu": {
        "cpu_num": 16, // 逻辑 CPU 数量
        "cpu_usage": 12.5, // 全局 CPU 使用率百分比
        "cpus": [
          {
            "name": "cpu0", // 逻辑 CPU 名称
            "brand": "Intel(R) Core(TM)", // CPU 品牌
            "vendor_id": "GenuineIntel", // 供应商 ID
            "usage": 10.2, // 当前逻辑 CPU 使用率百分比
            "frequency": 3200 // 当前频率，单位 MHz
          }
        ]
      },
      "memory": {
        "memory_total": 34359738368, // 物理内存总量，单位字节
        "memory_usage": 12884901888, // 已使用物理内存，单位字节
        "memory_available": 21474836480, // 可用物理内存，单位字节
        "memory_free": 10737418240, // 空闲物理内存，单位字节
        "swap_total": 8589934592, // swap 总量，单位字节
        "swap_usage": 1073741824, // 已使用 swap，单位字节
        "swap_free": 7516192768 // 空闲 swap，单位字节
      },
      "load_avg": {
        "one": 0.42, // 1 分钟平均负载
        "five": 0.36, // 5 分钟平均负载
        "fifteen": 0.31, // 15 分钟平均负载
        "supported": false // 当前平台是否可靠支持平均负载
      }
    }
  },
  "disk": {
    // 可选；disk.enabled=false 时整个 disk 字段不存在
    "sampled_at": 1710000000, // 磁盘指标采样时间，Unix 秒
    "value": {
      "warmed_up": true, // 是否已完成速度预热；第一次采样通常为 false
      "disks": [
        {
          "name": "C:", // 磁盘名称
          "total_space": 1024000000000, // 总容量，单位字节
          "available_space": 512000000000, // 可用容量，单位字节
          "kind": "SSD", // 磁盘类型
          "file_system": "NTFS", // 文件系统名称
          "is_read_only": false, // 是否只读
          "is_removable": false, // 是否可移动设备
          "mount_point": "C:\\", // 挂载点路径
          "read_bytes": 1048576, // 本次刷新周期内读取字节数
          "write_bytes": 524288, // 本次刷新周期内写入字节数
          "read_bytes_per_sec": 1048576.0, // 当前读取速度，单位字节/秒
          "write_bytes_per_sec": 524288.0, // 当前写入速度，单位字节/秒
          "io_bytes_per_sec": 1572864.0, // 当前总 IO 速度，单位字节/秒
          "total_read_bytes": 123456789, // 系统启动后累计读取字节数
          "total_written_bytes": 987654321, // 系统启动后累计写入字节数
          "total_io_bytes": 1111111110 // 系统启动后累计 IO 字节数
        }
      ],
      "total_space": 1024000000000, // 所有磁盘总容量，单位字节
      "available_space": 512000000000, // 所有磁盘可用容量，单位字节
      "read_bytes": 1048576, // 所有磁盘本次刷新周期内读取字节数
      "write_bytes": 524288, // 所有磁盘本次刷新周期内写入字节数
      "read_bytes_per_sec": 1048576.0, // 所有磁盘当前读取速度，单位字节/秒
      "write_bytes_per_sec": 524288.0, // 所有磁盘当前写入速度，单位字节/秒
      "io_bytes_per_sec": 1572864.0, // 所有磁盘当前总 IO 速度，单位字节/秒
      "total_read_bytes": 123456789, // 所有磁盘系统启动后累计读取字节数
      "total_written_bytes": 987654321, // 所有磁盘系统启动后累计写入字节数
      "total_io_bytes": 1111111110 // 所有磁盘系统启动后累计 IO 字节数
    }
  },
  "network": {
    // 可选；network.enabled=false 时整个 network 字段不存在
    "sampled_at": 1710000000, // 网络指标采样时间，Unix 秒
    "value": {
      "warmed_up": true, // 是否已完成速度预热；第一次采样通常为 false
      "networks": [
        {
          "name": "Ethernet", // 网卡名称
          "mtu": 1500, // 最大传输单元
          "received": 2048, // 本次刷新周期内接收字节数
          "errors_on_received": 0, // 本次刷新周期内接收错误数
          "errors_on_transmitted": 0, // 本次刷新周期内发送错误数
          "packets_received": 16, // 本次刷新周期内接收包数量
          "transmitted": 1024, // 本次刷新周期内发送字节数
          "received_bytes_per_sec": 2048.0, // 当前接收速度，单位字节/秒
          "transmitted_bytes_per_sec": 1024.0, // 当前发送速度，单位字节/秒
          "network_bytes_per_sec": 3072.0, // 当前总网络速度，单位字节/秒
          "total_received": 123456789, // 系统启动后累计接收字节数
          "total_errors_on_received": 0, // 系统启动后累计接收错误数
          "total_errors_on_transmitted": 0, // 系统启动后累计发送错误数
          "total_packets_received": 123456, // 系统启动后累计接收包数量
          "total_transmitted": 98765432, // 系统启动后累计发送字节数
          "used_traffic_bytes": 222222221, // 系统启动后累计使用流量，单位字节
          "mac": "00:11:22:33:44:55", // MAC 地址
          "ip": [
            {
              "ip": "192.168.1.10", // 网卡 IP
              "mask_len": 24 // 网络前缀长度
            }
          ]
        }
      ],
      "received": 2048, // 所有网卡本次刷新周期内接收字节数
      "errors_on_received": 0, // 所有网卡本次刷新周期内接收错误数
      "errors_on_transmitted": 0, // 所有网卡本次刷新周期内发送错误数
      "packets_received": 16, // 所有网卡本次刷新周期内接收包数量
      "transmitted": 1024, // 所有网卡本次刷新周期内发送字节数
      "received_bytes_per_sec": 2048.0, // 所有网卡当前接收速度，单位字节/秒
      "transmitted_bytes_per_sec": 1024.0, // 所有网卡当前发送速度，单位字节/秒
      "network_bytes_per_sec": 3072.0, // 所有网卡当前总网络速度，单位字节/秒
      "total_received": 123456789, // 所有网卡系统启动后累计接收字节数
      "total_errors_on_received": 0, // 所有网卡系统启动后累计接收错误数
      "total_errors_on_transmitted": 0, // 所有网卡系统启动后累计发送错误数
      "total_packets_received": 123456, // 所有网卡系统启动后累计接收包数量
      "total_transmitted": 98765432, // 所有网卡系统启动后累计发送字节数
      "used_traffic_bytes": 222222221 // 所有网卡系统启动后累计使用流量，单位字节
    }
  },
  "processes": {
    // 可选；processes.enabled=false 时整个 processes 字段不存在
    "sampled_at": 1710000000, // 进程采样时间，Unix 秒
    "value": {
      "count": 128, // 当前进程总数
      "status": "ready", // ready | stale | failed | unsupported
      "level": "details", // count | light | details
      "light": {
        // 可选；仅 processes.level=light 时出现
        "limit": 50, // 返回条数上限
        "truncated": true, // 是否因 limit 截断
        "items": [
          {
            "pid": 1234, // 进程 ID
            "name": "postgres", // 进程名称
            "status": "Run", // 平台进程状态字符串
            "cpu_usage": 1.25, // CPU 使用率百分比
            "memory_bytes": 268435456 // 常驻内存，单位字节
          }
        ]
      },
      "details": {
        // 可选；仅 processes.level=details 时出现
        "limit": 50, // 返回条数上限
        "truncated": true, // 是否因 limit 截断
        "items": [
          {
            "pid": 1234, // 进程 ID
            "parent_pid": 1, // 可选；父进程 ID
            "name": "postgres", // 进程名称
            "status": "Run", // 平台进程状态字符串
            "cpu_usage": 1.25, // CPU 使用率百分比
            "memory_bytes": 268435456, // 常驻内存，单位字节
            "virtual_memory_bytes": 1073741824, // 虚拟内存，单位字节
            "start_time": 1709990000, // 进程启动时间，Unix 秒
            "run_time": 10000, // 进程运行时长，单位秒
            "exe": "C:\\Program Files\\PostgreSQL\\bin\\postgres.exe", // 可选；可执行文件路径
            "cmd": ["postgres", "-D", "data"] // 命令行参数列表
          }
        ]
      },
      "error": "process table permission denied" // 可选；failed/stale/unsupported 时记录失败原因
    }
  },
  "sockets": {
    // 可选；sockets.enabled=false 时整个 sockets 字段不存在
    "sampled_at": 1710000000, // socket 采样时间，Unix 秒
    "value": {
      "tcp": 64, // 当前 TCP socket 总数
      "udp": 16, // 当前 UDP socket 总数
      "status": "ready", // ready | stale | failed | unsupported
      "source": "socket_table", // socket_table | fast_counter
      "accuracy": "socket_table", // socket_table | aggregate
      "level": "details", // count | light | details
      "light": {
        // 可选；仅 sockets.level=light 时出现
        "tcp_states": [
          {
            "state": "established", // TCP 状态名，使用小写 snake_case
            "count": 32 // 该状态下的 TCP socket 数量
          }
        ]
      },
      "details": {
        // 可选；仅 sockets.level=details 时出现
        "limit": 200, // 返回条数上限
        "truncated": true, // 是否因 limit 截断
        "items": [
          {
            "protocol": "tcp", // tcp | udp
            "local_addr": "127.0.0.1", // 本地地址
            "local_port": 5432, // 本地端口
            "remote_addr": "127.0.0.1", // 可选；远端地址，UDP 通常省略
            "remote_port": 52000, // 可选；远端端口，UDP 通常省略
            "state": "established", // 可选；TCP 状态，UDP 通常省略
            "pids": [1234] // 关联进程 ID 列表；平台或权限不支持时为空数组
          }
        ]
      },
      "error": "socket table permission denied" // 可选；failed/stale/unsupported 时记录失败原因
    }
  }
}
```

`processes.value.count` 在 `count` / `light` / `details` 三个级别都会上报；`processes.level=light` / `details` 只控制是否额外出现 `light` 或 `details` 字段。上方 JSONC 已展开 `ProcessLightInfo`、`ProcessDetailInfo`、`ProcessLight` 和 `ProcessDetail` 的全部字段。

`sockets.value.tcp` 和 `sockets.value.udp` 在 `count` / `light` / `details` 三个级别都会上报；`sockets.level=light` / `details` 只控制是否额外出现 `light` 或 `details` 字段。上方 JSONC 已展开 `SocketLightInfo`、`TcpStateCount`、`SocketDetailInfo` 和 `SocketDetail` 的全部字段。

字段省略规则：

- `core`、`disk`、`network`、`processes`、`sockets` 是可选采样组；对应配置为 `enabled=false` 时不会阻塞第一包上报，也不会出现在 JSON payload 中。
- `disk.value.disks` 在 `disk.include_per_device=false` 时为空数组，但磁盘汇总字段仍会上报。
- `network.value.networks` 在 `network.include_per_interface=false` 时为空数组，但网络汇总字段仍会上报。
- `processes.value.status` / `sockets.value.status` 表示采集状态；`stale` 会保留上一份可用值并附带 `error`，`failed` 表示没有可用旧值。
- `processes.value.light` / `processes.value.details` 和 `sockets.value.light` / `sockets.value.details` 只会在对应 `level` 下出现；默认 `level=count` 不携带明细，避免高频上报放大流量和 CPU 消耗。
- `processes.value.count`、`sockets.value.tcp`、`sockets.value.udp` 不受 `level` 影响，只要对应采样组启用并完成采样就会上报。
- `identity.public_ip` 是必填对象，但 `ip`、`source`、`sampled_at`、`verified_at`、`last_attempt_at`、`error` 会按状态和是否有值决定是否出现。

真实上报 loop 已接入导出发送；当前 `smalux_json` adapter 负责编码为 smalux `ClientFrame` JSON bytes，WebSocket transport 负责 binary wire 封包和可选 `secure_psk` 加密，`komari` adapter 负责编码为 Komari report / basic info / task result / ping result。`outbound.realtime_report` 控制 realtime delivery 开关和首次发送策略，发送跟随最新 report 触发；`outbound.basic_info` 控制 reporter 生成 Komari basic info 事件的开关、间隔和首次发送策略。remote task result、remote job result 和控制层 ack/error 属于即时事件，不受 interval delivery 控制。

## 当前状态

当前 agent 主线已经完整覆盖：采集、动态配置、reporter/export、`smalux_json` 自有协议、Komari 兼容、remote shell、remote task、`job_apply(kind=probe)` 和控制层 `ack/error`。更细的能力列表以文档开头 `当前已完成功能` 为准，完整交互 JSON 以 `数据格式` 和 `Komari 格式` 为准。

## 常用命令

```powershell
cargo check -p smalux-agent
cargo test -p smalux-agent
cargo test -p smalux-agent service::shell
cargo test -p smalux-agent service::tests::service_export_sends_komari
cargo run -p smalux-agent
```
