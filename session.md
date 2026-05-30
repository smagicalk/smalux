# Smalux Session Handoff

## 恢复目标

这个文件用于在其他电脑或新会话中恢复当前开发上下文。项目路径为 `F:/code/rust/smalux`，当前仓库是 Rust 2024 workspace。

## 项目概况

- `crates/smalux-agent`：探针端，已有系统身份、CPU、内存、磁盘、网络、进程/socket、公网 IP、动态配置、WebSocket/HTTP 导出、Komari 兼容、remote shell、remote task 和 remote probe。
- `crates/smalux-core`：共享模型和工具，包含 `model::info`、`flow` 单位换算、日志初始化和通用校验工具。
- `crates/smalux-server`：服务端 crate，依赖已准备 `axum`、`sea-orm`、`config`、`tracing` 等，但入口仍是 `Hello, world!`。
- `crates/smalux-protocol`：agent/server 共享通信协议 crate，当前包含最小 frame 和 JSON codec。
- 根目录 `src/main.rs` 当前不属于 workspace package 的有效入口，仍是占位代码。

## 本次会话已完成

- 查看并梳理了项目结构、README、workspace 成员和主要模块职责。
- 执行 `cargo fmt` 统一 Rust 格式。
- 修正命名和拼写问题：
  - `komari::Network.totalUp/totalDown` 改为 `total_up/total_down`。
  - 使用 `serde(rename = "totalUp")` 和 `serde(rename = "totalDown")` 保持 JSON 兼容。
  - `WebSocketMessage::SEND/CLOSE` 改为 `Send/Close`。
  - `new_with_defaut` 改为 `new_with_default`。
  - `netword_info` 改为 `network_info`。
- 清理了部分 unused imports、不必要的 `mut`，以及 `rustls` verifier 中接口要求但未使用的参数命名。
- 没有刻意用 `allow(dead_code)` 掩盖未接入主流程导致的 warning。
- 后续又重排了扩展目录：
  - `smalux-agent/src/info/*` 移动到 `smalux-agent/src/collect/*`。
  - `smalux-agent/src/send/*` 移动到 `smalux-agent/src/export/*`。
  - Komari 兼容模型最终放在 `smalux-agent/src/export/komari/*`，方便后续整体删除或替换第三方兼容层。
  - 新增预留模块：`agent/config.rs`、`agent/telemetry.rs`、`agent/service.rs`、`core/convert.rs`、`server/config.rs`、`server/http.rs`、`server/ingest.rs`、`server/query.rs`、`server/storage.rs`。
- 已迁移为 Rust 2018+ 模块文件布局，不再使用 `mod.rs`：
  - 例如 `agent/src/collect.rs` + `agent/src/collect/cpu.rs`。
  - 例如 `core/src/model.rs` + `core/src/model/info.rs` + `core/src/model/info/cpu.rs`。
  - 例如 `core/src/model.rs` + `core/src/model/info.rs` + `core/src/model/info/cpu.rs`。
- 日志初始化已迁移到 `crates/smalux-core/src/log.rs`，agent 和 server 共用：
  - `init_tracing(log_file)` 根据编译环境区分测试和正式环境。
  - 测试环境只输出控制台。
  - 正式环境输出控制台和日志文件。
  - 日志级别只读 Rust 标准环境变量 `RUST_LOG`。
  - `log_file`、`log_retention_files` 和 `log_max_size_mb` 是启动期参数，不接受 server `config_patch`。
  - `RUST_LOG` 不存在时，正式环境默认 `info`，测试环境默认 `debug`。
  - 使用 `tracing-appender` 做日滚动日志。
  - agent 默认日志文件前缀为 `logs/smalux-agent.log`，实际日滚动文件形如 `logs/smalux-agent.log.YYYY-MM-DD`。
  - server 默认日志文件前缀为 `logs/smalux-server.log`，实际日滚动文件形如 `logs/smalux-server.log.YYYY-MM-DD`。
  - 日志格式使用本地 RFC3339 时间，包含 level、target、file、line、thread 信息。
  - `.gitignore` 已忽略 `logs/`。
  - 日志初始化函数保持薄封装：`init_tracing` / `init_production_tracing` 只做组装调用，具体 `EnvFilter`、console layer、file layer、rolling appender 拆到 helper 函数里。
  - 同一文件内按环境分组：`#[cfg(not(test))]` 的正式环境实现集中放上面，公共 helper 放中间，`#[cfg(test)]` 的测试环境实现集中放下面。
- 已为全部仓库 Rust 源码补充中文注释：
  - 模块用 `//!` 说明职责和后续扩展边界。
  - 类型、字段、函数用 `///` 说明语义、单位和调用约束。
  - 关键实现细节用 `//` 解释原因，不做逐行机械注释。
- WebSocket 的不安全 TLS 校验路径已显式命名为 `UnsafeNoCertificateVerification`：
  - 仅在 `unsafe_cert = true` 时使用。
  - 启用时会输出 `warn` 日志。
  - 后续生产环境应优先加载自定义 CA，而不是跳过服务端证书校验。
  - verifier 的 `supported_verify_schemes()` 现在来自 rustls 当前 `ClientConfig` crypto provider，不再手写签名算法列表，避免和 provider 能力脱节。
  - `rustls.rs` 已补测试，验证签名算法列表来源和 Debug 输出。
  - workspace 级 feature 合并可能同时启用多个 rustls crypto provider，当前 unsafe TLS 路径已改为显式使用 `aws_lc_rs` provider 构造 `ClientConfig` builder，避免 `ClientConfig::builder()` 自动判定 provider 时 panic。
- WebSocket 客户端关闭和日志已优化：
  - 读流返回 `None`、命令通道返回 `None`、读写错误都会明确退出后台收发任务。
  - 主动关闭会发送 close frame，并等待对端 close 确认，超时后退出。
  - 主体代码不再用 `println!/eprintln!`，统一使用结构化 `tracing` 日志。
  - 日志消息使用英文；代码注释继续使用中文。
  - `Debug` 输出只展示 `token_set`，不输出 token 原文。
  - `close().await` 会等待后台收发任务退出；`Drop` 只做兜底关闭，有 Tokio runtime 时尝试发送 close，没有 runtime 时 abort 后台任务。
  - listener 存储为 `Arc<dyn ExportMessageListener>`，收到消息时只在读锁内 clone 当前 listener 快照，业务 `on_message().await` 不持有锁。
  - listener 替换语义：已经开始处理的消息允许继续使用旧 listener，后续消息使用新 listener。
  - listener 处理已从 WebSocket 协议读循环中拆出：每个连接额外启动 1 个 listener worker，通过有界队列顺序处理消息，避免慢 `on_message()` 阻塞 ping/pong/close。
  - listener 队列满时主动关闭 WebSocket，避免无界堆积。
  - 重复调用 `connect()` 直接返回错误，不自动覆盖旧连接。
  - `heartbeat = 0` 表示真正禁用心跳，不再创建 ping interval。
  - close 等待后台任务超时后会显式 abort，避免任务残留。
  - close 命令发送失败时只记录日志，仍继续等待/清理后台任务，避免 client 卡在半关闭状态。
  - WebSocket 认证配置已拆成 `WebSocketConfig` + `WebSocketAuth`：
    - `WebSocketAuth::None`：无认证，适合本地测试或明确无认证的第三方服务。
    - `WebSocketAuth::QueryToken { param, token }`：把 token 作为 query 参数追加，主要用于兼容。
    - `WebSocketAuth::BearerToken { token }`：通过 `Authorization: Bearer ...` 发送，是 smalux 自己 server 的推荐默认方式。
  - WebSocket 支持多个额外 query 参数，连接时会合并原 URL query、配置 query 和 QueryToken。
  - URL 日志与 `Debug` 已做敏感 query 脱敏，覆盖 `token`、`access_token`、`api_key`、`key`、`secret`、`signature`、`authorization`。
  - 重复敏感 query 参数会返回错误，避免服务端解析凭证时出现歧义；重复检查会按 form-url-encoded 规则解码 key，避免 `%xx` 编码绕过。
  - 旧构造函数仍保留兼容：传入非空 `token` 时会映射为 `QueryToken { param: "token", token }`。
- WebSocket 测试已改成本地 mock server：
  - 不再依赖 `wss://echo.websocket.org` 或外部 TLS 状态。
  - 覆盖未连接发送报错、Debug 不泄漏 token、本地连接/发送/接收/关闭、重复连接报错、listener 替换后后续消息进入新 listener、`heartbeat = 0` 禁用 ping、服务端主动 close 后客户端 close 可清理。
  - 覆盖多个 query 合并、URL 无 path 时补 `/`、QueryToken 握手 URI、BearerToken 握手 header、敏感 query 脱敏、重复敏感 query 参数报错及编码绕过。
  - mock server 收到 close frame 后直接退出，避免在 tungstenite closing 状态下二次发送 close。
- `smalux-agent` 的公网 IP 观察型测试已加 `#[ignore = "requires external network access"]`：
  - 默认 `cargo test -p smalux-agent` 不再访问外网。
  - 需要手工联调公网 IP 时可运行 `cargo test -p smalux-agent -- --ignored`，或指定单个 ignored 测试名。
- 最新一次拆分已把原 `io` 采集组拆成独立的 `disk` 和 `network`：
  - 配置从 `io.*` 改为 `disk.*` 和 `network.*`，CLI 短参数分别是 `-d` 和 `-n`。
  - `LocalCollector` 拆成 `sample_disk()` 和 `sample_network()`，各自维护采样间隔和 `warmed_up`。
  - 身份采样使用独立的 `identity_networks`，不会干扰网络速度 delta。
  - 当时 `AgentReport` schema 版本升到 `2`，上报字段改为可选的 `core`、`disk`、`network`。
  - `TelemetryState` 支持禁用采样组，禁用组不会阻塞第一包上报，也不会出现在 JSON payload 中。
  - README 已同步新的参数、调用流程和数据格式。
- 网络采样已支持网卡白名单：
  - `network.include_interfaces = []` 表示统计全部网卡。
  - CLI 使用 `--network-interface NAME` / `-I NAME`，可重复传入。
  - 配置白名单后，外层网络汇总和 `network.networks[]` 明细都只包含指定网卡。
  - 该筛选只影响 `sample_network()`，不影响 `sample_identity()` 的公网 IP / 本地 IP 识别。
- 网络采样已支持网卡黑名单和规范化：
  - `network.exclude_interfaces = []` 表示不排除网卡。
  - CLI 使用 `--network-exclude-interface NAME`，可重复传入。
  - `include_interfaces` 非空时优先使用 include，`exclude_interfaces` 不参与过滤；include 为空时 exclude 生效。
  - server patch 应用时会对 include/exclude 做 trim、过滤空字符串并按首次出现顺序去重；下发空列表可以清空对应筛选。
  - 配置了不存在的 include 网卡时会输出英文 warn 日志，方便发现拼写或平台名称问题。
- 公网 IP 第一包门禁已优化：
  - 公网 IP 默认可选：`public_ip.required_for_first_report=false`。
  - `AgentReport` schema 版本升到 `3`。
  - `identity.public_ip` 改为带状态对象，状态包括 `ready`、`failed`、`stale`、`disabled`、`pending`。
  - 启动阶段公网 IP 获取失败时仍会上报 identity，失败原因写入 `identity.public_ip.error`。
  - 低频刷新失败且已有旧 IP 时会上报 `stale`，保留旧 IP 并记录本次失败原因。
  - 公网 IP 默认低频刷新间隔改为 `24h`，启动时仍会立即采集一次。
  - `retry_identity_until_ready()` 现在复用统一的 `public_ip_required_for_first_report()` 判断。
  - 等待期间如果 server patch 把 `public_ip.enabled` 或 `public_ip.required_for_first_report` 改成 `false`，重试循环会停止阻塞启动流程。
  - 重试定时器第一次 tick 延迟到 `public_ip.retry_interval` 之后，不再刚进入循环就立即重试一次。
- agent 持续采集和真实上报闭环已接入：
  - `service::run()` 现在启动 `export_supervisor()`、`collector_loop()`、`public_ip_refresh_loop()` 和 `reporter_loop()`。
  - `collector_loop()` 使用共享 `TelemetryState` 按 core/disk/network 独立频率刷新指标。
  - `public_ip_refresh_loop()` 按 `public_ip.refresh_interval` 低频刷新身份信息。
  - `reporter_loop()` 按 `report.interval` 构建 `AgentReport`，并包装为 `OutboundEvent::Report` 写入有界 outbound event queue。
  - `export_supervisor()` 独占 `TransportHub`，把当前最新 `OutboundReport` 交给 `ExportRouter`，由 router 调用当前 `ExportAdapter` 编码为 `TransportRequest` 后投递到 transport queue；发送失败时关闭当前 hub 并按 `export.reconnect_interval` 重连。
  - outbound event queue 容量为 `256`；队列满时 reporter 等待 export 消费，避免无界堆积。
  - export 配置重连后会重置已发送 report 序号，确保新端点能拿到当前最新 report。
  - 公网 IP 低频刷新失败不会让身份信息失败；有旧公网 IP 时写入 `stale`，没有旧公网 IP 时写入 `failed`。
- agent telemetry 第一阶段模块化已完成：
  - 删除旧 `crates/smalux-agent/src/pipeline.rs`。
  - 新增 `crates/smalux-agent/src/telemetry.rs` 和 `src/telemetry/`。
  - `SnapshotStore` 已迁移并重命名为 `TelemetryState`。
  - 原 reporter 内部 `ReportPolicy` 已迁移为 `TelemetryAggregator`，负责 snapshot/delta/heartbeat 决策。
  - telemetry 到 export 的内部事件已统一为 `OutboundEvent`；reporter 和 remote task 共用 bounded `mpsc` outbound event queue。
- agent export router 已接入：
  - 新增 `crates/smalux-agent/src/export/router.rs`。
  - `service/export.rs` 不再直接调用 `adapter.encode_report()` 和逐条发送 transport request，改为通过 `ExportRouter::send_report()`。
  - `ExportRouter` 会把 adapter 生成的 `TransportRequest` 投递给 `TransportHub` 的 per-transport queue，再 flush 对应 transport。
- agent transport queue 已接入：
  - `TransportHub` 为每个 transport 启动 `export::worker` transport task，worker 命令队列默认容量 `128`。
  - 投递队列满时返回错误，不做无界堆积；错误继续交给当前 job failure policy 处理。
  - 真实发送成功或失败通过 `TransportEvent::Sent` / `TransportEvent::Failed` 回到 `export_supervisor`。
  - `last_sent_sequence` 只在收到 `Sent` 后更新；`Failed` 按 job failure policy 决定重连或只记录日志。
- 通信协议 crate 已按最小文件数落地：
  - 原模板 `smalux-proto` 已替换为 `smalux-protocol`。
  - `smalux-protocol` 当前包含 `OutboundReport`、`ClientFrame`、`ServerFrame`、`snapshot`、`delta`、`heartbeat`、`ack/error`、`remote_task_result`、`remote_probe_result`、`remote_probe_run` 和 JSON codec。
  - agent 当前已将周期上报包装成 `OutboundReport::Snapshot`，再由 export 层编码成 `ClientFrame::Snapshot` 通过 WebSocket 发送。
  - WebSocket/gRPC/HTTP transport 不放进 `smalux-protocol`，后续继续留在 agent/server 对应模块。
- agent 导出层已改成 adapter + transport hub 管道：
  - `ExportAdapter` 负责声明 transport + job plan，并按 job 把内部 `OutboundReport` 编码成零到多条 `TransportRequest`；`ExportRouter` 负责统一调用 adapter 并投递请求。
  - `TransportHub` 按 `TransportPlan` 管理 `RealtimeReport` 和 `BasicInfo` transport，并为每个 transport 维护独立 worker；`service/export.rs` 按 plan 中的 job 触发实时上报或固定间隔上报，并在 `TransportEvent` 回来后更新发送状态。
  - `TransportId::Primary` / `primary` 已重命名为 `TransportId::RealtimeReport` / `realtime_report`，避免 transport / job 命名不清晰。
  - 已新增 `jobs` 动态配置：
    - `jobs.realtime_report.enabled` / `interval` / `run_on_start`，默认 `true` / `5s` / `true`。
    - `jobs.basic_info.enabled` / `interval` / `run_on_start`，默认 `true` / `5m` / `true`。
    - `--report-interval` / `-R` 仍保留，并兼容同步覆盖 `jobs.realtime_report.interval`；也可以用 `--realtime-report-interval` 单独设置导出 job。
    - 新增 CLI：`--realtime-report-enabled`、`--realtime-report-interval`、`--realtime-report-run-on-start`、`--basic-info-enabled`、`--basic-info-interval`、`--basic-info-run-on-start`。
    - server `config_patch` 支持局部下发 `jobs.realtime_report` 和 `jobs.basic_info`；相同配置会被 `ConfigManager` 忽略，不通知运行任务重建。
  - `export_supervisor()` 会对 adapter 的 `TransportPlan` 应用 `jobs` 配置；job-only 配置变化只重建运行时 job，不重连 transport；export 配置变化才重建 transport。
  - interval job 现在按 `failure_policy` 处理错误：`realtime_report` 失败会重连 pipeline，`basic_info` 失败只记录日志并等待下一次调度。
  - 当前默认仍是 `smalux_json` + WebSocket，行为保持兼容。
  - 已新增 HTTP JSON POST transport，支持请求超时和 `unsafe_cert`。
  - 已新增 `komari` 导出格式：
    - 标准 agent 模式：实时 report 走 WebSocket `/api/clients/report`，basic info 走 HTTP `/api/clients/uploadBasicInfo`。
    - 显式传入 `https://host/api/clients/report` 时也会转换为 WebSocket report，不再隐式切到 HTTP POST report。
    - `komari` 只消费 snapshot；delta 和业务级 heartbeat 会在配置校验阶段被拒绝。
    - `auth_mode=bearer` 被拒绝；token 必须通过 query 提供，可以来自 URL、`export.query` 或 `auth_mode=query + export.token`。
    - Komari report 要求 `report.interval <= 10s` 且 `jobs.realtime_report.interval <= 10s`；basic info 是独立 interval job，默认 5 分钟。
    - 重复敏感 query 参数会被拒绝，避免 token 解析歧义。
  - `smalux_json` 和 `komari` 的 server 消息现在先由 listener 转换为 `service::InboundCommand`，再投递到有界入站命令队列。
  - `ControlDispatcher` 统一执行 `config_patch`、一次性进程/socket 采集、远程 shell 打开和 remote task 运行；协议 listener 不再直接调用 `ConfigManager`、`CollectorCommandSender`、`RemoteShellManager` 或 `RemoteTaskManager`。
  - `komari` server 文本消息当前由 `KomariMessageListener` 处理：terminal 消息转为 `InboundCommand::RemoteShellOpen`，其它消息安全忽略，避免误解析成 smalux `config_patch`。
  - README 已补充当前导出组合、job 配置、Komari 约束、启动参数示例和 Komari report/basic info JSON。
- Komari 兼容层已拆成 agent 内部独立模块，未新开 crate：
  - `crates/smalux-agent/src/export/komari.rs`：Komari adapter 入口。
  - `crates/smalux-agent/src/export/komari/model.rs`：Komari 兼容请求模型和 AgentReport 映射。
  - `crates/smalux-agent/src/export/komari/url.rs`：report/basic info URL 和 query token 规则。
  - `crates/smalux-agent/src/export/komari/message.rs`：Komari server message listener。
  - `crates/smalux-agent/src/export/komari/terminal.rs`：Komari terminal 消息解析。
- 已新增 Komari 本地 mock 验证：
  - `service_export_sends_komari_websocket_report_and_basic_info` 使用一个本地 TCP listener 同时 mock WebSocket report 和 HTTP basic info，验证 report 没有 smalux `type` 外壳，并验证 basic info URL / JSON。
  - mock 没有访问外网，也没有引入额外 HTTP server 依赖。
- remote shell 已接入 PTY 运行链路，remote task 已接入非交互执行和主出站回传：
  - `crates/smalux-agent/src/service/shell.rs` 现在是 shell 模块入口，子模块包括 `options.rs`、`message.rs`、`manager.rs`。
  - `RemoteShellOptions` 只保留 CLI-only `enabled` 开关；`max_sessions`、`idle_timeout`、`session_timeout`、`program` 已迁入动态 `AgentConfig.remote_shell`。
  - `crates/smalux-agent/src/service/task.rs` 定义 `RemoteTaskOptions`、`RemoteTaskRunRequest` 和 `RemoteTaskManager`；CLI-only `enabled` 只在启动时开启，`max_concurrent`、`timeout`、`max_stdout_bytes`、`max_stderr_bytes` 放在动态 `AgentConfig.remote_task`。
  - `CliArgs::into_startup()` 同时生成动态 `AgentConfig` 和 CLI-only `ServiceOptions`；`main.rs` 已把 `ServiceOptions` 传入 `service::run()`。
  - 远程能力只允许启动参数开启，server `config_patch` 不能开启或关闭能力，避免运行中扩大远程执行权限；运行限制可由 CLI 设置初始值，也可由 server patch 调整。
  - `ServiceControlListener` 支持 `remote_shell_open` 控制消息；启用后会调用 `RemoteShellManager::open()`。
  - `ServiceControlListener` 支持 `remote_task_run` 控制消息；启用后会调用 `RemoteTaskManager::start()`，直接执行 `program + args`，不会自动拼接 shell 字符串。
  - 每个 shell 会话会打开独立临时 WebSocket stream；server 发 `input` / `resize` / `close`，agent 回 `opened` / `output` / `exit` / `error`。
  - shell runner 使用 `portable-pty`，PTY 输出是合并后的原始字节流，agent 用 base64 编码 `output` 事件；输入统一使用 `input` 消息，不保留旧 pipe 协议。
  - `resize` 已调用 `MasterPty::resize()`；阻塞读写和 child wait 都放到独立 std 线程，避免阻塞 Tokio runtime。
  - Windows PowerShell 可能先输出 `ESC[6n` 查询光标位置，真实终端前端需要把终端模拟器响应通过 `input` 回写。
  - stream WebSocket 复用当前 `export` 的认证、额外 query、`unsafe_cert` 和 heartbeat 设置；新打开的 shell 会话使用当时的 `AgentConfig.remote_shell` 快照。
  - Komari WebSocket 模式收到 `{ "message": "terminal", "request_id": "..." }` 后，会推导 `/api/clients/terminal?id=...` stream，并复用同一个 `RemoteShellManager`。
  - remote task 结果包装为 `OutboundEvent::RemoteTaskResult`，经 `ExportRouter::send_remote_task_result()` 回传；`smalux_json` 编码为 `smalux_protocol::ClientFrame(type=remote_task_result)`，Komari 编码为 HTTP `POST /api/clients/task/result?token=...`。
  - README 已补充远程 shell 数据格式、调用流程和当前限制。
- Smalux 自有 WebSocket binary wire 和 `secure_psk` 已接入 agent：
  - `smalux_json` adapter 现在只负责编码 `smalux_protocol::ClientFrame` JSON bytes，WebSocket transport 负责封成 binary wire packet。
  - `binary_plain` 使用 `WirePacket(kind=PlainData)` 承载明文 JSON bytes，适合开发和内网联调。
  - 新增 `crates/smalux-agent/src/export/security.rs`，集中处理 `smx1.<key_id>.<secret_base64url>` token 解析、HKDF-SHA256 PSK 派生、Noise 握手和 payload 加解密。
  - `secure_psk` 使用 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`；token secret 不发送，只有 `key_id` 放在 Hello payload 里给 server 查 secret。
  - `secure_psk` 下 `export.auth_mode` 必须为 `none`，避免把完整 token 放到 URL query 或 Authorization header。
  - WebSocket secure 握手发生在 HTTP Upgrade 后、后台收发任务启动前：agent 发送 Hello 和 Noise initiator handshake，server 响应 Noise responder handshake，成功后进入 transport mode。
  - 后续业务上报和 server 控制消息使用 `SecureData`，payload 是 Noise ciphertext；`binary_plain` 使用 `PlainData`。
  - `secure_psk` 模式会拒绝明文 WebSocket text 控制消息；`binary_plain` 仍保留 text 兼容用于本地调试和旧实现。
  - Komari 兼容层不使用 Smalux wire 安全模式，即使配置里残留 `wire_mode=secure_psk`，Komari WebSocket 仍保持第三方 text/query token 行为。
  - remote shell stream 在 `export.format=smalux_json` 时也走 Smalux binary wire；`secure_psk` 下会先握手再传输 PTY 事件。`export.format=komari` 时继续使用 WebSocket text JSON。
  - README 已补充 `export.wire_mode`、`export.secure_required`、token 格式、wire packet header、secure 握手流程、server 后续实现约定和 shell stream 编码规则。
- 进程和 TCP/UDP socket 三层采集已接入 agent：
  - `AgentReport` schema 版本先升到 `4`，本轮为一次性诊断控制和权限语义升到 `5`。
  - `crates/smalux-core/src/model/info/process.rs` 新增 `ProcessInfo`，字段包括 `count`、`status`、`level`，以及可选 `light` / `details`。
  - `crates/smalux-core/src/model/info/socket.rs` 新增 `SocketInfo`，字段包括 `tcp`、`udp`、`status`、`source`、`accuracy`、`level`，以及可选 `light` / `details`。
  - `crates/smalux-agent/src/collect/process.rs` 使用 `sysinfo` 刷新进程列表；`level=count` 只统计总数，`level=light` 返回按内存排序的 top 进程，`level=details` 返回受 `limit` 限制的完整进程明细。
  - `crates/smalux-agent/src/collect/socket.rs` 支持三层：`count` 默认优先使用 Linux/Android `/proc/net/sockstat*` 快速计数，失败或其他平台回退 socket table；`light` 返回 TCP 状态聚合；`details` 返回受 `limit` 限制的 socket 明细。
  - 新增动态配置 `processes.enabled` / `processes.interval` / `processes.level` / `processes.limit` 和 `sockets.enabled` / `sockets.interval` / `sockets.level` / `sockets.limit`。
  - 默认均启用，采样间隔默认 `60s`，默认 `level=count`；`processes.limit=50`，`sockets.limit=200`。
  - 新增 CLI：`--processes-enabled`、`--processes-interval`、`--processes-level`、`--processes-limit`、`--sockets-enabled`、`--sockets-interval`、`--sockets-level`、`--sockets-limit`。
  - `TelemetryState`、`collector_loop()`、`bootstrap_once()`、`reporter_loop()` 已接入 `processes` / `sockets`，禁用时不会阻塞第一包上报。
  - `smalux-protocol::DeltaReport` 已补 `processes` / `sockets` 字段，启用 delta 时不会丢新指标变化。
  - Komari report 已从 `AgentReport.processes` / `AgentReport.sockets` 映射到 `process` 和 `connections.tcp/udp`，不再固定为 0。
  - 已补测试：禁用采样组时 `processes` / `sockets` 不出现在 snapshot；进程和 socket 变化会进入 delta；进程和 socket 的 `count/light/details` 路径有采集测试；运行级 delta/heartbeat 测试改为等待目标 frame，避免时序偶然性。
  - README 已同步三层配置、CLI、server patch 和 JSON 字段说明。
- 本轮新增进程/socket 诊断保护和一次性采集控制：
  - `AgentReport` schema 版本升到 `5`。
  - `processes.limit` 增加上限 `500`，`sockets.limit` 增加上限 `2000`。
  - `processes` / `sockets` 的 `level=light` 定时间隔至少 `1s`，`level=details` 定时间隔至少 `10s`，`count` 仍只受全局 `100ms` 最小间隔保护。
  - 新增 CLI-only 静态权限：`--allow-process-details true|false` 和 `--allow-socket-details true|false`，默认 `false`。
  - server patch 如果要打开 `processes.level=details` 或 `sockets.level=details`，必须有对应启动授权；server 不能通过 patch 动态打开授权本身。
  - 新增控制消息 `collect_processes_once` / `collect_sockets_once`，可选 `level` 和 `limit`；请求进入有界 collector command channel，由唯一 `collector_loop()` 执行并写入 `TelemetryState`。
  - 一次性采集结果不会立刻在控制通道回包，而是进入下一次 `snapshot` 或 `delta` 上报。
  - 新增测试覆盖：配置 limit/interval 保护、details 权限拒绝、一次性采集命令投递、collector command 写入缓存。
- 本轮新增 server 按需完整快照和控制层确认：
  - `smalux-protocol` 已新增 server `snapshot_request` frame，agent 收到后通过 reporter 生成完整 `snapshot`，不会绕过 `TelemetryAggregator`，因此后续 delta 基准会同步更新。
  - `ClientPayload` 已新增控制层 `error`；`ack` / `error` 都会编码为标准 `ClientFrame`，并通过 `smalux_json` WebSocket binary wire 回传。
  - `ProtocolError` 带 `sequence: Option<u64>`，用于把失败关联回 server 下发的命令序号。
  - `ReportConfig` / `ReportConfigPatch` 新增 `force_snapshot_min_interval`，默认 `10s`，CLI 参数为 `--report-force-snapshot-min-interval`；server patch 可动态调整。
  - `reporter_loop()` 新增 `ReporterCommand::ForceSnapshot` 命令队列；如果 server 重复请求过快，会合并为下一次允许的完整 snapshot，避免刷爆流量。
  - `ServiceControlListener` 会优先解析 `smalux_protocol::ServerFrame`，支持带 `sequence` 的 `snapshot_request`；legacy raw text 控制消息仍兼容，但不回 `ack/error`。
  - `ControlDispatcher` 统一处理带响应元信息的 `InboundCommandEnvelope`：成功调度回 `ControlAck`，被拒绝或执行失败回 `ControlError`。
  - `export_supervisor()` 已接入 `OutboundEvent::ControlAck` / `ControlError`，并像 remote task result 一样在 `Sent` 前保留 pending 副本，重连后会重投。
  - Komari 兼容层不会发送 Smalux 控制层 `ack/error`；不支持的控制响应由 adapter 返回空请求并跳过。
  - README 已补充 `snapshot_request`、`ack/error`、`report.force_snapshot_min_interval`、调用流程、配置示例和 JSON 数据格式。
- 本轮新增 Komari exec 兼容：
  - `KomariMessageListener` 现在识别 `{ "message": "exec", "task_id": "...", "command": "..." }`。
  - exec 不新写执行器，转换为内部 `InboundCommand::RemoteTaskRun`，继续复用 `RemoteTaskManager` 的 CLI-only 开关、并发、超时和输出大小限制。
  - Komari 的 `command` 字符串会按平台转换为 shell 执行：Windows 使用 `powershell.exe -NoProfile -Command <command>`，其它平台使用 `/bin/sh -c <command>`。
  - `KomariAdapter` 已实现 `encode_remote_task_result()`，把 `RemoteTaskResult` 编码为 HTTP `POST /api/clients/task/result?token=...`。
  - Komari task result 请求体包含 `task_id`、`result`、`exit_code`、`finished_at`；`result` 合并 stdout/stderr/error，`finished_at` 使用 RFC3339 秒级时间。
  - 已补测试：exec 消息解析、exec listener 入队、task result URL 推导、remote task result 到 Komari HTTP 请求编码、export supervisor 运行级 Komari task result 发送。

## 验证结果

已运行并通过：

```powershell
cargo fmt
cargo check
cargo test --no-run
cargo test -p smalux-core
cargo check -p smalux-agent
cargo test -p smalux-agent export::rustls::tests
cargo test -p smalux-agent export::ws::tests
cargo test -p smalux-agent export::komari
cargo test -p smalux-agent service::tests::service_export_sends_komari
cargo test -p smalux-agent config::cli
cargo test -p smalux-agent service::shell
cargo test -p smalux-agent service::task
cargo test -p smalux-agent
cargo check -p smalux-core
cargo check -p smalux-server
cargo test -p smalux-core
cargo check --workspace --all-targets
cargo test --workspace
cargo test -p smalux-agent service_export_sends_komari_websocket_report_and_basic_info
cargo test -p smalux-agent export::komari::tests::komari_plan_declares_report_and_basic_info_jobs
cargo test -p smalux-agent service::tests::service_export_sends_delta_and_business_heartbeat
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo test -p smalux-agent export::komari
cargo test -p smalux-agent service::tests::service_export_sends_komari_remote_task_result
cargo test -p smalux-agent service::tests::service_export_sends_delta_and_business_heartbeat
cargo test --workspace
```

`cargo test -p smalux-core` 最新结果：10 个测试通过。

`cargo test -p smalux-agent export::ws::tests` 历史结果：15 个 WebSocket 本地测试通过；当前已新增 binary wire 和 secure_psk 本地测试，完整 agent 测试已覆盖。

`cargo test -p smalux-agent export::rustls::tests` 结果：2 个 rustls verifier 测试通过。

`cargo test -p smalux-agent export::komari` 最新结果：29 个 Komari adapter/message/model/url 测试通过。

`cargo test -p smalux-agent service::tests::service_export_sends_komari` 结果：2 个 Komari 本地 mock 测试通过。

`cargo test -p smalux-agent config::cli` 结果：6 个 CLI 测试通过。

`cargo test -p smalux-agent service::shell` 结果：15 个 remote shell 选项、消息、manager 和本地 PTY stream 端到端测试通过。

`cargo test -p smalux-agent service::task` 结果：remote task 静态选项、禁用拒绝、启用执行和结果回传相关测试通过。

`cargo test -p smalux-agent` 最新结果：248 个测试通过，4 个公网 IP 测试被忽略。

`cargo check --workspace --all-targets` 最新结果：通过，无 warning。

`cargo test --workspace` 最新结果：agent 248 个测试通过、4 个公网 IP 测试被忽略；core 10 个测试通过；protocol 11 个测试通过；server 0 个测试。

## 当前仍存在的 warning

历史上 `cargo check` 出现过少量 `dead_code` warning，主要原因是：

- `smalux-agent` 仍保留部分测试/后续扩展辅助函数，例如 `sample_all()` 和部分公网 IP 手工联调函数。
- Komari 兼容模型已移到 `crates/smalux-agent/src/export/komari/model.rs`，便于后续整体删除或替换第三方兼容层。
- `smalux-server` 仍处于骨架阶段；`smalux-protocol` 已替代原模板 `smalux-proto`。

当前最新 `cargo check --workspace --all-targets` 已无 warning。

## 当前工作区状态

开始本次会话前仓库已有未提交改动和新增文件。本次会话没有回滚任何既有改动。

最后一次观察到的关键新增/迁移区域包括：

- `crates/smalux-agent/src/collect.rs` 和 `crates/smalux-agent/src/collect/`
- `crates/smalux-agent/src/config.rs` 和 `crates/smalux-agent/src/config/`
- `crates/smalux-agent/src/export.rs` 和 `crates/smalux-agent/src/export/`
- `crates/smalux-agent/src/telemetry.rs` 和 `crates/smalux-agent/src/telemetry/`
- `crates/smalux-agent/src/service.rs` 和 `crates/smalux-agent/src/service/`
- `crates/smalux-protocol/`
- `crates/smalux-agent/README.md`
- `session.md`

最后一次观察到的关键已修改区域包括：

- `Cargo.lock`
- `Cargo.toml`
- `crates/smalux-agent/Cargo.toml`
- `crates/smalux-core/Cargo.toml`
- `crates/smalux-core/src/model/info/*`
- `crates/smalux-server/Cargo.toml`
- `crates/smalux-server/src/main.rs`

继续开发前建议先运行：

```powershell
git status --short
cargo check
```

## 编码与协作注意事项

- 本项目含中文注释，读写含中文文件前需要检查 BOM。
- 当前检查过的源码文件均为 UTF-8 无 BOM。
- 修改含中文文件优先使用局部 patch，避免整文件重写。
- 后续回复与协作默认使用简体中文。
- 不要回滚用户已有未提交改动，除非用户明确要求。

## 建议下一步

1. 接 `smalux-server` 的 WebSocket ingest，接收并记录 agent 上报的 `ClientFrame::Snapshot`。
2. 明确 server 侧存储策略：先内存缓存，还是直接接数据库。
3. 再考虑进程明细、连接明细、温度、GPU、电池等新采集功能。
4. 新功能先放入现有 crate 的独立 module；只有需要独立复用、独立发布或依赖边界明显不同时，再拆成新 crate。

## 建议使用的技能

- `diagnose`：用于后续 bug 调试或测试失败定位。
- `tdd`：用于新增 agent 主流程、采集聚合和 server 接收端时做红绿重构。
- `improve-codebase-architecture`：用于确定 agent/core/server 边界和模型转换层。

## 最新 Komari 协议对照

- 已按官方文档 `https://www.komari.wiki/dev/agent.html` 和 Komari 服务端模型对照：
  - `network.up` / `network.down` 改为整数 byte/s，避免 JSON 小数被 Go `int64` 字段拒绝。
  - Komari basic info 默认刷新间隔从 10 分钟改为 5 分钟。
  - `https://host` 这类官方基础 endpoint 会自动派生为 `wss://host/api/clients/report`。
  - 显式传入 `https://host/api/clients/report` 时也会转换为 WebSocket report。
  - Komari report 和 basic info 已拆成两个 export job：`realtime_report` 按 `jobs.realtime_report.interval` 通过 WebSocket 发送，`basic_info` 按 `jobs.basic_info.interval` 通过 HTTP POST 发送。
  - Komari 默认会在第一份 report ready 后先发送 basic info，再 lazy connect WebSocket 发送 realtime report。
- 当前 Komari 已接入 server 下发的 `ping` 事件：
  - `terminal` 已接入 remote shell。
  - `exec` 已接入 remote task，并通过 HTTP `task/result` 回传结果。
  - `ping` 已接入 remote probe，并通过 WebSocket `ping_result` 回传结果。
- 最近验证：
  - `cargo check -p smalux-agent --all-targets` 通过。
  - `cargo check --workspace --all-targets` 通过。
  - `cargo test --workspace` 通过：agent 248 passed，4 ignored；core 10 passed；protocol 11 passed；server 0 tests。

## 最新远程能力配置边界

- CLI-only，只能启动时设置：
  - `remote_shell.enabled`：是否允许交互式远程 shell。
  - `remote_task.enabled`：是否允许后续非交互远程任务。
  - `diagnostics.allow_process_details` / `diagnostics.allow_socket_details`：是否允许 server 触发高成本 details 诊断。
- 动态配置，可由 CLI 给初始值，也可由 server `config_patch` 修改：
  - `remote_shell.max_sessions`，默认 `1`。
  - `remote_shell.idle_timeout`，默认 `10m`。
  - `remote_shell.session_timeout`，默认 `1h`。
  - `remote_shell.program`，默认 `null`，按平台使用默认 shell；patch 缺省表示不修改，`null` 表示清空为默认 shell，字符串表示覆盖。
  - `remote_task.max_concurrent`，默认 `1`。
  - `remote_task.timeout`，默认 `30s`。
  - `remote_task.max_stdout_bytes`，默认 `65536`。
  - `remote_task.max_stderr_bytes`，默认 `65536`。
- 动态配置，可由 CLI 给初始值，也可由 server `config_patch` 修改并开启：
  - `remote_probe.enabled`，默认 `false`。
  - `remote_probe.timeout`，默认 `3s`。
  - `remote_probe.global_min_interval`，默认 `500ms`。
  - `remote_probe.target_min_interval`，默认 `10s`。
- 进程和连接采集在 `level=count/light/details` 下都会保留总数字段：
  - `processes.value.count` 始终上报，除非 `processes.enabled=false`。
  - `sockets.value.tcp` / `sockets.value.udp` 始终上报，除非 `sockets.enabled=false`。

## 最新 remote probe / Komari ping 接入

- `AgentConfig.remote_probe` 已新增动态配置：
  - `enabled=false`。
  - `timeout=3s`，校验范围 `100ms..=10s`。
  - `global_min_interval=500ms`，校验范围 `200ms..=1h`。
  - `target_min_interval=10s`，校验范围 `1s..=24h`。
- `RemoteProbeManager` 已接入 `ControlDispatcher`：
  - 未启用、全局限频或同目标限频时不发包，立即回传 `value=-1`。
  - TCP 探测使用 `TcpStream::connect`。
  - HTTP 探测按 Komari 约定发送 `GET`，不读取响应 body。
  - ICMP 当前未实现，直接回传 `value=-1`。
- `smalux-protocol` 已支持：
  - server `remote_probe_run`。
  - client `remote_probe_result`。
- Komari 兼容层已支持：
  - 入站 `{ "message": "ping", "ping_task_id": ..., "ping_type": "tcp|http|icmp", "ping_target": "..." }`。
  - 出站 WebSocket `{ "type": "ping_result", "task_id": ..., "ping_type": "...", "value": ..., "finished_at": "..." }`。
- README 已同步 remote probe 配置、CLI 参数、server patch、Smalux JSON 和 Komari ping 数据格式。

## 最新 agent 收尾审计

- 已把 CLI 解析结果命名从 `AgentRuntime` / `into_runtime()` 改为 `AgentStartup` / `into_startup()`，避免和 Tokio runtime 或长期运行态混淆。
- 已复查旧命名和旧测试数量：README / session 中不再保留 `into_runtime`、`agent 230 passed`、`protocol 9 passed` 这类过期描述。
- 已补充 `crates/smalux-agent/README.md` 的 server 自实现对接流程，覆盖 WebSocket 接入、`binary_plain` / `secure_psk` 解包、`ClientFrame` 处理、delta 基准校验、`ServerFrame` 下发和动态 patch 边界。
- 已补充 `crates/smalux-agent/README.md` 的 server 最小实现 checklist，列出第一版 server 必做项和可后做项，方便后续按清单实现 server。
- 已增强 remote probe 日志字段：accepted / rejected / finished 日志包含 `task_id`、`probe_type`、`target`，完成日志额外包含 `value`、`duration_ms` 和 `error`。
- 已补充主代码注释覆盖，重点是 `service.rs`、`service/inbound.rs`、`service/export.rs`、`export.rs`、`export/router.rs`、`export/wire.rs`、`export/security.rs`、`export/ws/client.rs`、`service/probe.rs`、`service/task.rs` 和 server 日志常量。
- 已完成注释缺口收尾：`collect/socket.rs` 的 cfg 分支采样函数、`export/wire.rs` 和 `export/ws/config.rs` 的转换错误类型、`smalux-core/src/flow.rs` 的 `Display::fmt` 都已补充相邻中文注释。
- 已验证注释扫描结果为 `MISSING_COUNT=0`，并通过 `cargo fmt --all --check`、`cargo check --workspace --all-targets`、core/protocol/agent/server 的 `cargo rustdoc ... -D missing_docs`、`cargo test --workspace`。
