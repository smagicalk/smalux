# Smalux Session Handoff

## 恢复目标

这个文件用于在其他电脑或新会话中快速恢复当前开发上下文。项目路径为 `F:/code/rust/smalux`，当前仓库是 Rust 2024 workspace，主开发分支是 `dev`。

## 快速恢复

- 功能基线提交：`35106f5 refactor: regroup agent service modules`，已推送到远端 `dev`。
- 最近 session 保存提交：`b4be1df docs: compress session handoff`，后续恢复时以 `git log --oneline -1` 为准。
- 恢复后先执行：

```powershell
git pull
git status --short
cargo check --workspace --all-targets
```

预期 `git status --short` 为空。若不为空，先确认是否是其他机器或用户的新改动，不要直接回滚。

## 项目概况

- `crates/smalux-agent`：监控 agent，负责系统采集、动态配置、导出、Komari 兼容和远程能力。
- `crates/smalux-core`：共享模型、单位转换、日志初始化和通用校验。
- `crates/smalux-protocol`：agent/server 共享 frame、payload 和 JSON codec。
- `crates/smalux-server`：server crate 仍是骨架，README 已写好 WebSocket/wire/ingest/存储建议。
- 根目录 `src/main.rs` 是历史占位入口，不属于 workspace package；当前工作区存在该文件删除状态，提交前需确认是否保留删除。

## Agent 当前状态

- 采集：identity、system、CPU、单核 CPU、内存、swap、load average、磁盘、网络、公网 IP、进程、TCP/UDP socket。
- 采样控制：core、disk、network、processes、sockets、public_ip、report、export jobs 都有独立频率。
- 进程和 socket：支持 `count`、`light`、`details` 三层；总数字段在任一级别都会上报，除非对应采样组关闭。
- 公网 IP：默认可选，启动会尝试获取，失败会上报状态而不是阻塞第一包；成功后低频刷新，默认 `24h`。
- 上报：支持完整 `snapshot`、可选 `delta`、可选业务 `heartbeat`、server `snapshot_request` 强制完整快照。
- 动态配置：启动配置来自默认值和 CLI；连接 server 后可接收 `config_patch`，缺省字段保持当前值，相同 patch 会被忽略。
- 日志：读取 `RUST_LOG`；测试默认 `debug` 控制台输出，正式运行输出到控制台和滚动文件。日志字段只支持启动时设置，不接受 server patch。
- 远程能力：remote shell、remote task 默认关闭且只能 CLI 开启；remote probe 默认关闭但可由 server patch 动态开启。

## 关键目录

```text
crates/smalux-agent/src/
  main.rs          # CLI、日志、ConfigManager、service 启动入口
  config.rs        # 配置入口
  config/          # defaults、model、cli、manager
  collect.rs       # 本机采集入口，持有 sysinfo 长生命周期对象
  collect/         # CPU / memory / disk / network / process / socket
  telemetry.rs     # latest state、ReportEvent、TelemetryAggregator
  telemetry/       # state / aggregator
  export.rs        # ExportAdapter、TransportHub、wire 抽象入口
  export/          # WebSocket、HTTP、Komari、rustls、worker、security
  service.rs       # agent 运行编排入口
  service/
    message.rs     # message 子模块入口
    message/       # listener / inbound / outbound
    remote.rs      # remote 子模块入口
    remote/        # shell / task / probe
```

`service.rs` 仍保留旧模块别名导出，现有 `crate::service::outbound`、`crate::service::probe` 这类调用不需要一次性大改。

## 导出与协议

- 默认导出格式是 `smalux_json`，默认 transport 是 WebSocket。
- `export.format` 当前支持 `smalux_json` 和 `komari`。
- `smalux_json` 使用 `smalux-protocol::ClientFrame`，可编码 `snapshot`、`delta`、业务 `heartbeat`、`ack/error`、`remote_task_result`、`remote_probe_result`。
- `export.wire_mode=binary_plain` 时，WebSocket binary 承载 `WirePacket(kind=PlainData)`。
- `export.wire_mode=secure_psk` 时，使用 `smx1.<key_id>.<secret_base64url>` token 派生 PSK，Noise 模式为 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`；secret 不通过 URL/header 明文发送。
- `secure_psk` 的 HKDF 参数已写入 README：SHA-256，salt 为 `smalux secure psk v1 salt`，info 为 `smalux secure psk v1 ` + UTF-8 `key_id`，输出 32 字节并放入 Noise `psk(0)`；README 还包含 `agent-key` / 32 字节 `0x07` secret 的测试向量，派生 PSK hex 为 `a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2`；wire payload 上限 `1 MiB`，同一连接的 `session_id` 必须一致。
- `secure_psk` 模式要求 `export.auth_mode=none`，并拒绝 WebSocket text 控制消息。
- `export.secure_required=true` 是单向安全闸：要求 `smalux_json + secure_psk`，当前配置一旦为 `true`，server patch 不能关闭它或降级到明文/Komari。
- `ack/error` 只表示带 `sequence` 的 `ServerFrame` 已被调度或拒绝，不表示 remote task/probe 已完成。
- raw control JSON 当前支持 `config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_shell_open`、`remote_task_run`，没有自动 ack。

## Komari 兼容

- Komari 兼容代码保留在 `crates/smalux-agent/src/export/komari/`，方便后续整体删除或替换。
- Komari report 走 WebSocket `/api/clients/report?token=...`。
- Komari basic info 走 HTTP `POST /api/clients/uploadBasicInfo?token=...`，默认 `jobs.basic_info.interval=5m`。
- Komari task result 走 HTTP `POST /api/clients/task/result?token=...`。
- Komari `terminal` 复用 remote shell，`exec` 复用 remote task，`ping` 复用 remote probe。
- Komari 只消费 snapshot；开启 delta 或业务 heartbeat 会被配置校验拒绝。
- Komari 不使用 Smalux binary wire 和 secure_psk，保持第三方 text/query token 行为。

## 远程能力边界

- CLI-only，只能启动时开启：
  - `remote_shell.enabled`
  - `remote_task.enabled`
  - `diagnostics.allow_process_details`
  - `diagnostics.allow_socket_details`
- 动态配置，可由 CLI 给初始值，也可由 server patch 修改：
  - `remote_shell.max_sessions`、`idle_timeout`、`session_timeout`、`program`
  - `remote_task.max_concurrent`、`timeout`、`max_stdout_bytes`、`max_stderr_bytes`
  - `remote_probe.enabled`、`timeout`、`global_min_interval`、`target_min_interval`
- remote shell 使用 `portable-pty`；每个会话打开独立临时 WebSocket stream，PTY 输出 base64 编码。
- remote task 是非交互命令，结果通过主出站队列回传。
- remote probe 支持 TCP / HTTP；ICMP 当前返回 `value=-1`。

## 验证结果

最近完整验证通过：

```powershell
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo rustdoc -p smalux-agent --bin smalux-agent -- -D missing_docs
cargo rustdoc -p smalux-protocol --lib -- -D missing_docs
```

最新测试结果：agent `248 passed / 4 ignored`，core `10 passed`，protocol `11 passed`，server `0 tests`。当前 `cargo check --workspace --all-targets` 无 warning。

## 下一步建议

1. 优先接 `smalux-server` WebSocket ingest：完成 upgrade、wire 解包、`ClientFrame` 解析和 latest state 更新。
2. 先用内存 latest state 跑通 agent 到 server 闭环，再决定 SQLite 表结构和历史指标保留策略。
3. 接 server 下发 `ServerFrame::snapshot_request` 和后续 desired config；下发前按 README 的控制消息边界实现幂等和限频。
4. agent 后续采集项可继续补温度、GPU、电池；新功能先放现有 crate 的独立 module，只有依赖边界或复用边界明显时再拆新 crate。

## 协作注意

- 始终用简体中文沟通；代码标识符、命令、日志和报错保持原文。
- 代码注释用中文，日志内容用英文。
- 读写含中文文件前检查 BOM；当前 `session.md` 是 UTF-8 无 BOM。
- 修改现有文件优先用局部 patch，不要回滚用户未明确要求回滚的改动。
- 新增复杂功能时同步更新 README、测试和本文件的恢复摘要。
