# Smalux Session Handoff

## 恢复目标

用于在新电脑或新会话中快速恢复当前开发上下文。项目路径：`F:/code/rust/smalux`。当前分支：`dev`。

## 当前工作区状态

本次会话开始时工作区已有未提交改动，主要集中在 `crates/smalux-server`：

- `Cargo.lock`
- `crates/smalux-server/Cargo.toml`
- `crates/smalux-server/README.md`
- `crates/smalux-server/plan.md`
- `crates/smalux-server/src/bootstrap.rs`
- `crates/smalux-server/src/cli/args.rs`
- `crates/smalux-server/src/config/defaults.rs`
- `crates/smalux-server/src/config/model.rs`
- `crates/smalux-server/src/config/validation.rs`

本轮又补了非 server 模块修复，涉及：

- `crates/smalux-core/src/utils.rs`
- `crates/smalux-protocol/src/lib.rs`
- `crates/smalux-protocol/src/secure.rs`
- `crates/smalux-protocol/src/frame.rs`
- `crates/smalux-protocol/src/frame/remote.rs`
- `crates/smalux-protocol/src/frame/remote/probe.rs`
- `crates/smalux-protocol/src/codec.rs`
- `crates/smalux-agent/src/service/message/inbound.rs`
- `crates/smalux-agent/src/service/remote/task.rs`
- `crates/smalux-agent/src/service/remote/probe.rs`
- `crates/smalux-agent/src/service/export.rs`
- `crates/smalux-agent/src/service/export/pending.rs`
- `crates/smalux-agent/src/service.rs`
- `crates/smalux-agent/src/export.rs`
- `crates/smalux-agent/src/export/komari.rs`
- `crates/smalux-agent/src/export/komari/message.rs`
- `crates/smalux-agent/src/export/komari/model.rs`
- 根目录 `README.md`
- 本文件 `session.md`

恢复时先执行：

```powershell
git status --short
cargo fmt --all --check
cargo test -p smalux-core
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo check -p smalux-server
```

如果 `git status --short` 里除了上述文件之外还有别的改动，先确认来源，不要直接回滚。

## 当前项目状态

- `smalux-agent`：监控 agent，已有采集、动态配置、导出、Komari 兼容、remote task/job/shell。
- `smalux-core`：共享模型、日志、脱敏工具和公共工具。
- `smalux-protocol`：共享 `ClientFrame`、`ServerFrame`、wire、secure_psk、remote payload。
- `smalux-server`：当前还是骨架，但目录、依赖、README 和详细实现计划已经建立，CLI/config 已经落了一部分。

## 当前核心变更

当前自有协议已经统一到通用远程 job 模型：

- `ServerFrame(type=job_apply)`
- `ClientFrame(type=job_result)`
- 当前稳定 job 类型：`kind=probe`

当前状态要点：

- 自有协议不再使用 `remote_probe_apply` / `remote_probe_result`。
- `ClientPayload::RemoteProbeResult` 兼容分支已删除。
- `job_apply(kind=probe)` 只接受 `request_id`，不再接受 `task_id` alias。
- Komari 兼容仍保留，但只在 adapter/handler 内转换，不污染自有协议模型。
- 身份低频刷新文件已从 `service/collector/public_ip.rs` 调整为 `service/collector/identity.rs`。

关键入口：

- `crates/smalux-protocol/src/frame/remote/job.rs`
- `crates/smalux-agent/src/service/remote/job.rs`
- `crates/smalux-agent/src/service/collector/identity.rs`

当前交互矩阵：

| 方向 | 通道 | 主要消息 |
| --- | --- | --- |
| agent -> server | 主 WebSocket | `snapshot` / `delta` / `heartbeat` / `ack` / `error` / `remote_task_result` / `job_result` |
| server -> agent | 主 WebSocket | `snapshot_request` / `config_patch` / `collect_processes_once` / `collect_sockets_once` / `remote_task_run` / `job_apply` / `remote_shell_open` |
| shell 双向 | 临时 shell stream | `input` / `resize` / `close` / `heartbeat` / `opened` / `output` / `exit` / `error` |
| Komari -> agent | WebSocket 文本 | `terminal` / `exec` / `ping` |
| agent -> Komari | WebSocket / HTTP | report / `uploadBasicInfo` / `task/result` / `ping_result` |

## 当前验证状态

已通过：

```powershell
cargo fmt --all --check
cargo check --workspace
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo clippy --workspace --all-targets -- -D warnings
```

## 关键设计结论

### 1. 项目当前真实完成度

不是四个 crate 都已经完整：

- `smalux-agent` / `smalux-core` / `smalux-protocol` 已较完整。
- `smalux-server` 仍在搭骨架和推进实现计划。

阅读项目时应先看：

1. `crates/smalux-agent/README.md`
2. `crates/smalux-protocol/README.md`
3. `crates/smalux-agent/src/main.rs`
4. `crates/smalux-agent/src/service.rs`
5. `crates/smalux-agent/src/service/reporter.rs`
6. `crates/smalux-agent/src/service/export.rs`
7. `crates/smalux-server/README.md`
8. `crates/smalux-server/plan.md`

### 2. server 主连接路径已经统一

自有协议 agent 主连接路径已经统一为：

```text
/agent/v1/connect
```

旧的 `/api/agents/connect` 已不再使用。

### 3. server 删除 query 模块

`crates/smalux-server/src/query.rs` 和 `query/` 已删除，前端读模型职责改为：

- agent 列表、latest、在线状态：`service/agent.rs`
- dashboard 聚合：`service/dashboard.rs`
- REST handler：`http/rest.rs`
- 数据库存取：`storage/repository.rs`

### 4. React 前端托管规则

server 当前方向是：

```text
frontend enabled + 编译了 frontend-embed
  -> 使用内置前端

frontend enabled + 未编译 frontend-embed
  -> 使用 frontend-dir

frontend disabled
  -> server 只提供 agent/api/live，不托管前端
```

不再推荐运行时 `frontend-mode disabled|dir|embedded`。

## 当前验证结果

本轮已通过：

```powershell
cargo fmt --all --check
cargo check --workspace
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo clippy --workspace --all-targets -- -D warnings
```

`cargo test -p smalux-agent` 当前结果：

```text
297 passed, 4 ignored
```

恢复后如果要先确认协议/交互没有漂移，优先执行：

```powershell
cargo test -p smalux-protocol
cargo test -p smalux-agent export::komari::message::tests::inbound_handler_enqueues_ping_command
cargo test -p smalux-agent service::remote::job
```

## 下一步建议

下一步继续推进 `smalux-server`：

1. 把 `bootstrap::run()` 从“解析 CLI + 校验配置”推进到真正启动最小 HTTP server。
2. 加 `GET /api/v1/health`。
3. 先接 `MemoryRepository`。
4. 跑通 `/agent/v1/connect` + `snapshot/heartbeat` -> latest state。
5. 再接 REST 查询和命令下发。

## 协作注意

- 始终使用简体中文沟通；代码标识符、命令、日志、报错保持原文。
- 代码注释用中文，日志内容用英文。
- 修改现有文件优先用 `apply_patch`。
- 不要回滚用户未明确要求回滚的改动。
- 后续继续修改非 server 模块时，优先维护已有测试覆盖。
- 如果再改协议字段，必须同步 `smalux-agent` 和 `smalux-protocol` 两侧测试与 README。
