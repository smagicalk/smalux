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

- `smalux-agent`：监控 agent，已有采集、动态配置、导出、Komari 兼容、remote task/probe/shell。
- `smalux-core`：共享模型、日志、脱敏工具和公共工具。
- `smalux-protocol`：共享 `ClientFrame`、`ServerFrame`、wire、secure_psk、remote payload。
- `smalux-server`：当前还是骨架，但目录、依赖、README 和详细实现计划已经建立，CLI/config 已经落了一部分。

## 本轮完成的非 server 修复

### 1. 修复文本脱敏在转义引号场景下的泄漏

文件：

- `crates/smalux-core/src/utils.rs`

问题：

- `redact_sensitive_text()` 在处理 `token="abc\"def"` 这类文本时，会在遇到转义引号前提前结束 value 边界，导致敏感值尾部残留在日志里。

处理：

- `find_text_value_end()` 现在会正确跳过被反斜杠转义的引号。
- 新增测试覆盖转义引号场景。

### 2. 收紧 remote probe 的 task_id 协议边界

文件：

- `crates/smalux-protocol/src/frame/remote/probe.rs`
- `crates/smalux-protocol/src/frame/remote.rs`
- `crates/smalux-protocol/src/frame.rs`
- `crates/smalux-protocol/src/lib.rs`
- `crates/smalux-protocol/src/codec.rs`
- `crates/smalux-agent/src/service/remote/probe.rs`
- `crates/smalux-agent/src/export/komari/message.rs`
- `crates/smalux-agent/src/export/komari/model.rs`
- `crates/smalux-agent/src/export.rs`
- `crates/smalux-agent/src/export/komari.rs`
- `crates/smalux-agent/src/service.rs`
- `crates/smalux-agent/src/service/export.rs`

问题：

- 之前 `remote_probe.task_id` 直接用 `serde_json::Value`，协议层允许任意 JSON 结构，幂等、落库和跨语言实现边界过松。

处理：

- 新增 `RemoteProbeId`，只接受字符串或整数。
- 保留 Komari ping/result 对“字符串或整数 task id”的兼容。
- 为 `RemoteProbeId` 实现了 `Display`，日志和适配层可直接复用。

### 3. secure hello 现在会显式校验 Noise pattern

文件：

- `crates/smalux-protocol/src/secure.rs`

问题：

- 之前 `decode_secure_hello()` 只做 JSON 解析，不校验 `pattern`，错误会拖到更晚的 Noise 握手阶段。

处理：

- `decode_secure_hello()` 现在直接检查 `hello.pattern == NOISE_PATTERN`。
- 新增负向测试验证不匹配会在 decode 阶段失败。

### 4. 控制响应不再因为队列满而静默丢失

文件：

- `crates/smalux-agent/src/service/message/inbound.rs`
- `crates/smalux-agent/src/service.rs`

问题：

- `config_patch`、`snapshot_request` 等带 `sequence` 的 framed 控制命令，`ack/error` 之前走 `try_send`。出站队列满时会直接 drop，server 只能等超时。

处理：

- `ControlDispatcher::dispatch()` 改成 async。
- `queue_control_response()` 改为 `send().await`，不再在队列满时静默丢失。
- 相应测试 harness 调用链同步改为 await。

### 5. disabled/rate-limited 的 task/probe 拒绝结果改为可靠发送

文件：

- `crates/smalux-agent/src/service/remote/task.rs`
- `crates/smalux-agent/src/service/remote/probe.rs`

问题：

- remote task disabled / concurrency reached，remote probe disabled / rate-limited 时，之前的即时结果也走 `try_send`，在 backlog 场景下可能直接消失。

处理：

- `RemoteTaskManager::start()` / `RemoteProbeManager::start()` 改为 async。
- 即时拒绝结果改为 `send().await`。
- 相关单测同步切到 async 调用。

### 6. export 重连恢复失败不再只记日志后继续跑

文件：

- `crates/smalux-agent/src/service/export.rs`
- `crates/smalux-agent/src/service/export/pending.rs`

问题：

- 之前 `send_resume_events()` 失败后，`reconnect_pipeline_and_resume()` 只记 warn，不把失败继续上抛，导致部分 pending task/probe/ack/error 可能卡在 map 里，直到下一次断线才重试。

处理：

- `reconnect_pipeline_and_resume()` 现在会把恢复失败继续返回给上层。
- `send_resume_events()` 明确把最后一步也 `?` 出去并统一返回 `Ok(())`。
- 这样恢复失败会触发下一轮重连，而不是悄悄吞掉。

### 7. 进程和 socket 的 unsupported/stale 状态保留请求级别

文件：

- `crates/smalux-core/src/model/info/process.rs`
- `crates/smalux-core/src/model/info/socket.rs`
- `crates/smalux-agent/src/collect/process.rs`
- `crates/smalux-agent/src/collect/socket.rs`

问题：

- 之前 `ProcessInfo::unsupported()` 固定写成 `level=count`。
- `SocketInfo::unsupported()` 和 `stale_or_failed()` 也会回落到默认 `count` 语义，甚至在失败时保留旧的 level/light/details 组合。
- 这会让 server/UI 在 `light/details` 请求失败或平台不支持时，看起来像是一次正常的 `count` 采样。

处理：

- `ProcessInfo::unsupported(level, error)` 现在显式保留调用方请求的级别。
- `SocketInfo::unsupported(level, error)` 和 `stale_or_failed(previous, level, error)` 现在也显式保留当前请求级别。
- `SocketInfo::stale_or_failed()` 在级别变化时只保留与当前级别对应的 `light/details` 数据，避免旧结构误导调用方。
- agent 采集层调用点和测试已同步更新。

### 8. Komari exec 入站日志不再打印原始命令

文件：

- `crates/smalux-agent/src/export/komari/message.rs`

问题：

- 之前 Komari `exec` 入站被接受后，会把完整 `command` 原样写入 info 日志；如果第三方下发命令里带 token、密码或其它敏感参数，会直接落盘。

处理：

- 现在只记录 `task_id` 和 `command_len`，不再打印原始命令字符串。
- 行为不变，只收紧日志暴露面。

### 9. remote_shell_open 现在按 ready 语义确认

文件：

- `crates/smalux-agent/src/service/remote/shell/manager.rs`
- `crates/smalux-agent/src/service/remote/shell/session.rs`
- `crates/smalux-agent/src/service/message/inbound.rs`

问题：

- 之前 remote shell 会在“请求校验通过并 spawn 会话任务”后就返回成功；stream 连接、PTY 启动和 `opened` 事件都发生在 ack 之后，容易让控制面把 accepted 误读成 ready。

处理：

- `RemoteShellSession` 拆出 `establish_ready()`，先完成 stream 建连、PTY 启动并成功发送 `opened` 事件，再把会话交给后台循环运行。
- `RemoteShellManager::open()` 改成 async，只有 ready 阶段成功后才返回 `Ok(())`。
- `ControlDispatcher` 因此会在 ready 之后再回控制层 ack。

### 10. 出站事件改为高低优先级双通道

文件：

- `crates/smalux-agent/src/service/message/outbound.rs`
- `crates/smalux-agent/src/service/export.rs`

问题：

- 之前所有事件共用一条出站队列；即使 control/task/probe 已改成可靠发送，它们仍然可能在大量 report/basic info 背压下被拖慢。

处理：

- `OutboundSender` / `OutboundReceiver` 改为双通道。
- `ControlAck`、`ControlError`、`RemoteTaskResult`、`RemoteProbeResult` 进入高优先级通道。
- `Report`、`BasicInfo` 保持普通通道。
- `OutboundReceiver::recv()` 优先消费高优先级事件。

### 11. export 恢复流程拆成三层

文件：

- `crates/smalux-agent/src/service/export/pending.rs`

问题：

- 之前恢复逻辑虽然已经不会静默吞错，但结构上仍是一段顺序恢复，latest、控制响应、远程结果耦在一起，不利于后续独立调整策略。

处理：

- 把恢复流程拆成：`resume_latest_state()`、`resume_control_responses()`、`resume_remote_results()`。
- 当前行为仍然是顺序执行，但层次边界已经固定，后续如果要做分层退避或独立重试，不需要再重拆主流程。

### 12. 协议适配边界测试已补齐

文件：

- `crates/smalux-agent/src/service/message/handler.rs`
- `crates/smalux-agent/src/export/komari/message.rs`

处理：

- 增加测试保证 Smalux handler 不会误把 Komari `exec` 消息当成 `ServerFrame`。
- 增加测试保证 Komari handler 不会误把 Smalux `ServerFrame` 当成第三方控制消息。
- 这条边界现在不仅靠注释，也有测试固定。

### 13. 收紧 Smalux server error 入站日志

文件：

- `crates/smalux-agent/src/service/message/handler.rs`

问题：

- 之前 agent 收到 `ServerFrame(type=error)` 时，会把对端提供的原始 `message` 全量写入 warn 日志；如果 server 端把任意文本或敏感内容塞进该字段，会直接落盘。

处理：

- 现在只记录 `sequence`、`code` 和 `message_len`，不再直接打印原始 `message`。
- 协议行为不变，只收紧日志暴露面。

### 14. 收紧 remote task/probe 运行日志

文件：

- `crates/smalux-agent/src/service/remote/task.rs`
- `crates/smalux-agent/src/service/remote/probe.rs`

问题：

- 之前 remote task accepted 日志会打印本地 `program`，remote probe accepted/finished/rejected 会打印原始 `target`。这些字段都属于外部输入，可能包含敏感路径、主机、端口或其它不该长期落盘的内容。

处理：

- remote task accepted 现在只记录 `program_len`、`args_count`、超时和输出上限。
- remote probe accepted/finished/rejected 现在只记录 `probe_type`、`target_len`、耗时和值，不再打印原始 target。
- 行为不变，只继续收紧日志暴露面。

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
cargo test -p smalux-core
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo check -p smalux-agent
cargo fmt --all --check
```

`cargo test -p smalux-agent` 当前结果：

```text
278 passed, 4 ignored
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
