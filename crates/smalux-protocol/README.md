# smalux-protocol

`smalux-protocol` 是 agent 和 server 共享的稳定协议 crate。

## 当前职责

- 定义传输无关的 frame，例如 `ClientFrame` 和 `ServerFrame`。
- 定义 agent 内部上报语义，例如 `OutboundReport`。
- 定义首版上报 payload，例如 `snapshot`、`delta`、`heartbeat`、`ack`、`error`、`remote_task_result` 和 `remote_probe_result`。
- 提供 JSON codec，供 WebSocket、HTTP 或后续 gRPC adapter 复用。
- 维护协议版本、sequence 和基础错误结构。

## 不负责的内容

- 不实现 WebSocket、HTTP 或 gRPC 连接。
- 不做 agent 本机采集。
- 不做 server 存储、查询或鉴权。
- 不放 `tonic` / `prost` 生成代码；后续需要 gRPC 时再新增独立 crate。

## 消息分层

协议里有三层概念，不要混：

1. `OutboundReport`
   - agent 进程内部使用。
   - 表达“我要上报什么语义”，还没有决定最终外层 frame 形状。
2. `ClientFrame`
   - agent 发往 server 的最终 JSON frame。
   - 包含 `protocol_version`、`agent_id`、`sequence`、`sent_at` 和具体 `type`。
3. `ServerFrame`
   - server 发往 agent 的最终 JSON frame。
   - 当前只放少量稳定控制消息，主要是需要协议级 `sequence` 的命令。

## 当前 frame 约定

### ClientFrame

- `protocol_version`
  - 目前固定为 `1`。
  - server 收到后先做协议版本校验，再继续解析 payload。
- `agent_id`
  - agent 实例 ID。
  - server 可把它作为主键，也可以把它和 `identity.agent_id` 一起做一致性校验。
- `sequence`
  - agent 连接内递增序号。
  - server 用它判断乱序、跳号、delta 基准和控制响应关联。
- `sent_at`
  - agent 发送 frame 的 Unix 秒时间戳。
- `type`
  - 当前稳定值包括 `snapshot`、`delta`、`heartbeat`、`ack`、`error`、`remote_task_result`、`remote_probe_result`。

### ServerFrame

- `protocol_version`
  - 目前固定为 `1`。
- `sequence`
  - server 侧递增序号。
  - 只有带 `sequence` 的命令才会让 agent 回 `ack/error`。
- `sent_at`
  - server 发送 frame 的 Unix 秒时间戳。
- `type`
  - 当前稳定值包括 `snapshot_request` 和 `remote_probe_run`。
- 兼容控制 JSON
  - `config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_shell_open`、`remote_task_run` 目前是 raw control JSON，不在这个 crate 的稳定 `ServerPayload` 里。

## Frame JSON 示例

`ClientFrame` 和 `ServerFrame` 都使用 `serde(tag = "type", rename_all = "snake_case")`，也就是业务类型直接展开在顶层：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 10,
  "sent_at": 1710000000,
  "type": "heartbeat",
  "heartbeat": {
    "last_report_at": 1709999990,
    "last_report_sequence": 9
  }
}
```

server 下发稳定命令时也是同样的顶层 `type`：

```jsonc
{
  "protocol_version": 1,
  "sequence": 201,
  "sent_at": 1710000001,
  "type": "snapshot_request",
  "request": {
    "reason": "delta_base_missing"
  }
}
```

agent 回传 `ack/error` 时，`ack.sequence` 或 `error.sequence` 指向 server 的 `ServerFrame.sequence`，不是 agent 自己的 frame 序号：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 11,
  "sent_at": 1710000002,
  "type": "ack",
  "ack": {
    "sequence": 201
  }
}
```

`delta` 的 `base_sequence` 是 server 应该已经保存的上一份基准状态序号：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 12,
  "sent_at": 1710000003,
  "type": "delta",
  "delta": {
    "base_sequence": 10,
    "report_at": 1710000003,
    "core": {
      "sampled_at": 1710000003,
      "value": {
        "cpu": { "cpu_num": 4, "cpu_usage": 12.5, "cpus": [] },
        "memory": {
          "memory_total": 8589934592,
          "memory_usage": 2147483648,
          "memory_available": 6442450944,
          "memory_free": 6442450944,
          "swap_total": 0,
          "swap_usage": 0,
          "swap_free": 0
        },
        "load_avg": { "one": 0.1, "five": 0.2, "fifteen": 0.3, "supported": true }
      }
    },
    "network": null
  }
}
```

`network: null` 表示该采集组被关闭；`core` 出现对象表示整体替换 server 保存的 `core` 组。server 不需要理解每个嵌套字段才能正确合并 delta，但必须按顶层采集组替换。

## Server 对接流程

server 按下面顺序实现，最容易先跑通闭环：

```text
1. 连接建立
   -> WebSocket / HTTP upgrade
   -> 按 transport 层规则识别连接；binary_plain 可校验 query/Authorization，secure_psk 通过后续 Noise 握手认证

2. wire 解包
   -> binary_plain: 读取 WirePacket(kind=PlainData)
   -> secure_psk: 先完成 Hello + Handshake，再读取 WirePacket(kind=SecureData)

3. 处理 agent -> server
   -> decode ClientFrame
   -> 按 ClientFrame.type 分发
   -> 不认识的 type 记录日志并忽略，避免单条未知消息断开主连接

4. 更新 server 状态
   -> snapshot: 整体替换 latest state
   -> delta: 按顶层采集组整体覆盖
   -> heartbeat: 只刷新在线时间
   -> ack/error: 关联 server 下发命令
   -> remote_task_result / remote_probe_result: 关联任务结果

5. 下发控制命令
   -> snapshot_request / remote_probe_run 用 ServerFrame，可收到 ack/error
   -> config_patch / collect_* / remote_shell_open / remote_task_run 当前走 raw control JSON
   -> 按当前 wire_mode 封成 PlainData 或 SecureData
```

`smalux-protocol` 只定义解密后的 JSON frame，不定义 WebSocket wire header 和 Noise 状态机。`secure_psk` 的精确实现参数在 agent/server README 中维护：token 使用 `smx1.<key_id>.<secret_base64url>`，HKDF-SHA256 salt 是 `smalux secure psk v1 salt`，info 是 `smalux secure psk v1 ` 加 UTF-8 `key_id`，输出 32 字节并放入 Noise `psk(0)`，pattern 是 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`。server 对接前应先跑 agent/server README 里的 HKDF 测试向量，确认派生结果为 `a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2`。

## 兼容边界

- `snapshot_request`、`remote_probe_run` 现在属于稳定 `ServerFrame`。
- `config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_shell_open`、`remote_task_run` 目前属于 agent 兼容 JSON，不是这个 crate 的稳定协议面。
- `ack/error` 只对带 `sequence` 的 `ServerFrame` 有意义。
- server 如果没有 delta 基准，就应该发 `snapshot_request`，不要猜测补齐。
- server 不要做字段级深度 merge，`snapshot` 是完整替换，`delta` 是顶层采集组替换。

## 版本和兼容策略

当前有两个版本号，含义不同：

- `protocol_version`
  - 通信 frame 版本，当前是 `1`。
  - server 收到未知版本时应拒绝或降级处理，不要按当前结构强行解析。
- `report.meta.schema_version`
  - 监控数据模型版本，当前由 `smalux-core` 定义。
  - server 可以支持多个 schema，但首版建议只接受当前版本，避免错误落库。

JSON 解析建议：

- 对未知 `type`：记录日志并忽略该 frame，不要断开整个连接，除非它违反安全边界。
- 对未知字段：默认保留向前兼容，server 不需要因为多了字段就拒绝。
- 对缺失必填字段：拒绝该 frame，并记录具体字段名。
- 对可选字段：按缺省语义处理，例如 `heartbeat.last_report_at` 不存在表示 agent 没有可报告的完整快照时间。
- 对数字时间戳：当前统一使用 Unix 秒，server 不要混用毫秒存储到同一字段。

## 错误码建议

`ClientFrame(type=error)` 目前用于回应带 `sequence` 的 `ServerFrame`，`error.sequence` 指向 server 下发的 `ServerFrame.sequence`。server 后续自己扩展错误码时建议保持稳定、短小、可搜索，使用 `snake_case`：

| 错误码 | 建议含义 | server 处理 |
| --- | --- | --- |
| `unsupported_protocol_version` | agent 不支持该 `protocol_version` | 停止下发该版本命令，必要时断开 |
| `unsupported_command` | agent 不认识该 `ServerFrame.type` | 标记 pending command 失败 |
| `invalid_payload` | 命令字段缺失或类型错误 | 修正 server 生成逻辑，不自动重试 |
| `config_rejected` | `config_patch` 校验失败或违反本地限制 | 保存失败原因，不覆盖 desired config |
| `permission_denied` | CLI-only 权限未开启，例如 details / shell / task | UI 提示需要 agent 启动参数 |
| `rate_limited` | agent 本地频率保护拒绝执行 | 按建议间隔后再试，不要立即循环重发 |
| `busy` | 并发已满，例如 remote shell session 已达到上限 | 稍后重试或让用户关闭旧会话 |
| `internal_error` | agent 内部不可预期错误 | 记录上下文，避免无限重试 |

错误消息 `message` 面向日志和排查，不建议让 server 依赖其中的自然语言做逻辑判断；逻辑判断只看 `code` 和 `sequence`。

server 自己的 ingest 错误可以使用另一套内部错误码，不必通过 `ClientFrame(type=error)` 回给 agent。agent 当前没有等待 server 对上报 frame 做协议级 ack，因此 server 收到非法 `snapshot` / `delta` 时优先记录、丢弃或发送 `snapshot_request`。

## 顺序和幂等

当前协议有三个容易混淆的序号或 ID：

| 字段 | 生成方 | 作用 |
| --- | --- | --- |
| `ClientFrame.sequence` | agent | agent 全局出站序号，snapshot、delta、heartbeat、ack/error、remote task/probe result 共用 |
| `ServerFrame.sequence` | server | server 下发稳定控制命令的序号，agent 的 `ack.sequence` / `error.sequence` 会引用它 |
| `task_id` | server 或第三方兼容层 | remote task / remote probe 的业务结果关联 ID |

server 处理建议：

- `ClientFrame.sequence` 可以用于记录 last seen 和发现明显乱序，但不要把缺号直接当成协议错误。agent 导出 job 可能只发送最新 report，中间 report 被最新状态覆盖时会出现序号跳跃。
- `delta.base_sequence` 才是合并增量的强约束；它不匹配时必须请求 snapshot，而不是靠 `ClientFrame.sequence` 猜测。
- `ack/error` 的业务关联字段是内部 payload 里的 `ack.sequence` / `error.sequence`，不是外层 `ClientFrame.sequence`。
- `remote_task_result` 和 `remote_probe_result` 以 `task_id` 幂等。重复收到同一个 `task_id` 时覆盖同一条结果，不创建重复任务。
- 重连后 agent 的 `ClientFrame.sequence` 会从当前进程内的出站序号继续增长；如果 agent 进程重启，序号可能重新从 `1` 开始。server 不能只靠 sequence 判断 agent 是否是同一个进程，应该结合连接时间、agent version、latest snapshot 和后续认证信息。

server 发送建议：

- `ServerFrame.sequence` 在 server 侧按 agent 递增即可，不要求全局唯一。
- 有副作用的命令需要业务 ID，例如 `remote_task_run.task_id`，避免重连或重试导致重复执行。
- `snapshot_request` 可以重复发送，但 agent 有 `report.force_snapshot_min_interval` 合并保护；server 也应做自己的频率限制。
- raw control JSON 没有协议级 `sequence`，不能期待 `ack/error`。如果某个 raw 命令需要标准确认，应先提升为 `ServerPayload`。

## 提升到 ServerFrame 的标准

raw control JSON 不是最终形态。后续一个 server 命令满足下面条件时，建议提升到 `ServerPayload`：

- 需要标准 `ack/error`，让 server 能确认命令是否被 agent 接收并调度。
- 需要跨 transport 复用，例如 WebSocket、HTTP callback 或 gRPC 都要下发同一语义。
- 需要稳定的协议测试和版本兼容。
- 不再只是某个 adapter 的兼容行为。

例如 `config_patch` 后续很可能提升到 `ServerPayload`，因为它是自有 server 的核心能力；而某些第三方兼容消息可以继续留在各自 adapter 中。

## 扩展原则

- 先保持文件少，等单文件职责不再清晰后再拆。
- `snapshot`、`delta`、`heartbeat` 已落地；后续再加 capability、第三方兼容格式。
- transport 放到 agent/server 自己的模块中，不放进本 crate。

## 常用命令

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol
```
