# smalux-protocol

`smalux-protocol` 是 agent 和 server 共享的稳定协议 crate。

## 当前职责

- 定义传输无关的 frame，例如 `ClientFrame` 和 `ServerFrame`。
- 定义 agent 内部上报语义，例如 `OutboundReport`。
- 定义首版上报 payload，例如 `snapshot`、`delta`、`heartbeat`、`ack`、`error`、`remote_task_result` 和 `job_result`。
- 提供 JSON codec，供 WebSocket、HTTP 或后续 gRPC adapter 复用。
- 提供 Smalux binary wire packet codec，也就是 `PlainData`、`Hello`、`Handshake`、`SecureData` 这些外层二进制包。
- 提供 `secure_psk` 共享安全通道工具，包括 token 解析、HKDF-SHA256 PSK 派生、Noise initiator/responder 和 payload 加解密。
- 维护协议版本、sequence 和基础错误结构。

## 不负责的内容

- 不实现 WebSocket、HTTP 或 gRPC 连接。
- 不做 agent 本机采集。
- 不做 server 存储、查询或鉴权。
- 不保存连接状态；Noise `TransportState` 的生命周期由 agent/server 的 transport 层持有。
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

## 消息矩阵

协议层当前稳定消息如下：

| 方向 | 顶层 `type` | 作用 | 是否带业务结果 |
| --- | --- | --- | --- |
| agent -> server | `snapshot` | 完整状态快照 | 是 |
| agent -> server | `delta` | 顶层采集组增量替换 | 是 |
| agent -> server | `heartbeat` | 业务在线信号 | 否 |
| agent -> server | `ack` | 控制命令已接收并调度 | 否 |
| agent -> server | `error` | 控制命令调度失败 | 否 |
| agent -> server | `remote_task_result` | 非交互任务执行结果 | 是 |
| agent -> server | `job_result` | 通用远程 job 结果 | 是 |
| server -> agent | `snapshot_request` | 请求完整快照 | 否 |
| server -> agent | `config_patch` | 动态配置更新 | 否 |
| server -> agent | `collect_processes_once` | 一次性进程采样 | 否 |
| server -> agent | `collect_sockets_once` | 一次性 socket 采样 | 否 |
| server -> agent | `remote_task_run` | 一次性非交互命令 | 否 |
| server -> agent | `job_apply` | 通用远程 job 同步/运行 | 否 |
| server -> agent | `remote_shell_open` | 打开临时 shell stream | 否 |

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
  - 当前稳定值包括 `snapshot`、`delta`、`heartbeat`、`ack`、`error`、`remote_task_result`、`job_result`。

### ServerFrame

- `protocol_version`
  - 目前固定为 `1`。
- `sequence`
  - server 侧递增序号。
  - 只有带 `sequence` 的命令才会让 agent 回 `ack/error`。
- `sent_at`
  - server 发送 frame 的 Unix 秒时间戳。
- `target_agent_id`
  - 可选目标 agent ID。
  - 为空时表示当前连接上的 agent；不为空且和当前 agent 不匹配时，agent 会直接丢弃该命令。
  - 该字段只做路由保护，不做认证。
- `type`
  - 当前稳定值包括 `snapshot_request`、`config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_task_run`、`job_apply` 和 `remote_shell_open`。

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

远程 shell 打开请求也走稳定 `ServerFrame`，真实终端 IO 会在 `stream_url` 指向的临时 WebSocket 上继续交换：

```jsonc
{
  "protocol_version": 1,
  "sequence": 202,
  "sent_at": 1710000002,
  "type": "remote_shell_open",
  "request": {
    "session_id": "shell-1",
    "stream_url": "wss://example.com/agent/shell/shell-1",
    "cols": 120,
    "rows": 30
  }
}
```

通用远程 job 的一次性运行请求：

```jsonc
{
  "protocol_version": 1,
  "sequence": 203,
  "sent_at": 1710000003,
  "type": "job_apply",
  "request": {
    "operation": "once",
    "runs": [
      {
        "kind": "probe",
        "request_id": "probe-once-1",
        "point_id": "point-main-api",
        "probe_type": "tcp",
        "target": "example.com:443",
        "timeout": "5s"
      }
    ]
  }
}
```

通用远程 job 的持续任务同步请求：

```jsonc
{
  "protocol_version": 1,
  "sequence": 204,
  "sent_at": 1710000004,
  "type": "job_apply",
  "request": {
    "operation": "replace",
    "generation": 12,
    "jobs": [
      {
        "kind": "probe",
        "job_id": "main-api",
        "point_id": "point-main-api",
        "enabled": true,
        "probe_type": "tcp",
        "target": "example.com:443",
        "interval": "30s",
        "timeout": "5s"
      }
    ]
  }
}
```

通用远程 job 结果当前通过 `job_result(kind=probe)` 回传：

```jsonc
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 13,
  "sent_at": 1710000005,
  "type": "job_result",
  "result": {
    "kind": "probe",
    "result": {
      "run_id": "4c21e6f8-8ef3-4ee5-9c91-8f630f650b92",
      "source": "once",
      "point_id": "point-main-api",
      "request_id": "probe-once-1",
      "probe_type": "tcp",
      "target": "example.com:443",
      "status": "success",
      "latency_ms": 13,
      "started_at": 1710000004,
      "finished_at": 1710000005,
      "duration_ms": 13
    }
  }
}
```

当前自有协议里：

- `job_apply(kind=probe)` 只接受 `request_id`，不接受 `task_id` 这类第三方字段别名。
- `job_result.kind` 是后续扩展点；server 应先按 `type=job_result` 再按 `kind` 分发。

远程 shell stream 的业务 JSON 也定义在本 crate 中，agent 通过
`decode_remote_shell_stream_command()` 解析 server 发来的 command，通过
`encode_remote_shell_stream_event()` 编码 agent 发回的 event。transport 仍由外层决定：
`binary_plain` 时它们是 `WirePacket(kind=PlainData)` 的 payload，`secure_psk` 时它们是解密后的
`WirePacket(kind=SecureData)` payload，只有本地调试才建议直接走 WebSocket text。

```jsonc
{ "type": "input", "data": "echo hello\r\n" } // UTF-8 文本输入，encoding 缺省为 utf8
{ "type": "input", "encoding": "base64", "data": "AAEC" } // 原始字节输入
{ "type": "resize", "cols": 120, "rows": 30 } // 调整 PTY 尺寸
{ "type": "close" } // 请求关闭本次 shell 会话
{ "type": "heartbeat" } // 可选 stream 保活，不写入 PTY
```

```jsonc
{ "type": "opened", "session_id": "shell-1" } // shell 已启动
{ "type": "output", "session_id": "shell-1", "encoding": "base64", "data": "aGVsbG8NCg==" } // PTY 输出
{ "type": "exit", "session_id": "shell-1", "code": null } // shell 退出；无进程退出码时固定带 null
{ "type": "error", "session_id": "shell-1", "message": "..." } // 会话错误
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

## 连接状态

本 crate 不保存连接状态，但协议语义默认外层 transport 至少经历下面几个阶段：

```text
binary_plain
  connecting
    -> ready
    -> closing

secure_psk
  connecting
    -> hello_sent / hello_received
    -> handshake_sent / handshake_received
    -> ready
    -> closing
```

建议规则：

- `binary_plain` 的 ready 条件：能稳定收发 `WirePacket(kind=PlainData)`。
- `secure_psk` 的 ready 条件：Hello 和 Noise 握手都成功，已经拿到 `TransportState`。
- 未进入 ready 前，不要把业务 JSON 直接交给 `decode_client_frame()` / `decode_server_frame()`。
- `secure_psk` 任意一步失败，都应按连接级错误处理，不要尝试降级到明文。

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
   -> remote_task_result / job_result: 关联任务结果

5. 下发控制命令
   -> 自有协议命令统一构造 ServerFrame，可选 target_agent_id 做路由保护
   -> snapshot_request / config_patch / collect_* / remote_task_run / job_apply / remote_shell_open 都可收到 ack/error
   -> 按当前 wire_mode 封成 PlainData 或 SecureData
```

`smalux-protocol` 同时提供解密后的 JSON frame codec、二进制 wire packet codec 和 `secure_psk` Noise PSK 工具。agent 和 server 都应该直接复用：

- `smalux_protocol::wire::encode_wire_packet()` / `decode_wire_packet()`
- `smalux_protocol::secure::parse_secure_token()` / `decode_secure_hello()`
- `smalux_protocol::secure::build_noise_initiator()` / `build_noise_responder()`
- `smalux_protocol::secure::encrypt_payload()` / `decrypt_payload()`

`secure_psk` 的精确参数在本 crate 的 `secure` 模块测试中固化：token 使用 `smx1.<key_id>.<secret_base64url>`，HKDF-SHA256 salt 是 `smalux secure psk v1 salt`，info 是 `smalux secure psk v1 ` 加 UTF-8 `key_id`，输出 32 字节并放入 Noise `psk(0)`，pattern 是 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`。server 对接前应先跑 `cargo test -p smalux-protocol secure::tests::secure_psk_hkdf_test_vector_is_stable`，确认派生结果为 `a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2`。

### Server 复用函数对照

server 首版不要重新写 wire、frame 或加密细节。按下面顺序把 transport 收到的 bytes 逐层还原即可：

| 阶段 | 输入 | 调用 | 输出 | 说明 |
| --- | --- | --- | --- | --- |
| WebSocket binary | WebSocket binary frame | `wire::decode_wire_packet(bytes)` | `WirePacket` | 先校验 magic/version/kind/长度；失败属于连接级错误 |
| `binary_plain` 上行 | `WirePacket(kind=PlainData)` | 直接取 `packet.payload` | JSON bytes | 开发期可额外接受 text，但正式自有协议建议始终用 binary wire |
| `secure_psk` hello | `WirePacket(kind=Hello)` | `secure::decode_secure_hello(&packet.payload)` | `key_id` + pattern | `key_id` 只能用于查 secret，不能单独当认证成功 |
| `secure_psk` 握手 | server 保存的 secret | 查询 secret 后按本 crate 相同规则派生 PSK，调用 `secure::build_noise_responder()` | `HandshakeState` | Rust server 可以用同一套 secure 工具；其它语言必须复现 HKDF 参数 |
| `secure_psk` 密文 | `WirePacket(kind=SecureData)` | `secure::decrypt_payload(&mut transport, &packet.payload)` | JSON bytes | 解密失败应关闭连接，不要降级到明文解析 |
| JSON 上行 | JSON UTF-8 | `codec::decode_client_frame(text)` | `ClientFrame` | 再按 `ClientPayload` 分发 snapshot/delta/heartbeat 等 |
| JSON 下行 | `ServerFrame` | `codec::encode_server_frame(&frame)` | JSON text | 下发前再按当前 wire mode 封成 `PlainData` 或 `SecureData` |
| binary 下行 | JSON bytes | `WirePacket::plain_data()` / `WirePacket::secure_data()` + `wire::encode_wire_packet()` | WebSocket binary frame | `secure_psk` 需要先 `secure::encrypt_payload()` |

解包后再进入业务分发，不要让 HTTP/WebSocket 入口直接修改数据库。推荐 server 层次是：

```text
transport adapter
  -> wire / secure
  -> codec::decode_client_frame()
  -> ingest::handle_client_frame()
  -> storage / query
```

## 兼容边界

- `snapshot_request`、`config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_task_run`、`job_apply`、`remote_shell_open` 现在都属于稳定 `ServerFrame`。
- `ack/error` 只对带 `sequence` 的 `ServerFrame` 有意义。
- `target_agent_id` 不匹配时 agent 会丢弃命令，不回 `ack/error`。
- 第三方兼容消息不进入本 crate 的稳定协议面，应在对应 adapter/handler 中转换成 agent 内部命令。
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

错误消息 `message` 面向日志和排查，不建议让 server 依赖其中的自然语言做逻辑判断；逻辑判断只看 `code` 和 `sequence`。当前 agent 对 `ServerFrame` 调度失败时会按 `{server_frame_type}_failed` 生成错误码，例如 `snapshot_request_failed`、`job_apply_failed` 和 `remote_shell_open_failed`。

server 自己的 ingest 错误可以使用另一套内部错误码，不必通过 `ClientFrame(type=error)` 回给 agent。agent 当前没有等待 server 对上报 frame 做协议级 ack，因此 server 收到非法 `snapshot` / `delta` 时优先记录、丢弃或发送 `snapshot_request`。

## 顺序和幂等

当前协议有三个容易混淆的序号或 ID：

| 字段 | 生成方 | 作用 |
| --- | --- | --- |
| `ClientFrame.sequence` | agent | agent 全局出站序号，snapshot、delta、heartbeat、ack/error、remote task/job result 共用 |
| `ServerFrame.sequence` | server | server 下发稳定控制命令的序号，agent 的 `ack.sequence` / `error.sequence` 会引用它 |
| `task_id` | server 或第三方兼容层 | remote task 的业务结果关联 ID |
| `job_result.kind` | agent | 通用远程 job 结果类型；首版稳定值是 `probe` |
| `run_id` | agent | `job_result(kind=probe)` 每次实际运行或拒绝运行的唯一结果 ID，可作为探测结果表主键或幂等键 |
| `point_id` | server | `kind=probe` 的业务探测点 ID；一次性探测和持续任务都可以携带，结果会原样带回 |
| `request_id` / `job_id` | server | `kind=probe` 的执行关联 ID；一次性探测使用 `request_id`，持续任务使用 `job_id` |

server 处理建议：

- `ClientFrame.sequence` 可以用于记录 last seen 和发现明显乱序，但不要把缺号直接当成协议错误。agent 导出 job 可能只发送最新 report，中间 report 被最新状态覆盖时会出现序号跳跃。
- `delta.base_sequence` 才是合并增量的强约束；它不匹配时必须请求 snapshot，而不是靠 `ClientFrame.sequence` 猜测。
- `ack/error` 的业务关联字段是内部 payload 里的 `ack.sequence` / `error.sequence`，不是外层 `ClientFrame.sequence`。
- `remote_task_result` 以 `task_id` 幂等。`job_result(kind=probe)` 以内部 `result.run_id` 幂等；`point_id` 用于关联 server 业务探测点；`source=once` 额外用 `request_id` 关联一次性请求，`source=job` 额外用 `job_id` 关联持续探测任务。
- 重连后 agent 的 `ClientFrame.sequence` 会从当前进程内的出站序号继续增长；如果 agent 进程重启，序号可能重新从 `1` 开始。server 不能只靠 sequence 判断 agent 是否是同一个进程，应该结合连接时间、agent version、latest snapshot 和后续认证信息。

server 发送建议：

- `ServerFrame.sequence` 在 server 侧按 agent 递增即可，不要求全局唯一。
- 有副作用的命令需要业务 ID，例如 `remote_task_run.task_id`，避免重连或重试导致重复执行。
- `snapshot_request` 可以重复发送，但 agent 有 `report.force_snapshot_min_interval` 合并保护；server 也应做自己的频率限制。
- 自有协议下行不要发送 raw JSON 命令；所有 server 命令都应放进 `ServerFrame`。

## 第三方兼容扩展

本 crate 只承载自有协议的稳定 frame。Komari 或后续其它服务端的特殊消息应留在 agent 的对应 adapter/handler 中处理，并转换为统一内部命令：

- 需要 agent/server 双方长期稳定理解、需要 `ack/error`、需要跨 transport 复用的命令，放入 `ServerPayload`。
- 只属于某个第三方协议的字段、路径、事件名或文本格式，留在第三方兼容 adapter 中。
- adapter 可以复用内部 remote task、remote job、remote shell manager，但不应把第三方 raw 消息暴露成自有协议格式。

## 扩展原则

- 先保持文件少，等单文件职责不再清晰后再拆。
- `snapshot`、`delta`、`heartbeat` 已落地；后续再加 capability、第三方兼容格式。
- transport 放到 agent/server 自己的模块中，不放进本 crate。

## 常用命令

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol
```
