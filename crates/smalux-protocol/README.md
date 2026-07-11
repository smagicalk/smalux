# smalux-protocol

`smalux-protocol` 是 agent 和 server 共享的协议 crate。server 端不要重新实现 frame、wire 或 `secure_psk`，直接复用这里。

## 职责

- 定义传输无关的 `ClientFrame` / `ServerFrame`。
- 定义内部上报事件 `ClientEvent`。
- 定义 `snapshot`、`delta`、`heartbeat`、`ack`、`error`、`remote_task_result`、`job_result` 等 payload。
- 提供 JSON codec。
- 提供 Smalux binary wire packet codec。
- 提供 `secure_psk` token 解析、HKDF PSK 派生、Noise 握手和 payload 加解密。

不负责：

- 不实现 WebSocket、HTTP、gRPC transport。
- 不做本机采集。
- 不做 server 存储、查询、鉴权。
- 不保存连接状态或 Noise transport 生命周期。

## 目录

```text
src/
  lib.rs              # 对外导出入口
  codec.rs            # ClientFrame / ServerFrame / shell stream JSON codec
  frame.rs            # frame 模块入口
  frame/
    client.rs         # agent -> server frame
    server.rs         # server -> agent frame
    control.rs        # Ack / ProtocolError
    client_event.rs   # agent 内部待导出语义
    report.rs         # heartbeat / delta / snapshot request
    remote.rs         # remote 模块入口
    remote/
      task.rs         # 非交互远程任务
      job.rs          # 通用远程 job
      probe.rs        # 远程探测
      shell.rs        # 交互 shell stream
    version.rs        # 协议版本
  wire.rs             # 二进制 wire packet
  secure.rs           # secure_psk / Noise 工具
```

## Frame 矩阵

| 方向 | `type` | 作用 |
| --- | --- | --- |
| agent -> server | `snapshot` | 完整状态快照 |
| agent -> server | `delta` | 采集组级增量 |
| agent -> server | `heartbeat` | 业务在线心跳 |
| agent -> server | `ack` | 确认 server 命令已接收或已调度 |
| agent -> server | `error` | 命令调度失败或协议错误 |
| agent -> server | `remote_task_result` | 非交互任务结果 |
| agent -> server | `job_result` | 通用远程 job 结果 |
| server -> agent | `snapshot_request` | 请求完整快照 |
| server -> agent | `config_patch` | 动态配置 patch |
| server -> agent | `collect_processes_once` | 一次性进程采集 |
| server -> agent | `collect_sockets_once` | 一次性 socket 采集 |
| server -> agent | `remote_task_run` | 非交互任务 |
| server -> agent | `job_apply` | 通用远程 job：一次运行、整组替换、增量 patch |
| server -> agent | `remote_shell_open` | 打开交互 shell stream |

## Frame 形状

`ClientFrame`：

```json
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

`ServerFrame`：

```json
{
  "protocol_version": 1,
  "sequence": 201,
  "sent_at": 1710000001,
  "target_agent_id": "agent-1",
  "type": "snapshot_request",
  "request": {
    "reason": "manual_refresh"
  }
}
```

完整字段速查放在 [../smalux-server/plan.md](../smalux-server/plan.md)，避免协议字段在多个文档里重复漂移。

实现约束：

- `sequence` 是协议关联字段，不等于 transport packet 序号。
- `target_agent_id` 只做路由保护，不做认证。
- 第三方兼容字段不要加进 `ClientFrame` / `ServerFrame`，应由 adapter 转换。
- `ServerPayload` 新增命令前要明确 agent 是否必须回 `ack/error`。

## Wire 和 secure

wire packet 只负责二进制封包：

- `plain_data`
- `hello`
- `handshake`
- `secure_data`
- `close`

`secure_psk` 使用：

- token 格式：`smx1.<key_id>.<secret_base64url>`
- Noise pattern：`Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`
- HKDF-SHA256 派生 32 字节 PSK
- 当前公开 helper 是 `secure::parse_secure_token()`；server 如果保存 `key_id + secret_base64url`，可按 token 格式组装后复用它。

安全边界：

- `key_id` 只用于查找 secret，不代表认证成功。
- 认证成功的依据是双方用同一 secret 派生 PSK 并完成 Noise 握手。
- 日志只能输出 key id、握手阶段和脱敏后的错误，不输出 token、secret 或 PSK。

## Server 复用 API

server 对接时优先使用这些函数：

- `smalux_protocol::decode_client_frame_bytes`
- `smalux_protocol::encode_server_frame_bytes`
- `smalux_protocol::wire::decode_wire_packet`
- `smalux_protocol::wire::encode_wire_packet`
- `smalux_protocol::secure::decode_secure_hello`
- `smalux_protocol::secure::build_noise_responder`
- `smalux_protocol::secure::read_handshake_message`
- `smalux_protocol::secure::write_handshake_message`
- `smalux_protocol::secure::decrypt_payload`
- `smalux_protocol::secure::encrypt_payload`

agent 侧同理使用对应 encode/decode 函数，不要在 transport 层手写 JSON 字段名。

## 扩展规则

- 新增 agent -> server 消息：扩展 `ClientPayload`，补 codec roundtrip 测试。
- 新增 server -> agent 命令：扩展 `ServerPayload`，明确是否需要 `ack/error`。
- 新增远程能力：优先扩展 `RemoteJobKind` / `RemoteJobSpec` / `RemoteJobResult`，不要为每个能力新造一套顶层消息。
- 第三方协议字段只进 adapter，不要加到自有协议模型里。

## 常用命令

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol
```
