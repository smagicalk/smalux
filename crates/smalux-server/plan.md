# smalux-server 交互协议与参数速查

`plan.md` 现在只保留 server 实现时最常查的内容：启动参数、路由、frame、wire、安全和远程 shell / job / probe 字段。

## 1. 启动参数

以下参数均来自 `crates/smalux-server/src/cli/args.rs`，以当前代码为准。

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--bind-addr` | `SMALUX_SERVER_BIND_ADDR` | `127.0.0.1` | HTTP 监听地址。 |
| `-p, --bind-port` | `SMALUX_SERVER_BIND_PORT` | `3000` | HTTP 监听端口。 |
| `--database-driver` | `SMALUX_SERVER_DATABASE_DRIVER` | `sqlite` | `sqlite` / `postgres` / `mysql`。 |
| `--database-host` | `SMALUX_SERVER_DATABASE_HOST` | `127.0.0.1` | PostgreSQL / MySQL 主机。 |
| `--database-port` | `SMALUX_SERVER_DATABASE_PORT` | 驱动默认端口 | PostgreSQL 默认 `5432`，MySQL 默认 `3306`。 |
| `--database-name` | `SMALUX_SERVER_DATABASE_NAME` | 驱动默认数据库名 | SQLite 默认 `smalux-server.db`，网络库默认 `smalux`。 |
| `--database-user` | `SMALUX_SERVER_DATABASE_USER` | 空 | PostgreSQL / MySQL 用户名。 |
| `--database-password` | `SMALUX_SERVER_DATABASE_PASSWORD` | 空 | PostgreSQL / MySQL 密码，日志里脱敏。 |
| `--database-param KEY=VALUE` | 无 | 空 | 可重复传入，生成数据库连接 query 参数。重复 key 会报错。 |
| `--serve-frontend[=BOOL]` | `SMALUX_SERVER_SERVE_FRONTEND` | `false` | 是否由 server 托管前端；只传 `--serve-frontend` 等价于 `true`。 |
| `--site-mode` | `SMALUX_SERVER_SITE_MODE` | `embedded` | `embedded` / `directory` / `external`。 |
| `--site-dir` | `SMALUX_SERVER_SITE_DIR` | `apps/smalux-web/dist` | 站点前端目录，仅 `directory` 模式有效。 |
| `--site-external-url` | `SMALUX_SERVER_SITE_EXTERNAL_URL` | 空 | 站点前端外部地址，仅 `external` 模式有效。 |
| `--admin-mode` | `SMALUX_SERVER_ADMIN_MODE` | `embedded` | 管理后台模式。 |
| `--admin-dir` | `SMALUX_SERVER_ADMIN_DIR` | `apps/smalux-web/dist` | 管理后台目录，仅 `directory` 模式有效。 |
| `--admin-external-url` | `SMALUX_SERVER_ADMIN_EXTERNAL_URL` | 空 | 管理后台外部地址，仅 `external` 模式有效。 |
| `--frontend-spa-fallback` | `SMALUX_SERVER_FRONTEND_SPA_FALLBACK` | `true` | SPA 路由回退。 |
| `--log-file` | `SMALUX_SERVER_LOG_FILE` | `logs/smalux-server.log` | 滚动日志文件路径。 |
| `-L, --log-retention-files` | `SMALUX_SERVER_LOG_RETENTION_FILES` | `14` | 最多保留的滚动日志文件数。 |
| `--log-max-size-mb` | `SMALUX_SERVER_LOG_MAX_SIZE_MB` | `64` | 单个日志文件最大大小。 |

补充约定：

- 日志级别只读 `RUST_LOG`。
- `agent` 的 token / key 不属于 server 启动参数，后续由“添加 agent”流程动态生成。
- `database-param` 只保留“关键参数 + 自定义映射”这一路，不再为每个驱动单独拆出一堆细碎参数。

## 2. 路由边界

当前 server 只保留这几类入口：

- `GET /agent/v1/connect`
  - agent 主 WebSocket。
  - 只负责连接、认证、frame 收发和控制帧下发。
- `GET /api/v1/health`
  - 健康检查。
- `GET /api/v1/realtime/*`
  - 前端实时通道预留，当前 router 已预留但还没有具体 endpoint。
- `GET /`
  - 站点前端。
- `GET /admin`
  - 管理后台前端。
- `GET /assets/site/*`
  - 站点前端静态资源。
- `GET /assets/admin/*`
  - 管理后台静态资源。

前端模式约定：

- `serve_frontend=false` 时不挂前端路由。
- `embedded` 依赖 `frontend-embed` feature。
- `directory` 从本地目录读取。
- `external` 直接重定向到外部地址。

## 3. 连接与协议

### 3.1 Agent 主连接

agent 通过 WebSocket 连到 `/agent/v1/connect`。

协议层分两层：

- `codec`
  - 只管 `ClientFrame` / `ServerFrame` 和 JSON 的互转。
- `wire`
  - 只管二进制包封装、会话路由和 secure 载荷，不负责业务语义。

server 接入顺序建议：

```text
WebSocket binary message
  -> smalux_protocol::wire::decode_wire_packet()
  -> 按 WirePacketKind 分支
     -> plain_data: payload 直接 decode ClientFrame
     -> hello: 解析 SecureHello，按 key_id 查 secret
     -> handshake: 推进 Noise responder
     -> secure_data: Noise 解密后 decode ClientFrame
     -> close: 清理连接
  -> smalux_protocol::decode_client_frame_bytes()
  -> service/agent/input/frame.rs
  -> service/agent/input/report.rs 或 control result handler
```

安全通道接入顺序：

```text
agent 发送 WirePacket(kind=hello, payload=SecureHello)
  -> server 解码 key_id
  -> server 从数据库查 agent secret
  -> server 按 smx1.<key_id>.<secret_base64url> 派生 PSK
  -> server 创建 Noise responder
  -> 双方交换 WirePacket(kind=handshake)
  -> 握手完成后只接受 WirePacket(kind=secure_data)
```

当前 `smalux-protocol` 公开的是 `secure::parse_secure_token()`；如果 server 数据库保存的是 `key_id + secret_base64url`，可以按 token 格式组装后复用该函数，后续也可以在 protocol crate 中补一个更直接的 server helper。

### 3.2 JSON Frame 总形状

`ClientFrame`：

```json
{
  "protocol_version": 1,
  "agent_id": "agent-1",
  "sequence": 1,
  "sent_at": 1710000000,
  "type": "heartbeat",
  "heartbeat": {
    "last_report_at": 1709999990,
    "last_report_sequence": 10
  }
}
```

`ServerFrame`：

```json
{
  "protocol_version": 1,
  "sequence": 2,
  "sent_at": 1710000001,
  "type": "snapshot_request",
  "request": {
    "reason": "manual_refresh"
  }
}
```

顶层字段：

- `protocol_version`
  - 协议版本，当前由 `SMALUX_PROTOCOL_VERSION` 统一控制。
- `agent_id`
  - agent 侧实例 ID。
- `sequence`
  - 消息序号。
- `sent_at`
  - Unix 时间戳，单位秒。
- `target_agent_id`
  - 仅 `ServerFrame` 使用。
  - 只做路由保护，不做认证。
- `type`
  - payload 类型。

## 4. ClientFrame payload

agent 发往 server 的 payload：

- `snapshot`
  - 字段：`report`
  - 完整 `AgentReport`。
- `heartbeat`
  - 字段：`heartbeat`
  - 低成本在线心跳。
- `delta`
  - 字段：`delta`
  - 增量监控上报。
- `ack`
  - 字段：`ack`
  - 对 server 消息的确认。
- `error`
  - 字段：`error`
  - 协议级错误。
- `remote_task_result`
  - 字段：`result`
  - 远程非交互任务结果。
- `job_result`
  - 字段：`result`
  - 通用远程 job 结果。

### 4.1 `Heartbeat`

- `last_report_at`
- `last_report_sequence`

### 4.2 `DeltaReport`

- `base_sequence`
- `report_at`
- `identity`
- `core`
- `disk`
- `network`
- `processes`
- `sockets`

语义：

- 字段缺省表示“相对上一份状态无变化”。
- 采集组字段出现 `null` 表示该组被关闭。

## 5. ServerFrame payload

server 发往 agent 的 payload：

- `snapshot_request`
  - 字段：`request`
  - 请求完整快照。
- `ack`
  - 字段：`ack`
  - 协议确认。
- `error`
  - 字段：`error`
  - 协议错误。
- `config_patch`
  - 字段：`patch`
  - 动态配置 patch，使用 JSON `Value` 承载。
- `collect_processes_once`
  - 字段：`request`
  - 一次性进程采集。
- `collect_sockets_once`
  - 字段：`request`
  - 一次性 socket 采集。
- `remote_task_run`
  - 字段：`request`
  - 一次性远程任务。
- `job_apply`
  - 字段：`request`
  - 通用持续 job 下发。
- `remote_shell_open`
  - 字段：`request`
  - 打开交互式远程 shell stream。

### 5.1 `SnapshotRequest`

- `reason`
  - 可选原因字符串。

### 5.2 `MetricCollectionRequest`

- `level`
  - 本次采集级别，缺省使用 agent 当前配置。
- `limit`
  - 本次返回条数上限，缺省使用 agent 当前配置。

### 5.3 控制字段

`Ack`：

- `sequence`
  - 被确认的对端消息序号。

`ProtocolError`：

- `sequence`
  - 可选，被错误关联的对端消息序号。
- `code`
  - 稳定错误码。
- `message`
  - 日志和调试说明。

### 5.4 `config_patch` 边界

`config_patch.patch` 是 agent 运行期部分更新，不是完整配置替换。

当前 agent 允许的顶层 patch 分组：

- `core`
- `disk`
- `network`
- `processes`
- `sockets`
- `public_ip`
- `report`
- `outbound`
- `remote_shell`
- `remote_task`
- `remote_probe`
- `export`

约束：

- 未出现的分组保持原值。
- 分组内未出现的字段保持原值。
- 未知字段会被 agent 拒绝，不会静默忽略。
- 日志字段不支持运行时 patch。
- remote shell / remote task 的“是否启用”是 CLI-only，server patch 只能改运行限制。
- remote probe 可以由 server patch 动态开启或关闭。

### 5.5 ack / result 关联

- `ack.sequence`
  - 指向被确认的 `ServerFrame.sequence`。
- `error.sequence`
  - 指向失败的 `ServerFrame.sequence`，没有关联消息时可为空。
- `remote_task_result.task_id`
  - 指向 `remote_task_run.request.task_id`。
- `job_result(kind=probe).result.request_id`
  - 指向 `job_apply.operation=once` 中的 probe `request_id`。
- `job_result(kind=probe).result.job_id`
  - 指向持续 probe job 的 `job_id`。
- `job_result(kind=probe).result.point_id`
  - 原样带回 server 下发的业务探测点 ID，方便 server 关联 UI/数据库中的探测点。

## 6. 远程 task

`remote_task_run` 适合一次性、非交互命令。

### 6.1 `RemoteTaskRequest`

- `task_id`
- `program`
- `args`
- `timeout`

### 6.2 `RemoteTaskResult`

- `task_id`
- `status`
- `exit_code`
- `stdout`
- `stderr`
- `started_at`
- `finished_at`
- `duration_ms`
- `timed_out`
- `stdout_truncated`
- `stderr_truncated`
- `error`

`RemoteTaskStatus`：

- `success`
- `failed`
- `timed_out`
- `rejected`

## 7. 远程 job / probe

### 7.1 `RemoteJobApplyRequest`

- `operation`
  - `once`
  - `replace`
  - `patch`
- `generation`
  - 代际号，用于 server 乱序保护。
- `runs`
  - 一次性运行请求列表。
- `jobs`
  - 持续 job 整组替换列表。
- `upsert_jobs`
  - 持续 job 增量 upsert 列表。
- `remove_job_ids`
  - 持续 job 删除列表。

### 7.2 `RemoteJobRunRequest::Probe`

- `request_id`
- `point_id`
- `probe_type`
- `target`
- `timeout`

### 7.3 `RemoteJobSpec::Probe`

- `job_id`
- `point_id`
- `enabled`
- `probe_type`
- `target`
- `interval`
- `timeout`

### 7.4 `RemoteProbeResult`

- `run_id`
- `source`
- `point_id`
- `request_id`
- `job_id`
- `probe_type`
- `target`
- `status`
- `latency_ms`
- `started_at`
- `finished_at`
- `duration_ms`
- `error`

`RemoteProbeId` 只接受字符串或整数。

`RemoteProbeResultStatus`：

- `success`
- `failed`
- `rejected`

## 8. 远程 shell

### 8.1 `RemoteShellOpenRequest`

- `session_id`
- `stream_url`
- `cols`
- `rows`

### 8.2 `RemoteShellStreamCommand`

- `input`
  - `data`
  - `encoding`
- `resize`
  - `cols`
  - `rows`
- `close`
- `heartbeat`

### 8.3 `RemoteShellStreamEvent`

- `opened`
  - `session_id`
- `output`
  - `session_id`
  - `data`
  - `encoding`
- `exit`
  - `session_id`
  - `code`
- `error`
  - `session_id`
  - `message`

`RemoteShellDataEncoding`：

- `utf8`
- `base64`

## 9. 二进制 wire 和 secure

### 9.1 `WirePacket`

固定头：

- `magic`
  - 固定 `SMX1`。
- `version`
  - 当前 `1`。
- `kind`
- `flags`
- `session_id`
- `sequence`
- `payload_len`
- `payload`

字节序：

- `flags`
- `sequence`
- `payload_len`

以上整数均使用 big-endian。

`WirePacketKind`：

- `1 = plain_data`
- `2 = hello`
- `3 = handshake`
- `4 = secure_data`
- `5 = close`

限制：

- 单个 wire payload 最大 `1 MiB`。

### 9.2 secure token

安全 token 格式：

```text
smx1.<key_id>.<secret_base64url>
```

约定：

- `key_id` 只用于查找 secret 和参与 HKDF info。
- `secret` 不直接发送。
- `secret` base64url 解码后至少 `32` 字节。
- token 解析后派生 Noise PSK。

HKDF 参数：

- `PSK_LEN = 32`
- `salt = "smalux secure psk v1 salt"`
- `info = "smalux secure psk v1 " + key_id`

### 9.3 Noise

当前 pattern：

```text
Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s
```

约定：

- `Hello` 只携带 `key_id` 和 pattern。
- `Handshake` 之后才进入 `SecureData`。
- `wire` 不负责认证，真正可信数据在 Noise 解密后。
