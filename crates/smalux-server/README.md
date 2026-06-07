# smalux-server

`smalux-server` 是接收、存储和查询 agent 上报数据的服务端进程。

## 当前职责

- 初始化 server 日志。
- 预留 HTTP 路由、agent 接入、查询和存储模块。
- 后续接收 `smalux-agent` 上报的 `ClientFrame`，完成校验、标准化、持久化和查询。

## 目录结构

```text
src/
  main.rs      # 二进制入口，当前只初始化日志
  config.rs    # server 运行配置
  http.rs      # axum router、handler、中间件
  ingest.rs    # agent 上报接入和校验
  storage.rs   # 持久化接口和数据库适配
  query.rs     # 查询读模型和 API 编排
```

## 后续接入点

- `http.rs`: 构建 axum router。
- `ingest.rs`: 接收 `ClientFrame`，校验 agent 身份和 payload。
- `storage.rs`: 定义存储 trait，接入数据库。
- `query.rs`: 面向仪表盘或 API 提供查询模型。

建议职责边界：

- `http.rs` 只处理 HTTP/WebSocket 框架细节：路由、upgrade、请求参数、响应码、连接超时。
- `ingest.rs` 只处理协议语义：decode `ClientFrame`、校验、snapshot/delta/heartbeat 分发、控制响应关联。
- `storage.rs` 只处理持久化：latest state、pending command、remote task/probe result，不直接依赖 axum。
- `query.rs` 只处理读模型：把 storage 数据转换成 API 返回结构，不直接解析 wire frame。
- `config.rs` 只放 server 启动参数，例如监听地址、数据库路径、wire mode、token/secret 加载方式。

这样后续增加 REST、gRPC 或 Web UI 时，不需要重写 agent 上报接入逻辑；只新增入口层或查询层。

## Agent 上报接入设计

首版 server 先做“接收并保存最新快照”，不急着做历史时序库。这样可以先把 agent 到 server 的协议闭环跑通，再根据 UI 和查询需求决定是否落库、如何分表、是否保留明细历史。

完整上报 JSON 参数见 `crates/smalux-agent/README.md` 的 `ClientFrame` 和 `AgentReport` JSONC 示例，稳定 frame、wire packet 和 `secure_psk` 规则见 `crates/smalux-protocol/README.md`。server 侧只接收实际标准 JSON，不接收文档里的注释。

### 接入入口

建议首版使用 WebSocket：

```text
GET /api/agents/connect
  -> WebSocket upgrade
  -> 按 wire mode 做连接级识别；binary_plain 可用 query/bearer，secure_psk 不使用明文 token
  -> 接收 smalux binary wire frame；开发兼容模式可接收 text frame
  -> binary_plain: WirePacket(PlainData).payload 得到 JSON bytes
  -> secure_psk: Hello + Noise 握手后，WirePacket(SecureData).payload 解密得到 JSON bytes
  -> wire/secure 直接复用 smalux_protocol::{wire, secure}
  -> smalux_protocol::decode_client_frame()
  -> ingest::validate_report()
  -> storage::save_latest_report()
```

后续如果加 gRPC，不改变 `ingest` 和 `storage` 的领域接口，只新增 transport adapter：

```text
WebSocket adapter ┐
HTTP adapter      ├─> ingest::handle_report(report)
gRPC adapter      ┘
```

### 交换流程

server 第一版按下面流程写，能覆盖 agent 当前自有协议闭环：

```text
agent connects /api/agents/connect
  -> server 按 wire mode 选择连接识别方式
     -> binary_plain: 可按 query token / bearer token / none 识别
     -> secure_psk: 先只接收 Hello，Noise 握手成功后才算认证通过
  -> server 按 wire_mode 解包 JSON bytes
  -> server decode ClientFrame
  -> server 按 ClientFrame.type 分发
     -> snapshot: 保存完整最新状态
     -> delta: 校验 base_sequence 后按采样组覆盖
     -> heartbeat: 更新业务在线时间
     -> ack/error: 关联 server 下发的 ServerFrame.sequence
     -> remote_task_result: 更新任务结果
     -> remote_probe_result: 更新探测结果
  -> server 需要控制 agent 时，按当前 wire_mode 发送 ServerFrame
```

`secure_psk` 模式下，server 需要保存 `key_id -> secret`。收到 agent 的 `Hello` 后，直接使用 `smalux_protocol::secure` 里的共享实现解析 token、派生 32 字节 PSK，并以 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` responder 身份回复第二条 handshake。握手成功后，所有业务 JSON 都必须先加密再放入 `SecureData`。

server 不应重复手写下面这些参数，优先调用 `smalux_protocol::secure`；如果后续用其它语言实现 server，也必须使用同样精确参数派生 PSK：

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

HKDF 测试向量，server 第一版必须覆盖：

```text
key_id                = "agent-key"
secret bytes          = 32 bytes of 0x07
secret_base64url      = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
token                 = "smx1.agent-key.BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
derived_psk_hex       = "a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2"
derived_psk_base64url = "plsq_xK2fp0l-ucJSyQkgTOgQ6Hy8roWFXJ5gGstYqI"
```

server 测试时不要直接拿 token 里的 secret 当 Noise PSK。正确流程是：从 Hello 读取 `key_id`，查询 server 保存的 `secret_base64url`，base64url 解码出原始 secret bytes，再按上面 HKDF 参数派生 `derived_psk`，最后放入 Noise `psk(0)`。

`key_id` 只用于查 secret 和参与 HKDF info，不能当成认证已通过。只有 Noise 握手能用派生 PSK 成功完成时，server 才能把连接状态切到 ready。server 日志只能记录 `key_id`、wire kind、session id 和错误码，不要打印完整 token、secret、PSK、Authorization header 或带 token 的 URL。

### 连接状态机

server 侧可以把每条 agent 主连接按下面状态管理：

```text
accepted
  -> authenticating        # binary_plain 校验 query/bearer/none；secure_psk 校验是否允许该 wire mode
  -> wire_negotiating      # binary_plain 直接进入 ready；secure_psk 用 key_id 查 secret 并完成 Noise 握手
  -> ready                 # 可以收 ClientFrame，也可以下发控制消息
  -> closing               # 收到 close、读写失败或协议错误
  -> disconnected          # 清理内存连接态，latest snapshot 可保留
```

建议把“连接态”和“监控最新状态”分开保存。连接断开只清理 WebSocket sink、Noise transport、未完成的请求等待器，不删除 `latest_report`；这样 UI 还能显示最后一次上报和离线时间。

### Wire 解包伪代码

server 的 WebSocket binary 处理逻辑可以按这个顺序写：

```text
on_binary(bytes):
  packet = decode_wire_packet(bytes)
  assert packet.magic == "SMX1"
  assert packet.version == 1
  assert packet.payload_len == bytes.len - 36

  if mode == binary_plain:
    assert packet.kind == PlainData
    json_bytes = packet.payload

  if mode == secure_psk:
    if state == waiting_hello:
      assert packet.kind == Hello
      assert packet.sequence == 0
      hello = json_decode(packet.payload)
      assert hello.pattern == "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s"
      session_id = packet.session_id
      secret = lookup_secret(hello.key_id)
      psk = hkdf_sha256(secret, key_id=hello.key_id)
      start_noise_responder(psk)
      state = waiting_handshake

    else if state == waiting_handshake:
      assert packet.kind == Handshake
      assert packet.session_id == session_id
      assert packet.sequence == 1
      read_noise_msg1(packet.payload, handshake_payload=b"")
      msg2 = write_noise_msg2(handshake_payload=b"")
      send WirePacket(kind=Handshake, same session_id, payload=msg2)
      state = ready

    else if state == ready:
      assert packet.kind == SecureData
      assert packet.session_id == session_id
      json_bytes = noise_decrypt(packet.payload)

  text = utf8(json_bytes)
  route_json(text)
```

`binary_plain` 开发期可以允许 WebSocket text frame 直接进入 `route_json()`；`secure_psk` 不允许 text frame，因为 text 会绕过业务加密。

WirePacket 固定头和 agent 一致，所有整数都是 big-endian：

```text
magic       4 bytes   "SMX1"
version     1 byte    当前固定 1
kind        1 byte    1 PlainData, 2 Hello, 3 Handshake, 4 SecureData, 5 Close
flags       2 bytes   当前保留，写 0
session_id 16 bytes   当前连接或 stream 的随机 session id
sequence    8 bytes   业务消息序号；Hello=0，首个 Handshake=1
payload_len 4 bytes   payload 长度
payload     N bytes   明文 JSON、Noise 握手消息或密文
```

`payload_len` 最大为 `1 MiB`，超过应按连接级错误处理。`flags` 当前固定写 `0`；首版 server 可以拒绝非 0 flags，后续如果 wire 版本扩展再放宽。`session_id` 在同一条连接内必须一致：Hello 建立 session，Handshake、SecureData 和 server 回握手包都沿用同一个 `session_id`。

### 消息格式

agent 内部先生成 `OutboundReport::Snapshot`，当前默认 `smalux_json` 格式会把它编码成 `ClientFrame::Snapshot` JSON，snapshot payload 内包含完整 `AgentReport`。核心结构：

```jsonc
{
  "protocol_version": 1, // server 必须先校验通信协议版本
  "agent_id": "agent-1", // server 侧连接和最新快照主键
  "sequence": 1, // agent 侧递增消息序号
  "sent_at": 1710000000, // agent 发送 frame 的 Unix 秒
  "type": "snapshot",
  "report": {
    "meta": {
      "schema_version": 5, // server 必须校验上报模型版本
      "agent_version": "0.1.0",
      "report_at": 1710000000
    },
    "identity": {
      "agent_id": "agent-1",
      "hostname": "host-1",
      "public_ip": {
        "status": "ready" // ready | failed | stale | disabled | pending
      },
      "local_ips": []
    },
    "system": {}
  }
}
```

### Frame 分发语义

server 不应该把所有 payload 都当成完整指标：

- `snapshot`：完整状态，直接覆盖该 agent 的 latest state，并把当前 frame `sequence` 作为 delta 基准。
- `delta`：只包含变化的顶层采集组；server 只做顶层组替换，不做字段级 merge；如果 `base_sequence` 不匹配，应下发 `snapshot_request`。
- `heartbeat`：只说明 agent 业务上仍在线，不修改 CPU、磁盘、网络等指标。
- `ack` / `error`：只关联 server 之前下发的 `ServerFrame.sequence`，不代表远程任务或探测已经完成。
- `remote_task_result`：通过 `result.task_id` 关联非交互任务。
- `remote_probe_result`：通过 `result.task_id` 关联网络探测；`value=-1` 表示失败、禁用、限频或暂不支持。

### Delta 合并伪代码

server 只需要保存一份 latest state 和一个 delta 基准序号：

```text
on_snapshot(frame):
  latest_report = frame.report
  base_sequence = frame.sequence
  last_seen_at = now()

on_delta(frame):
  if latest_report is None:
    send_snapshot_request("missing_snapshot")
    return

  if frame.delta.base_sequence != base_sequence:
    send_snapshot_request("delta_base_mismatch")
    return

  for group in [identity, core, disk, network, processes, sockets]:
    if group field is absent:
      keep existing group
    else if group field is null:
      clear existing group
    else:
      replace whole group with incoming group

  base_sequence = frame.sequence
  last_seen_at = now()
```

`identity` 是对象，不是 `Option<Option<...>>`；它出现时整体替换，不出现时保持旧值。`core/disk/network/processes/sockets` 出现 `null` 时表示该采集组被关闭。

### 校验规则

`ingest.rs` 首版只做轻量 fail-fast 校验：

- `protocol_version` 必须等于当前支持版本 `1`。
- `agent_id` 必须非空，并且 snapshot 中 `report.identity.agent_id` 应与 frame 顶层 `agent_id` 一致。
- `sequence` 必须大于 `0`，同一 agent 后续可用于判断跳号、乱序或 delta 基准。
- `sent_at` 必须大于 `0`。
- `type=snapshot` 时必须包含 `report`。
- `report.meta.schema_version` 必须等于当前支持版本 `5`。
- `report.meta.agent_version` 不能为空。
- `report.meta.report_at` 必须大于 `0`。
- `report.identity.hostname` 允许为空但建议记录 warn，部分平台可能取不到主机名。
- `report.identity.public_ip.status=ready` 或 `stale` 时，`ip` 应存在。
- `report.identity.public_ip.status=failed` 时，`error` 应存在。
- `report.core`、`report.disk`、`report.network`、`report.processes`、`report.sockets` 都是可选分组；缺失表示 agent 侧禁用或尚未启用，不视为错误。
- `report.disk.value.disks=[]` 和 `report.network.value.networks=[]` 是合法状态，表示只上报汇总。
- `report.processes.value.level` / `report.sockets.value.level` 可能为 `count`、`light` 或 `details`；server 需要按字段是否存在处理 `light/details`，不要假设每次都有明细。

认证和授权后续单独设计。当前实现应明确分成三类：`none` 只用于本地开发或可信内网；query/bearer 只用于 `binary_plain` 这类兼容明文识别；`secure_psk` 通过 `key_id -> secret` 和 Noise 握手完成认证，不能再叠加 query/bearer token。

### 错误处理

server 收到异常数据时要区分“单条消息错误”和“连接级错误”，不要因为某个可忽略业务字段导致主连接频繁断开。

建议规则：

- 连接级错误：wire magic/version 错误、`secure_psk` 握手失败、token 或 key_id 不存在、密文解不开、payload 超过上限。这类错误应关闭连接。
- frame 级错误：JSON 语法错误、缺少 `protocol_version` / `agent_id` / `sequence` / `type`、`protocol_version` 不支持。这类错误记录后可以关闭连接，避免双方状态继续错位。
- 业务级错误：`delta.base_sequence` 不匹配、未知 `type`、未知可选字段、单个采集组字段不完整。这类错误优先记录日志和指标；`delta` 不匹配时发送 `snapshot_request`，未知字段默认忽略。
- 控制级错误：server 发出的 `ServerFrame` 收到 `error.sequence` 时，只把对应 pending command 标记为失败，不要关闭主连接。

建议给日志打上固定字段，方便后续排查：

```text
agent_id
connection_id
client_sequence
server_sequence
frame_type
wire_mode
error_code
error_message
```

### 控制消息

server 通过同一条 Smalux WebSocket 控制通道下发 `ServerFrame`。当前只保留这一种控制入口，避免不同命令有的回 ack、有的不回 ack，导致 server 状态难以维护。

当前稳定 `ServerFrame` 支持：

- `snapshot_request`
- `config_patch`
- `collect_processes_once`
- `collect_sockets_once`
- `remote_shell_open`
- `remote_task_run`
- `remote_probe_run`

第一版 server 建议先实现：

- `ServerFrame(type=snapshot_request)`：当 server 没有完整状态、delta 基准不匹配或用户主动刷新时发送。
- `ServerFrame(type=config_patch)`：动态调整 `AgentConfig` 中的采集、上报和导出参数。
- `ack/error` 接收：只用于确认 agent 是否接收并调度了带 `sequence` 的控制命令。

后续再接：

- `collect_processes_once`：请求 agent 立即采样一次进程信息，结果进入下一次 snapshot/delta。
- `collect_sockets_once`：请求 agent 立即采样一次 socket 信息，结果进入下一次 snapshot/delta。
- `remote_shell_open`：打开远程交互式 shell，前提是 agent 启动时显式开启。
- `remote_task_run`：执行一次非交互命令，前提是 agent 启动时显式开启。
- `remote_probe_run`：执行一次 TCP/HTTP 探测；默认关闭，但可以通过 `config_patch.remote_probe.enabled=true` 动态开启。

server 如果要远程打开 `processes.level=details` 或 `sockets.level=details`，agent 必须启动时带对应 CLI-only 授权：`--allow-process-level details` 或 `--allow-socket-level details`。一次性 details 采集同样受这个限制。

控制消息发送规则：

- 发送 `ServerFrame` 前先分配 server 侧递增 `sequence`，保存一条 pending command。
- 收到 `ack.sequence` 后，只能把该 command 标记为“已调度”；不能把远程 task/probe 标记为完成。
- 收到 `error.sequence` 后，把该 command 标记为失败，并记录 `error.code` 和 `error.message`。
- 重连后不要盲目重发所有有副作用命令。`config_patch` 可以按当前 desired config 重发；`remote_task_run` 这类有副作用的命令必须靠 `task_id` 去重。

### 控制消息示例

请求完整快照：

```jsonc
{
  "protocol_version": 1,
  "sequence": 201,
  "sent_at": 1710001000,
  "type": "snapshot_request",
  "request": { "reason": "manual_refresh" }
}
```

动态调整采样频率：

```jsonc
{
  "type": "config_patch",
  "patch": {
    "core": { "interval": "2s" },
    "network": { "interval": "10s" },
    "report": { "interval": "10s" },
    "outbound": {
      "realtime_report": { "send_on_start": true },
      "basic_info": { "refresh_interval": "5m" }
    }
  }
}
```

开启远程 probe 并请求 TCP 探测：

```jsonc
{ "type": "config_patch", "patch": { "remote_probe": { "enabled": true } } }
```

```jsonc
{
  "protocol_version": 1,
  "sequence": 202,
  "sent_at": 1710001001,
  "type": "remote_probe_run",
  "request": {
    "task_id": "probe-1",
    "probe_type": "tcp",
    "target": "example.com:443"
  }
}
```

### Server 内部状态建议

首版 server 不需要一开始就做复杂领域模型，但建议把下面几类状态分开：

```text
AgentConnectionState
  agent_id
  connection_id
  connected_at
  last_frame_at
  wire_mode
  secure_key_id
  websocket_sink
  noise_transport

AgentLatestState
  agent_id
  last_seen_at
  last_frame_sequence
  delta_base_sequence
  latest_report
  last_heartbeat_at
  connection_state

PendingCommand
  server_sequence
  agent_id
  command_type
  sent_at
  status          # queued | sent | acked | failed | timed_out
  error_code
  error_message

PendingRemoteTask
  task_id
  agent_id
  sent_at
  status          # sent | running | success | failed | timed_out | rejected
  result

PendingRemoteProbe
  task_id
  agent_id
  sent_at
  status          # sent | success | failed | rejected
  result
```

这样拆分后，WebSocket 重连不会影响最新监控状态；server 下发命令的 ack/error 也不会和 remote task/probe 的最终结果混在一起。

### 幂等和重连策略

server 需要把三类数据分开处理：

- 最新状态：`snapshot` / `delta` / `heartbeat`。只保存最新状态，旧 report 不排队，防止高频 agent 把 server 内存打满。
- 一次性结果：`ack` / `error` / `remote_task_result` / `remote_probe_result`。用 `sequence` 或 `task_id` 关联 pending 记录，可重复接收同一结果并做幂等覆盖。
- 控制命令：server 主动发送给 agent。`config_patch` 可以在重连后按 desired config 重新下发；`remote_task_run` 这类有副作用的命令不要自动重发，除非 server 能根据 `task_id` 确认 agent 没有执行过。

建议规则：

- 同一 agent 的 `ClientFrame.sequence` 小于等于已处理序号时，记录为重复或乱序，默认忽略。
- 收到 `delta.base_sequence != delta_base_sequence` 时，不处理该 delta，立即发送 `snapshot_request`。
- 收到新的 `snapshot` 后，用它重建 latest state，并把 `delta_base_sequence` 设置为该 frame 的 `sequence`。
- 收到 `heartbeat` 时只更新 `last_heartbeat_at` 和 `last_seen_at`，不要覆盖指标。
- WebSocket 断开时，把连接态改成 disconnected，但保留 `latest_report` 和 pending 任务结果等待状态。
- pending command 超时只说明 agent 没回 ack/error，不代表命令一定没执行；对有副作用命令要靠业务结果或人工确认。

### 安全底线

即使首版只用于自用，也建议先固定下面的底线，避免后面补安全时推翻协议：

- 生产环境优先使用 `wss`；如果用 `ws`，至少限制在可信内网。
- `secure_psk` 模式下不要同时使用 query/bearer token，agent 当前也会拒绝这种组合，避免 token 明文出现在 URL 或 header。
- 如果 agent 当前 `export.secure_required=true`，server 不要下发关闭 `secure_required`、切到 `binary_plain` 或切到 `komari` 的 patch；agent 会拒绝这类降级。
- server 日志不要打印完整 token、secure secret、PSK、Authorization header、带 token 的 URL。
- `key_id` 只能用于查 secret，不是认证成功本身；认证成功发生在 Noise 握手能完成时。
- `remote_task_run` / `remote_shell_open` 默认不要在 UI 中暴露，必须确认 agent 启动时显式开启。
- server 下发 details 采集前，先确认 agent 启动时开启了 `--allow-process-level details` 或 `--allow-socket-level details`。
- 对单 agent 和单连接做基础频率限制，尤其是 `snapshot_request`、`remote_probe_run` 和未来的 remote task。

### 测试清单

server 第一版建议至少覆盖这些测试：

| 类型 | 场景 | 期望 |
| --- | --- | --- |
| wire | `PlainData` 正常解包 | 得到 JSON bytes |
| wire | magic/version/payload_len 错误 | 拒绝 frame，不 panic |
| wire | payload 超过 `1 MiB` | 关闭连接或拒绝 frame |
| wire | flags 非 0 | 首版拒绝，避免未知语义 |
| secure | HKDF 测试向量 | 派生 PSK 等于 `a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2` |
| secure | Hello pattern 不支持 | 关闭连接，不能进入 ready |
| secure | Hello key_id 不存在 | 关闭连接或返回协议错误 |
| secure | Handshake / SecureData 的 session_id 不匹配 | 关闭连接 |
| secure | secret base64url 解码失败或不足 32 字节 | 拒绝配置或拒绝连接 |
| secure | PSK 不匹配 | Noise 握手失败，不能进入 ready |
| secure | `SecureData` AEAD 解密失败 | 关闭连接 |
| secure | secure_psk 收到 text frame | 拒绝并关闭连接 |
| secure | server 下发控制 JSON | 先 Noise encrypt，再封 `WirePacket(kind=SecureData)` |
| frame | `snapshot` 写入 | latest state 被完整覆盖 |
| frame | `delta.base_sequence` 匹配 | 顶层采集组整体替换 |
| frame | `delta.base_sequence` 不匹配 | 不修改 latest，发送 `snapshot_request` |
| frame | `heartbeat` | 只更新在线时间，不修改指标 |
| control | `snapshot_request` ack | pending command 标记为 acked |
| control | `snapshot_request` error | pending command 标记失败并保存错误 |
| task | 重复 `remote_task_result.task_id` | 幂等覆盖，不创建重复记录 |
| reconnect | agent 断开重连后发 snapshot | connection state 更新，latest state 正常覆盖 |

### 存储策略

首版使用内存 latest-only 缓存：

```text
HashMap<agent_id, AgentRuntimeState>
```

建议状态结构：

```text
AgentRuntimeState
  agent_id
  last_seen_at        # server 收到上报的时间
  last_report_at      # agent payload 里的 meta.report_at
  agent_version
  hostname
  public_ip_status
  latest_report       # 完整 AgentReport
```

写入语义：

- 同一个 `agent_id` 的新 report 覆盖旧 report。
- 不做 report 队列，避免 server 因 agent 高频上报堆积。
- 如果后续需要历史曲线，再把 `core/disk/network/processes/sockets` 拆成时序写入，不影响 latest 缓存。

如果首版就接 SQLite，建议仍然先保持 latest-only 思路，把“在线最新状态”和“历史曲线”分开：

```text
agents
  agent_id TEXT PRIMARY KEY
  display_name TEXT NULL
  created_at INTEGER NOT NULL
  updated_at INTEGER NOT NULL

agent_connections
  connection_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  connected_at INTEGER NOT NULL
  disconnected_at INTEGER NULL
  remote_addr TEXT NULL
  wire_mode TEXT NOT NULL
  close_reason TEXT NULL

agent_latest_reports
  agent_id TEXT PRIMARY KEY
  last_seen_at INTEGER NOT NULL
  last_report_at INTEGER NOT NULL
  last_sequence INTEGER NOT NULL
  delta_base_sequence INTEGER NOT NULL
  schema_version INTEGER NOT NULL
  hostname TEXT NULL
  public_ip_status TEXT NOT NULL
  report_json TEXT NOT NULL

pending_commands
  command_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  server_sequence INTEGER NOT NULL
  command_type TEXT NOT NULL
  status TEXT NOT NULL        # sent | acked | failed | timeout
  request_json TEXT NOT NULL
  response_json TEXT NULL
  created_at INTEGER NOT NULL
  updated_at INTEGER NOT NULL

remote_task_results
  task_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  status TEXT NOT NULL
  exit_code INTEGER NULL
  stdout_truncated INTEGER NOT NULL
  stderr_truncated INTEGER NOT NULL
  result_json TEXT NOT NULL
  updated_at INTEGER NOT NULL

remote_probe_results
  task_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  probe_type TEXT NOT NULL
  target TEXT NOT NULL
  value INTEGER NOT NULL
  error TEXT NULL
  updated_at INTEGER NOT NULL
```

落库事务建议：

- `snapshot`：一个事务内更新 `agents.updated_at`、覆盖 `agent_latest_reports.report_json`、更新 `last_sequence` 和 `delta_base_sequence`。
- `delta`：先读取当前 `delta_base_sequence`；匹配才合并 JSON 并写回，不匹配不写库，只发送 `snapshot_request`。
- `heartbeat`：只更新 `last_seen_at`，不改 `report_json` 和 `delta_base_sequence`。
- `ack/error`：只更新 `pending_commands`，不要修改 latest report。
- `remote_task_result` / `remote_probe_result`：按 `task_id` upsert，重复结果覆盖同一行，保证幂等。

### 查询接口

首版查询可以先提供两个只读接口：

```text
GET /agents
  -> 返回 agent 列表和 last_seen_at、hostname、public_ip_status

GET /agents/{agent_id}
  -> 返回该 agent 的 latest_report
```

等 Web UI 需求明确后，再补：

- 按标签、主机名、公网 IP 状态筛选。
- 查询历史 CPU/内存/磁盘/网络曲线。
- 查询离线 agent 和最近错误状态。

### HTTP 端点规划

首版 server 可以按“写入入口少、查询入口清晰”的方式规划端点：

| 端点 | 方法 | 作用 | 首版是否需要 |
| --- | --- | --- | --- |
| `/api/agents/connect` | `GET` upgrade | Smalux agent 主 WebSocket，接收 `ClientFrame` 和下发控制消息 | 必须 |
| `/agents` | `GET` | 查询 agent 列表、在线状态和摘要字段 | 必须 |
| `/agents/{agent_id}` | `GET` | 查询单个 agent 的 latest report | 必须 |
| `/agents/{agent_id}/commands` | `POST` | 创建 server 控制命令，例如 `snapshot_request`、`remote_probe_run` | 可后做 |
| `/agents/{agent_id}/commands/{command_id}` | `GET` | 查询 pending command 的 ack/error 状态 | 可后做 |
| `/agents/{agent_id}/tasks/{task_id}` | `GET` | 查询 remote task 结果 | 可后做 |
| `/agents/{agent_id}/probes/{task_id}` | `GET` | 查询 remote probe 结果 | 可后做 |

端点职责建议：

- `/api/agents/connect` 不直接做复杂查询，只负责连接、解包、分发和发送控制消息。
- 查询端点只读 storage，不直接访问 WebSocket sink。
- 创建控制命令时先写 `pending_commands`，再投递到当前在线连接；如果 agent 离线，按命令类型决定是拒绝、排队还是只保存 desired config。
- `config_patch` 更像 desired config，不建议作为普通一次性命令长期排队；agent 重连后 server 可以比较 desired config 和当前 effective 状态后再下发。

### Ingest 分发伪代码

server 的 `ingest.rs` 可以把 transport 细节隔离掉，只接收已经解包出来的 JSON bytes：

```text
handle_client_json(connection, json_bytes):
  frame = decode ClientFrame(json_bytes)
  validate_common_fields(frame)

  if frame.agent_id != connection.agent_id:
    return protocol_error("agent_id_mismatch")

  match frame.type:
    snapshot:
      validate_report(frame.report)
      storage.apply_snapshot(frame.agent_id, frame.sequence, frame.report)
      connection.last_seen_at = now()

    delta:
      result = storage.apply_delta(frame.agent_id, frame.sequence, frame.delta)
      if result == DeltaBaseMismatch:
        send_server_frame(snapshot_request("delta_base_mismatch"))

    heartbeat:
      storage.touch_heartbeat(frame.agent_id, frame.sequence, frame.heartbeat)

    ack:
      storage.mark_command_acked(frame.agent_id, frame.ack.sequence)

    error:
      storage.mark_command_failed(frame.agent_id, frame.error.sequence, frame.error)

    remote_task_result:
      storage.upsert_remote_task_result(frame.agent_id, frame.result)

    remote_probe_result:
      storage.upsert_remote_probe_result(frame.agent_id, frame.result)

    unknown:
      log and ignore
```

注意 `ClientFrame.sequence` 是 agent 的全局出站序号，不是每种消息各自递增。server 可以用它判断“该连接上是否见过更新的 frame”，但不要假设连续序号一定都到达；导出 job 可能因为只发送最新 report 而跳过中间 report。

### 实现顺序

建议按下面顺序写代码：

1. 在 `storage.rs` 定义 latest-only 存储 trait 和内存实现。
2. 在 `ingest.rs` 实现 `validate_report()` 和 `handle_report()`。
3. 在 `ingest.rs` 实现 `apply_snapshot()` / `apply_delta()` / `apply_heartbeat()`。
4. 在 `http.rs` 增加 `/api/agents/connect` WebSocket handler。
5. 实现 `binary_plain` wire 解包和 `ClientFrame` 分发。
6. 增加本地测试：合法 report 写入成功、schema 不匹配失败、同 agent 覆盖旧快照、delta base 不匹配会请求 snapshot。
7. 再补 `GET /agents` 和 `GET /agents/{agent_id}` 查询接口。
8. 增加 `pending_commands` 和 desired config 状态，支持手动发送 `snapshot_request` 和 `config_patch`。
9. 最后接 `secure_psk`、remote task/probe/shell、历史指标落库和 Web UI。

## 常用命令

```powershell
cargo check -p smalux-server
cargo test -p smalux-server
cargo run -p smalux-server
```
