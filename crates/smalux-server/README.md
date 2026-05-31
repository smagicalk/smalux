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

## Agent 上报接入设计

首版 server 先做“接收并保存最新快照”，不急着做历史时序库。这样可以先把 agent 到 server 的协议闭环跑通，再根据 UI 和查询需求决定是否落库、如何分表、是否保留明细历史。

完整上报 JSON 参数见 `crates/smalux-agent/README.md` 的 `ClientFrame` 和 `AgentReport` JSONC 示例，稳定 frame 规则见 `crates/smalux-protocol/README.md`。server 侧只接收实际标准 JSON，不接收文档里的注释。

### 接入入口

建议首版使用 WebSocket：

```text
GET /ws
  -> WebSocket upgrade
  -> 按 query token / bearer token 做连接级识别
  -> 接收 smalux binary wire frame；开发兼容模式可接收 text frame
  -> binary_plain: WirePacket(PlainData).payload 得到 JSON bytes
  -> secure_psk: Hello + Noise 握手后，WirePacket(SecureData).payload 解密得到 JSON bytes
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
agent connects /ws
  -> server 校验连接凭证
  -> server 按 wire_mode 解包 JSON bytes
  -> server decode ClientFrame
  -> server 按 ClientFrame.type 分发
     -> snapshot: 保存完整最新状态
     -> delta: 校验 base_sequence 后按采样组覆盖
     -> heartbeat: 更新业务在线时间
     -> ack/error: 关联 server 下发的 ServerFrame.sequence
     -> remote_task_result: 更新任务结果
     -> remote_probe_result: 更新探测结果
  -> server 需要控制 agent 时，按当前 wire_mode 发送 ServerFrame 或 raw control JSON
```

`secure_psk` 模式下，server 需要保存 `key_id -> secret`。收到 agent 的 `Hello` 后，用同样 HKDF-SHA256 参数派生 32 字节 PSK，再以 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` responder 身份回复第二条 handshake。握手成功后，所有业务 JSON 都必须先加密再放入 `SecureData`。

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

认证和授权后续单独设计。当前可以先只支持本地开发无认证，或临时使用 WebSocket 握手里的 query/bearer token 做简单识别。

### 控制消息

server 通过同一条 Smalux WebSocket 控制通道下发 JSON。当前有两种外层：

- `ServerFrame`：当前稳定支持 `snapshot_request` 和 `remote_probe_run`，带 server `sequence`，agent 调度后回 `ack/error`。
- raw control JSON：当前支持 `config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_shell_open`、`remote_task_run` 和 raw `remote_probe_run`，没有 `sequence`，agent 不会自动回控制 ack。

第一版 server 建议先实现：

- `ServerFrame(type=snapshot_request)`：当 server 没有完整状态、delta 基准不匹配或用户主动刷新时发送。
- raw `config_patch`：动态调整 `AgentConfig` 中的采集、上报和导出参数。
- `ack/error` 接收：只用于确认 agent 是否接收并调度了带 `sequence` 的控制命令。

后续再接：

- `collect_processes_once`：请求 agent 立即采样一次进程信息，结果进入下一次 snapshot/delta。
- `collect_sockets_once`：请求 agent 立即采样一次 socket 信息，结果进入下一次 snapshot/delta。
- `remote_shell_open`：打开远程交互式 shell，前提是 agent 启动时显式开启。
- `remote_task_run`：执行一次非交互命令，前提是 agent 启动时显式开启。
- `remote_probe_run`：执行一次 TCP/HTTP 探测；默认关闭，但可以通过 `config_patch.remote_probe.enabled=true` 动态开启。

server 如果要远程打开 `processes.level=details` 或 `sockets.level=details`，agent 必须启动时带对应 CLI-only 授权：`--allow-process-details true` 或 `--allow-socket-details true`。一次性 details 采集同样受这个限制。

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

### 实现顺序

建议按下面顺序写代码：

1. 在 `storage.rs` 定义 latest-only 存储 trait 和内存实现。
2. 在 `ingest.rs` 实现 `validate_report()` 和 `handle_report()`。
3. 在 `http.rs` 增加 `/ws` WebSocket handler。
4. 增加本地测试：合法 report 写入成功、schema 不匹配失败、同 agent 覆盖旧快照。
5. 再补 `GET /agents` 和 `GET /agents/{agent_id}` 查询接口。

## 常用命令

```powershell
cargo check -p smalux-server
cargo test -p smalux-server
cargo run -p smalux-server
```
