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

## Server 对接流程

server 按下面顺序实现，最容易先跑通闭环：

```text
1. 连接建立
   -> WebSocket / HTTP upgrade
   -> 按 transport 层规则校验 query / Authorization

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

## 兼容边界

- `snapshot_request`、`remote_probe_run` 现在属于稳定 `ServerFrame`。
- `config_patch`、`collect_processes_once`、`collect_sockets_once`、`remote_shell_open`、`remote_task_run` 目前属于 agent 兼容 JSON，不是这个 crate 的稳定协议面。
- `ack/error` 只对带 `sequence` 的 `ServerFrame` 有意义。
- server 如果没有 delta 基准，就应该发 `snapshot_request`，不要猜测补齐。
- server 不要做字段级深度 merge，`snapshot` 是完整替换，`delta` 是顶层采集组替换。

## 扩展原则

- 先保持文件少，等单文件职责不再清晰后再拆。
- `snapshot`、`delta`、`heartbeat` 已落地；后续再加 capability、第三方兼容格式。
- transport 放到 agent/server 自己的模块中，不放进本 crate。

## 常用命令

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol
```
