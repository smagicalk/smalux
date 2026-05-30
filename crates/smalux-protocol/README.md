# smalux-protocol

`smalux-protocol` 是 agent 和 server 共享的通信协议 crate。

## 当前职责

- 定义传输无关的 frame，例如 `ClientFrame` 和 `ServerFrame`。
- 定义传输无关的内部上报语义，例如 `OutboundReport`。
- 定义首版上报 payload，例如 `snapshot`、`delta` 和 `heartbeat`。
- 提供 JSON codec，供 WebSocket、HTTP 或后续 gRPC adapter 复用。
- 维护协议版本、sequence 和基础 ack/error 结构。

## 不负责的内容

- 不实现 WebSocket、HTTP 或 gRPC 连接。
- 不做 agent 本机采集。
- 不做 server 存储、查询或鉴权。
- 不放 `tonic` / `prost` 生成代码；后续需要 gRPC 时再新增独立 crate。

## 目录结构

```text
src/
  lib.rs      # crate 入口和类型重导出
  frame.rs    # OutboundReport / ClientFrame / ServerFrame / payload
  codec.rs    # OutboundReport -> smalux_json / JSON decode
```

## 扩展原则

- 先保持文件少，等单文件超过清晰职责后再拆。
- `snapshot`、`delta` 和 `heartbeat` 已落地；`capability`、第三方兼容格式后续再加。
- transport 放到 agent/server 自己的模块中，不放进本 crate。

## 常用命令

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol
```
