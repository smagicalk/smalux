# TLS + Noise 单端口示例

该目录是正式 `smalux.agent.v1` 协议的可运行交互示例。Axum REST、WebSocket 和
Tonic gRPC 共用一个端口，Noise 在 Agent gRPC 双向流内部提供端到端认证与加密。
示例只保留路由、Token/Agent 注册表和本地文件；握手、密文帧、心跳与 rekey 都调用
协议 crate 的公开方法。

Server 路由结构：

```text
127.0.0.1:8080
├── GET /api/v1/health       # 普通文本健康检查
├── GET /api/v1/status       # 普通 REST JSON 响应
├── GET /api/v1/ws           # WebSocket 文本/二进制 Echo
└── /api/v1/grpc/*           # Tonic AgentTransport，HTTP/2 + Noise
```

REST 与 WebSocket 用于演示普通应用接口和 gRPC 如何共享 Axum Router。它们本身没有
自动使用 Noise；正式浏览器接口应通过 TLS/WSS，并独立实现登录、授权和消息校验。

## 为什么使用 XXpsk3 + IK

首次注册使用 `Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s`：Client 不需要也不保存任何
预置的 Server 公钥。管理员只把一次性 256 位 Token 交给 Client；两端把同一 Token
解码为 32 字节 PSK，并在 XX 的第 3 个消息位置混入握手。Client 只有读到 Server 用
相同 PSK 生成的加密注册确认后，才保存握手中得到的 Server 静态公钥。

这比“普通 XX 后直接首次信任”更安全：主动中间人不知道 Token，就无法完成 XXpsk3，
也无法产生 Client 可解密的注册确认。Token 必须经可信渠道交付，不能出现在 URL、日志
收集系统或不受信任的代理配置中。

注册后的连接使用 `Noise_IK_25519_ChaChaPoly_BLAKE2s`：Client 已固定 Server 公钥，
Server 也已登记 Client 公钥，因此两条握手消息就能完成双向身份认证。Server 在 IK
首包解密出 Client 静态公钥，并映射到 Agent 身份。

这里不使用 KX。KX 适合 Initiator 预先不知道 Responder 静态公钥的场景，但它不能像
IK 一样直接表达“Client 已固定 Server，Server 已登记 Client”这个关系。

## 交互流程

```text
首次连接（XXpsk3）
Client                         Server
  | ---- XXpsk3 message 1 -----> |
  | <--- XXpsk3 message 2 ------ |  Client 暂存握手得到的 Server 静态公钥
  | ---- XXpsk3 message 3 -----> |  双方在此处混入同一个 Token PSK
  | ---- RegistrationRequest --> |  Server 验证 Token 并保存 pending 事务
  | <--- RegistrationPrepared -- |  Client 保存身份、公钥、agent_id 和事务 ID
  | ---- RegistrationCommit ---> |  Server 激活 Agent 并最终消费 Token
  | <--- RegistrationCommitted - |  Client 标记本地注册完成
  | <== encrypted business =====>|  当前 XX Session 直接进入业务阶段，不主动断开

断线或重启后的连接（IK）
Client                         Server
  | ---- IK message 1 --------> |  Server 查找已登记的 Client 公钥
  | <--- IK message 2 ---------- |  双方进入 Noise transport mode
  | <== encrypted gRPC stream ==>|
```

外层 TLS 与 Noise 的职责不同：

- Noise 始终启用，负责 Agent 到 Rust Server 业务消息的端到端认证与加密；
- TLS 可选，负责标准 HTTPS 传输、域名验证以及隐藏 gRPC/Noise 流量特征；
- Cloudflare 或 Nginx 终止 TLS 时，只能看到 Noise 密文，不能读取 Token 和监控数据；
- 没有 TLS 时，HTTP/2 和元数据仍可被观察，但 Noise 握手后的业务内容不可读、不可篡改。

## 文件

```text
examples/
└── noise_shared_port/
    ├── client/main.rs                # 正式协议 Client、身份持久化与业务会话
    ├── server/main.rs                # Axum REST/WS + Tonic gRPC、注册表与业务处理
    ├── common.rs                     # 双方地址、路由和环境变量
    ├── support.rs                    # 密钥、Token、Agent 公钥注册表及测试
    └── README.md                     # 本文
```

wire 契约只有 `proto/smalux/agent/v1/*.proto` 一份；示例不再生成私有 proto 类型。

## 直接运行

先启动 Server：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
```

默认 `manual` 模式逐步调用小方法，便于阅读流程；`driver` 模式把长流交给自动 Driver：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
```

Server 输出固定的示例 Token 和 Client 启动命令：

```powershell
$env:SMALUX_EXAMPLE_REGISTRATION_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cargo run -p smalux-protocol --example noise_shared_port_client
```

Client 和 Server 可以独立选择模式，wire 协议完全相同；Driver Client 的启动方式为：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

Server 启动后可直接检查普通接口：

```powershell
curl.exe http://127.0.0.1:8080/api/v1/status
```

返回：

```json
{"status":"ok","transports":["rest","websocket","grpc"]}
```

WebSocket Client 连接 `ws://127.0.0.1:8080/api/v1/ws` 后，Server 会回显收到的文本或
二进制帧。Agent 示例仍连接 `/api/v1/grpc`，完成 XXpsk3、IK 和加密业务流。

首次 Client 运行会执行 XXpsk3 注册，保存身份后直接在当前 XX Session 发送三条消息，
不会为了切换 IK 主动断开。身份默认保存在 `target/smalux-noise-agent/`；再次运行或断线
重连时不再需要 Token，直接使用 IK。

Server 数据默认保存在 `target/smalux-noise-server/`：

```text
noise/static-private.bin  # Server Noise 私钥
noise/static-public.bin   # Server Noise 公钥
agents/<agent>.key        # 已登记的 Agent Noise 公钥
registrations/<agent>/    # pending/committed 注册事务及恢复字段
```

## 密钥生命周期与更换

首次注册会产生或保存以下材料：

| 位置 | 文件或值 | 谁创建 | 用途 |
| --- | --- | --- | --- |
| Server | `noise/static-private.bin`、`noise/static-public.bin` | Server 第一次启动 | 长期 Noise 身份；XXpsk3 和 IK 都使用同一对密钥。 |
| Server | 固定的 64 位十六进制示例 Token | 示例代码 | 32 字节注册 PSK；只允许绑定同一注册事务，不是 Server 长期密钥。 |
| Server | `registrations/<agent>/` | `prepare` | 保存事务 ID、Token、Agent 名称、公钥和 committed 标记。 |
| Server | `agents/<agent>.key` | XXpsk3 注册成功后 | Agent 公钥到 Agent 名称的授权记录；后续 IK 用它识别 Agent。 |
| Agent | `noise/static-private.bin`、`noise/static-public.bin` | Agent 首次注册前 | Agent 长期 Noise 身份；私钥只保留在本机，握手中只证明其持有。 |
| Agent | `server-public.bin`、`agent-id.txt`、`registration-id.bin` | 收到 prepared 后 | commit 前保存的 pending 身份材料。 |
| Agent | `registration-committed` | 收到 committed 后 | 只有存在该标记，下次启动才允许直接使用 IK。 |

Token 既参与 XXpsk3 的 `psk(3, ...)`，也位于 Noise 加密的注册请求中。第一次 prepare 后，Token
只允许相同 Agent 名称和公钥恢复同一事务；不同公钥重用会得到 `TokenAlreadyUsed`。Agent 在 commit
前退出时，下次仍用原 Noise 身份和 Token 继续；Server 已 commit 但最终响应丢失时，也会返回同一事务。
生产代码仍应生成随机、短期、单次且可审计的 Token。已完成注册的 IK 连接不读取 Token。

不要只删除 Agent 目录中的一个文件。Client 发现 `noise/`、`server-public.bin`、`agent-id.txt` 只要
缺少任意一项就会报 `incomplete Agent Noise identity directory`，避免混用新旧身份。要让该 Agent
重新走首次注册，应删除整个 Agent 数据目录。

### 更换 Agent 密钥

当前示例不支持在 IK 流中静默替换 Agent 公钥。Server 用握手取得的公钥直接查询
`agents/<agent>.key`；新公钥没有记录就会被拒绝，旧公钥也不能自动授权新公钥。正确的示例操作是：

1. 停止旧 Agent，在 Server 控制台输入 `revoke example-agent` 删除该 Agent 的注册表记录；也可以
   使用 `SMALUX_EXAMPLE_REVOKE_AGENT=example-agent` 重启 Server 完成同一操作。
2. 删除 Agent 的整个本地身份目录，使下次启动生成新的 Agent 静态密钥对：

   ```powershell
   Remove-Item -Recurse -Force target/smalux-noise-agent
   $env:SMALUX_EXAMPLE_REGISTRATION_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
   cargo run -p smalux-protocol --example noise_shared_port_client
   ```

3. Client 用新密钥完成 XXpsk3，Server 写入新的 `agents/example-agent.key`，随后双方用新密钥 IK。

`SMALUX_EXAMPLE_REVOKE_AGENT` 和控制台都是示例管理入口，不是生产管理 API。固定 Token 在单次
Server 进程内只能注册一个 Agent；若已被消费，需要重启示例 Server 才会重新启用该固定值。

### 更换 Server 密钥

更换 Server Noise 密钥会使所有 Agent 已保存的 `server-public.bin` 失效。它与替换外层 TLS 证书不同：
TLS 证书可以独立更换，而 Server Noise 公钥是 IK 的固定身份。当前示例的恢复方式是为每个 Agent 重新
注册：

1. 停止 Server，并同时替换 `target/smalux-noise-server/noise/` 中的私钥和公钥；删除整个 `noise/`
   目录后下次启动会生成一对新密钥，绝不能只替换其中一个文件。
2. 对每个已注册 Agent，通过控制台或 `SMALUX_EXAMPLE_REVOKE_AGENT` 吊销旧公钥。
3. 在该 Agent 上删除完整的 Agent 数据目录，使用固定示例 Token 再运行 Client。XXpsk3 会认证新 Server，
   Client 只在收到加密注册确认后保存新的 `server-public.bin`。

可执行示例没有接入数据库，因此没有在控制台暴露在线轮换命令；正式协议层已经提供由旧会话认证的
Agent/Server 换钥消息、双键重叠窗口和回滚状态。调用方应使用 `prepare_rotation` 或 `stage` 取得
待保存 snapshot，持久化成功后再发送请求/确认，最后调用 `promote_pending` 或 `promote_next`。

## Server 控制台与握手日志

Server 启动后可以直接在同一控制台输入命令：

```text
server> help
server> token
server> agents
server> revoke example-agent
server> quit
```

- `token` 打印固定示例 Token；
- `agents` 列出磁盘注册表中已登记的 Agent；
- `revoke <agent>` 删除该 Agent 公钥，已经建立的流保持到自行关闭，新的 IK 会被拒绝；
- `quit` 触发 Tonic/Axum 正常关闭，不需要直接终止进程。

每个 gRPC RPC 都会分配递增编号，例如 `rpc:1`。一次正常 XXpsk3 会输出：

```text
[server][rpc:1] opened; waiting for first handshake frame
[server][rpc:1] Noise handshake completed mode=RegistrationXxPsk3
[server][rpc:1][xxpsk3] waiting for encrypted RegistrationRequest
[server][rpc:1][xxpsk3] pending agent=example-agent resumed=false
[server][rpc:1][xxpsk3] committed agent=example-agent
[server][rpc:1] completed
```

如果 Client 在握手中关闭请求流，Server 会打印 `aborted: gRPC stream closed`；如果连接保持但
不发送下一条握手帧，默认 5 秒后打印握手超时。Server 会尝试发送不含敏感细节的外层
`ProtocolError`；若 Client 已完全断开，会追加 `Client already closed; error could not be delivered`。
这些错误只终止当前 RPC，不会关闭监听端口。

## 可选 TLS

默认使用 `http://127.0.0.1:8080`，但 Noise 仍然启用。若 Server 直接提供 HTTPS，设置
公开证书链和私钥路径：

```powershell
$env:SMALUX_EXAMPLE_TLS_CERT = "C:/certs/fullchain.pem"
$env:SMALUX_EXAMPLE_TLS_KEY = "C:/certs/private-key.pem"
cargo run -p smalux-protocol --example noise_shared_port_server
```

Client 使用完整 HTTPS 地址，证书由系统根证书验证：

```powershell
$env:SMALUX_EXAMPLE_ENDPOINT = "https://agent.example.com"
cargo run -p smalux-protocol --example noise_shared_port_client
```

未设置证书时，示例使用标准 `axum::serve` 同时接受普通 HTTP/1 和 gRPC h2c。设置证书时，
同一个 Axum Router 交给 `tonic::transport::Server`，由 Tonic 配置 TLS 并同时接受 HTTP/1
和 HTTP/2。这两个分支只替换监听与 TLS 层，不改变 REST、WebSocket、gRPC 路由或 Noise
握手逻辑。

如果 TLS 在 Cloudflare 或 Nginx 终止，Rust Server 可保持当前 h2c 配置，或按代理到源站
的策略启用 TLS。无论 TLS 在哪里终止，首次仍由 Token PSK 认证，后续由保存的 Noise
公钥使用 IK 认证。

## 按代码执行

1. `client/main.rs::main` 只有读到完整身份和 `registration-committed` 才进入 IK；只有 Noise
   身份或缺少 committed 标记时，使用原身份恢复首次注册。
2. `register_agent` 调用 `AgentProtocolClient::prepare_registration` 完成 XXpsk3，发送
   `RegistrationRequest` 并取得 `AgentPendingRegistration`。
3. Client 调用 `save_pending_registration`，保存成功后才调用 `pending.commit()`；收到
   `RegistrationCommitted` 后写入完成标记。协议层不替调用方决定具体存储。
4. Server 的 `open_session` 把 Tonic 流交给
   `ServerSessionAcceptor::accept_incoming`，得到 `IncomingSession::Registration` 或
   `IncomingSession::Authentication`。
5. 注册分支依次调用 `receive_request`、注册表 `prepare`、协议 `prepare`、`wait_for_commit`、
   注册表 `commit` 和协议 `complete`；失败返回加密 `SecureError`。
6. 首次注册和后续 IK 最终都调用 `run_messages`。只有已有身份或断线重连时才由
   `open_ik_session` 调用 `AgentProtocolClient::connect`；成功后双方通过
   manual 模式使用 `TonicNoiseSession` 小方法，driver 模式使用 `SessionHandle/SessionEventReceiver`。
7. `receive_event` 和 Driver 自动处理 Ping/Pong 与 rekey。手动循环也可定期调用
   `maintenance_status/perform_maintenance`；
   静态密钥轮换则使用 `AgentKeySet`、`ServerKeyRing`、`PinnedServerKeys` 的方法和 snapshot。

## 超时与错误演示

`ServerSessionAcceptor` 和 `AgentProtocolClient` 的握手默认上限都是 5 秒。业务 IK 流不套用握手
超时，而是由 `HeartbeatPolicy` 的 Ping/Pong 和失联上限管理，避免把正常长连接误判为半开握手。

错误 Token：使用一个全新的 Agent 数据目录，并设置任意非 Server 输出的 64 位 Token。
Client 会在 XXpsk3 第三条消息阶段得到通用 Noise 认证失败：

```powershell
$env:SMALUX_EXAMPLE_AGENT_DATA_DIR = "target/noise-bad-token-agent"
$env:SMALUX_EXAMPLE_REGISTRATION_TOKEN = "0000000000000000000000000000000000000000000000000000000000000000"
cargo run -p smalux-protocol --example noise_shared_port_client
```

协议故意不区分 Token 输错与握手被篡改，避免向攻击者泄露更多认证细节。半握手关闭、半握手超时、
错误 PSK 和加密业务错误由 `tests/official_protocol.rs` 通过正式公开接口验证，不再在正常 Client
入口中保留故意卡住握手的运行开关。

## 示例边界

该实现有意保持简单：注册 Token 没有 TTL、数据库事务或限流，多个事务文件也不是数据库式原子提交，
私钥文件没有接入系统密钥库，在线换钥状态也没有写入真实存储。生产实现还应增加 Token 过期、审计、原子持久化、
密钥文件权限、重放/消息幂等策略和受认证的管理接口。
