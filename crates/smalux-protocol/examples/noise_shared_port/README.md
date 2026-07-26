# TLS + Noise 单端口示例

该目录是正式 `smalux.agent.v1` 协议的可运行交互示例。Axum HTTP 和 Tonic gRPC
共用一个端口，Noise 在 gRPC 双向流内部提供端到端认证与加密。示例只保留路由、
Token/Agent 注册表和本地文件；握手、密文帧、心跳与 rekey 都调用协议 crate 的公开方法。

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
  | ---- encrypted Token ------> |  Token 与 Agent 名称都在 Noise 密文内
  | <--- encrypted agent_id ---- |  Client 验证 PSK 成功后保存 Server 公钥

后续连接（IK）
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
    ├── server/main.rs                # Axum + Tonic Server、注册表与业务处理
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

Server 输出固定的示例 Token 和 Client 启动命令：

```powershell
$env:SMALUX_EXAMPLE_ENROLLMENT_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cargo run -p smalux-protocol --example noise_shared_port_client
```

首次 Client 运行会先执行 XXpsk3 注册，再自动建立 IK 会话并发送三条消息。身份默认
保存在 `target/smalux-noise-agent/`。再次运行 Client 时不再需要 Token，直接使用 IK。

Server 数据默认保存在 `target/smalux-noise-server/`：

```text
noise/static-private.bin  # Server Noise 私钥
noise/static-public.bin   # Server Noise 公钥
agents/<agent>.key        # 已登记的 Agent Noise 公钥
```

## 密钥生命周期与更换

首次注册会产生或保存以下材料：

| 位置 | 文件或值 | 谁创建 | 用途 |
| --- | --- | --- | --- |
| Server | `noise/static-private.bin`、`noise/static-public.bin` | Server 第一次启动 | 长期 Noise 身份；XXpsk3 和 IK 都使用同一对密钥。 |
| Server | 固定的 64 位十六进制示例 Token | 示例代码 | 32 字节注册 PSK；单次 Server 进程中只允许成功注册一次，不是 Server 长期密钥。 |
| Server | `agents/<agent>.key` | XXpsk3 注册成功后 | Agent 公钥到 Agent 名称的授权记录；后续 IK 用它识别 Agent。 |
| Agent | `noise/static-private.bin`、`noise/static-public.bin` | Agent 首次注册前 | Agent 长期 Noise 身份；私钥只保留在本机，握手中只证明其持有。 |
| Agent | `server-public.bin`、`agent-id.txt` | 收到加密注册确认后 | 固定 Server 身份，并保存 Server 确认的 Agent 名称。 |

Token 既参与 XXpsk3 的 `psk(3, ...)`，也被放在 Noise 加密的注册请求中供注册表消费。公钥文件写入
成功后，注册表立即将它标记为已用；本次 Server 进程内再次注册会得到 `TokenAlreadyUsed`。为了方便
反复手工运行，本示例在 Server 重启后会重新启用同一个固定 Token。生产代码绝不能这样做，应生成
随机、短期、单次且可审计的 Token。已注册的 IK 连接不读取 Token。

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
   $env:SMALUX_EXAMPLE_ENROLLMENT_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
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
[server][rpc:1] Noise handshake completed mode=EnrollmentXxPsk3
[server][rpc:1][xxpsk3] waiting for encrypted TokenRequest
[server][rpc:1][xxpsk3] registered agent=example-agent
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

如果 TLS 在 Cloudflare 或 Nginx 终止，Rust Server 可保持当前 h2c 配置，或按代理到源站
的策略启用 TLS。无论 TLS 在哪里终止，首次仍由 Token PSK 认证，后续由保存的 Noise
公钥使用 IK 认证。

## 按代码执行

1. `client/main.rs::main` 先查找本地 `agent-id.txt`、Server 公钥和 Agent
   静态密钥；完整时进入 IK，完全不存在时进入 `enroll`。
2. `enroll` 把磁盘字节恢复为 `NoiseIdentity`，然后调用
   `AgentProtocolClient::enroll`。该方法内部完成 XXpsk3、发送加密 `TokenRequest`，并返回
   `EnrollmentOutcome { agent_id, server_public_key, session }`。
3. Client 只有取得 `EnrollmentOutcome` 后才调用 `save_agent_identity`。协议层只返回状态，
   不替调用方决定保存到文件、数据库还是 KMS。
4. Server 的 `open_session` 把 Tonic 流交给
   `ServerSessionAcceptor::accept_session`。返回的 `ServerPendingSession` 提供
   `handshake_mode()`、`peer_public_key()` 与 `authorize()`，业务层据此执行注册或授权查询。
5. XXpsk3 分支读取加密 `TokenMessage`，注册表写入 Agent 公钥成功后再发送加密
   `TokenResponse`；失败则发送加密 `SecureError`，不会把 Token 放进外层 gRPC 状态。
6. `run_ik_session` 调用 `AgentProtocolClient::connect`，Server 用握手得到的 Agent 公钥
   查询注册表。成功后双方通过 `TonicNoiseSession::send/receive` 收发 `Messages`。
7. `receive` 内部自动处理 Ping/Pong 与 responder rekey。主动端可调用 `request_rekey`；
   静态密钥轮换则使用 `AgentKeySet`、`ServerKeyRing`、`PinnedServerKeys` 的方法和 snapshot。

## 超时与错误演示

`ServerSessionAcceptor` 和 `AgentProtocolClient` 的握手默认上限都是 5 秒。业务 IK 流不套用握手
超时，而是由 `HeartbeatPolicy` 的 Ping/Pong 和失联上限管理，避免把正常长连接误判为半开握手。

错误 Token：使用一个全新的 Agent 数据目录，并设置任意非 Server 输出的 64 位 Token。
Client 会在 XXpsk3 第三条消息阶段得到通用 Noise 认证失败：

```powershell
$env:SMALUX_EXAMPLE_AGENT_DATA_DIR = "target/noise-bad-token-agent"
$env:SMALUX_EXAMPLE_ENROLLMENT_TOKEN = "0000000000000000000000000000000000000000000000000000000000000000"
cargo run -p smalux-protocol --example noise_shared_port_client
```

协议故意不区分 Token 输错与握手被篡改，避免向攻击者泄露更多认证细节。半握手关闭、半握手超时、
错误 PSK 和加密业务错误由 `tests/official_protocol.rs` 通过正式公开接口验证，不再在正常 Client
入口中保留故意卡住握手的运行开关。

## 示例边界

该实现有意保持简单：注册 Token 没有 TTL、数据库事务或限流，私钥文件没有接入系统
密钥库，在线换钥状态也没有写入真实存储。生产实现还应增加 Token 过期、审计、原子持久化、
密钥文件权限、重放/消息幂等策略和受认证的管理接口。
