//! Axum REST、WebSocket 与 Tonic gRPC 共用端口的 Noise 示例 Server。
//!
//! - 首次连接使用 Noise XXpsk3，以一次性 Token 认证握手，不要求 Client 预置公钥；
//! - 注册成功后使用 Noise IK，根据 Client 静态公钥恢复 Agent 身份；
//! - TLS 是可选外层。Cloudflare/Nginx 可以终止 TLS，Noise 仍端到端保护业务数据。
//! - REST、WebSocket 与 gRPC service 通过 Axum Router 在同一监听端口提供；
//! - h2c 使用标准 `axum::serve`，直接 TLS 模式由 Tonic transport 提供证书配置。

use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    http::header,
    response::IntoResponse,
    routing::get,
};
use common::{
    ADDRESS_ENV, DEFAULT_ADDRESS, DEFAULT_SERVER_DATA_DIR, ExampleMode, FIXED_REGISTRATION_TOKEN,
    GRPC_PREFIX, HEALTH_PATH, REGISTRATION_TOKEN_ENV, REVOKE_AGENT_ENV, SERVER_DATA_DIR_ENV,
    STATUS_PATH, TLS_CERT_ENV, TLS_KEY_ENV, WEBSOCKET_PATH,
};
use smalux_protocol::{
    agent::v1::{
        HealthRequest, HealthResponse, Messages, MessagesResponse, ProtocolFrame, SecureError,
        SecureErrorCode, SecureMessage,
        agent_transport_server::{AgentTransport, AgentTransportServer},
        messages, messages_request, messages_response, protocol_frame, secure_message,
    },
    noise::{NoiseIdentity, ServerKeyRing},
    tonic_transport::{
        IncomingSession, ServerRegistration, ServerSessionAcceptor, SessionDriver,
        SessionDriverConfig, SessionEvent, TonicNoiseSession, TransportError,
    },
};
use support::{
    AgentRegistry, ExampleResult, RegistrationError, load_or_generate_identity,
    parse_registration_psk, public_key_hex,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming, service::Routes};

#[path = "../common.rs"]
mod common;
#[path = "../support.rs"]
mod support;

/// Tonic 要求的 Server streaming 返回类型；每个 item 可以是正式帧或 gRPC 状态。
type ResponseStream = Pin<Box<dyn Stream<Item = Result<ProtocolFrame, Status>> + Send + 'static>>;

#[derive(Clone)]
/// 每个并发 RPC 共享的只读协议配置和线程安全注册表。
struct ExampleService {
    /// 可同时接受当前、下一把和上一把 Server 静态密钥的协议层密钥环。
    keyring: Arc<ServerKeyRing>,
    /// 本次启动的一次性注册 Token 解码后的 PSK，仅用于 XXpsk3。
    registration_psk: Arc<[u8; 32]>,
    /// Agent 公钥注册表；XXpsk3 写入，IK 查询。
    registry: Arc<AgentRegistry>,
    /// 给并发 RPC 分配可读编号，方便在控制台关联同一次握手的所有日志。
    next_session_id: Arc<AtomicU64>,
    /// 业务阶段使用逐步方法还是自动 Driver；握手和注册流程两种模式完全相同。
    mode: ExampleMode,
}

#[tonic::async_trait]
impl AgentTransport for ExampleService {
    /// `OpenSession` 的响应流具体类型。
    type OpenSessionStream = ResponseStream;

    /// 普通 unary 健康检查，不建立 Noise 会话，也不执行 Agent 认证。
    async fn health_check(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        // code=0 和 message=ok 只表示服务可达，不表示 Token 或 Agent 身份有效。
        Ok(Response::new(HealthResponse {
            message: "ok".to_owned(),
            code: 0,
        }))
    }

    /// 为每条 gRPC 双向流创建独立 Noise 状态机 task。
    async fn open_session(
        &self,
        request: Request<Streaming<ProtocolFrame>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        // Relaxed 足以生成日志关联编号；它不参与安全决策或业务顺序。
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        // 有界 channel 为 Server 响应侧提供背压。
        let (sender, receiver) = mpsc::channel(8);
        // Service 内部字段都是 Arc，可安全移动到独立异步 task。
        let service = self.clone();
        let inbound = request.into_inner();

        // RPC 立即返回响应流；Noise 状态机在独立任务内按顺序读写。
        tokio::spawn(async move {
            println!("[server][rpc:{session_id}] opened; waiting for first handshake frame");
            match service
                .handle_session(session_id, inbound, sender.clone())
                .await
            {
                Ok(()) => println!("[server][rpc:{session_id}] completed"),
                Err(error) => {
                    eprintln!("[server][rpc:{session_id}] aborted: {error}");
                    // 握手外层只发送通用分类，绝不回显 Token、密钥或业务明文。
                    // Client 仍读取时会看到 ProtocolError；完全断开时发送失败是正常结果。
                    if sender
                        .send(Ok(ProtocolFrame {
                            body: Some(protocol_frame::Body::ProtocolError(error.protocol_error())),
                        }))
                        .await
                        .is_err()
                    {
                        eprintln!(
                            "[server][rpc:{session_id}] Client already closed; error could not be delivered"
                        );
                    }
                }
            }
        });

        // Tonic 立即得到 receiver，长期业务不会阻塞 RPC handler 返回。
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

impl ExampleService {
    /// 完成握手、执行注册或授权，并把成功会话交给对应业务循环。
    async fn handle_session(
        &self,
        session_id: u64,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
    ) -> Result<(), TransportError> {
        // 协议层完成带超时的 XXpsk3/IK 握手，并返回认证过的对端静态公钥。
        let incoming = ServerSessionAcceptor::default()
            .accept_incoming(
                inbound,
                sender,
                &self.keyring,
                self.registration_psk.as_ref(),
            )
            .await?;
        let (agent_id, mut session) = match incoming {
            IncomingSession::Registration(registration) => {
                println!(
                    "[server][rpc:{session_id}] Noise handshake completed mode=RegistrationXxPsk3"
                );
                let Some(result) = self.register_agent(session_id, registration).await? else {
                    return Ok(());
                };
                result
            }
            IncomingSession::Authentication(authentication) => {
                println!(
                    "[server][rpc:{session_id}] Noise handshake completed mode=AuthenticatedIk"
                );
                let agent_id = match self
                    .registry
                    .authenticate(authentication.peer_public_key().as_bytes())
                {
                    Ok(agent_id) => agent_id,
                    Err(error) => {
                        authentication
                            .reject(registration_secure_error(error))
                            .await?;
                        return Ok(());
                    }
                };
                println!("[server][rpc:{session_id}][ik] authenticated agent={agent_id}");
                (agent_id, authentication.authorize())
            }
        };
        // 注册成功的 XX 与后续 IK 都已经获得业务身份，共用同一个长期消息循环。
        match self.mode {
            ExampleMode::Manual => self.messages_loop_manual(&mut session, &agent_id).await,
            ExampleMode::Driver => self.messages_loop_driver(session, &agent_id).await,
        }
    }

    /// 处理 XXpsk3 后的四阶段注册，并返回已提交的 Agent 业务身份与当前会话。
    ///
    /// `Some(agent_id)` 表示当前 XX Session 已授权，可直接进入业务循环；`None` 表示已经
    /// 向 Client 发送加密错误，调用方应结束当前 Session。
    async fn register_agent(
        &self,
        session_id: u64,
        mut registration: ServerRegistration,
    ) -> Result<Option<(String, TonicNoiseSession)>, TransportError> {
        println!("[server][rpc:{session_id}][xxpsk3] waiting for encrypted RegistrationRequest");
        let remote_public_key = registration.peer_public_key();
        let request = match registration.receive_request().await {
            Ok(request) => request,
            Err(_) => {
                registration
                    .reject(SecureError {
                        code: SecureErrorCode::InvalidMessage as i32,
                        message: "XXpsk3 session requires RegistrationRequest".to_owned(),
                    })
                    .await?;
                return Ok(None);
            }
        };
        let prepared = match self.registry.prepare(
            &request.token,
            &request.agent_name,
            remote_public_key.as_bytes(),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                registration
                    .reject(registration_secure_error(error))
                    .await?;
                return Ok(None);
            }
        };
        println!(
            "[server][rpc:{session_id}][xxpsk3] pending agent={} resumed={}",
            prepared.agent_id, prepared.already_committed
        );
        registration
            .prepare(prepared.registration_id, prepared.agent_id.clone())
            .await?;
        registration
            .wait_for_commit(prepared.registration_id, Duration::from_secs(10))
            .await?;
        let agent_id = self
            .registry
            .commit(prepared.registration_id, remote_public_key.as_bytes())
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        let session = registration.complete(prepared.registration_id).await?;
        println!("[server][rpc:{session_id}][xxpsk3] committed agent={agent_id}");
        Ok(Some((agent_id, session)))
    }

    /// 循环处理已授权 IK 会话中的业务 Messages 请求。
    async fn messages_loop_manual(
        &self,
        session: &mut TonicNoiseSession,
        agent_id: &str,
    ) -> Result<(), TransportError> {
        // receive() 已在内部处理 Ping/Pong 和 responder rekey。
        while let Some(message) = session.receive().await? {
            let Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(request)),
            })) = message.body
            else {
                // 非 MessagesRequest 返回加密错误，但不关闭整个 Server。
                send_secure_error(
                    session,
                    SecureErrorCode::InvalidMessage,
                    "IK session requires MessagesRequest",
                )
                .await?;
                continue;
            };
            println!(
                "[server][noise] <- agent={agent_id} sequence={}",
                request.sequence
            );
            // oneof payload 按类型映射，展示 bytes、string 和 typed Echo 的处理方式。
            let response_payload = request.payload.map(|payload| match payload {
                messages_request::Payload::BytesPayload(value) => {
                    messages_response::Payload::BytesPayload(value)
                }
                messages_request::Payload::StringPayload(value) => {
                    messages_response::Payload::StringPayload(value)
                }
                messages_request::Payload::EchoRequest(value) => {
                    messages_response::Payload::EchoResponse(
                        smalux_protocol::agent::v1::EchoResponse {
                            payload: value.payload,
                        },
                    )
                }
            });
            // acknowledged_sequence 把响应关联到原请求。
            session
                .send(SecureMessage {
                    body: Some(secure_message::Body::Messages(Messages {
                        body: Some(messages::Body::Response(MessagesResponse {
                            acknowledged_sequence: request.sequence,
                            payload: response_payload,
                        })),
                    })),
                })
                .await?;
        }
        println!("[server][noise] session closed agent={agent_id}");
        Ok(())
    }

    /// 使用可选 Driver 处理业务消息；心跳、rekey 和 Noise nonce 均由 Driver task 管理。
    async fn messages_loop_driver(
        &self,
        session: TonicNoiseSession,
        agent_id: &str,
    ) -> Result<(), TransportError> {
        let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());
        while let Some(event) = running.events.recv().await {
            let event = match event {
                Ok(event) => event,
                Err(TransportError::Closed) => {
                    println!("[server][driver] session closed agent={agent_id}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let SessionEvent::Messages(Messages {
                body: Some(messages::Body::Request(request)),
            }) = event
            else {
                running
                    .handle
                    .send(SecureMessage {
                        body: Some(secure_message::Body::Error(SecureError {
                            code: SecureErrorCode::InvalidMessage as i32,
                            message: "business session requires MessagesRequest".to_owned(),
                        })),
                    })
                    .await?;
                continue;
            };
            println!(
                "[server][driver] <- agent={agent_id} sequence={}",
                request.sequence
            );
            let response_payload = request.payload.map(|payload| match payload {
                messages_request::Payload::BytesPayload(value) => {
                    messages_response::Payload::BytesPayload(value)
                }
                messages_request::Payload::StringPayload(value) => {
                    messages_response::Payload::StringPayload(value)
                }
                messages_request::Payload::EchoRequest(value) => {
                    messages_response::Payload::EchoResponse(
                        smalux_protocol::agent::v1::EchoResponse {
                            payload: value.payload,
                        },
                    )
                }
            });
            running
                .handle
                .send_messages(Messages {
                    body: Some(messages::Body::Response(MessagesResponse {
                        acknowledged_sequence: request.sequence,
                        payload: response_payload,
                    })),
                })
                .await?;
        }
        Ok(())
    }
}

/// 返回一个不依赖 gRPC 或 Noise 会话的普通 REST 状态响应。
///
/// 示例手写固定 JSON，避免为了一个静态响应额外引入 Serde；正式 API 应使用稳定响应类型。
async fn rest_status() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        r#"{"status":"ok","transports":["rest","websocket","grpc"]}"#,
    )
}

/// 校验 WebSocket Upgrade 请求，并把升级后的连接交给独立 Echo 会话。
async fn open_websocket(upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(websocket_echo)
}

/// 逐帧回显文本和二进制消息，同时显式处理 Ping 与关闭帧。
async fn websocket_echo(mut socket: WebSocket) {
    println!("[server][ws] connection opened");
    while let Some(frame) = socket.recv().await {
        // 接收错误表示连接已经不可继续，记录后结束本次 WebSocket，不影响其他路由。
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                eprintln!("[server][ws] receive failed: {error}");
                break;
            }
        };
        // Echo 只演示双向长连接；正式前端消息应定义版本化的应用层结构。
        let response = match frame {
            Message::Text(value) => Message::Text(value),
            Message::Binary(value) => Message::Binary(value),
            Message::Ping(value) => Message::Pong(value),
            Message::Pong(_) => continue,
            Message::Close(reason) => {
                // 回送 Close 帧完成 WebSocket 关闭握手，然后退出读取循环。
                let _ = socket.send(Message::Close(reason)).await;
                break;
            }
        };
        if let Err(error) = socket.send(response).await {
            eprintln!("[server][ws] send failed: {error}");
            break;
        }
    }
    println!("[server][ws] connection closed");
}

/// 恢复 Server 身份与注册表，组合 REST/WS/gRPC Router，并按配置选择 h2c 或 TLS。
#[tokio::main]
async fn main() -> ExampleResult<()> {
    // 监听地址只包含 socket，不包含外部代理使用的域名或路径。
    let address = env::var(ADDRESS_ENV).unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let address = address.parse()?;
    let mode = ExampleMode::from_args()?;
    // 示例数据目录可配置，便于并行测试不同 Server 身份。
    let data_dir = PathBuf::from(
        env::var(SERVER_DATA_DIR_ENV).unwrap_or_else(|_| DEFAULT_SERVER_DATA_DIR.to_owned()),
    );
    // Server 静态密钥首次生成后持久化，重启不能随意改变，否则已有 Agent 的 IK 会失败。
    let stored_server_identity = load_or_generate_identity(&data_dir.join("noise"))?;
    // support 返回原始字节，正式协议类型再次验证固定长度。
    let server_identity = NoiseIdentity::from_parts(
        &stored_server_identity.private_key,
        &stored_server_identity.public_key,
    )?;
    // 为了便于反复手工测试，示例固定使用同一个 256 位 Token。
    // 生产环境必须改为密码学安全的随机短期 Token，并在成功注册后立即作废。
    let token = FIXED_REGISTRATION_TOKEN.to_owned();
    // 同一个 Token 解码成 32 字节 PSK，直接参与 XXpsk3 握手认证。
    let registration_psk = parse_registration_psk(&token)?;
    // 注册表恢复旧 Agent，同时允许该 Token 成功注册一台新 Agent。
    let registry = Arc::new(AgentRegistry::load(token.clone(), &data_dir)?);
    // 可选变量允许启动前吊销 Agent，演示后续 IK 被拒绝。
    if let Ok(agent_id) = env::var(REVOKE_AGENT_ENV) {
        println!(
            "[server][revoke] agent={agent_id} removed={}",
            registry.revoke(&agent_id)?
        );
    }
    // 仅打印公钥，Server 私钥不会进入控制台输出。
    let server_public = public_key_hex(server_identity.public_key().as_bytes());
    let service = AgentTransportServer::new(ExampleService {
        // 密钥环由协议层管理，并允许后续平滑执行 Server 静态密钥轮换。
        keyring: Arc::new(ServerKeyRing::new(server_identity)),
        // PSK 只保存在 Server 进程内存与管理员拿到的注册命令中。
        registration_psk: Arc::new(registration_psk),
        // 注册表内部使用 Mutex 串行化注册、查询和吊销。
        registry: Arc::clone(&registry),
        next_session_id: Arc::new(AtomicU64::new(1)),
        mode,
    });
    // 把生成的 Tonic service 转成可与普通 HTTP route 合并的 Axum Router。
    let grpc_router = Routes::new(service).into_axum_router();
    // REST、WebSocket 和 gRPC 使用不同路径，但最终由同一个 socket 接受连接。
    let app = Router::new()
        .route(HEALTH_PATH, get(|| async { "ok" }))
        .route(STATUS_PATH, get(rest_status))
        .route(WEBSOCKET_PATH, get(open_websocket))
        .nest(GRPC_PREFIX, grpc_router);

    println!("[server] Noise public key (Client learns it during XXpsk3): {server_public}");
    println!("[server] session mode={mode:?}");
    println!("[server] registration command:");
    // 首次 Client 只需要 Token，不再需要预置 Server 公钥。
    println!("  $env:{REGISTRATION_TOKEN_ENV} = \"{token}\"");
    println!("  cargo run -p smalux-protocol --example noise_shared_port_client");
    println!("[server] REST status: http://{address}{STATUS_PATH}");
    println!("[server] WebSocket echo: ws://{address}{WEBSOCKET_PATH}");
    println!("[server][console] type 'help' for interactive commands");

    // 控制台与网络 Server 并行，通过 oneshot 请求优雅关闭。
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    tokio::spawn(console_loop(Arc::clone(&registry), token, shutdown_sender));
    // shutdown future 只等待一次信号，再交给当前 transport 停止接受新连接。
    let shutdown = async move {
        let _ = shutdown_receiver.await;
    };

    // 两个 TLS 路径必须同时存在；都不设置时使用 h2c。
    match (env::var(TLS_CERT_ENV).ok(), env::var(TLS_KEY_ENV).ok()) {
        (None, None) => {
            println!("[server] listening on http://{address} (Noise still enabled)");
            // Axum 自动接受普通 HTTP/1 请求和 gRPC 所需的明文 HTTP/2 prior knowledge。
            let listener = TcpListener::bind(address).await?;
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await?;
        }
        (Some(certificate), Some(private_key)) => {
            // Server 直接终止 TLS 时从 PEM 文件构造公开证书链和对应私钥。
            let tls = ServerTlsConfig::new().identity(Identity::from_pem(
                fs::read(certificate)?,
                fs::read(private_key)?,
            ));
            println!("[server] listening on https://{address} (TLS + Noise)");
            // Noise 位于 TLS 内层，因此 TLS 开关不改变注册和 IK 代码路径。
            Server::builder()
                .accept_http1(true)
                .tls_config(tls)?
                .serve_with_shutdown(address, app, shutdown)
                .await?;
        }
        // 只设置一项是明确配置错误，禁止静默降级到明文。
        _ => return Err(format!("{TLS_CERT_ENV} and {TLS_KEY_ENV} must be set together").into()),
    }
    Ok(())
}

/// 运行一个只面向本地示例操作者的控制台，不暴露任何网络管理接口。
async fn console_loop(
    registry: Arc<AgentRegistry>,
    token: String,
    shutdown_sender: oneshot::Sender<()>,
) {
    // Tokio 异步 stdin 避免阻塞网络 runtime worker。
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    // Option 确保 oneshot sender 最多消费一次。
    let mut shutdown_sender = Some(shutdown_sender);
    loop {
        // print! 不自动换行，需要显式 flush 才能立即显示提示符。
        print!("server> ");
        let _ = io::stdout().flush();
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => {
                // IDE/后台运行常没有 stdin；此时保持网络服务，而不是误触发关闭。
                println!(
                    "\n[server][console] standard input closed; network service remains active"
                );
                // 等待接收端被 Server 主任务释放，同时让 Sender 跨越 await 保持存活。
                // 只调用 pending() 可能使编译器提前释放后续不再使用的 Sender，反而触发关闭。
                if let Some(sender) = shutdown_sender.as_mut() {
                    sender.closed().await;
                }
                return;
            }
            Err(error) => {
                // 单次控制台读取失败不影响网络服务，继续等待下一条输入。
                eprintln!("[server][console] failed to read input: {error}");
                continue;
            }
        };
        // trim 让命令解析不受行尾和多余空格影响。
        let command = line.trim();
        match command {
            "" => {}
            "help" => {
                println!("  help                 show commands");
                println!("  token                print the fixed example registration token");
                println!("  agents               list registered Agent names");
                println!("  revoke <agent>       revoke an Agent for future IK sessions");
                println!("  quit                 gracefully stop the example Server");
            }
            "token" => println!("[server][console] registration token={token}"),
            "agents" => match registry.registered_agents() {
                Ok(agents) if agents.is_empty() => {
                    println!("[server][console] no registered Agents")
                }
                Ok(agents) => {
                    println!("[server][console] registered Agents: {}", agents.join(", "))
                }
                Err(error) => eprintln!("[server][console] failed to list Agents: {error}"),
            },
            "quit" | "exit" => {
                println!("[server][console] graceful shutdown requested");
                // 发送后立即返回，避免再次使用已消费的 oneshot sender。
                if let Some(sender) = shutdown_sender.take() {
                    let _ = sender.send(());
                }
                return;
            }
            _ if command.starts_with("revoke ") => {
                // 只截取固定前缀后的 Agent ID，不执行 shell 或动态命令。
                let agent_id = command["revoke ".len()..].trim();
                if agent_id.is_empty() {
                    eprintln!("[server][console] usage: revoke <agent>");
                    continue;
                }
                match registry.revoke(agent_id) {
                    Ok(true) => println!(
                        "[server][console] revoked agent={agent_id}; existing streams remain open, future IK is denied"
                    ),
                    Ok(false) => println!("[server][console] agent not found: {agent_id}"),
                    Err(error) => {
                        eprintln!("[server][console] failed to revoke {agent_id}: {error}")
                    }
                }
            }
            _ => eprintln!("[server][console] unknown command '{command}'; type 'help'"),
        }
    }
}

/// 把存储错误转换为可在 Noise 密文内发送的稳定错误，不泄露文件路径。
fn registration_secure_error(error: RegistrationError) -> SecureError {
    let code = match error {
        RegistrationError::InvalidToken => SecureErrorCode::InvalidToken,
        RegistrationError::TokenAlreadyUsed => SecureErrorCode::TokenAlreadyUsed,
        RegistrationError::UnknownAgent => SecureErrorCode::AgentNotAuthorized,
        RegistrationError::AgentAlreadyRegistered => SecureErrorCode::AgentAlreadyRegistered,
        RegistrationError::InvalidAgentName
        | RegistrationError::InvalidPublicKey
        | RegistrationError::UnknownRegistration
        | RegistrationError::InvalidRegistrationId => SecureErrorCode::InvalidMessage,
        RegistrationError::Storage => SecureErrorCode::Internal,
    };
    SecureError {
        code: code as i32,
        message: error.to_string(),
    }
}

/// 在已经建立的 Noise 会话中发送一条 `SecureError`。
async fn send_secure_error(
    session: &mut TonicNoiseSession,
    code: SecureErrorCode,
    message: &str,
) -> Result<(), TransportError> {
    session
        .send(SecureMessage {
            body: Some(secure_message::Body::Error(SecureError {
                code: code as i32,
                message: message.to_owned(),
            })),
        })
        .await
}
