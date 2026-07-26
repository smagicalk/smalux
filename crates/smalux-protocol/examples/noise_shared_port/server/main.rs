//! Axum HTTP 与 Tonic gRPC 共用端口的 Noise 示例 Server。
//!
//! - 首次连接使用 Noise XXpsk3，以一次性 Token 认证握手，不要求 Client 预置公钥；
//! - 注册成功后使用 Noise IK，根据 Client 静态公钥恢复 Agent 身份；
//! - TLS 是可选外层。Cloudflare/Nginx 可以终止 TLS，Noise 仍端到端保护业务数据。
//! - HTTP health route 与 gRPC service 通过 Axum Router 在同一监听端口提供。

use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{Router, routing::get};
use common::{
    ADDRESS_ENV, DEFAULT_ADDRESS, DEFAULT_SERVER_DATA_DIR, ENROLLMENT_TOKEN_ENV,
    FIXED_ENROLLMENT_TOKEN, GRPC_PREFIX, HEALTH_PATH, REVOKE_AGENT_ENV, SERVER_DATA_DIR_ENV,
    TLS_CERT_ENV, TLS_KEY_ENV,
};
use smalux_protocol::{
    agent::v1::{
        HealthRequest, HealthResponse, Messages, MessagesResponse, ProtocolFrame, SecureError,
        SecureErrorCode, SecureMessage, TokenMessage, TokenResponse,
        agent_transport_server::{AgentTransport, AgentTransportServer},
        messages, messages_request, messages_response, protocol_frame, secure_message,
        token_message,
    },
    noise::{HandshakeMode, NoiseIdentity, ServerKeyRing},
    tonic_transport::{ServerSessionAcceptor, TonicNoiseSession, TransportError},
};
use support::{
    EnrollmentError, EnrollmentRegistry, ExampleResult, load_or_generate_identity,
    parse_enrollment_psk, public_key_hex,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
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
    enrollment_psk: Arc<[u8; 32]>,
    /// Agent 公钥注册表；XXpsk3 写入，IK 查询。
    registry: Arc<EnrollmentRegistry>,
    /// 给并发 RPC 分配可读编号，方便在控制台关联同一次握手的所有日志。
    next_session_id: Arc<AtomicU64>,
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
        let pending = ServerSessionAcceptor::default()
            .accept_session(inbound, sender, &self.keyring, self.enrollment_psk.as_ref())
            .await?;
        // 先读取模式和认证公钥，再消费 pending 取得 session。
        let mode = pending.handshake_mode();
        let peer_public_key = pending.peer_public_key();
        let mut session = pending.authorize();
        println!("[server][rpc:{session_id}] Noise handshake completed mode={mode:?}");

        // XX 分支验证 Token；IK 分支查询已持久化 Agent 公钥。
        match mode {
            HandshakeMode::EnrollmentXxPsk3 => {
                self.enroll(session_id, &mut session, peer_public_key.as_bytes())
                    .await
            }
            HandshakeMode::AuthenticatedIk => {
                let agent_id = match self.registry.authenticate(peer_public_key.as_bytes()) {
                    Ok(agent_id) => agent_id,
                    Err(error) => {
                        // Noise 已建立，所以授权失败作为加密 SecureError 返回。
                        send_enrollment_error(&mut session, error).await?;
                        return Ok(());
                    }
                };
                println!("[server][rpc:{session_id}][ik] authenticated agent={agent_id}");
                self.messages_loop(&mut session, &agent_id).await
            }
        }
    }

    /// 处理 XXpsk3 后的加密 TokenRequest，并把 Agent 公钥写入示例注册表。
    async fn enroll(
        &self,
        session_id: u64,
        session: &mut TonicNoiseSession,
        remote_public_key: &[u8],
    ) -> Result<(), TransportError> {
        println!("[server][rpc:{session_id}][xxpsk3] waiting for encrypted TokenRequest");
        // 握手成功不等于注册完成；下一条消息必须是 TokenRequest。
        let message = session.receive().await?.ok_or(TransportError::Closed)?;
        let Some(secure_message::Body::TokenMessage(TokenMessage {
            body: Some(token_message::Body::Request(request)),
        })) = message.body
        else {
            // 错误也走 Noise 密文，外部代理看不到具体业务原因。
            send_secure_error(
                session,
                SecureErrorCode::InvalidMessage,
                "XXpsk3 session requires TokenRequest",
            )
            .await?;
            return Ok(());
        };
        // 注册表验证一次性 Token 后，持久化握手认证得到的 Agent 公钥。
        let agent_id =
            match self
                .registry
                .register(&request.token, &request.agent_name, remote_public_key)
            {
                Ok(agent_id) => agent_id,
                Err(error) => {
                    send_enrollment_error(session, error).await?;
                    return Ok(());
                }
            };
        println!("[server][rpc:{session_id}][xxpsk3] registered agent={agent_id}");
        // 只有注册表提交成功后才返回 TokenResponse。
        session
            .send(SecureMessage {
                body: Some(secure_message::Body::TokenMessage(TokenMessage {
                    body: Some(token_message::Body::Response(TokenResponse { agent_id })),
                })),
            })
            .await
    }

    /// 循环处理已授权 IK 会话中的业务 Messages 请求。
    async fn messages_loop(
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
}

/// 恢复 Server 身份与注册表，组合 Axum/Tonic Router，并按配置选择 h2c 或 TLS。
#[tokio::main]
async fn main() -> ExampleResult<()> {
    // 监听地址只包含 socket，不包含外部代理使用的域名或路径。
    let address = env::var(ADDRESS_ENV).unwrap_or_else(|_| DEFAULT_ADDRESS.to_owned());
    let address = address.parse()?;
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
    let token = FIXED_ENROLLMENT_TOKEN.to_owned();
    // 同一个 Token 解码成 32 字节 PSK，直接参与 XXpsk3 握手认证。
    let enrollment_psk = parse_enrollment_psk(&token)?;
    // 注册表恢复旧 Agent，同时允许该 Token 成功注册一台新 Agent。
    let registry = Arc::new(EnrollmentRegistry::load(token.clone(), &data_dir)?);
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
        enrollment_psk: Arc::new(enrollment_psk),
        // 注册表内部使用 Mutex 串行化注册、查询和吊销。
        registry: Arc::clone(&registry),
        next_session_id: Arc::new(AtomicU64::new(1)),
    });
    // 把生成的 Tonic service 转成可与普通 HTTP route 合并的 Axum Router。
    let grpc_router = Routes::new(service).into_axum_router();
    // HTTP health 与 gRPC 使用不同路径，但最终由同一个 socket 接受连接。
    let app = Router::new()
        .route(HEALTH_PATH, get(|| async { "ok" }))
        .nest(GRPC_PREFIX, grpc_router);

    println!("[server] Noise public key (Client learns it during XXpsk3): {server_public}");
    println!("[server] enrollment command:");
    // 首次 Client 只需要 Token，不再需要预置 Server 公钥。
    println!("  $env:{ENROLLMENT_TOKEN_ENV} = \"{token}\"");
    println!("  cargo run -p smalux-protocol --example noise_shared_port_client");
    println!("[server][console] type 'help' for interactive commands");

    // 控制台与网络 Server 并行，通过 oneshot 请求优雅关闭。
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    tokio::spawn(console_loop(Arc::clone(&registry), token, shutdown_sender));
    // shutdown future 只等待一次信号，然后交给 Tonic 停止接受新连接。
    let shutdown = async move {
        let _ = shutdown_receiver.await;
    };

    // 两个 TLS 路径必须同时存在；都不设置时使用 h2c。
    match (env::var(TLS_CERT_ENV).ok(), env::var(TLS_KEY_ENV).ok()) {
        (None, None) => {
            println!("[server] listening on http://{address} (Noise still enabled)");
            // HTTP/1 服务 health route；gRPC 客户端仍通过 HTTP/2 连接同一端口。
            Server::builder()
                .accept_http1(true)
                .serve_with_shutdown(address, app, shutdown)
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
    registry: Arc<EnrollmentRegistry>,
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
                std::future::pending::<()>().await;
                unreachable!();
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
                println!("  token                print the fixed example enrollment token");
                println!("  agents               list registered Agent names");
                println!("  revoke <agent>       revoke an Agent for future IK sessions");
                println!("  quit                 gracefully stop the example Server");
            }
            "token" => println!("[server][console] enrollment token={token}"),
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

/// 把示例注册表错误映射为稳定的加密协议错误码。
async fn send_enrollment_error(
    session: &mut TonicNoiseSession,
    error: EnrollmentError,
) -> Result<(), TransportError> {
    // 存储错误不向 Client 暴露文件路径或底层 I/O 细节。
    let code = match error {
        EnrollmentError::InvalidToken => SecureErrorCode::InvalidToken,
        EnrollmentError::TokenAlreadyUsed => SecureErrorCode::TokenAlreadyUsed,
        EnrollmentError::UnknownAgent => SecureErrorCode::AgentNotAuthorized,
        EnrollmentError::AgentAlreadyRegistered => SecureErrorCode::AgentAlreadyRegistered,
        EnrollmentError::InvalidAgentName | EnrollmentError::InvalidPublicKey => {
            SecureErrorCode::InvalidMessage
        }
        EnrollmentError::Storage => SecureErrorCode::Internal,
    };
    send_secure_error(session, code, &error.to_string()).await
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
