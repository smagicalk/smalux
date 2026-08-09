use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use smalux_protocol::{
    agent::v1::{
        HealthRequest, HealthResponse, Messages, MessagesResponse, ProtocolErrorCode,
        ProtocolFrame, SecureError, SecureErrorCode, SecureMessage,
        agent_transport_client::AgentTransportClient,
        agent_transport_server::{AgentTransport, AgentTransportServer},
        messages, messages_request, messages_response, protocol_frame, secure_message,
    },
    noise::{ClientXxHandshake, NoiseIdentity, ServerKeyRing},
    tonic_transport::{
        AgentProtocolClient, IncomingSession, ServerSessionAcceptor, TransportError,
    },
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{Request, Response, Status, Streaming, transport::Server};

#[test]
fn registration_message_has_explicit_prepare_commit_and_completed_stages() {
    use smalux_protocol::agent::v1::{
        RegistrationCommit, RegistrationCommitted, RegistrationMessage, RegistrationPrepared,
        registration_message,
    };

    let transaction_id = vec![7; 16];
    let prepared = RegistrationMessage {
        body: Some(registration_message::Body::Prepared(RegistrationPrepared {
            registration_id: transaction_id.clone(),
            agent_id: "agent-1".to_owned(),
        })),
    };
    let commit = RegistrationMessage {
        body: Some(registration_message::Body::Commit(RegistrationCommit {
            registration_id: transaction_id.clone(),
        })),
    };
    let committed = RegistrationMessage {
        body: Some(registration_message::Body::Committed(
            RegistrationCommitted {
                registration_id: transaction_id,
            },
        )),
    };

    assert!(matches!(
        prepared.body,
        Some(registration_message::Body::Prepared(_))
    ));
    assert!(matches!(
        commit.body,
        Some(registration_message::Body::Commit(_))
    ));
    assert!(matches!(
        committed.body,
        Some(registration_message::Body::Committed(_))
    ));
}

#[test]
fn secure_messages_are_classified_as_typed_session_events() {
    use smalux_protocol::{
        agent::v1::{TaskReport, secure_message},
        tonic_transport::SessionEvent,
    };

    let report = TaskReport {
        job_id: vec![1; 16],
        job_revision: 7,
        run_id: vec![2; 16],
        attempt: 1,
        scheduled_at: None,
        started_at: None,
        result: None,
    };
    let event = SessionEvent::try_from(SecureMessage {
        body: Some(secure_message::Body::TaskReport(Box::new(report))),
    })
    .unwrap();

    let SessionEvent::TaskReport(report) = event else {
        panic!("expected a typed TaskReport event");
    };
    assert_eq!(report.job_revision, 7);
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ProtocolFrame, Status>> + Send + 'static>>;

#[derive(Clone)]
struct TestService {
    keyring: Arc<ServerKeyRing>,
    psk: [u8; 32],
    agents: Arc<Mutex<HashMap<Vec<u8>, String>>>,
    handshake_timeout: Duration,
}

#[tonic::async_trait]
impl AgentTransport for TestService {
    type OpenSessionStream = ResponseStream;

    async fn health_check(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            message: "ok".to_owned(),
            code: 0,
        }))
    }

    async fn open_session(
        &self,
        request: Request<Streaming<ProtocolFrame>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            if let Err(error) = service.run(request.into_inner(), sender.clone()).await {
                let _ = sender
                    .send(Ok(ProtocolFrame {
                        body: Some(
                            smalux_protocol::agent::v1::protocol_frame::Body::ProtocolError(
                                error.protocol_error(),
                            ),
                        ),
                    }))
                    .await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

impl TestService {
    async fn run(
        &self,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
    ) -> Result<(), smalux_protocol::tonic_transport::TransportError> {
        let psk = self.psk;
        let incoming = ServerSessionAcceptor::new(self.handshake_timeout)
            .accept_incoming_with_psk_resolver(
                inbound,
                sender,
                &self.keyring,
                move |_| async move {
                    // 强制让出一次执行权，确保测试覆盖的是真正可挂起的异步 resolver。
                    tokio::task::yield_now().await;
                    Ok(psk)
                },
            )
            .await?;
        let mut session = match incoming {
            IncomingSession::Registration(mut registration) => {
                let peer_key = registration.peer_public_key();
                let request = registration.receive_request().await?;
                if request.token != "test-token" {
                    registration
                        .reject(SecureError {
                            code: SecureErrorCode::InvalidToken as i32,
                            message: "invalid token".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }
                let registration_id = [3; 16];
                // 测试 Server 分配稳定 ID；展示名称不得直接充当业务身份。
                let agent_id = "server-assigned-agent-id".to_owned();
                registration
                    .prepare(registration_id, agent_id.clone())
                    .await?;
                registration
                    .wait_for_commit(registration_id, self.handshake_timeout)
                    .await?;
                self.agents
                    .lock()
                    .await
                    .insert(peer_key.as_bytes().to_vec(), agent_id);
                registration.complete(registration_id).await?
            }
            IncomingSession::Authentication(authentication) => {
                let peer_key = authentication.peer_public_key();
                if !self
                    .agents
                    .lock()
                    .await
                    .contains_key(&peer_key.as_bytes()[..])
                {
                    return Err(smalux_protocol::tonic_transport::TransportError::Protocol(
                        "Agent is not registered".to_owned(),
                    ));
                }
                let mut session = authentication.authorize();
                // 故意在 Agent 发起 rekey 前插入业务帧，验证等待 Ack 时会缓存而不是报错。
                session
                    .send(SecureMessage {
                        body: Some(secure_message::Body::Messages(Messages {
                            body: Some(messages::Body::Response(MessagesResponse {
                                acknowledged_sequence: 999,
                                payload: Some(messages_response::Payload::StringPayload(
                                    "queued-before-rekey".to_owned(),
                                )),
                            })),
                        })),
                    })
                    .await?;
                session
            }
        };
        // 注册成功的 XX 与已登记的 IK 都已完成授权，随后共享同一业务消息阶段。
        // 持续读取而不是只处理一条消息，才能让测试覆盖长流中的自动 Ping/Pong。
        while let Some(message) = session.receive().await? {
            let Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(request)),
            })) = message.body
            else {
                return Err(smalux_protocol::tonic_transport::TransportError::Protocol(
                    "expected MessagesRequest".to_owned(),
                ));
            };
            session
                .send(SecureMessage {
                    body: Some(secure_message::Body::Messages(Messages {
                        body: Some(messages::Body::Response(MessagesResponse {
                            acknowledged_sequence: request.sequence,
                            payload: request.payload.map(|payload| match payload {
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
                            }),
                        })),
                    })),
                })
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn registration_session_handles_business_then_ik_reconnects() {
    let server_identity = NoiseIdentity::generate().unwrap();
    let service = TestService {
        keyring: Arc::new(ServerKeyRing::new(server_identity)),
        psk: [9; 32],
        agents: Arc::new(Mutex::new(HashMap::new())),
        handshake_timeout: Duration::from_secs(5),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AgentTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let client = AgentProtocolClient::new(format!("http://{address}"));
    let identity = NoiseIdentity::generate().unwrap();
    let mut registration = client
        .register_agent_with_token_id(
            identity.clone(),
            &[9; 32],
            "test-token-id",
            "test-token".to_owned(),
            "agent-1".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(registration.agent_id, "server-assigned-agent-id");

    // 首次注册确认后不应强制断开；同一条 XXpsk3 Session 必须能直接进入业务阶段。
    registration
        .session
        .send(SecureMessage {
            body: Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(
                    smalux_protocol::agent::v1::MessagesRequest {
                        sequence: 1,
                        payload: Some(messages_request::Payload::StringPayload(
                            "first-registration-report".to_owned(),
                        )),
                    },
                )),
            })),
        })
        .await
        .unwrap();
    let first_response = registration.session.receive().await.unwrap().unwrap();
    let Some(secure_message::Body::Messages(Messages {
        body: Some(messages::Body::Response(first_response)),
    })) = first_response.body
    else {
        panic!("expected MessagesResponse on registration session");
    };
    assert_eq!(first_response.acknowledged_sequence, 1);
    drop(registration.session);

    let mut session = client
        .connect(&identity, registration.server_public_key)
        .await
        .unwrap();
    // rekey 不新建 gRPC 流；双方同步更新 Noise cipher state 后继续复用当前会话。
    assert_eq!(session.request_rekey().await.unwrap(), 1);
    assert_eq!(session.generation(), 1);
    let queued = session.receive().await.unwrap().unwrap();
    let Some(secure_message::Body::Messages(Messages {
        body: Some(messages::Body::Response(queued)),
    })) = queued.body
    else {
        panic!("expected the business message buffered during rekey");
    };
    assert_eq!(queued.acknowledged_sequence, 999);
    session
        .send(SecureMessage {
            body: Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(
                    smalux_protocol::agent::v1::MessagesRequest {
                        sequence: 7,
                        payload: Some(
                            smalux_protocol::agent::v1::messages_request::Payload::StringPayload(
                                "metric".to_owned(),
                            ),
                        ),
                    },
                )),
            })),
        })
        .await
        .unwrap();
    let response = session.receive().await.unwrap().unwrap();
    let Some(secure_message::Body::Messages(Messages {
        body: Some(messages::Body::Response(response)),
    })) = response.body
    else {
        panic!("expected MessagesResponse");
    };
    assert_eq!(response.acknowledged_sequence, 7);
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_driver_sends_and_receives_typed_events() {
    use smalux_protocol::tonic_transport::{
        HeartbeatPolicy, SessionDriver, SessionDriverConfig, SessionEvent,
    };

    let service = TestService {
        keyring: Arc::new(ServerKeyRing::new(NoiseIdentity::generate().unwrap())),
        psk: [9; 32],
        agents: Arc::new(Mutex::new(HashMap::new())),
        handshake_timeout: Duration::from_secs(5),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AgentTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let registration = AgentProtocolClient::new(format!("http://{address}"))
        .register_agent_with_token_id(
            NoiseIdentity::generate().unwrap(),
            &[9; 32],
            "test-token-id",
            "test-token".to_owned(),
            "driver-agent".to_owned(),
        )
        .await
        .unwrap();
    let mut session = registration.session;
    // 缩短间隔，让测试验证 Driver 自动发 Ping、Server 自动回 Pong 和 RTT 统计。
    session.set_heartbeat_policy(HeartbeatPolicy {
        interval: Duration::from_millis(20),
        timeout: Duration::from_millis(500),
    });
    let mut running = SessionDriver::spawn(session, SessionDriverConfig::default());
    assert_eq!(running.handle.request_rekey().await.unwrap(), 1);
    running
        .handle
        .send_messages(Messages {
            body: Some(messages::Body::Request(
                smalux_protocol::agent::v1::MessagesRequest {
                    sequence: 41,
                    payload: Some(messages_request::Payload::StringPayload(
                        "driver".to_owned(),
                    )),
                },
            )),
        })
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), running.events.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let SessionEvent::Messages(Messages {
        body: Some(messages::Body::Response(response)),
    }) = event
    else {
        panic!("expected a typed Messages response");
    };
    assert_eq!(response.acknowledged_sequence, 41);
    let heartbeat_stats = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stats = running.handle.heartbeat_stats().await.unwrap();
            if stats.received_count > 0 {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(heartbeat_stats.sent_count > 0);
    assert!(heartbeat_stats.received_count > 0);
    let sample = heartbeat_stats.last_sample.unwrap();
    assert!(sample.sent_at_unix_micros > 0);
    assert!(sample.responder_received_at_unix_micros > 0);
    assert!(sample.responder_sent_at_unix_micros >= sample.responder_received_at_unix_micros);
    assert!(sample.received_at_unix_micros > 0);
    assert!(heartbeat_stats.min_rtt.is_some());
    running.handle.shutdown().await.unwrap();
    running.task.await.unwrap();
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn registration_returns_the_encrypted_token_error() {
    let service = TestService {
        keyring: Arc::new(ServerKeyRing::new(NoiseIdentity::generate().unwrap())),
        psk: [9; 32],
        agents: Arc::new(Mutex::new(HashMap::new())),
        handshake_timeout: Duration::from_secs(5),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AgentTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let result = AgentProtocolClient::new(format!("http://{address}"))
        .register_agent_with_token_id(
            NoiseIdentity::generate().unwrap(),
            &[9; 32],
            "test-token-id",
            "wrong-token".to_owned(),
            "agent-1".to_owned(),
        )
        .await;
    let error = match result {
        Ok(_) => panic!("the Server must reject an invalid encrypted token"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        TransportError::RemoteSecure(SecureErrorCode::InvalidToken, _)
    ));
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn stalled_xx_handshake_returns_a_safe_timeout_frame() {
    let service = TestService {
        keyring: Arc::new(ServerKeyRing::new(NoiseIdentity::generate().unwrap())),
        psk: [9; 32],
        agents: Arc::new(Mutex::new(HashMap::new())),
        handshake_timeout: Duration::from_millis(30),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AgentTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let identity = NoiseIdentity::generate().unwrap();
    let (_waiting, mut first) = ClientXxHandshake::start(&identity, &[9; 32]).unwrap();
    first.registration_token_id = "test-token-id".to_owned();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = AgentTransportClient::new(channel);
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ProtocolFrame {
            body: Some(protocol_frame::Body::Handshake(first)),
        })
        .await
        .unwrap();
    let mut inbound = client
        .open_session(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    let second = inbound.message().await.unwrap().unwrap();
    assert!(matches!(
        second.body,
        Some(protocol_frame::Body::Handshake(_))
    ));

    // 保持 sender 存活但不发送 message 3，模拟握手到一半后卡住的 RPC。
    let timeout_frame = tokio::time::timeout(Duration::from_secs(1), inbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(protocol_frame::Body::ProtocolError(error)) = timeout_frame.body else {
        panic!("expected a safe ProtocolError after the handshake timeout");
    };
    assert_eq!(
        ProtocolErrorCode::try_from(error.code).unwrap(),
        ProtocolErrorCode::HandshakeTimeout
    );
    drop(sender);
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn closed_xx_handshake_returns_an_early_end_error() {
    let service = TestService {
        keyring: Arc::new(ServerKeyRing::new(NoiseIdentity::generate().unwrap())),
        psk: [9; 32],
        agents: Arc::new(Mutex::new(HashMap::new())),
        handshake_timeout: Duration::from_secs(1),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AgentTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let identity = NoiseIdentity::generate().unwrap();
    let (_waiting, mut first) = ClientXxHandshake::start(&identity, &[9; 32]).unwrap();
    first.registration_token_id = "test-token-id".to_owned();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = AgentTransportClient::new(channel);
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ProtocolFrame {
            body: Some(protocol_frame::Body::Handshake(first)),
        })
        .await
        .unwrap();
    let mut inbound = client
        .open_session(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().body,
        Some(protocol_frame::Body::Handshake(_))
    ));
    drop(sender);

    let error_frame = inbound.message().await.unwrap().unwrap();
    let Some(protocol_frame::Body::ProtocolError(error)) = error_frame.body else {
        panic!("expected ProtocolError when the request stream closes during handshake");
    };
    assert_eq!(
        ProtocolErrorCode::try_from(error.code).unwrap(),
        ProtocolErrorCode::InvalidFrame
    );
    assert_eq!(error.message, "Noise handshake ended before completion");
    let _ = shutdown_tx.send(());
}
