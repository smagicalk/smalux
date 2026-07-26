use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use smalux_protocol::{
    agent::v1::{
        HealthRequest, HealthResponse, Messages, MessagesResponse, ProtocolErrorCode,
        ProtocolFrame, SecureError, SecureErrorCode, SecureMessage, TokenMessage, TokenResponse,
        agent_transport_client::AgentTransportClient,
        agent_transport_server::{AgentTransport, AgentTransportServer},
        messages, messages_response, protocol_frame, secure_message, token_message,
    },
    noise::{ClientXxHandshake, HandshakeMode, NoiseIdentity, ServerKeyRing},
    tonic_transport::{AgentProtocolClient, ServerSessionAcceptor, TransportError},
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{Request, Response, Status, Streaming, transport::Server};

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
        let pending = ServerSessionAcceptor::new(self.handshake_timeout)
            .accept_session(inbound, sender, &self.keyring, &self.psk)
            .await?;
        let peer_key = pending.peer_public_key();
        let mode = pending.handshake_mode();
        let mut session = pending.authorize();
        match mode {
            HandshakeMode::EnrollmentXxPsk3 => {
                let message = session
                    .receive()
                    .await?
                    .ok_or(smalux_protocol::tonic_transport::TransportError::Closed)?;
                let Some(secure_message::Body::TokenMessage(TokenMessage {
                    body: Some(token_message::Body::Request(request)),
                })) = message.body
                else {
                    return Err(smalux_protocol::tonic_transport::TransportError::Protocol(
                        "expected TokenRequest".to_owned(),
                    ));
                };
                if request.token != "test-token" {
                    session
                        .send(SecureMessage {
                            body: Some(secure_message::Body::Error(SecureError {
                                code: SecureErrorCode::InvalidToken as i32,
                                message: "invalid token".to_owned(),
                            })),
                        })
                        .await?;
                    return Ok(());
                }
                self.agents
                    .lock()
                    .await
                    .insert(peer_key.as_bytes().to_vec(), request.agent_name.clone());
                session
                    .send(SecureMessage {
                        body: Some(secure_message::Body::TokenMessage(TokenMessage {
                            body: Some(token_message::Body::Response(TokenResponse {
                                agent_id: request.agent_name,
                            })),
                        })),
                    })
                    .await?;
            }
            HandshakeMode::AuthenticatedIk => {
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
                let message = session
                    .receive()
                    .await?
                    .ok_or(smalux_protocol::tonic_transport::TransportError::Closed)?;
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
                                    smalux_protocol::agent::v1::messages_request::Payload::BytesPayload(value) => messages_response::Payload::BytesPayload(value),
                                    smalux_protocol::agent::v1::messages_request::Payload::StringPayload(value) => messages_response::Payload::StringPayload(value),
                                    smalux_protocol::agent::v1::messages_request::Payload::EchoRequest(value) => messages_response::Payload::EchoResponse(smalux_protocol::agent::v1::EchoResponse { payload: value.payload }),
                                }),
                            })),
                        })),
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn official_protocol_enrolls_then_opens_an_ik_session() {
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
    let enrolled = client
        .enroll(
            identity.clone(),
            &[9; 32],
            "test-token".to_owned(),
            "agent-1".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(enrolled.agent_id, "agent-1");
    drop(enrolled.session);

    let mut session = client
        .connect(&identity, enrolled.server_public_key)
        .await
        .unwrap();
    // rekey 不新建 gRPC 流；双方同步更新 Noise cipher state 后继续复用当前会话。
    assert_eq!(session.request_rekey().await.unwrap(), 1);
    assert_eq!(session.generation(), 1);
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
async fn enrollment_returns_the_encrypted_token_error() {
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
        .enroll(
            NoiseIdentity::generate().unwrap(),
            &[9; 32],
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
    let (_waiting, first) = ClientXxHandshake::start(&identity, &[9; 32]).unwrap();
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
    let (_waiting, first) = ClientXxHandshake::start(&identity, &[9; 32]).unwrap();
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
