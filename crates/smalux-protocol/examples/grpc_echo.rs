//! 使用 Tonic gRPC 双向流演示 Smalux 协议 Echo 消息。

mod support;

use std::{io::Write, pin::Pin, sync::Arc};

use futures_util::Stream;
use smalux_protocol::{
    ClientFrame, CodecLimits, NoiseProvider, ProtocolSession, SecurityRole, SecuritySession,
    ServerFrame, SessionRole, client_frame, negotiate_server_hello, server_frame, session_content,
    unpack_any,
};
use support::{
    EchoMessage, application_content, client_handshake_frame, client_hello, close_content,
    id_from_sequence, noise_providers, protected_client_frame, protected_server_frame,
    receive_client_content, receive_client_handshake, receive_server_content,
    receive_server_handshake, server_handshake_frame, server_policy, start_noise_session,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};

mod grpc {
    tonic::include_proto!("smalux.example.grpc.v1");
}

use grpc::{
    protocol_transport_client::ProtocolTransportClient,
    protocol_transport_server::{ProtocolTransport, ProtocolTransportServer},
};

type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send>>;

#[derive(Debug)]
struct EchoTransport {
    /// 可为每条 gRPC 流创建独立 Noise 会话的 Server Provider。
    security: Arc<NoiseProvider>,
}

#[tonic::async_trait]
impl ProtocolTransport for EchoTransport {
    type OpenSessionStream = ResponseStream;

    async fn open_session(
        &self,
        request: Request<Streaming<ClientFrame>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let mut inbound = request.into_inner();
        let (responses, response_stream) = mpsc::channel(8);
        let security = self.security.clone();

        tokio::spawn(async move {
            if let Err(status) = handle_server_stream(&mut inbound, &responses, &security).await {
                let _ = responses.send(Err(status)).await;
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(
            response_stream,
        ))))
    }
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown, shutdown_signal) = oneshot::channel::<()>();
    let max_frame_bytes = CodecLimits::default().max_frame_bytes();
    let (client_security, server_security) = noise_providers()?;
    println!("[grpc server] listening on http://{address}");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(
                ProtocolTransportServer::new(EchoTransport {
                    security: Arc::new(server_security),
                })
                .max_decoding_message_size(max_frame_bytes)
                .max_encoding_message_size(max_frame_bytes),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_signal.await;
            })
            .await
    });

    run_client(address, client_security).await?;
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

async fn handle_server_stream(
    inbound: &mut Streaming<ClientFrame>,
    responses: &mpsc::Sender<Result<ServerFrame, Status>>,
    provider: &NoiseProvider,
) -> Result<(), Status> {
    let limits = CodecLimits::default();
    let mut session = ProtocolSession::new(SessionRole::Server, limits).map_err(protocol_status)?;

    let client_frame = inbound
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("stream ended before ClientHello"))?;
    let hello = match client_frame.body.as_ref() {
        Some(client_frame::Body::Hello(hello)) => hello,
        _ => return Err(Status::invalid_argument("expected ClientHello")),
    };
    let selected = negotiate_server_hello(
        hello,
        &server_policy(limits, provider),
        vec![2; 16],
        vec![3; 32],
    )
    .map_err(protocol_status)?;
    session
        .on_client_frame(&client_frame)
        .map_err(protocol_status)?;
    println!("[grpc server] <- ClientHello (stream item)");

    let server_hello = ServerFrame {
        body: Some(server_frame::Body::Hello(selected)),
    };
    session
        .on_server_frame(&server_hello)
        .map_err(protocol_status)?;
    send_response(responses, server_hello).await?;
    println!("[grpc server] -> ServerHello (stream item)");

    let security =
        start_noise_session(&session, SecurityRole::Server, provider).map_err(protocol_status)?;
    let client_handshake = inbound
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("stream ended during Noise handshake"))?;
    let client_step = handshake_step_from_client(&client_handshake)?;
    receive_client_handshake(&mut session, security.as_ref(), &client_handshake)
        .map_err(protocol_status)?;
    println!("[grpc server] <- Noise XX step={client_step}");

    let message = security
        .next_handshake()
        .map_err(protocol_status)?
        .ok_or_else(|| Status::internal("Noise server did not produce handshake step 1"))?;
    let server_step = message.step;
    let server_handshake = server_handshake_frame(security.as_ref(), message);
    session
        .on_server_frame(&server_handshake)
        .map_err(protocol_status)?;
    send_response(responses, server_handshake).await?;
    println!("[grpc server] -> Noise XX step={server_step}");

    let client_handshake = inbound
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("stream ended during Noise handshake"))?;
    let client_step = handshake_step_from_client(&client_handshake)?;
    receive_client_handshake(&mut session, security.as_ref(), &client_handshake)
        .map_err(protocol_status)?;
    println!("[grpc server] <- Noise XX step={client_step}");
    session
        .complete_security(security.as_ref())
        .map_err(protocol_status)?;
    println!("[grpc server] Noise transport ready");

    let mut server_sequence = 1_u64;
    while let Some(frame) = inbound.message().await? {
        let content = receive_client_content(&mut session, security.as_ref(), &frame)
            .map_err(protocol_status)?;
        match content.body {
            Some(session_content::Body::Application(request)) => {
                let echo: EchoMessage = unpack_any(
                    request
                        .payload
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("missing echo payload"))?,
                )
                .map_err(protocol_status)?;
                println!(
                    "[grpc server] <- EchoMessage sequence={} text={:?}",
                    request.sequence, echo.text
                );

                let response_content = application_content(
                    id_from_sequence(0x53, server_sequence),
                    Some(request.message_id),
                    server_sequence,
                    echo.text,
                );
                let response =
                    protected_server_frame(&mut session, security.as_ref(), &response_content)
                        .map_err(protocol_status)?;
                send_response(responses, response).await?;
                println!(
                    "[grpc server] -> ProtectedPayload EchoMessage sequence={server_sequence}"
                );
                server_sequence += 1;
            }
            Some(session_content::Body::Close(_)) => {
                println!("[grpc server] <- Protocol Close");
                let response =
                    protected_server_frame(&mut session, security.as_ref(), &close_content())
                        .map_err(protocol_status)?;
                send_response(responses, response).await?;
                println!("[grpc server] -> ProtectedPayload Protocol Close");
                security.close();
                return Ok(());
            }
            _ => return Err(Status::invalid_argument("unexpected SessionContent")),
        }
    }
    Ok(())
}

async fn run_client(address: std::net::SocketAddr, provider: NoiseProvider) -> ExampleResult<()> {
    let endpoint = format!("http://{address}");
    let limits = CodecLimits::default();
    let mut client = ProtocolTransportClient::connect(endpoint)
        .await?
        .max_decoding_message_size(limits.max_frame_bytes())
        .max_encoding_message_size(limits.max_frame_bytes());
    let (requests, request_stream) = mpsc::channel(8);
    let mut responses = client
        .open_session(ReceiverStream::new(request_stream))
        .await?
        .into_inner();
    println!("[grpc client] HTTP/2 stream connected");

    let mut session = ProtocolSession::new(SessionRole::Client, limits)?;
    let hello = ClientFrame {
        body: Some(client_frame::Body::Hello(client_hello(limits, &provider))),
    };
    session.on_client_frame(&hello)?;
    requests.send(hello).await?;
    println!("[grpc client] -> ClientHello (stream item)");

    let server_hello = responses
        .message()
        .await?
        .ok_or("stream ended before ServerHello")?;
    session.on_server_frame(&server_hello)?;
    println!("[grpc client] <- ServerHello (stream item)");

    let security = start_noise_session(&session, SecurityRole::Client, &provider)?;
    let message = security
        .next_handshake()?
        .ok_or("Noise client did not produce handshake step 0")?;
    let client_step = message.step;
    let client_handshake = client_handshake_frame(security.as_ref(), message);
    session.on_client_frame(&client_handshake)?;
    requests.send(client_handshake).await?;
    println!("[grpc client] -> Noise XX step={client_step}");

    let server_handshake = responses
        .message()
        .await?
        .ok_or("stream ended during Noise handshake")?;
    let server_step = handshake_step_from_server(&server_handshake)
        .map_err(|status| -> Box<dyn std::error::Error + Send + Sync> { Box::new(status) })?;
    receive_server_handshake(&mut session, security.as_ref(), &server_handshake)?;
    println!("[grpc client] <- Noise XX step={server_step}");

    let message = security
        .next_handshake()?
        .ok_or("Noise client did not produce handshake step 2")?;
    let client_step = message.step;
    let client_handshake = client_handshake_frame(security.as_ref(), message);
    session.on_client_frame(&client_handshake)?;
    requests.send(client_handshake).await?;
    println!("[grpc client] -> Noise XX step={client_step}");
    session.complete_security(security.as_ref())?;
    println!("[grpc client] Noise transport ready");

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut client_sequence = 1_u64;
    loop {
        print!("grpc echo> ");
        std::io::stdout().flush()?;
        let Some(text) = lines.next_line().await? else {
            return close_client(&requests, &mut responses, &mut session, security.as_ref()).await;
        };
        if text.eq_ignore_ascii_case("exit") {
            return close_client(&requests, &mut responses, &mut session, security.as_ref()).await;
        }
        if text.is_empty() {
            continue;
        }

        let request_id = id_from_sequence(0x43, client_sequence);
        let request_content = application_content(request_id.clone(), None, client_sequence, text);
        let request = protected_client_frame(&mut session, security.as_ref(), &request_content)?;
        requests.send(request).await?;
        println!("[grpc client] -> ProtectedPayload EchoMessage sequence={client_sequence}");

        let response = responses
            .message()
            .await?
            .ok_or("stream ended before echo response")?;
        let response_content = receive_server_content(&mut session, security.as_ref(), &response)?;
        let envelope = match response_content.body {
            Some(session_content::Body::Application(envelope)) => envelope,
            _ => return Err("expected echo response".into()),
        };
        if envelope.correlation_id.as_ref() != Some(&request_id) {
            return Err("echo response correlation_id does not match request".into());
        }
        let echo: EchoMessage = unpack_any(
            envelope
                .payload
                .as_ref()
                .ok_or("echo response does not contain payload")?,
        )?;
        println!(
            "[grpc client] <- EchoMessage sequence={} text={:?}",
            envelope.sequence, echo.text
        );
        println!("server returned: {}", echo.text);
        client_sequence += 1;
    }
}

async fn close_client(
    requests: &mpsc::Sender<ClientFrame>,
    responses: &mut Streaming<ServerFrame>,
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
) -> ExampleResult<()> {
    let close = protected_client_frame(session, security, &close_content())?;
    requests.send(close).await?;
    println!("[grpc client] -> ProtectedPayload Protocol Close");

    let response = responses
        .message()
        .await?
        .ok_or("stream ended before Close response")?;
    let response_content = receive_server_content(session, security, &response)?;
    if !matches!(response_content.body, Some(session_content::Body::Close(_))) {
        return Err("expected protocol Close response".into());
    }
    println!("[grpc client] <- ProtectedPayload Protocol Close");
    security.close();
    Ok(())
}

fn handshake_step_from_client(frame: &ClientFrame) -> Result<u32, Status> {
    match frame.body.as_ref() {
        Some(client_frame::Body::SecurityHandshake(handshake)) => Ok(handshake.step),
        _ => Err(Status::invalid_argument(
            "expected client Noise SecurityHandshake",
        )),
    }
}

fn handshake_step_from_server(frame: &ServerFrame) -> Result<u32, Status> {
    match frame.body.as_ref() {
        Some(server_frame::Body::SecurityHandshake(handshake)) => Ok(handshake.step),
        _ => Err(Status::invalid_argument(
            "expected server Noise SecurityHandshake",
        )),
    }
}

async fn send_response(
    responses: &mpsc::Sender<Result<ServerFrame, Status>>,
    frame: ServerFrame,
) -> Result<(), Status> {
    responses
        .send(Ok(frame))
        .await
        .map_err(|_| Status::cancelled("client stopped receiving responses"))
}

fn protocol_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
