//! 使用本地临时证书演示 WSS 承载 Smalux 协议 Echo 消息。

mod support;

use std::{io::Write, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
};
use smalux_protocol::{
    ClientFrame, CodecLimits, FrameCodec, NoiseProvider, ProtocolSession, SecurityRole,
    SecuritySession, ServerFrame, SessionRole, client_frame, negotiate_server_hello, server_frame,
    session_content, unpack_any,
};
use support::{
    EchoMessage, application_content, client_handshake_frame, client_hello, close_content,
    id_from_sequence, noise_providers, protected_client_frame, protected_server_frame,
    receive_client_content, receive_client_handshake, receive_server_content,
    receive_server_handshake, server_handshake_frame, server_policy, start_noise_session,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    net::TcpListener,
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    Connector, WebSocketStream, accept_async, connect_async_tls_with_config, tungstenite::Message,
};

type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> ExampleResult<()> {
    // 显式选择 Ring，避免同时启用多个 Rustls provider 时无法确定默认实现。
    let _ = rustls::crypto::ring::default_provider().install_default();

    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let certificate = cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (client_security, server_security) = noise_providers()?;
    println!("[wss server] listening on wss://{address}");

    let server = tokio::spawn(run_server(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        server_security,
    ));
    run_client(address, certificate, client_security).await?;
    server.await??;
    Ok(())
}

async fn run_server(
    listener: TcpListener,
    tls: TlsAcceptor,
    provider: NoiseProvider,
) -> ExampleResult<()> {
    let (tcp, peer) = listener.accept().await?;
    tcp.set_nodelay(true)?;
    let tls_stream = tls.accept(tcp).await?;
    let mut websocket = accept_async(tls_stream).await?;
    println!("[wss server] TLS and WebSocket accepted from {peer}");

    let codec = FrameCodec::new(CodecLimits::default())?;
    let mut session = ProtocolSession::new(SessionRole::Server, codec.limits())?;

    let client_frame = receive_client_frame(&mut websocket, &codec).await?;
    let hello = match client_frame.body.as_ref() {
        Some(client_frame::Body::Hello(hello)) => hello,
        _ => return Err("expected ClientHello".into()),
    };
    let selected = negotiate_server_hello(
        hello,
        &server_policy(codec.limits(), &provider),
        vec![2; 16],
        vec![3; 32],
    )?;
    session.on_client_frame(&client_frame)?;
    println!("[wss server] <- ClientHello (Binary)");

    let server_hello = ServerFrame {
        body: Some(server_frame::Body::Hello(selected)),
    };
    session.on_server_frame(&server_hello)?;
    send_server_frame(&mut websocket, &codec, &server_hello).await?;
    println!("[wss server] -> ServerHello (Binary)");

    let security = start_noise_session(&session, SecurityRole::Server, &provider)?;
    let client_handshake = receive_client_frame(&mut websocket, &codec).await?;
    let client_step = handshake_step_from_client(&client_handshake)?;
    receive_client_handshake(&mut session, security.as_ref(), &client_handshake)?;
    println!("[wss server] <- Noise XX step={client_step}");

    let message = security
        .next_handshake()?
        .ok_or("Noise server did not produce handshake step 1")?;
    let server_step = message.step;
    let server_handshake = server_handshake_frame(security.as_ref(), message);
    session.on_server_frame(&server_handshake)?;
    send_server_frame(&mut websocket, &codec, &server_handshake).await?;
    println!("[wss server] -> Noise XX step={server_step}");

    let client_handshake = receive_client_frame(&mut websocket, &codec).await?;
    let client_step = handshake_step_from_client(&client_handshake)?;
    receive_client_handshake(&mut session, security.as_ref(), &client_handshake)?;
    println!("[wss server] <- Noise XX step={client_step}");
    session.complete_security(security.as_ref())?;
    println!("[wss server] Noise transport ready");

    let mut server_sequence = 1_u64;
    loop {
        let frame = receive_client_frame(&mut websocket, &codec).await?;
        let content = receive_client_content(&mut session, security.as_ref(), &frame)?;
        match content.body {
            Some(session_content::Body::Application(request)) => {
                let payload = request
                    .payload
                    .as_ref()
                    .ok_or("echo request does not contain payload")?;
                let echo: EchoMessage = unpack_any(payload)?;
                println!(
                    "[wss server] <- EchoMessage sequence={} text={:?}",
                    request.sequence, echo.text
                );

                let response_content = application_content(
                    id_from_sequence(0x53, server_sequence),
                    Some(request.message_id),
                    server_sequence,
                    echo.text,
                );
                let response =
                    protected_server_frame(&mut session, security.as_ref(), &response_content)?;
                send_server_frame(&mut websocket, &codec, &response).await?;
                println!("[wss server] -> ProtectedPayload EchoMessage sequence={server_sequence}");
                server_sequence += 1;
            }
            Some(session_content::Body::Close(_)) => {
                println!("[wss server] <- Protocol Close");
                let response =
                    protected_server_frame(&mut session, security.as_ref(), &close_content())?;
                send_server_frame(&mut websocket, &codec, &response).await?;
                println!("[wss server] -> ProtectedPayload Protocol Close");
                security.close();
                websocket.close(None).await?;
                return Ok(());
            }
            _ => return Err("unexpected SessionContent".into()),
        }
    }
}

async fn run_client(
    address: std::net::SocketAddr,
    certificate: CertificateDer<'static>,
    provider: NoiseProvider,
) -> ExampleResult<()> {
    let mut roots = RootCertStore::empty();
    roots.add(certificate)?;
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = Connector::Rustls(Arc::new(client_config));
    let url = format!("wss://{address}");
    let (mut websocket, _) =
        connect_async_tls_with_config(url, None, true, Some(connector)).await?;
    println!("[wss client] TLS and WebSocket connected");

    let codec = FrameCodec::new(CodecLimits::default())?;
    let mut session = ProtocolSession::new(SessionRole::Client, codec.limits())?;
    let hello = ClientFrame {
        body: Some(client_frame::Body::Hello(client_hello(
            codec.limits(),
            &provider,
        ))),
    };
    session.on_client_frame(&hello)?;
    send_client_frame(&mut websocket, &codec, &hello).await?;
    println!("[wss client] -> ClientHello (Binary)");

    let server_hello = receive_server_frame(&mut websocket, &codec).await?;
    session.on_server_frame(&server_hello)?;
    println!("[wss client] <- ServerHello (Binary)");

    let security = start_noise_session(&session, SecurityRole::Client, &provider)?;
    let message = security
        .next_handshake()?
        .ok_or("Noise client did not produce handshake step 0")?;
    let client_step = message.step;
    let client_handshake = client_handshake_frame(security.as_ref(), message);
    session.on_client_frame(&client_handshake)?;
    send_client_frame(&mut websocket, &codec, &client_handshake).await?;
    println!("[wss client] -> Noise XX step={client_step}");

    let server_handshake = receive_server_frame(&mut websocket, &codec).await?;
    let server_step = handshake_step_from_server(&server_handshake)?;
    receive_server_handshake(&mut session, security.as_ref(), &server_handshake)?;
    println!("[wss client] <- Noise XX step={server_step}");

    let message = security
        .next_handshake()?
        .ok_or("Noise client did not produce handshake step 2")?;
    let client_step = message.step;
    let client_handshake = client_handshake_frame(security.as_ref(), message);
    session.on_client_frame(&client_handshake)?;
    send_client_frame(&mut websocket, &codec, &client_handshake).await?;
    println!("[wss client] -> Noise XX step={client_step}");
    session.complete_security(security.as_ref())?;
    println!("[wss client] Noise transport ready");

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut client_sequence = 1_u64;
    loop {
        print!("wss echo> ");
        std::io::stdout().flush()?;
        let Some(text) = lines.next_line().await? else {
            return close_client(&mut websocket, &codec, &mut session, security.as_ref()).await;
        };
        if text.eq_ignore_ascii_case("exit") {
            return close_client(&mut websocket, &codec, &mut session, security.as_ref()).await;
        }
        if text.is_empty() {
            continue;
        }

        let request_id = id_from_sequence(0x43, client_sequence);
        let request_content = application_content(request_id.clone(), None, client_sequence, text);
        let request = protected_client_frame(&mut session, security.as_ref(), &request_content)?;
        send_client_frame(&mut websocket, &codec, &request).await?;
        println!("[wss client] -> ProtectedPayload EchoMessage sequence={client_sequence}");

        let response = receive_server_frame(&mut websocket, &codec).await?;
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
            "[wss client] <- EchoMessage sequence={} text={:?}",
            envelope.sequence, echo.text
        );
        println!("server returned: {}", echo.text);
        client_sequence += 1;
    }
}

async fn close_client<S>(
    websocket: &mut WebSocketStream<S>,
    codec: &FrameCodec,
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
) -> ExampleResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let close = protected_client_frame(session, security, &close_content())?;
    send_client_frame(websocket, codec, &close).await?;
    println!("[wss client] -> ProtectedPayload Protocol Close");

    let response = receive_server_frame(websocket, codec).await?;
    let response_content = receive_server_content(session, security, &response)?;
    if !matches!(response_content.body, Some(session_content::Body::Close(_))) {
        return Err("expected protocol Close response".into());
    }
    println!("[wss client] <- ProtectedPayload Protocol Close");
    security.close();
    websocket.close(None).await?;
    Ok(())
}

fn handshake_step_from_client(frame: &ClientFrame) -> ExampleResult<u32> {
    match frame.body.as_ref() {
        Some(client_frame::Body::SecurityHandshake(handshake)) => Ok(handshake.step),
        _ => Err("expected client Noise SecurityHandshake".into()),
    }
}

fn handshake_step_from_server(frame: &ServerFrame) -> ExampleResult<u32> {
    match frame.body.as_ref() {
        Some(server_frame::Body::SecurityHandshake(handshake)) => Ok(handshake.step),
        _ => Err("expected server Noise SecurityHandshake".into()),
    }
}

async fn send_client_frame<S>(
    websocket: &mut WebSocketStream<S>,
    codec: &FrameCodec,
    frame: &ClientFrame,
) -> ExampleResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Binary(codec.encode_client(frame)?.into()))
        .await?;
    Ok(())
}

async fn send_server_frame<S>(
    websocket: &mut WebSocketStream<S>,
    codec: &FrameCodec,
    frame: &ServerFrame,
) -> ExampleResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Binary(codec.encode_server(frame)?.into()))
        .await?;
    Ok(())
}

async fn receive_client_frame<S>(
    websocket: &mut WebSocketStream<S>,
    codec: &FrameCodec,
) -> ExampleResult<ClientFrame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    codec
        .decode_client(&receive_binary(websocket).await?)
        .map_err(Into::into)
}

async fn receive_server_frame<S>(
    websocket: &mut WebSocketStream<S>,
    codec: &FrameCodec,
) -> ExampleResult<ServerFrame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    codec
        .decode_server(&receive_binary(websocket).await?)
        .map_err(Into::into)
}

async fn receive_binary<S>(websocket: &mut WebSocketStream<S>) -> ExampleResult<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match websocket.next().await {
            Some(Ok(Message::Binary(bytes))) => return Ok(bytes.to_vec()),
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | None => return Err("WebSocket closed".into()),
            Some(Ok(_)) => return Err("expected WebSocket Binary message".into()),
            Some(Err(error)) => return Err(error.into()),
        }
    }
}
