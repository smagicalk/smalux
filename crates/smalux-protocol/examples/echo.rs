//! 在单个进程内启动本地 TCP Server，并通过 Smalux 协议完成 Echo 往返。

use std::{
    error::Error,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use prost::{Message, Name};
use smalux_protocol::{
    ApplicationEnvelope, ClientFrame, ClientHello, Close, CloseCode, CodecLimits, FrameCodec,
    HandshakeMessage, NegotiationPolicy, ProtocolSession, Result as ProtocolResult,
    SecuritySession, SecurityState, ServerFrame, SessionContent, SessionRole, VersionRange,
    client_frame, negotiate_server_hello, pack_any, server_frame, session_content, unpack_any,
};

type ExampleResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const SECURITY_SCHEME: &str = "smalux.security.example-plaintext.v1";
const CAPABILITY: &str = "smalux.example.echo.v1";

/// 示例自己的业务消息，通过 Any 放入核心协议，不修改核心 Frame。
#[derive(Clone, PartialEq, Message)]
struct EchoMessage {
    /// Client 输入或 Server 回显的文本。
    #[prost(string, tag = "1")]
    text: String,
}

impl Name for EchoMessage {
    const NAME: &'static str = "EchoMessage";
    const PACKAGE: &'static str = "smalux.example.echo.v1";
}

/// 仅供本地示例使用的明文安全会话，生产环境不得使用。
struct ExamplePlaintextSecurity;

impl SecuritySession for ExamplePlaintextSecurity {
    fn scheme(&self) -> &str {
        SECURITY_SCHEME
    }

    fn state(&self) -> SecurityState {
        SecurityState::Ready
    }

    fn protects_content(&self) -> bool {
        false
    }

    fn next_handshake(&self) -> ProtocolResult<Option<HandshakeMessage>> {
        Ok(None)
    }

    fn receive_handshake(&self, _message: HandshakeMessage) -> ProtocolResult<()> {
        Ok(())
    }

    fn protect(&self, plaintext: &[u8]) -> ProtocolResult<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn unprotect(&self, ciphertext: &[u8]) -> ProtocolResult<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }

    fn close(&self) {}
}

fn main() -> ExampleResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    println!("[server] listening on {address}");

    let server = thread::spawn(move || run_server(listener));
    run_client(address)?;

    server
        .join()
        .map_err(|_| io::Error::other("echo server thread panicked"))??;
    Ok(())
}

fn run_server(listener: TcpListener) -> ExampleResult<()> {
    let (mut stream, peer) = listener.accept()?;
    stream.set_nodelay(true)?;
    println!("[server] accepted {peer}");

    let codec = FrameCodec::new(CodecLimits::default())?;
    let mut receive_buffer = BytesMut::new();
    let mut session = ProtocolSession::new(SessionRole::Server, codec.limits())?;

    let client_frame = read_client_frame(&mut stream, &codec, &mut receive_buffer)?;
    let client_hello = match client_frame.body.as_ref() {
        Some(client_frame::Body::Hello(hello)) => hello.clone(),
        _ => return Err("expected ClientHello".into()),
    };
    session.on_client_frame(&client_frame)?;
    println!("[server] <- ClientHello");

    let policy = NegotiationPolicy {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: vec![CAPABILITY.to_owned()],
        required_capabilities: vec![CAPABILITY.to_owned()],
        security_schemes: vec![SECURITY_SCHEME.to_owned()],
        codec_limits: codec.limits(),
    };
    let selected = negotiate_server_hello(&client_hello, &policy, vec![2; 16], vec![3; 32])?;
    let server_frame = ServerFrame {
        body: Some(server_frame::Body::Hello(selected)),
    };
    session.on_server_frame(&server_frame)?;
    write_server_frame(&mut stream, &codec, &server_frame)?;
    println!("[server] -> ServerHello");

    session.complete_security(&ExamplePlaintextSecurity)?;
    println!("[server] session ready (example plaintext)");

    let mut server_sequence = 1_u64;
    loop {
        let frame = read_client_frame(&mut stream, &codec, &mut receive_buffer)?;
        session.on_client_frame(&frame)?;
        let content = match frame.body {
            Some(client_frame::Body::PlaintextContent(content)) => content,
            _ => return Err("expected plaintext SessionContent".into()),
        };

        match content.body {
            Some(session_content::Body::Application(request)) => {
                let request_payload = request
                    .payload
                    .as_ref()
                    .ok_or("echo request does not contain payload")?;
                let echo: EchoMessage = unpack_any(request_payload)?;
                println!(
                    "[server] <- EchoMessage sequence={} text={:?}",
                    request.sequence, echo.text
                );

                let response = application_content(
                    id_from_sequence(0x53, server_sequence),
                    Some(request.message_id),
                    server_sequence,
                    EchoMessage { text: echo.text },
                );
                let response_frame = ServerFrame {
                    body: Some(server_frame::Body::PlaintextContent(response)),
                };
                session.on_server_frame(&response_frame)?;
                write_server_frame(&mut stream, &codec, &response_frame)?;
                println!("[server] -> EchoMessage sequence={server_sequence}");
                server_sequence += 1;
            }
            Some(session_content::Body::Close(_)) => {
                println!("[server] <- Close");
                let response = ServerFrame {
                    body: Some(server_frame::Body::PlaintextContent(close_content())),
                };
                session.on_server_frame(&response)?;
                write_server_frame(&mut stream, &codec, &response)?;
                println!("[server] -> Close");
                return Ok(());
            }
            _ => return Err("unexpected SessionContent".into()),
        }
    }
}

fn run_client(address: std::net::SocketAddr) -> ExampleResult<()> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    println!("[client] connected to {address}");

    let codec = FrameCodec::new(CodecLimits::default())?;
    let mut receive_buffer = BytesMut::new();
    let mut session = ProtocolSession::new(SessionRole::Client, codec.limits())?;

    let hello = ClientFrame {
        body: Some(client_frame::Body::Hello(ClientHello {
            supported_versions: vec![VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 0,
            }],
            supported_capabilities: vec![CAPABILITY.to_owned()],
            required_capabilities: vec![CAPABILITY.to_owned()],
            supported_security_schemes: vec![SECURITY_SCHEME.to_owned()],
            max_frame_bytes: codec.limits().max_frame_bytes() as u32,
            nonce: vec![1; 32],
        })),
    };
    session.on_client_frame(&hello)?;
    write_client_frame(&mut stream, &codec, &hello)?;
    println!("[client] -> ClientHello");

    let server_hello = read_server_frame(&mut stream, &codec, &mut receive_buffer)?;
    session.on_server_frame(&server_hello)?;
    println!("[client] <- ServerHello");
    session.complete_security(&ExamplePlaintextSecurity)?;
    println!("[client] session ready (example plaintext)");

    let mut client_sequence = 1_u64;
    loop {
        print!("echo> ");
        io::stdout().flush()?;

        let mut input = String::new();
        let bytes_read = io::stdin().read_line(&mut input)?;
        let text = input.trim_end_matches(['\r', '\n']);
        if bytes_read == 0 || text.eq_ignore_ascii_case("exit") {
            let close = ClientFrame {
                body: Some(client_frame::Body::PlaintextContent(close_content())),
            };
            session.on_client_frame(&close)?;
            write_client_frame(&mut stream, &codec, &close)?;
            println!("[client] -> Close");

            let response = read_server_frame(&mut stream, &codec, &mut receive_buffer)?;
            session.on_server_frame(&response)?;
            println!("[client] <- Close");
            return Ok(());
        }
        if text.is_empty() {
            continue;
        }

        let request_id = id_from_sequence(0x43, client_sequence);
        let request = application_content(
            request_id.clone(),
            None,
            client_sequence,
            EchoMessage {
                text: text.to_owned(),
            },
        );
        let request_frame = ClientFrame {
            body: Some(client_frame::Body::PlaintextContent(request)),
        };
        session.on_client_frame(&request_frame)?;
        write_client_frame(&mut stream, &codec, &request_frame)?;
        println!("[client] -> EchoMessage sequence={client_sequence}");

        let response_frame = read_server_frame(&mut stream, &codec, &mut receive_buffer)?;
        session.on_server_frame(&response_frame)?;
        let response = match response_frame.body {
            Some(server_frame::Body::PlaintextContent(SessionContent {
                body: Some(session_content::Body::Application(response)),
            })) => response,
            _ => return Err("expected echo response".into()),
        };
        if response.correlation_id.as_ref() != Some(&request_id) {
            return Err("echo response correlation_id does not match request".into());
        }
        let payload = response
            .payload
            .as_ref()
            .ok_or("echo response does not contain payload")?;
        let echo: EchoMessage = unpack_any(payload)?;
        println!(
            "[client] <- EchoMessage sequence={} text={:?}",
            response.sequence, echo.text
        );
        println!("server returned: {}", echo.text);
        client_sequence += 1;
    }
}

fn application_content(
    message_id: Vec<u8>,
    correlation_id: Option<Vec<u8>>,
    sequence: u64,
    message: EchoMessage,
) -> SessionContent {
    SessionContent {
        body: Some(session_content::Body::Application(ApplicationEnvelope {
            message_id,
            correlation_id,
            sequence,
            sent_at_unix_ms: unix_time_ms(),
            payload: Some(pack_any(&message)),
        })),
    }
}

fn close_content() -> SessionContent {
    SessionContent {
        body: Some(session_content::Body::Close(Close {
            code: CloseCode::Normal as i32,
            reason: "example finished".to_owned(),
            retryable: false,
        })),
    }
}

fn id_from_sequence(marker: u8, sequence: u64) -> Vec<u8> {
    let mut id = vec![0_u8; 16];
    id[0] = marker;
    id[8..].copy_from_slice(&sequence.to_be_bytes());
    id
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn write_client_frame(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    frame: &ClientFrame,
) -> ExampleResult<()> {
    stream.write_all(&codec.encode_client_delimited(frame)?)?;
    Ok(())
}

fn write_server_frame(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    frame: &ServerFrame,
) -> ExampleResult<()> {
    stream.write_all(&codec.encode_server_delimited(frame)?)?;
    Ok(())
}

fn read_client_frame(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    buffer: &mut BytesMut,
) -> ExampleResult<ClientFrame> {
    read_delimited(stream, buffer, |buffer| {
        codec.decode_client_delimited(buffer)
    })
}

fn read_server_frame(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    buffer: &mut BytesMut,
) -> ExampleResult<ServerFrame> {
    read_delimited(stream, buffer, |buffer| {
        codec.decode_server_delimited(buffer)
    })
}

fn read_delimited<T>(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    mut decode: impl FnMut(&mut BytesMut) -> ProtocolResult<Option<T>>,
) -> ExampleResult<T> {
    loop {
        if let Some(frame) = decode(buffer)? {
            return Ok(frame);
        }

        let mut chunk = [0_u8; 4096];
        let bytes_read = stream.read(&mut chunk)?;
        if bytes_read == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into());
        }
        buffer.extend_from_slice(&chunk[..bytes_read]);
    }
}
