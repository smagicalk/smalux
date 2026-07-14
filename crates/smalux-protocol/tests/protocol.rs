//! 核心编解码、协商、状态机、sequence 和资源限制集成测试。

use bytes::BytesMut;
use prost::Name;
use smalux_protocol::{
    ApplicationEnvelope, ClientFrame, ClientHello, Close, CloseCode, CodecLimits, FrameCodec,
    HandshakeMessage, NegotiationPolicy, Ping, ProtocolSession, ProtocolVersion, Result,
    SecurityHandshake, SecuritySession, SecurityState, ServerFrame, ServerHello, SessionContent,
    SessionRole, SessionState, VersionRange, client_frame, negotiate_server_hello, pack_any,
    server_frame, session_content, unpack_any,
};

fn client_hello() -> ClientHello {
    ClientHello {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 2,
        }],
        supported_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        required_capabilities: Vec::new(),
        supported_security_schemes: vec!["smalux.security.transport.v1".to_owned()],
        max_frame_bytes: 1024 * 1024,
        nonce: vec![1; 32],
    }
}

fn server_hello() -> ServerHello {
    ServerHello {
        selected_version: Some(ProtocolVersion { major: 1, minor: 2 }),
        selected_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        selected_security_scheme: "smalux.security.transport.v1".to_owned(),
        max_frame_bytes: 1024 * 1024,
        session_id: vec![2; 16],
        nonce: vec![3; 32],
    }
}

#[test]
fn raw_client_frame_round_trips() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();
    let frame = ClientFrame {
        body: Some(client_frame::Body::Hello(client_hello())),
    };

    let encoded = codec.encode_client(&frame).unwrap();
    let decoded = codec.decode_client(&encoded).unwrap();

    assert_eq!(frame, decoded);
}

#[test]
fn raw_server_frame_round_trips() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();
    let frame = ServerFrame {
        body: Some(server_frame::Body::Hello(server_hello())),
    };

    let encoded = codec.encode_server(&frame).unwrap();
    let decoded = codec.decode_server(&encoded).unwrap();

    assert_eq!(frame, decoded);
}

#[test]
fn codec_rejects_frame_without_oneof_body() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();

    assert!(codec.encode_client(&ClientFrame { body: None }).is_err());
    assert!(codec.decode_client(&[]).is_err());
}

#[test]
fn delimited_decoder_waits_for_complete_frame_and_keeps_following_data() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();
    let first = ClientFrame {
        body: Some(client_frame::Body::Hello(client_hello())),
    };
    let second = first.clone();
    let first_bytes = codec.encode_client_delimited(&first).unwrap();
    let second_bytes = codec.encode_client_delimited(&second).unwrap();

    let split_at = first_bytes.len() - 1;
    let mut buffer = BytesMut::from(&first_bytes[..split_at]);
    assert!(
        codec
            .decode_client_delimited(&mut buffer)
            .unwrap()
            .is_none()
    );

    buffer.extend_from_slice(&first_bytes[split_at..]);
    buffer.extend_from_slice(&second_bytes);
    assert_eq!(
        codec.decode_client_delimited(&mut buffer).unwrap(),
        Some(first)
    );
    assert_eq!(
        codec.decode_client_delimited(&mut buffer).unwrap(),
        Some(second)
    );
    assert!(buffer.is_empty());
}

#[test]
fn codec_rejects_frame_above_local_limit() {
    let codec = FrameCodec::new(CodecLimits::new(4096).unwrap()).unwrap();
    let frame = ClientFrame {
        body: Some(client_frame::Body::Hello(ClientHello {
            nonce: vec![0; 5000],
            ..client_hello()
        })),
    };

    assert!(codec.encode_client(&frame).is_err());
}

#[test]
fn session_accepts_valid_client_handshake_order() {
    let mut session = ProtocolSession::new(SessionRole::Client, CodecLimits::default()).unwrap();
    let outbound = ClientFrame {
        body: Some(client_frame::Body::Hello(client_hello())),
    };
    session.on_client_frame(&outbound).unwrap();
    assert_eq!(session.state(), SessionState::AwaitingServerHello);

    let inbound = ServerFrame {
        body: Some(server_frame::Body::Hello(server_hello())),
    };
    session.on_server_frame(&inbound).unwrap();
    assert_eq!(session.state(), SessionState::NegotiatingSecurity);
    session
        .complete_security(&TestSecurity::plaintext())
        .unwrap();
    assert_eq!(session.state(), SessionState::Ready);
    assert_eq!(session.negotiated().unwrap().version().minor, 2);
}

#[test]
fn session_rejects_application_content_before_hello() {
    let mut session = ProtocolSession::new(SessionRole::Client, CodecLimits::default()).unwrap();
    let frame = ClientFrame {
        body: Some(client_frame::Body::PlaintextContent(Default::default())),
    };

    assert!(session.on_client_frame(&frame).is_err());
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn generated_messages_support_prost_name_for_any() {
    assert_eq!(ClientHello::type_url(), "/smalux.protocol.v1.ClientHello");
}

#[test]
fn server_negotiation_selects_highest_common_version_and_server_security_priority() {
    let client = ClientHello {
        supported_versions: vec![
            VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 5,
            },
            VersionRange {
                major: 2,
                min_minor: 0,
                max_minor: 1,
            },
        ],
        supported_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        supported_security_schemes: vec![
            "smalux.security.transport.v1".to_owned(),
            "smalux.security.test.v1".to_owned(),
        ],
        ..client_hello()
    };
    let policy = NegotiationPolicy {
        supported_versions: vec![
            VersionRange {
                major: 1,
                min_minor: 2,
                max_minor: 7,
            },
            VersionRange {
                major: 2,
                min_minor: 0,
                max_minor: 0,
            },
        ],
        supported_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        required_capabilities: Vec::new(),
        security_schemes: vec![
            "smalux.security.test.v1".to_owned(),
            "smalux.security.transport.v1".to_owned(),
        ],
        codec_limits: CodecLimits::default(),
    };

    let selected = negotiate_server_hello(&client, &policy, vec![4; 16], vec![5; 32]).unwrap();

    assert_eq!(
        selected.selected_version,
        Some(ProtocolVersion { major: 2, minor: 0 })
    );
    assert_eq!(selected.selected_security_scheme, "smalux.security.test.v1");
}

#[test]
fn server_negotiation_rejects_missing_required_capability() {
    let client = ClientHello {
        supported_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        required_capabilities: vec!["smalux.core.ping.v1".to_owned()],
        ..client_hello()
    };
    let policy = NegotiationPolicy {
        supported_versions: client.supported_versions.clone(),
        supported_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        security_schemes: client.supported_security_schemes.clone(),
        codec_limits: CodecLimits::default(),
    };

    assert!(negotiate_server_hello(&client, &policy, vec![4; 16], vec![5; 32]).is_err());
}

#[test]
fn server_negotiation_rejects_missing_version_and_security_intersections() {
    let policy = NegotiationPolicy {
        supported_versions: vec![VersionRange {
            major: 2,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        security_schemes: vec!["smalux.security.other.v1".to_owned()],
        codec_limits: CodecLimits::default(),
    };
    assert!(negotiate_server_hello(&client_hello(), &policy, vec![4; 16], vec![5; 32]).is_err());

    let policy = NegotiationPolicy {
        supported_versions: client_hello().supported_versions,
        ..policy
    };
    assert!(negotiate_server_hello(&client_hello(), &policy, vec![4; 16], vec![5; 32]).is_err());
}

#[test]
fn application_sequence_is_checked_per_direction() {
    let mut session = ready_protected_client_session();
    let content = SessionContent {
        body: Some(session_content::Body::Application(ApplicationEnvelope {
            message_id: vec![7; 16],
            correlation_id: None,
            sequence: 1,
            sent_at_unix_ms: 1,
            payload: Some(pack_any(&Ping {
                nonce: 9,
                sent_at_unix_ms: 1,
            })),
        })),
    };

    session.on_decrypted_client_content(&content).unwrap();
    assert!(session.on_decrypted_client_content(&content).is_err());
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn any_helpers_round_trip_named_message() {
    let input = Ping {
        nonce: 42,
        sent_at_unix_ms: 100,
    };

    let packed = pack_any(&input);
    let output: Ping = unpack_any(&packed).unwrap();

    assert_eq!(input, output);
}

#[test]
fn delimited_decoder_rejects_non_canonical_varint() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();
    let mut buffer = bytes::BytesMut::from(&[0x80, 0x00][..]);

    assert!(codec.decode_client_delimited(&mut buffer).is_err());
}

#[test]
fn security_handshake_steps_are_global_across_both_directions() {
    let mut session = negotiating_client_session();
    session
        .on_client_frame(&ClientFrame {
            body: Some(client_frame::Body::SecurityHandshake(SecurityHandshake {
                scheme: "smalux.security.transport.v1".to_owned(),
                step: 0,
                payload: vec![1],
            })),
        })
        .unwrap();
    session
        .on_server_frame(&ServerFrame {
            body: Some(server_frame::Body::SecurityHandshake(SecurityHandshake {
                scheme: "smalux.security.transport.v1".to_owned(),
                step: 1,
                payload: vec![2],
            })),
        })
        .unwrap();

    assert_eq!(session.state(), SessionState::NegotiatingSecurity);
}

#[test]
fn security_handshake_rejects_wrong_step() {
    let mut session = negotiating_client_session();

    assert!(
        session
            .on_client_frame(&ClientFrame {
                body: Some(client_frame::Body::SecurityHandshake(SecurityHandshake {
                    scheme: "smalux.security.transport.v1".to_owned(),
                    step: 1,
                    payload: vec![1],
                })),
            })
            .is_err()
    );
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn server_cannot_raise_the_clients_local_frame_limit() {
    let limits = CodecLimits::new(4096).unwrap();
    let mut session = ProtocolSession::new(SessionRole::Client, limits).unwrap();
    session
        .on_client_frame(&ClientFrame {
            body: Some(client_frame::Body::Hello(client_hello())),
        })
        .unwrap();

    assert!(
        session
            .on_server_frame(&ServerFrame {
                body: Some(server_frame::Body::Hello(server_hello())),
            })
            .is_err()
    );
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn arbitrary_bytes_never_panic_the_raw_decoders() {
    let codec = FrameCodec::new(CodecLimits::default()).unwrap();
    let mut state = 0x9e37_79b9_u32;

    for length in 0..512 {
        let mut input = vec![0_u8; length];
        for byte in &mut input {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *byte = (state >> 24) as u8;
        }
        let _ = codec.decode_client(&input);
        let _ = codec.decode_server(&input);
    }
}

#[test]
fn close_exchange_transitions_through_closing_to_closed() {
    let mut session = ready_client_session();
    let close = SessionContent {
        body: Some(session_content::Body::Close(Close {
            code: CloseCode::Normal as i32,
            reason: "done".to_owned(),
            retryable: false,
        })),
    };

    session
        .on_client_frame(&ClientFrame {
            body: Some(client_frame::Body::PlaintextContent(close.clone())),
        })
        .unwrap();
    assert_eq!(session.state(), SessionState::Closing);
    session
        .on_server_frame(&ServerFrame {
            body: Some(server_frame::Body::PlaintextContent(close)),
        })
        .unwrap();
    assert_eq!(session.state(), SessionState::Closed);
}

#[test]
fn protected_session_rejects_plaintext_content() {
    let mut session = ready_protected_client_session();
    let frame = ClientFrame {
        body: Some(client_frame::Body::PlaintextContent(SessionContent {
            body: Some(session_content::Body::Ping(Ping {
                nonce: 1,
                sent_at_unix_ms: 1,
            })),
        })),
    };

    assert!(session.on_client_frame(&frame).is_err());
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn security_completion_rejects_wrong_scheme() {
    let mut session = negotiating_client_session();

    assert!(
        session
            .complete_security(&TestSecurity::with(
                "smalux.security.other.v1",
                SecurityState::Ready,
                true,
            ))
            .is_err()
    );
    assert_eq!(session.state(), SessionState::Failed);
}

#[test]
fn security_completion_rejects_session_that_is_not_ready() {
    let mut session = negotiating_client_session();

    assert!(
        session
            .complete_security(&TestSecurity::with(
                "smalux.security.transport.v1",
                SecurityState::Handshaking,
                true,
            ))
            .is_err()
    );
    assert_eq!(session.state(), SessionState::Failed);
}

fn ready_client_session() -> ProtocolSession {
    let mut session = negotiating_client_session();
    session
        .complete_security(&TestSecurity::plaintext())
        .unwrap();
    session
}

fn ready_protected_client_session() -> ProtocolSession {
    let mut session = negotiating_client_session();
    session
        .complete_security(&TestSecurity::protected())
        .unwrap();
    session
}

fn negotiating_client_session() -> ProtocolSession {
    let mut session = ProtocolSession::new(SessionRole::Client, CodecLimits::default()).unwrap();
    session
        .on_client_frame(&ClientFrame {
            body: Some(client_frame::Body::Hello(client_hello())),
        })
        .unwrap();
    session
        .on_server_frame(&ServerFrame {
            body: Some(server_frame::Body::Hello(server_hello())),
        })
        .unwrap();
    session
}

struct TestSecurity {
    scheme: &'static str,
    state: SecurityState,
    protects_content: bool,
}

impl TestSecurity {
    fn plaintext() -> Self {
        Self::with("smalux.security.transport.v1", SecurityState::Ready, false)
    }

    fn protected() -> Self {
        Self::with("smalux.security.transport.v1", SecurityState::Ready, true)
    }

    fn with(scheme: &'static str, state: SecurityState, protects_content: bool) -> Self {
        Self {
            scheme,
            state,
            protects_content,
        }
    }
}

impl SecuritySession for TestSecurity {
    fn scheme(&self) -> &str {
        self.scheme
    }

    fn state(&self) -> SecurityState {
        self.state
    }

    fn protects_content(&self) -> bool {
        self.protects_content
    }

    fn next_handshake(&self) -> Result<Option<HandshakeMessage>> {
        Ok(None)
    }

    fn receive_handshake(&self, _message: HandshakeMessage) -> Result<()> {
        Ok(())
    }

    fn protect(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn unprotect(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }

    fn close(&self) {}
}
