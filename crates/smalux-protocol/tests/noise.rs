//! Noise XX/IK 身份校验、分片、篡改、重放和并发行为集成测试。

use std::{sync::Arc, thread};

use smalux_protocol::{
    ClientFrame, ClientHello, CodecLimits, NOISE_IK_SCHEME, NOISE_RECORD_PLAINTEXT_BYTES,
    NOISE_XX_SCHEME, NegotiationPolicy, NoiseKeypair, NoiseProvider, PinnedRemoteKey,
    ProtocolSession, SecurityContext, SecurityProvider, SecurityRole, SecuritySession,
    SecurityState, ServerFrame, SessionRole, VersionRange, client_frame, negotiate_server_hello,
    server_frame,
};

fn security_contexts(scheme: &str, session_marker: u8) -> (SecurityContext, SecurityContext) {
    let limits = CodecLimits::default();
    let hello = ClientHello {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        supported_security_schemes: vec![scheme.to_owned()],
        max_frame_bytes: limits.max_frame_bytes() as u32,
        nonce: vec![1; 32],
    };
    let policy = NegotiationPolicy {
        supported_versions: hello.supported_versions.clone(),
        supported_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        security_schemes: vec![scheme.to_owned()],
        codec_limits: limits,
    };
    let selected =
        negotiate_server_hello(&hello, &policy, vec![session_marker; 16], vec![2; 32]).unwrap();
    let mut protocol = ProtocolSession::new(SessionRole::Client, limits).unwrap();
    protocol
        .on_client_frame(&ClientFrame {
            body: Some(client_frame::Body::Hello(hello)),
        })
        .unwrap();
    protocol
        .on_server_frame(&ServerFrame {
            body: Some(server_frame::Body::Hello(selected)),
        })
        .unwrap();
    let negotiated = protocol.negotiated().unwrap();
    (
        negotiated.security_context(SecurityRole::Client),
        negotiated.security_context(SecurityRole::Server),
    )
}

fn xx_sessions() -> (Arc<dyn SecuritySession>, Arc<dyn SecuritySession>) {
    let client_keys = NoiseKeypair::generate().unwrap();
    let server_keys = NoiseKeypair::generate().unwrap();
    let client_public = client_keys.public_key();
    let server_public = server_keys.public_key();
    let client_provider = NoiseProvider::builder(
        SecurityRole::Client,
        client_keys,
        PinnedRemoteKey::new(server_public),
    )
    .enable_xx()
    .build()
    .unwrap();
    let server_provider = NoiseProvider::builder(
        SecurityRole::Server,
        server_keys,
        PinnedRemoteKey::new(client_public),
    )
    .enable_xx()
    .build()
    .unwrap();
    let (client_context, server_context) = security_contexts(NOISE_XX_SCHEME, 3);
    let client = client_provider.start(client_context).unwrap();
    let server = server_provider.start(server_context).unwrap();
    drive_handshake(&client, &server).unwrap();
    (client, server)
}

fn ik_sessions() -> (Arc<dyn SecuritySession>, Arc<dyn SecuritySession>) {
    let client_keys = NoiseKeypair::generate().unwrap();
    let server_keys = NoiseKeypair::generate().unwrap();
    let client_public = client_keys.public_key();
    let server_public = server_keys.public_key();
    let client_provider = NoiseProvider::builder(
        SecurityRole::Client,
        client_keys,
        PinnedRemoteKey::new(server_public),
    )
    .enable_ik(Some(server_public))
    .build()
    .unwrap();
    let server_provider = NoiseProvider::builder(
        SecurityRole::Server,
        server_keys,
        PinnedRemoteKey::new(client_public),
    )
    .enable_ik(None)
    .build()
    .unwrap();
    let (client_context, server_context) = security_contexts(NOISE_IK_SCHEME, 4);
    let client = client_provider.start(client_context).unwrap();
    let server = server_provider.start(server_context).unwrap();
    drive_handshake(&client, &server).unwrap();
    (client, server)
}

fn drive_handshake(
    client: &Arc<dyn SecuritySession>,
    server: &Arc<dyn SecuritySession>,
) -> smalux_protocol::Result<()> {
    for _ in 0..8 {
        if let Some(message) = client.next_handshake()? {
            server.receive_handshake(message)?;
        }
        if let Some(message) = server.next_handshake()? {
            client.receive_handshake(message)?;
        }
        if client.state() == SecurityState::Ready && server.state() == SecurityState::Ready {
            return Ok(());
        }
    }
    Err(smalux_protocol::Error::Security(
        "Noise handshake did not finish".to_owned(),
    ))
}

#[test]
fn noise_xx_round_trips_and_exposes_verified_identity() {
    let (client, server) = xx_sessions();
    let plaintext = b"hello through Noise XX";

    let ciphertext = client.protect(plaintext).unwrap();
    assert_ne!(ciphertext, plaintext);
    assert_eq!(server.unprotect(&ciphertext).unwrap(), plaintext);
    assert!(client.remote_static_key().is_some());
    assert!(server.remote_static_key().is_some());
    assert_eq!(client.channel_binding(), server.channel_binding());
}

#[test]
fn noise_ik_round_trips_in_both_directions() {
    let (client, server) = ik_sessions();

    let client_ciphertext = client.protect(b"client to server").unwrap();
    assert_eq!(
        server.unprotect(&client_ciphertext).unwrap(),
        b"client to server"
    );
    let server_ciphertext = server.protect(b"server to client").unwrap();
    assert_eq!(
        client.unprotect(&server_ciphertext).unwrap(),
        b"server to client"
    );
}

#[test]
fn noise_rejects_wrong_pinned_remote_key() {
    let client_keys = NoiseKeypair::generate().unwrap();
    let server_keys = NoiseKeypair::generate().unwrap();
    let server_public = server_keys.public_key();
    let client_provider = NoiseProvider::builder(
        SecurityRole::Client,
        client_keys,
        PinnedRemoteKey::new(server_public),
    )
    .enable_xx()
    .build()
    .unwrap();
    let server_provider = NoiseProvider::builder(
        SecurityRole::Server,
        server_keys,
        PinnedRemoteKey::new([0x55; 32]),
    )
    .enable_xx()
    .build()
    .unwrap();
    let (client_context, server_context) = security_contexts(NOISE_XX_SCHEME, 5);
    let client = client_provider.start(client_context).unwrap();
    let server = server_provider.start(server_context).unwrap();

    assert!(drive_handshake(&client, &server).is_err());
    assert_eq!(server.state(), SecurityState::Failed);
}

#[test]
fn noise_binds_the_negotiation_transcript() {
    let client_keys = NoiseKeypair::generate().unwrap();
    let server_keys = NoiseKeypair::generate().unwrap();
    let client_public = client_keys.public_key();
    let server_public = server_keys.public_key();
    let client_provider = NoiseProvider::builder(
        SecurityRole::Client,
        client_keys,
        PinnedRemoteKey::new(server_public),
    )
    .enable_xx()
    .build()
    .unwrap();
    let server_provider = NoiseProvider::builder(
        SecurityRole::Server,
        server_keys,
        PinnedRemoteKey::new(client_public),
    )
    .enable_xx()
    .build()
    .unwrap();
    let (client_context, _) = security_contexts(NOISE_XX_SCHEME, 6);
    let (_, different_server_context) = security_contexts(NOISE_XX_SCHEME, 7);
    let client = client_provider.start(client_context).unwrap();
    let server = server_provider.start(different_server_context).unwrap();

    assert!(drive_handshake(&client, &server).is_err());
    assert!(
        client.state() == SecurityState::Failed || server.state() == SecurityState::Failed,
        "at least one peer must fail when negotiation transcripts differ"
    );
}

#[test]
fn noise_transparently_fragments_payloads_above_one_record() {
    let (client, server) = xx_sessions();
    let plaintext = vec![0xA5; NOISE_RECORD_PLAINTEXT_BYTES * 3 + 17];

    let ciphertext = client.protect(&plaintext).unwrap();

    assert!(ciphertext.starts_with(b"SNR1"));
    assert_eq!(server.unprotect(&ciphertext).unwrap(), plaintext);
}

#[test]
fn noise_supports_the_negotiated_maximum_plaintext() {
    let (client, server) = xx_sessions();
    let plaintext = vec![0x3C; client.max_plaintext_bytes()];

    let ciphertext = client.protect(&plaintext).unwrap();

    assert!(ciphertext.len() <= CodecLimits::default().max_frame_bytes());
    assert_eq!(server.unprotect(&ciphertext).unwrap(), plaintext);
}

#[test]
fn noise_rejects_tampering_and_replay() {
    let (client, server) = xx_sessions();
    let ciphertext = client.protect(b"authenticated").unwrap();
    let mut tampered = ciphertext.clone();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(server.unprotect(&tampered).is_err());
    assert_eq!(server.state(), SecurityState::Failed);

    let (client, server) = xx_sessions();
    let ciphertext = client.protect(b"only once").unwrap();
    assert_eq!(server.unprotect(&ciphertext).unwrap(), b"only once");
    assert!(server.unprotect(&ciphertext).is_err());
    assert_eq!(server.state(), SecurityState::Failed);
}

#[test]
fn noise_session_can_be_shared_by_concurrent_read_and_write_tasks() {
    let (client, server) = xx_sessions();
    thread::scope(|scope| {
        let client_sender = client.clone();
        let server_receiver = server.clone();
        scope.spawn(move || {
            for value in 0_u8..64 {
                let ciphertext = client_sender.protect(&[value; 128]).unwrap();
                assert_eq!(
                    server_receiver.unprotect(&ciphertext).unwrap(),
                    [value; 128]
                );
            }
        });

        let server_sender = server.clone();
        let client_receiver = client.clone();
        scope.spawn(move || {
            for value in 64_u8..128 {
                let ciphertext = server_sender.protect(&[value; 128]).unwrap();
                assert_eq!(
                    client_receiver.unprotect(&ciphertext).unwrap(),
                    [value; 128]
                );
            }
        });
    });
}
