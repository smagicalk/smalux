//! 与具体网络传输无关的 Noise 协议核心。
//!
//! `client` 和 `server` 实现握手 typestate；`session` 负责业务帧加解密；`rotation`
//! 管理跨重启长期静态密钥。Tonic、HTTP 路由和数据库不属于本模块。

mod client;
mod error;
mod identity;
mod rotation;
mod server;
mod session;

pub use client::{
    ClientIkAwaitMessage2, ClientIkHandshake, ClientXxAwaitMessage2, ClientXxHandshake,
};
pub use error::NoiseError;
pub use identity::{KeyId, NoiseIdentity, NoisePublicKey, RotationId, SecretKeyBytes};
pub use rotation::{
    AgentKeySet, AgentKeySetSnapshot, AgentPublicKeySet, AgentPublicKeySetSnapshot,
    AgentRotationPrepared, PinnedServerKeys, PinnedServerKeysSnapshot, ServerKeyRing,
    ServerKeyRingSnapshot, ServerRotationPrepared,
};
pub use server::{ServerIkHandshake, ServerXxAwaitMessage3, ServerXxHandshake};
pub use session::SecureSession;

use crate::agent::v1::{NoiseHandshake, noise_handshake::HandshakeType};

/// 首次注册使用的 Noise suite：XX + psk3 + X25519 + ChaChaPoly + BLAKE2s。
pub const NOISE_XX_PSK3: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";
/// 已注册 Agent 后续连接使用的 IK suite。
pub const NOISE_IK: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";

/// 已建立会话所采用的认证模式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandshakeMode {
    /// 首次注册：双方用 32 字节 PSK 认证，Client 事先不知道 Server 公钥。
    RegistrationXxPsk3,
    /// 后续连接：双方通过已经保存的长期静态公钥互相认证。
    AuthenticatedIk,
}

/// 完成 Noise 握手后，与具体传输无关的统一结果。
pub struct EstablishedNoise {
    /// 可立即用于加解密 `SecureMessage` 的 transport mode。
    pub session: SecureSession,
    /// 当前会话来自首次注册还是后续认证连接。
    pub mode: HandshakeMode,
    /// 握手认证得到的对端长期静态公钥。
    pub remote_static_key: NoisePublicKey,
    /// 本次 responder 实际使用的 Server 公钥指纹。
    pub responder_key_id: KeyId,
}

/// 构造正式 Protobuf 握手消息，并可选附带 responder key ID。
fn handshake_frame(kind: HandshakeType, key_id: Option<KeyId>, payload: Vec<u8>) -> NoiseHandshake {
    NoiseHandshake {
        r#type: kind as i32,
        responder_key_id: key_id
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_default(),
        payload,
    }
}

/// 确认收到的握手帧仍属于当前 typestate 期待的模式。
fn validate_handshake(frame: &NoiseHandshake, expected: HandshakeType) -> Result<(), NoiseError> {
    let actual =
        HandshakeType::try_from(frame.r#type).map_err(|_| NoiseError::InvalidHandshakeType)?;
    if actual == expected {
        Ok(())
    } else {
        Err(NoiseError::InvalidHandshakeType)
    }
}

#[cfg(test)]
mod tests {
    use crate::agent::v1::{
        RegistrationMessage, RegistrationRequest, SecureMessage, registration_message,
        secure_message,
    };

    use super::{
        AgentKeySet, AgentPublicKeySet, ClientIkHandshake, ClientXxHandshake, NoiseError,
        NoiseIdentity, PinnedServerKeys, ServerIkHandshake, ServerKeyRing, ServerXxHandshake,
    };

    fn establish_xx(
        client: &NoiseIdentity,
        server: &NoiseIdentity,
        client_psk: &[u8; 32],
        server_psk: &[u8; 32],
    ) -> Result<(super::EstablishedNoise, super::EstablishedNoise), NoiseError> {
        let (client_waiting, first) = ClientXxHandshake::start(client, client_psk)?;
        let (server_waiting, second) =
            ServerXxHandshake::receive_message1(server, server_psk, first)?;
        let (client_established, third) = client_waiting.receive_message2(second)?;
        let server_established = server_waiting.receive_message3(third)?;
        Ok((client_established, server_established))
    }

    #[test]
    fn xxpsk3_establishes_an_encrypted_session_without_pinned_server_key() {
        let client = NoiseIdentity::generate().unwrap();
        let server = NoiseIdentity::generate().unwrap();
        let psk = [7; 32];
        let (mut client, mut server_session) = establish_xx(&client, &server, &psk, &psk).unwrap();
        assert_eq!(client.remote_static_key, server.public_key());
        let message = SecureMessage {
            body: Some(secure_message::Body::RegistrationMessage(
                RegistrationMessage {
                    body: Some(registration_message::Body::Request(RegistrationRequest {
                        token: "token".to_owned(),
                        agent_name: "agent".to_owned(),
                    })),
                },
            )),
        };
        let frame = client.session.encrypt(&message).unwrap();
        assert_eq!(server_session.session.decrypt(frame).unwrap(), message);
    }

    #[test]
    fn xxpsk3_rejects_a_different_psk() {
        let result = establish_xx(
            &NoiseIdentity::generate().unwrap(),
            &NoiseIdentity::generate().unwrap(),
            &[1; 32],
            &[2; 32],
        );
        assert!(matches!(result, Err(NoiseError::AuthenticationFailed)));
    }

    #[test]
    fn ik_authenticates_both_static_keys() {
        let client = NoiseIdentity::generate().unwrap();
        let server = NoiseIdentity::generate().unwrap();
        let (waiting, first) = ClientIkHandshake::start(&client, server.public_key()).unwrap();
        let (server_session, second) = ServerIkHandshake::receive_message1(&server, first).unwrap();
        let client_session = waiting.receive_message2(second).unwrap();
        assert_eq!(client_session.remote_static_key, server.public_key());
        assert_eq!(server_session.remote_static_key, client.public_key());
    }

    #[test]
    fn agent_rotation_returns_snapshots_without_persisting_them() {
        let current = NoiseIdentity::generate().unwrap();
        let mut agent = AgentKeySet::new(current.clone());
        let prepared = agent.prepare_rotation().unwrap();
        assert_eq!(agent.connection_candidates().len(), 2);

        let mut server = AgentPublicKeySet::new(current.public_key());
        server.stage(&prepared.request).unwrap();
        assert!(server.authorize(prepared.new_identity.public_key()));

        agent.promote_pending(prepared.rotation_id).unwrap();
        server.promote_pending(prepared.rotation_id).unwrap();
        assert_eq!(agent.connection_candidates().len(), 1);
        assert!(server.authorize(prepared.new_identity.public_key()));
    }

    #[test]
    fn server_rotation_supports_current_and_next_keys() {
        let current = NoiseIdentity::generate().unwrap();
        let mut ring = ServerKeyRing::new(current.clone());
        let prepared = ring.prepare_rotation().unwrap();
        assert_eq!(ring.active_keys().len(), 2);

        let mut pinned = PinnedServerKeys::new(current.public_key());
        pinned.stage(&prepared.announcement).unwrap();
        assert_eq!(
            pinned.connection_candidates()[0],
            prepared.next_identity.public_key()
        );

        ring.promote_next(prepared.rotation_id).unwrap();
        pinned.promote_pending(prepared.rotation_id).unwrap();
        assert_eq!(ring.active_keys().len(), 2);
        assert_eq!(pinned.connection_candidates().len(), 2);
        ring.retire_previous().unwrap();
        pinned.retire_previous().unwrap();
    }

    #[test]
    fn synchronized_transport_rekey_keeps_both_directions_readable() {
        let client_identity = NoiseIdentity::generate().unwrap();
        let server_identity = NoiseIdentity::generate().unwrap();
        let (waiting, first) =
            ClientIkHandshake::start(&client_identity, server_identity.public_key()).unwrap();
        let (mut server, second) =
            ServerIkHandshake::receive_message1(&server_identity, first).unwrap();
        let mut client = waiting.receive_message2(second).unwrap();

        let before = SecureMessage {
            body: Some(secure_message::Body::RegistrationMessage(
                RegistrationMessage {
                    body: Some(registration_message::Body::Request(RegistrationRequest {
                        token: "before".to_owned(),
                        agent_name: "agent".to_owned(),
                    })),
                },
            )),
        };
        let frame = client.session.encrypt(&before).unwrap();
        assert_eq!(server.session.decrypt(frame).unwrap(), before);

        // Responder 先切 incoming，再用旧 outgoing 发送 Ack，随后才切 outgoing。
        server.session.rekey_incoming();
        let ack = server.session.encrypt(&before).unwrap();
        server.session.rekey_outgoing();
        server.session.finish_rekey(1);
        assert_eq!(client.session.decrypt(ack).unwrap(), before);
        client.session.rekey_incoming();
        client.session.rekey_outgoing();
        client.session.finish_rekey(1);

        let after = SecureMessage {
            body: Some(secure_message::Body::RegistrationMessage(
                RegistrationMessage {
                    body: Some(registration_message::Body::Request(RegistrationRequest {
                        token: "after".to_owned(),
                        agent_name: "agent".to_owned(),
                    })),
                },
            )),
        };
        let frame = client.session.encrypt(&after).unwrap();
        assert_eq!(server.session.decrypt(frame).unwrap(), after);
    }
}
