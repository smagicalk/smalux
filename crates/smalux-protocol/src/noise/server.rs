//! Noise responder（Server）侧 XXpsk3 与 IK 握手状态机。
//!
//! Server 只负责密码学认证并返回对端静态公钥；Token、Agent 注册表和业务授权属于上层。

use snow::{Builder, HandshakeState, params::NoiseParams};
use tracing::{debug, trace, warn};

use crate::agent::v1::{NoiseHandshake, noise_handshake::HandshakeType};

use super::{
    EstablishedNoise, HandshakeMode, NOISE_IK, NOISE_XX_PSK3, NoiseError, NoiseIdentity,
    NoisePublicKey, SecureSession, handshake_frame, validate_handshake,
};

/// XXpsk3 message 2 已发送，正在等待 Agent message 3 的一次性状态。
pub struct ServerXxAwaitMessage3 {
    /// `snow` 内部握手状态和 transcript hash。
    handshake: HandshakeState,
    /// 本次响应实际使用的 Server 静态公钥指纹。
    responder_key_id: super::KeyId,
}

/// Server 首次注册握手的 XXpsk3 入口。
pub struct ServerXxHandshake;

impl ServerXxHandshake {
    /// 读取 Agent message 1，使用 Server 身份和 PSK 写出 message 2。
    pub fn receive_message1(
        identity: &NoiseIdentity,
        psk: &[u8],
        frame: NoiseHandshake,
    ) -> Result<(ServerXxAwaitMessage3, NoiseHandshake), NoiseError> {
        debug!(
            responder_key_id = ?identity.key_id(),
            payload_len = frame.payload.len(),
            "Server received Client XXpsk3 message 1"
        );
        let psk: &[u8; 32] = psk.try_into().map_err(|_| NoiseError::InvalidPskLength)?;
        validate_handshake(&frame, HandshakeType::XxPsk3)?;
        let params: NoiseParams = NOISE_XX_PSK3.parse()?;
        let mut handshake = Builder::new(params)
            .local_private_key(identity.private_key())?
            .psk(3, psk)?
            .build_responder()?;
        read_handshake(&mut handshake, &frame.payload)?;
        let key_id = identity.key_id();
        let second = write_handshake(&mut handshake, HandshakeType::XxPsk3, key_id)?;
        trace!(
            payload_len = second.payload.len(),
            "Server wrote XXpsk3 message 2"
        );
        Ok((
            ServerXxAwaitMessage3 {
                handshake,
                responder_key_id: key_id,
            },
            second,
        ))
    }
}

impl ServerXxAwaitMessage3 {
    /// 读取 Agent message 3、完成 PSK/Agent 静态密钥认证并进入 transport mode。
    pub fn receive_message3(
        mut self,
        frame: NoiseHandshake,
    ) -> Result<EstablishedNoise, NoiseError> {
        debug!(
            payload_len = frame.payload.len(),
            "Server received Client XXpsk3 message 3"
        );
        validate_handshake(&frame, HandshakeType::XxPsk3)?;
        read_handshake(&mut self.handshake, &frame.payload).map_err(|error| {
            warn!(error = ?error, "Server XXpsk3 authentication failed at message 3");
            NoiseError::AuthenticationFailed
        })?;
        debug!(
            responder_key_id = ?self.responder_key_id,
            "Server XXpsk3 authentication completed"
        );
        finish(
            self.handshake,
            HandshakeMode::RegistrationXxPsk3,
            self.responder_key_id,
        )
    }
}

/// Server 已注册会话的 IK responder 入口。
pub struct ServerIkHandshake;

impl ServerIkHandshake {
    /// 读取 IK message 1、认证 Agent 静态公钥并写出 message 2。
    pub fn receive_message1(
        identity: &NoiseIdentity,
        frame: NoiseHandshake,
    ) -> Result<(EstablishedNoise, NoiseHandshake), NoiseError> {
        debug!(
            responder_key_id = ?identity.key_id(),
            payload_len = frame.payload.len(),
            "Server received Client IK message 1"
        );
        validate_handshake(&frame, HandshakeType::Ik)?;
        if frame.responder_key_id != identity.key_id().as_bytes() {
            warn!("Server IK request selected a different responder key ID");
            return Err(NoiseError::UnknownKeyId);
        }
        let params: NoiseParams = NOISE_IK.parse()?;
        let mut handshake = Builder::new(params)
            .local_private_key(identity.private_key())?
            .build_responder()?;
        read_handshake(&mut handshake, &frame.payload)?;
        let second = write_handshake(&mut handshake, HandshakeType::Ik, identity.key_id())?;
        let established = finish(handshake, HandshakeMode::AuthenticatedIk, identity.key_id())?;
        debug!(
            payload_len = second.payload.len(),
            "Server IK authentication completed"
        );
        Ok((established, second))
    }
}

/// 提取认证后的 Agent 公钥，并把完成的 responder 握手统一转换为会话结果。
fn finish(
    handshake: HandshakeState,
    mode: HandshakeMode,
    responder_key_id: super::KeyId,
) -> Result<EstablishedNoise, NoiseError> {
    let remote = NoisePublicKey::from_bytes(
        handshake
            .get_remote_static()
            .ok_or(NoiseError::MissingRemoteKey)?,
    )?;
    let transport = handshake.into_transport_mode()?;
    debug!(mode = ?mode, responder_key_id = ?responder_key_id, "created established Server Noise session");
    Ok(EstablishedNoise {
        session: SecureSession::new(transport),
        mode,
        remote_static_key: remote,
        responder_key_id,
    })
}

/// 把 responder 下一条空 payload 握手消息封装成正式 Protobuf 帧。
fn write_handshake(
    handshake: &mut HandshakeState,
    kind: HandshakeType,
    key_id: super::KeyId,
) -> Result<NoiseHandshake, NoiseError> {
    let mut output = vec![0; 65_535];
    let written = handshake.write_message(&[], &mut output)?;
    output.truncate(written);
    trace!(
        handshake = ?kind,
        payload_len = output.len(),
        responder_key_id = ?key_id,
        "Server wrote Noise handshake message"
    );
    Ok(handshake_frame(kind, Some(key_id), output))
}

/// 按当前 responder transcript 顺序读取一条握手 payload。
fn read_handshake(handshake: &mut HandshakeState, payload: &[u8]) -> Result<(), NoiseError> {
    let mut plaintext = vec![0; 65_535];
    handshake.read_message(payload, &mut plaintext)?;
    trace!(
        payload_len = payload.len(),
        "Server consumed Noise handshake message"
    );
    Ok(())
}
