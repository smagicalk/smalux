//! Noise initiator（Agent）侧 XXpsk3 与 IK 握手状态机。
//!
//! 中间状态类型按值消费自己，调用方无法重复处理同一条握手消息，也不能跳过步骤直接进入
//! transport mode。这种 typestate 设计用于保护 Noise 严格的消息顺序。

use snow::{Builder, HandshakeState, params::NoiseParams};
use tracing::{debug, trace, warn};

use crate::agent::v1::{NoiseHandshake, noise_handshake::HandshakeType};

use super::{
    EstablishedNoise, HandshakeMode, KeyId, NOISE_IK, NOISE_XX_PSK3, NoiseError, NoiseIdentity,
    NoisePublicKey, SecureSession, handshake_frame, validate_handshake,
};

/// XXpsk3 message 1 已发送，正在等待 Server message 2 的一次性状态。
pub struct ClientXxAwaitMessage2 {
    /// `snow` 内部握手状态，包含临时密钥和 transcript hash。
    handshake: HandshakeState,
}

/// Agent 首次注册使用的 XXpsk3 握手入口。
pub struct ClientXxHandshake;

impl ClientXxHandshake {
    /// 创建 initiator、写出 XXpsk3 message 1，并返回等待 message 2 的状态。
    ///
    /// `psk` 必须是 32 字节；Agent 此时不需要预置 Server 静态公钥。
    pub fn start(
        identity: &NoiseIdentity,
        psk: &[u8],
    ) -> Result<(ClientXxAwaitMessage2, NoiseHandshake), NoiseError> {
        debug!(
            local_key_id = ?identity.key_id(),
            "starting Client XXpsk3 registration handshake"
        );
        let psk: &[u8; 32] = psk.try_into().map_err(|_| NoiseError::InvalidPskLength)?;
        let params: NoiseParams = NOISE_XX_PSK3.parse()?;
        let mut handshake = Builder::new(params)
            .local_private_key(identity.private_key())?
            .psk(3, psk)?
            .build_initiator()?;
        let first = write_handshake(&mut handshake, HandshakeType::XxPsk3, None)?;
        trace!(
            payload_len = first.payload.len(),
            "Client XXpsk3 message 1 created"
        );
        Ok((Self::waiting(handshake), first))
    }

    fn waiting(handshake: HandshakeState) -> ClientXxAwaitMessage2 {
        ClientXxAwaitMessage2 { handshake }
    }
}

impl ClientXxAwaitMessage2 {
    /// 验证并读取 Server message 2，写出 message 3，然后进入 transport mode。
    ///
    /// 成功结果同时返回认证后的 Server 公钥和待发送的第三条握手消息。调用方必须先发送
    /// message 3，之后才可以使用 `EstablishedNoise.session` 发送业务密文。
    pub fn receive_message2(
        mut self,
        frame: NoiseHandshake,
    ) -> Result<(EstablishedNoise, NoiseHandshake), NoiseError> {
        debug!(
            payload_len = frame.payload.len(),
            "Client received XXpsk3 message 2"
        );
        validate_handshake(&frame, HandshakeType::XxPsk3)?;
        read_handshake(&mut self.handshake, &frame.payload)?;
        let remote = NoisePublicKey::from_bytes(
            self.handshake
                .get_remote_static()
                .ok_or(NoiseError::MissingRemoteKey)?,
        )?;
        let key_id = remote.key_id();
        if !frame.responder_key_id.is_empty()
            && KeyId::from_bytes(&frame.responder_key_id)? != key_id
        {
            warn!("Client XXpsk3 responder key ID does not match authenticated key");
            return Err(NoiseError::AuthenticationFailed);
        }
        let third = write_handshake(&mut self.handshake, HandshakeType::XxPsk3, Some(key_id))?;
        let transport = self.handshake.into_transport_mode()?;
        debug!(
            remote_key_id = ?key_id,
            third_payload_len = third.payload.len(),
            "Client XXpsk3 authentication completed"
        );
        Ok((
            EstablishedNoise {
                session: SecureSession::new(transport),
                mode: HandshakeMode::RegistrationXxPsk3,
                remote_static_key: remote,
                responder_key_id: key_id,
            },
            third,
        ))
    }
}

/// IK message 1 已发送，正在等待 Server message 2 的一次性状态。
pub struct ClientIkAwaitMessage2 {
    /// `snow` 内部 IK transcript 和临时密钥。
    handshake: HandshakeState,
    /// Agent 本地固定的 Server 公钥，用于核对响应 key ID。
    server_key: NoisePublicKey,
}

/// 已注册 Agent 后续连接使用的 IK 握手入口。
pub struct ClientIkHandshake;

impl ClientIkHandshake {
    /// 使用 Agent 长期身份和已固定 Server 公钥写出 IK message 1。
    ///
    /// IK 在第一条消息中已经认证 Agent 静态身份；不再读取注册 Token 或 PSK。
    pub fn start(
        identity: &NoiseIdentity,
        server_key: NoisePublicKey,
    ) -> Result<(ClientIkAwaitMessage2, NoiseHandshake), NoiseError> {
        debug!(
            local_key_id = ?identity.key_id(),
            server_key_id = ?server_key.key_id(),
            "starting Client IK handshake"
        );
        let params: NoiseParams = NOISE_IK.parse()?;
        let mut handshake = Builder::new(params)
            .local_private_key(identity.private_key())?
            .remote_public_key(server_key.as_bytes())?
            .build_initiator()?;
        let first = write_handshake(&mut handshake, HandshakeType::Ik, Some(server_key.key_id()))?;
        trace!(
            payload_len = first.payload.len(),
            "Client IK message 1 created"
        );
        Ok((
            ClientIkAwaitMessage2 {
                handshake,
                server_key,
            },
            first,
        ))
    }
}

impl ClientIkAwaitMessage2 {
    /// 验证 Server message 2 并进入 IK transport mode。
    pub fn receive_message2(
        mut self,
        frame: NoiseHandshake,
    ) -> Result<EstablishedNoise, NoiseError> {
        debug!(
            payload_len = frame.payload.len(),
            "Client received IK message 2"
        );
        validate_handshake(&frame, HandshakeType::Ik)?;
        if KeyId::from_bytes(&frame.responder_key_id)? != self.server_key.key_id() {
            warn!("Client IK responder key ID is not the pinned Server key");
            return Err(NoiseError::UnknownKeyId);
        }
        read_handshake(&mut self.handshake, &frame.payload)?;
        let transport = self.handshake.into_transport_mode()?;
        debug!(
            server_key_id = ?self.server_key.key_id(),
            "Client IK authentication completed"
        );
        Ok(EstablishedNoise {
            session: SecureSession::new(transport),
            mode: HandshakeMode::AuthenticatedIk,
            remote_static_key: self.server_key,
            responder_key_id: self.server_key.key_id(),
        })
    }
}

/// 把当前 initiator 握手状态的下一条空 payload 消息封装成 Protobuf 帧。
fn write_handshake(
    handshake: &mut HandshakeState,
    kind: HandshakeType,
    key_id: Option<KeyId>,
) -> Result<NoiseHandshake, NoiseError> {
    let mut output = vec![0; 65_535];
    let written = handshake.write_message(&[], &mut output)?;
    output.truncate(written);
    trace!(
        handshake = ?kind,
        payload_len = output.len(),
        key_id_present = key_id.is_some(),
        "Client wrote Noise handshake message"
    );
    Ok(handshake_frame(kind, key_id, output))
}

/// 读取一条握手 payload；本协议不在 Noise handshake payload 中夹带业务数据。
fn read_handshake(handshake: &mut HandshakeState, payload: &[u8]) -> Result<(), NoiseError> {
    let mut plaintext = vec![0; 65_535];
    handshake.read_message(payload, &mut plaintext)?;
    trace!(
        payload_len = payload.len(),
        "Client consumed Noise handshake message"
    );
    Ok(())
}
