//! 握手完成后的底层 Noise transport mode 会话。
//!
//! 该层只负责 `SecureMessage` 与 ciphertext 帧之间的转换。Tonic 流、心跳和同步 rekey
//! 由 `tonic_transport::TonicNoiseSession` 负责。

use prost::Message;
use snow::TransportState;

use crate::agent::v1::{ProtocolFrame, SecureMessage, protocol_frame};
use tracing::{debug, trace, warn};

use super::NoiseError;

/// 完成握手后的双向加密状态。该类型独占 nonce，不应被多个任务并发操作。
pub struct SecureSession {
    /// `snow` 持有的双向 AEAD key 和严格递增 nonce。
    transport: TransportState,
    /// 同步 rekey 成功次数；初始值为 0。
    generation: u64,
    /// 当前 generation 内加密与解密成功的帧数总和。
    encrypted_frames: u64,
}

impl SecureSession {
    /// 由已完成的 XXpsk3/IK 握手创建，外部不能绕过握手直接构造。
    pub(crate) fn new(transport: TransportState) -> Self {
        debug!("created Noise transport session");
        Self {
            transport,
            generation: 0,
            encrypted_frames: 0,
        }
    }

    /// Prost 编码业务消息，再用当前 sending nonce 加密为外层 ciphertext 帧。
    ///
    /// 必须顺序调用；同一个 `SecureSession` 不能被多个发送任务并发操作。
    pub fn encrypt(&mut self, message: &SecureMessage) -> Result<ProtocolFrame, NoiseError> {
        // 先得到确定的 Protobuf 明文字节，再为 ChaChaPoly 的认证标签预留 16 字节。
        let plaintext = message.encode_to_vec();
        let mut ciphertext = vec![0; plaintext.len() + 16];
        let written = self.transport.write_message(&plaintext, &mut ciphertext)?;
        ciphertext.truncate(written);
        self.encrypted_frames = self.encrypted_frames.saturating_add(1);
        trace!(
            generation = self.generation,
            plaintext_len = plaintext.len(),
            ciphertext_len = ciphertext.len(),
            encrypted_frames = self.encrypted_frames,
            next_nonce = self.transport.sending_nonce(),
            "encrypted Noise business frame"
        );
        Ok(ProtocolFrame {
            body: Some(protocol_frame::Body::Ciphertext(ciphertext)),
        })
    }

    /// 验证外层帧类型和 AEAD 标签，再把明文解码为 `SecureMessage`。
    ///
    /// 收包顺序错误、重复帧或篡改都会导致解密失败，当前会话不应继续使用。
    pub fn decrypt(&mut self, frame: ProtocolFrame) -> Result<SecureMessage, NoiseError> {
        // 握手帧和外层错误不能出现在已经建立的加密业务路径中。
        let Some(protocol_frame::Body::Ciphertext(ciphertext)) = frame.body else {
            warn!("received a non-ciphertext frame in Noise transport mode");
            return Err(NoiseError::InvalidFrame);
        };
        let mut plaintext = vec![0; 65_535];
        let read = self
            .transport
            .read_message(&ciphertext, &mut plaintext)
            .map_err(|error| {
                warn!(
                    error = ?error,
                    ciphertext_len = ciphertext.len(),
                    "Noise frame decryption failed"
                );
                NoiseError::from(error)
            })?;
        self.encrypted_frames = self.encrypted_frames.saturating_add(1);
        trace!(
            generation = self.generation,
            ciphertext_len = ciphertext.len(),
            plaintext_len = read,
            encrypted_frames = self.encrypted_frames,
            next_nonce = self.transport.receiving_nonce(),
            "decrypted Noise business frame"
        );
        SecureMessage::decode(&plaintext[..read]).map_err(|error| {
            warn!(
                error = ?error,
                plaintext_len = read,
                "decrypted Noise payload is not a SecureMessage"
            );
            NoiseError::from(error)
        })
    }

    /// 返回已完成同步 rekey 的次数；初始 transport key 属于 generation 0。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 返回当前 generation 内成功加密和解密的帧数总和。
    pub fn encrypted_frames(&self) -> u64 {
        self.encrypted_frames
    }

    /// 返回下一次发送将使用的 Noise nonce，仅用于诊断和测试。
    pub fn sending_nonce(&self) -> u64 {
        self.transport.sending_nonce()
    }

    /// 返回下一次接收预期的 Noise nonce，仅用于诊断和测试。
    pub fn receiving_nonce(&self) -> u64 {
        self.transport.receiving_nonce()
    }

    /// 把接收方向切换到 Noise 规范派生的下一把对称密钥。
    pub(crate) fn rekey_incoming(&mut self) {
        debug!(generation = self.generation, "switching Noise incoming key");
        self.transport.rekey_incoming();
    }

    /// 把发送方向切换到 Noise 规范派生的下一把对称密钥。
    pub(crate) fn rekey_outgoing(&mut self) {
        debug!(generation = self.generation, "switching Noise outgoing key");
        self.transport.rekey_outgoing();
    }

    /// 双向密钥都切换完成后提交 generation，并清零本代帧计数。
    pub(crate) fn finish_rekey(&mut self, generation: u64) {
        debug!(
            previous_generation = self.generation,
            generation,
            frames_before_rekey = self.encrypted_frames,
            "completed Noise transport rekey"
        );
        self.generation = generation;
        self.encrypted_frames = 0;
    }
}
