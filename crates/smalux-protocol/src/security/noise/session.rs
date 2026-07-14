//! 单连接 Noise 握手、传输状态、record 分片和 rekey 实现。

use std::{
    fmt, mem,
    sync::{Arc, Mutex, MutexGuard},
};

use snow::{HandshakeState, TransportState, params::NoiseParams};
use zeroize::{Zeroize, Zeroizing};

use crate::{
    Error, Result,
    security::{HandshakeMessage, SecurityContext, SecuritySession, SecurityState},
};

use super::provider::NoiseRemoteKeyVerifier;
use super::record::{
    AEAD_TAG_BYTES, OUTER_PROTOBUF_OVERHEAD, RECORD_HEADER, encrypt_record,
    encrypted_container_len, max_plaintext_for_frame, parse_records,
};

/// Noise XX 的稳定协商名称。
pub const NOISE_XX_SCHEME: &str = "smalux.security.noise.xx.25519.chachapoly.blake2s.v1";
/// Noise IK 的稳定协商名称。
pub const NOISE_IK_SCHEME: &str = "smalux.security.noise.ik.25519.chachapoly.blake2s.v1";
/// 每个方向处理指定数量的 record 后执行一次确定性 rekey。
pub const NOISE_REKEY_INTERVAL_RECORDS: u64 = 1_048_576;

/// 传给 snow 的 XX 标准 Noise 参数名。
pub(super) const XX_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
/// 传给 snow 的 IK 标准 Noise 参数名。
pub(super) const IK_PARAMS: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";
/// X25519 静态公钥和私钥的固定字节数。
pub(super) const STATIC_KEY_BYTES: usize = 32;
/// snow 单条握手或传输消息允许的最大总字节数。
const NOISE_MAX_MESSAGE_BYTES: usize = 65_535;
/// 绑定 Smalux 协议版本语义、避免与其他 Noise 应用混用握手的 prologue 前缀。
pub(super) const PROLOGUE_PREFIX: &[u8] = b"smalux/noise/v1\0";

/// 一条连接独享的 Noise 安全会话。
///
/// `NoiseSession` 可通过 `Arc` 同时交给读写任务，但 snow 的 `TransportState` 同时保存
/// 两个方向的 nonce，因此全部状态转换由同一 Mutex 串行保护。
pub(super) struct NoiseSession {
    /// Hello 最终选择的稳定安全方案名称。
    scheme: String,
    /// 经过规范化且必须绑定到握手的协议协商上下文。
    context: SecurityContext,
    /// 握手结束后决定是否信任远端静态公钥的同步策略。
    verifier: Arc<dyn NoiseRemoteKeyVerifier>,
    /// 根据最终 Frame 上限反推的单次 protect 最大明文字节数。
    max_plaintext_bytes: usize,
    /// 每个方向在多少个 record 后执行一次确定性 rekey。
    rekey_interval_records: u64,
    /// 握手、身份确认、传输 nonce 和计数器的唯一可变状态。
    inner: Mutex<NoiseInner>,
}

/// Mutex 内的连接状态；任何字段都禁止在锁外独立修改。
struct NoiseInner {
    /// 当前握手、待认证、传输或终止阶段。
    phase: NoisePhase,
    /// 跨 Client/Server 两个方向严格递增的下一握手步骤。
    next_handshake_step: u32,
    /// 握手确认且通过 Verifier 的远端 X25519 静态公钥。
    remote_static_key: Option<[u8; STATIC_KEY_BYTES]>,
    /// Noise handshake hash，用于上层通道绑定和审计。
    channel_binding: Option<Vec<u8>>,
    /// 当前方向已成功加密的 record 数，用于发送 rekey。
    outgoing_records: u64,
    /// 当前方向已成功解密的 record 数，用于接收 rekey。
    incoming_records: u64,
}

/// Noise 会话内部阶段；`Verifying` 阶段禁止发送业务密文。
enum NoisePhase {
    /// snow 仍在交换 XX 或 IK 握手消息；Box 避免放大整个枚举尺寸。
    Handshake(Box<HandshakeState>),
    /// 握手已产生 TransportState，正在执行应用定义的远端身份校验。
    Verifying(TransportState),
    /// 身份已确认，可以双向保护 SessionContent。
    Transport(TransportState),
    /// 调用方已主动关闭，会话不得复用。
    Closed,
    /// 发生认证、顺序或内部错误，只能断开并创建新会话。
    Failed,
}

impl NoiseSession {
    /// 从协商上下文和刚创建的 snow 握手状态初始化单连接会话。
    pub(super) fn new(
        context: SecurityContext,
        verifier: Arc<dyn NoiseRemoteKeyVerifier>,
        handshake: HandshakeState,
        rekey_interval_records: u64,
    ) -> Self {
        let max_plaintext_bytes = max_plaintext_for_frame(context.max_frame_bytes());
        Self {
            scheme: context.scheme().to_owned(),
            context,
            verifier,
            max_plaintext_bytes,
            rekey_interval_records,
            inner: Mutex::new(NoiseInner {
                phase: NoisePhase::Handshake(Box::new(handshake)),
                next_handshake_step: 0,
                remote_static_key: None,
                channel_binding: None,
                outgoing_records: 0,
                incoming_records: 0,
            }),
        }
    }

    /// 获取唯一可变状态锁；锁中毒按不可恢复安全错误处理。
    fn lock(&self) -> Result<MutexGuard<'_, NoiseInner>> {
        self.inner
            .lock()
            .map_err(|_| Error::Security("Noise session state mutex is poisoned".to_owned()))
    }

    /// 在握手刚完成时提取远端公钥和 handshake hash，并进入待身份确认阶段。
    ///
    /// 返回公钥表示调用方必须在锁外执行 Verifier，避免用户回调持锁造成死锁。
    fn prepare_transport(inner: &mut NoiseInner) -> Result<Option<[u8; 32]>> {
        let finished = matches!(
            &inner.phase,
            NoisePhase::Handshake(handshake) if handshake.is_handshake_finished()
        );
        if !finished {
            return Ok(None);
        }

        let phase = mem::replace(&mut inner.phase, NoisePhase::Failed);
        let handshake = match phase {
            NoisePhase::Handshake(handshake) => handshake,
            _ => {
                return Err(Error::Security(
                    "Noise handshake state changed before transport transition".to_owned(),
                ));
            }
        };
        let remote_static_key = copy_fixed_key(
            "remote static key",
            handshake.get_remote_static().ok_or_else(|| {
                Error::Security("Noise handshake did not establish a remote static key".to_owned())
            })?,
        )?;
        let channel_binding = handshake.get_handshake_hash().to_vec();
        let transport = (*handshake)
            .into_transport_mode()
            .map_err(|error| noise_error("enter transport mode", error))?;

        inner.remote_static_key = Some(remote_static_key);
        inner.channel_binding = Some(channel_binding);
        inner.phase = NoisePhase::Verifying(transport);
        Ok(Some(remote_static_key))
    }

    /// 在不持有内部锁时校验远端身份，成功后原子进入 Transport 阶段。
    fn verify_and_activate(&self, remote_static_key: [u8; 32]) -> Result<()> {
        if let Err(error) = self.verifier.verify(&self.context, &remote_static_key) {
            if let Ok(mut inner) = self.lock() {
                inner.phase = NoisePhase::Failed;
            }
            return Err(error);
        }

        let mut inner = self.lock()?;
        let phase = mem::replace(&mut inner.phase, NoisePhase::Failed);
        match phase {
            NoisePhase::Verifying(transport) => {
                inner.phase = NoisePhase::Transport(transport);
                Ok(())
            }
            _ => Err(Error::Security(
                "Noise identity verification completed in an invalid state".to_owned(),
            )),
        }
    }

    /// 尽力把当前会话永久标记为 Failed；锁已中毒时状态查询也会返回 Failed。
    fn mark_failed(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.phase = NoisePhase::Failed;
        }
    }

    /// 仅供单元测试确认双向 record 计数跨越了 rekey 边界。
    #[cfg(test)]
    fn record_counts(&self) -> Result<(u64, u64)> {
        let inner = self.lock()?;
        Ok((inner.outgoing_records, inner.incoming_records))
    }

    /// 在本地写轮次生成下一步握手消息，并在最后一步后准备 TransportState。
    fn next_handshake_inner(&self) -> Result<(Option<HandshakeMessage>, Option<[u8; 32]>)> {
        let mut inner = self.lock()?;
        let step = inner.next_handshake_step;
        let payload = match &mut inner.phase {
            NoisePhase::Handshake(handshake) if handshake.is_my_turn() => {
                let mut output = vec![0_u8; NOISE_MAX_MESSAGE_BYTES];
                let written = handshake
                    .write_message(&[], &mut output)
                    .map_err(|error| noise_error("write handshake message", error))?;
                output.truncate(written);
                Some(output)
            }
            NoisePhase::Handshake(_) | NoisePhase::Verifying(_) => None,
            NoisePhase::Transport(_) => return Ok((None, None)),
            NoisePhase::Closed => {
                return Err(Error::Security("Noise session is closed".to_owned()));
            }
            NoisePhase::Failed => {
                return Err(Error::Security("Noise session has failed".to_owned()));
            }
        };

        let message = if let Some(payload) = payload {
            inner.next_handshake_step = inner
                .next_handshake_step
                .checked_add(1)
                .ok_or_else(|| Error::Security("Noise handshake step overflow".to_owned()))?;
            Some(HandshakeMessage { step, payload })
        } else {
            None
        };
        let remote_key = Self::prepare_transport(&mut inner)?;
        Ok((message, remote_key))
    }

    /// 校验全局 step 并读取对端握手消息，必要时返回待校验的远端静态公钥。
    fn receive_handshake_inner(&self, message: HandshakeMessage) -> Result<Option<[u8; 32]>> {
        let mut inner = self.lock()?;
        if message.step != inner.next_handshake_step {
            inner.phase = NoisePhase::Failed;
            return Err(Error::Security(format!(
                "Noise handshake expected step {}, received {}",
                inner.next_handshake_step, message.step
            )));
        }
        match &mut inner.phase {
            NoisePhase::Handshake(handshake) if !handshake.is_my_turn() => {
                let mut payload = Zeroizing::new(vec![0_u8; NOISE_MAX_MESSAGE_BYTES]);
                handshake
                    .read_message(&message.payload, &mut payload)
                    .map_err(|error| noise_error("read handshake message", error))?;
            }
            NoisePhase::Handshake(_) => {
                inner.phase = NoisePhase::Failed;
                return Err(Error::Security(
                    "received Noise handshake message while it is the local write turn".to_owned(),
                ));
            }
            _ => {
                inner.phase = NoisePhase::Failed;
                return Err(Error::Security(
                    "received Noise handshake message outside handshake state".to_owned(),
                ));
            }
        }
        inner.next_handshake_step = inner
            .next_handshake_step
            .checked_add(1)
            .ok_or_else(|| Error::Security("Noise handshake step overflow".to_owned()))?;
        Self::prepare_transport(&mut inner)
    }

    /// 把一条完整 SessionContent 明文分片为 record，并生成单个 SNR1 容器。
    fn protect_inner(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        if plaintext.len() > self.max_plaintext_bytes {
            return Err(Error::Security(format!(
                "Noise plaintext size {} exceeds secure limit {}",
                plaintext.len(),
                self.max_plaintext_bytes
            )));
        }

        let mut inner = self.lock()?;
        let NoiseInner {
            phase,
            outgoing_records,
            ..
        } = &mut *inner;
        let transport = match phase {
            NoisePhase::Transport(transport) => transport,
            _ => return Err(Error::Security("Noise transport is not ready".to_owned())),
        };

        let mut output = Vec::with_capacity(encrypted_container_len(plaintext.len()));
        output.extend_from_slice(RECORD_HEADER);
        let result = if plaintext.is_empty() {
            encrypt_record(
                transport,
                &[],
                &mut output,
                outgoing_records,
                self.rekey_interval_records,
            )
        } else {
            plaintext
                .chunks(super::record::NOISE_RECORD_PLAINTEXT_BYTES)
                .try_for_each(|chunk| {
                    encrypt_record(
                        transport,
                        chunk,
                        &mut output,
                        outgoing_records,
                        self.rekey_interval_records,
                    )
                })
        };
        if let Err(error) = result {
            inner.phase = NoisePhase::Failed;
            return Err(error);
        }
        if output.len() + OUTER_PROTOBUF_OVERHEAD > self.context.max_frame_bytes() {
            inner.phase = NoisePhase::Failed;
            return Err(Error::Security(
                "Noise protected payload exceeds negotiated frame limit".to_owned(),
            ));
        }
        Ok(output)
    }

    /// 严格解析 SNR1 容器，按顺序认证解密全部 record 并拼回完整明文。
    fn unprotect_inner(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let records = match parse_records(ciphertext, self.max_plaintext_bytes) {
            Ok(records) => records,
            Err(error) => {
                self.mark_failed();
                return Err(error);
            }
        };
        let output_capacity = records
            .iter()
            .map(|record| record.len() - AEAD_TAG_BYTES)
            .sum();
        let mut output = Zeroizing::new(Vec::with_capacity(output_capacity));

        let mut inner = self.lock()?;
        let NoiseInner {
            phase,
            incoming_records,
            ..
        } = &mut *inner;
        let transport = match phase {
            NoisePhase::Transport(transport) => transport,
            _ => return Err(Error::Security("Noise transport is not ready".to_owned())),
        };

        for record in records {
            let mut plaintext = Zeroizing::new(vec![0_u8; record.len()]);
            let written = match transport.read_message(record, &mut plaintext) {
                Ok(written) => written,
                Err(error) => {
                    inner.phase = NoisePhase::Failed;
                    return Err(noise_error("decrypt transport record", error));
                }
            };
            plaintext.truncate(written);
            output.extend_from_slice(&plaintext);
            *incoming_records = incoming_records.checked_add(1).ok_or_else(|| {
                Error::Security("Noise incoming record counter overflow".to_owned())
            })?;
            if (*incoming_records).is_multiple_of(self.rekey_interval_records) {
                transport.rekey_incoming();
            }
        }
        Ok(mem::take(&mut *output))
    }
}

impl SecuritySession for NoiseSession {
    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn state(&self) -> SecurityState {
        match self.inner.lock() {
            Ok(inner) => match inner.phase {
                NoisePhase::Handshake(_) | NoisePhase::Verifying(_) => SecurityState::Handshaking,
                NoisePhase::Transport(_) => SecurityState::Ready,
                NoisePhase::Closed => SecurityState::Closed,
                NoisePhase::Failed => SecurityState::Failed,
            },
            Err(_) => SecurityState::Failed,
        }
    }

    fn protects_content(&self) -> bool {
        true
    }

    fn next_handshake(&self) -> Result<Option<HandshakeMessage>> {
        let result = self.next_handshake_inner();
        let (message, remote_key) = match result {
            Ok(value) => value,
            Err(error) => {
                self.mark_failed();
                return Err(error);
            }
        };
        if let Some(remote_key) = remote_key {
            self.verify_and_activate(remote_key)?;
        }
        Ok(message)
    }

    fn receive_handshake(&self, message: HandshakeMessage) -> Result<()> {
        let remote_key = match self.receive_handshake_inner(message) {
            Ok(remote_key) => remote_key,
            Err(error) => {
                self.mark_failed();
                return Err(error);
            }
        };
        if let Some(remote_key) = remote_key {
            self.verify_and_activate(remote_key)?;
        }
        Ok(())
    }

    fn protect(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let result = self.protect_inner(plaintext);
        if result.is_err() {
            self.mark_failed();
        }
        result
    }

    fn unprotect(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.unprotect_inner(ciphertext)
    }

    fn max_plaintext_bytes(&self) -> usize {
        self.max_plaintext_bytes
    }

    fn remote_static_key(&self) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.remote_static_key.map(|key| key.to_vec()))
    }

    fn channel_binding(&self) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.channel_binding.clone())
    }

    fn close(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.phase = NoisePhase::Closed;
            inner.remote_static_key = None;
            if let Some(binding) = inner.channel_binding.as_mut() {
                binding.zeroize();
            }
            inner.channel_binding = None;
        }
    }
}

/// 解析编译期固定的 Noise 参数名，并统一转换 snow 错误。
pub(super) fn parse_params(value: &str) -> Result<NoiseParams> {
    value
        .parse()
        .map_err(|error| noise_error("parse Noise parameters", error))
}

/// 将 snow 返回的动态字节切片严格转换为固定长度 X25519 密钥。
pub(super) fn copy_fixed_key(field: &'static str, value: &[u8]) -> Result<[u8; STATIC_KEY_BYTES]> {
    value.try_into().map_err(|_| {
        Error::Security(format!(
            "Noise {field} must contain exactly {STATIC_KEY_BYTES} bytes"
        ))
    })
}

/// 为 snow 的各阶段错误附加稳定操作上下文。
pub(super) fn noise_error(stage: &'static str, error: impl fmt::Display) -> Error {
    Error::Security(format!("Noise {stage} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoiseKeypair, NoiseProvider, PinnedRemoteKey, ProtocolVersion, SecurityRole};

    const TEST_REKEY_INTERVAL: u64 = 2;

    #[test]
    fn transport_remains_synchronized_across_multiple_rekeys() {
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
        let client = client_provider
            .start_with_rekey_interval(test_context(SecurityRole::Client), TEST_REKEY_INTERVAL)
            .unwrap();
        let server = server_provider
            .start_with_rekey_interval(test_context(SecurityRole::Server), TEST_REKEY_INTERVAL)
            .unwrap();

        for _ in 0..4 {
            if let Some(message) = client.next_handshake().unwrap() {
                server.receive_handshake(message).unwrap();
            }
            if let Some(message) = server.next_handshake().unwrap() {
                client.receive_handshake(message).unwrap();
            }
            if client.state() == SecurityState::Ready && server.state() == SecurityState::Ready {
                break;
            }
        }
        assert_eq!(client.state(), SecurityState::Ready);
        assert_eq!(server.state(), SecurityState::Ready);

        for sequence in 0_u8..5 {
            let client_plaintext = [sequence; 32];
            let encrypted = client.protect(&client_plaintext).unwrap();
            assert_eq!(server.unprotect(&encrypted).unwrap(), client_plaintext);

            let server_plaintext = [sequence.wrapping_add(10); 32];
            let encrypted = server.protect(&server_plaintext).unwrap();
            assert_eq!(client.unprotect(&encrypted).unwrap(), server_plaintext);
        }

        assert_eq!(client.record_counts().unwrap(), (5, 5));
        assert_eq!(server.record_counts().unwrap(), (5, 5));
    }

    fn test_context(role: SecurityRole) -> SecurityContext {
        SecurityContext::new(
            role,
            NOISE_XX_SCHEME.to_owned(),
            ProtocolVersion { major: 1, minor: 0 },
            Vec::new(),
            crate::DEFAULT_MAX_FRAME_BYTES,
            vec![0x11; 16],
            b"noise-rekey-test-transcript".to_vec(),
        )
    }
}
