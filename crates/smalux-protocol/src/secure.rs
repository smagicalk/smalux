//! Smalux 自有协议安全通道工具。
//!
//! 本模块只处理 token、PSK 派生和 Noise 状态机，不绑定 WebSocket、HTTP 或 gRPC。
//! agent 和 server 必须复用这里的参数，避免两端 HKDF、Noise pattern 或 packet 语义写偏。

use base64::Engine;
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// 安全 token 前缀。
const TOKEN_PREFIX: &str = "smx1";
/// Noise pattern，agent 和 server 必须使用同一个 pattern。
pub const NOISE_PATTERN: &str = "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s";
/// 派生 32 字节 PSK，满足 Noise PSK 长度要求。
pub const PSK_LEN: usize = 32;
/// 解密时预留的 AEAD tag 长度。
const NOISE_TAG_LEN: usize = 16;
/// HKDF salt，固定域隔离，避免 secret 在其他用途复用时混淆。
const HKDF_SALT: &[u8] = b"smalux secure psk v1 salt";
/// HKDF info 前缀。
const HKDF_INFO_PREFIX: &[u8] = b"smalux secure psk v1 ";

/// 解析后的安全 key。
#[derive(Clone, Eq, PartialEq)]
pub struct SecurePskKey {
    /// 明文 key id，只用于 server 查找对应 secret。
    pub key_id: String,
    /// 派生后的 Noise PSK，不直接发送。
    pub psk: [u8; PSK_LEN],
}

impl std::fmt::Debug for SecurePskKey {
    /// Debug 只输出 key id，避免 PSK 泄露到日志。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecurePskKey")
            .field("key_id", &self.key_id)
            .field("psk", &"<redacted>")
            .finish()
    }
}

/// 安全握手 hello payload。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecureHello {
    /// token 中的 key id。
    pub key_id: String,
    /// Noise pattern 名称。
    pub pattern: String,
}

/// 解析 `smx1.<key_id>.<secret_base64url>` 并派生 Noise PSK。
pub fn parse_secure_token(token: &str) -> anyhow::Result<SecurePskKey> {
    // token 只作为本地输入使用：key_id 会放到 Hello，secret 不会发送。
    // server 需要通过同一个 key_id 找到自己保存的 secret，再按相同 HKDF 参数派生 PSK。
    let mut parts = token.split('.');
    let prefix = parts.next().unwrap_or_default();
    let key_id = parts.next().unwrap_or_default();
    let secret = parts.next().unwrap_or_default();
    if parts.next().is_some() || prefix != TOKEN_PREFIX {
        anyhow::bail!("secure token must use smx1.<key_id>.<secret> format");
    }
    if key_id.trim().is_empty() {
        anyhow::bail!("secure token key_id cannot be empty");
    }
    if secret.trim().is_empty() {
        anyhow::bail!("secure token secret cannot be empty");
    }

    let secret = decode_secret(secret)?;
    if secret.len() < PSK_LEN {
        anyhow::bail!("secure token secret must decode to at least 32 bytes");
    }

    Ok(SecurePskKey {
        key_id: key_id.to_string(),
        psk: derive_psk(key_id, &secret)?,
    })
}

/// 编码 hello payload。
pub fn encode_secure_hello(key_id: &str) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&SecureHello {
        key_id: key_id.to_string(),
        pattern: NOISE_PATTERN.to_string(),
    })?)
}

/// 解码 hello payload。
pub fn decode_secure_hello(input: &[u8]) -> anyhow::Result<SecureHello> {
    Ok(serde_json::from_slice(input)?)
}

/// 创建 Noise initiator，agent 主动连接时使用。
pub fn build_noise_initiator(psk: &[u8; PSK_LEN]) -> anyhow::Result<snow::HandshakeState> {
    let params: snow::params::NoiseParams = NOISE_PATTERN.parse()?;
    Ok(snow::Builder::new(params).psk(0, psk).build_initiator()?)
}

/// 创建 Noise responder，server 接收连接时使用。
pub fn build_noise_responder(psk: &[u8; PSK_LEN]) -> anyhow::Result<snow::HandshakeState> {
    let params: snow::params::NoiseParams = NOISE_PATTERN.parse()?;
    Ok(snow::Builder::new(params).psk(0, psk).build_responder()?)
}

/// 写出一条 Noise 握手消息。
pub fn write_handshake_message(
    state: &mut snow::HandshakeState,
    payload: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut out = vec![0u8; payload.len() + NOISE_TAG_LEN + 256];
    let len = state.write_message(payload, &mut out)?;
    out.truncate(len);
    Ok(out)
}

/// 读取一条 Noise 握手消息。
pub fn read_handshake_message(
    state: &mut snow::HandshakeState,
    input: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut out = vec![0u8; input.len()];
    let len = state.read_message(input, &mut out)?;
    out.truncate(len);
    Ok(out)
}

/// 加密业务 payload。
pub fn encrypt_payload(
    state: &mut snow::TransportState,
    payload: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut out = vec![0u8; payload.len() + NOISE_TAG_LEN];
    let len = state.write_message(payload, &mut out)?;
    out.truncate(len);
    Ok(out)
}

/// 解密业务 payload。
pub fn decrypt_payload(state: &mut snow::TransportState, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = vec![0u8; input.len()];
    let len = state.read_message(input, &mut out)?;
    out.truncate(len);
    Ok(out)
}

/// base64url 解码 secret，兼容带 padding 和不带 padding 两种形式。
fn decode_secret(input: &str) -> anyhow::Result<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(input))
        .map_err(Into::into)
}

/// 按 key id 做域隔离派生。
fn derive_psk(key_id: &str, secret: &[u8]) -> anyhow::Result<[u8; PSK_LEN]> {
    let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), secret);
    let mut info = Vec::with_capacity(HKDF_INFO_PREFIX.len() + key_id.len());
    info.extend_from_slice(HKDF_INFO_PREFIX);
    info.extend_from_slice(key_id.as_bytes());
    let mut psk = [0u8; PSK_LEN];
    // 把 key_id 放进 info，避免不同 agent 共用同一 secret 时派生出完全相同的 PSK。
    hk.expand(&info, &mut psk)
        .map_err(|_| anyhow::anyhow!("failed to derive secure psk"))?;
    Ok(psk)
}

#[cfg(test)]
mod tests {
    //! 安全通道基础测试。

    use super::*;

    /// 构造测试 token。
    fn test_token(key_id: &str) -> String {
        let secret = [7u8; 32];
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        format!("smx1.{key_id}.{encoded}")
    }

    /// 验证 token 可以解析并派生稳定 PSK。
    #[test]
    fn secure_token_parses_key_id_and_derives_psk() {
        let first = parse_secure_token(&test_token("agent-key")).unwrap();
        let second = parse_secure_token(&test_token("agent-key")).unwrap();

        assert_eq!(first.key_id, "agent-key");
        assert_eq!(first.psk, second.psk);
    }

    /// 固化 HKDF 测试向量，方便 server 实现时确认参数和字节拼接完全一致。
    #[test]
    fn secure_psk_hkdf_test_vector_is_stable() {
        let key = parse_secure_token(&test_token("agent-key")).unwrap();

        // hex: a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2
        assert_eq!(
            key.psk,
            [
                166, 91, 42, 255, 18, 182, 126, 157, 37, 250, 231, 9, 75, 36, 36, 129, 51, 160, 67,
                161, 242, 242, 186, 22, 21, 114, 121, 128, 107, 45, 98, 162,
            ]
        );
    }

    /// 验证 token 格式错误会被拒绝。
    #[test]
    fn secure_token_rejects_invalid_format() {
        let error = parse_secure_token("bad.token").unwrap_err();

        assert!(error.to_string().contains("smx1"));
    }

    /// 验证 Noise PSK 握手后可以加密解密 payload。
    #[test]
    fn noise_psk_transport_encrypts_and_decrypts_payload() {
        let key = parse_secure_token(&test_token("agent-key")).unwrap();
        let mut initiator = build_noise_initiator(&key.psk).unwrap();
        let mut responder = build_noise_responder(&key.psk).unwrap();

        let first = write_handshake_message(&mut initiator, b"").unwrap();
        read_handshake_message(&mut responder, &first).unwrap();
        let second = write_handshake_message(&mut responder, b"").unwrap();
        read_handshake_message(&mut initiator, &second).unwrap();

        let mut initiator = initiator.into_transport_mode().unwrap();
        let mut responder = responder.into_transport_mode().unwrap();
        let encrypted = encrypt_payload(&mut initiator, b"hello").unwrap();
        let decrypted = decrypt_payload(&mut responder, &encrypted).unwrap();

        assert_ne!(encrypted, b"hello");
        assert_eq!(decrypted, b"hello");
    }
}
