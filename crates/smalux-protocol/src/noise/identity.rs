//! Noise 长期静态身份、公钥指纹和轮换事务标识。
//!
//! 私钥只存在于 `NoiseIdentity` 和显式导出的 `SecretKeyBytes` 中；这两个类型故意不实现
//! `Debug`，避免普通日志或错误格式化意外泄露私钥。

use blake2::{Blake2s256, Digest};
use snow::{Builder, params::NoiseParams};
use tracing::debug;

use super::{NOISE_XX_PSK3, NoiseError};

/// X25519 静态公钥、私钥和 key ID 的固定字节长度。
pub const KEY_LEN: usize = 32;
/// 静态密钥轮换事务 ID 的固定字节长度。
pub const ROTATION_ID_LEN: usize = 16;

/// 可公开比较和持久化的 Noise 公钥。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NoisePublicKey([u8; KEY_LEN]);

impl NoisePublicKey {
    /// 从网络或持久化字节恢复公钥；长度不是 32 字节时立即失败。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NoiseError> {
        Ok(Self(
            bytes.try_into().map_err(|_| NoiseError::InvalidKeyLength)?,
        ))
    }

    /// 返回适合写入数据库、文件或 Protobuf `bytes` 字段的固定长度字节。
    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// 使用 BLAKE2s-256 计算稳定公钥指纹，供 IK responder 密钥选择使用。
    pub fn key_id(&self) -> KeyId {
        // key ID 只由公钥派生，不是秘密，也不能代替身份认证。
        let digest = Blake2s256::digest(self.0);
        KeyId(digest.into())
    }
}

/// 私钥字节的显式导出结果；故意不实现 Debug。
#[derive(Clone)]
pub struct SecretKeyBytes([u8; KEY_LEN]);

impl SecretKeyBytes {
    /// 返回私钥字节视图；调用方必须把它交给受保护存储，不能发送到网络。
    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

/// Server/Agent 的长期 Noise 静态身份；故意不实现 Debug。
#[derive(Clone)]
pub struct NoiseIdentity {
    /// X25519 长期静态私钥，仅 crate 内握手构造器可以直接借用。
    private_key: [u8; KEY_LEN],
    /// 与私钥对应的长期静态公钥。
    public_key: NoisePublicKey,
}

impl NoiseIdentity {
    /// 使用操作系统安全随机源生成新的 X25519 长期静态身份。
    ///
    /// 返回成功后应立即持久化；进程重启时生成新身份会使已有 IK 信任关系失效。
    pub fn generate() -> Result<Self, NoiseError> {
        debug!("generating a new Noise static identity");
        // 使用正式 XXpsk3 suite 取得与协议一致的 25519 keypair 生成器。
        let params: NoiseParams = NOISE_XX_PSK3.parse()?;
        let pair = Builder::new(params).generate_keypair()?;
        let identity = Self::from_parts(&pair.private, &pair.public)?;
        debug!(key_id = ?identity.key_id(), "generated Noise static identity");
        Ok(identity)
    }

    /// 从调用方存储的私钥和公钥恢复身份，并严格验证两者长度。
    ///
    /// 当前方法不重新推导公钥；调用方必须保证这一对字节来自同一身份记录。
    pub fn from_parts(private_key: &[u8], public_key: &[u8]) -> Result<Self, NoiseError> {
        let identity = Self {
            private_key: private_key
                .try_into()
                .map_err(|_| NoiseError::InvalidKeyLength)?,
            public_key: NoisePublicKey::from_bytes(public_key)?,
        };
        debug!(key_id = ?identity.key_id(), "restored Noise static identity from persisted bytes");
        Ok(identity)
    }

    /// 返回可公开复制的静态公钥。
    pub fn public_key(&self) -> NoisePublicKey {
        self.public_key
    }

    /// 返回当前身份公钥的稳定指纹，不读取或暴露私钥。
    pub fn key_id(&self) -> KeyId {
        self.public_key.key_id()
    }

    /// 导出私钥副本供调用方持久化；返回值应按凭据处理且不得记录日志。
    pub fn export_private_key(&self) -> SecretKeyBytes {
        SecretKeyBytes(self.private_key)
    }

    /// 仅向 crate 内握手构造器借用私钥，避免扩大敏感字节的公开 API。
    pub(crate) fn private_key(&self) -> &[u8; KEY_LEN] {
        &self.private_key
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
/// Server 静态公钥的稳定 BLAKE2s-256 指纹。
pub struct KeyId([u8; KEY_LEN]);

impl KeyId {
    /// 从 Protobuf 或持久化字节恢复 key ID。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NoiseError> {
        Ok(Self(
            bytes.try_into().map_err(|_| NoiseError::InvalidKeyLength)?,
        ))
    }

    /// 返回固定长度 key ID 字节，适合写入 Protobuf 或用作数据库索引。
    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
/// 一次静态密钥轮换事务的随机标识。
pub struct RotationId([u8; ROTATION_ID_LEN]);

impl RotationId {
    /// 使用操作系统安全随机源创建新的轮换事务 ID。
    pub fn generate() -> Result<Self, NoiseError> {
        let mut bytes = [0; ROTATION_ID_LEN];
        getrandom::fill(&mut bytes)?;
        debug!("generated a Noise key rotation transaction ID");
        Ok(Self(bytes))
    }

    /// 从网络或存储恢复轮换事务 ID，并严格校验 16 字节长度。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NoiseError> {
        Ok(Self(
            bytes
                .try_into()
                .map_err(|_| NoiseError::RotationIdMismatch)?,
        ))
    }

    /// 返回适合持久化或放入 Protobuf 字段的固定长度事务 ID。
    pub fn as_bytes(&self) -> &[u8; ROTATION_ID_LEN] {
        &self.0
    }
}
