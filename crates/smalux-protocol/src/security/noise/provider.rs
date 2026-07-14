//! Noise 静态身份、远端公钥校验策略和可复用会话工厂。
//!
//! Provider 可以跨连接复用长期静态密钥，但每次 `start` 都创建独立 HandshakeState
//! 和 TransportState；注册、授权和密钥持久化仍由上层应用负责。

use std::{fmt, sync::Arc};

use snow::Builder;
use zeroize::{Zeroize, Zeroizing};

use crate::{Error, Result};

use super::session::{
    IK_PARAMS, NOISE_IK_SCHEME, NOISE_REKEY_INTERVAL_RECORDS, NOISE_XX_SCHEME, NoiseSession,
    PROLOGUE_PREFIX, STATIC_KEY_BYTES, XX_PARAMS, copy_fixed_key, noise_error, parse_params,
};
use crate::security::{SecurityContext, SecurityProvider, SecurityRole, SecuritySession};

/// Noise 静态身份密钥对；Debug 永远不会输出私钥。
pub struct NoiseKeypair {
    /// 必须保密并在释放时主动清零的 X25519 私钥。
    private: Zeroizing<[u8; STATIC_KEY_BYTES]>,
    /// 可公开、持久化并用于远端身份校验的 X25519 公钥。
    public: [u8; STATIC_KEY_BYTES],
}

impl fmt::Debug for NoiseKeypair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NoiseKeypair")
            .field("private", &"[REDACTED]")
            .field("public", &self.public)
            .finish()
    }
}

impl NoiseKeypair {
    /// 使用 snow 的密码随机数实现生成 X25519 静态身份密钥。
    pub fn generate() -> Result<Self> {
        let params = parse_params(XX_PARAMS)?;
        let mut generated = Builder::new(params)
            .generate_keypair()
            .map_err(|error| noise_error("generate static keypair", error))?;
        let private = copy_fixed_key("generated private key", &generated.private)?;
        let public = copy_fixed_key("generated public key", &generated.public)?;
        generated.private.zeroize();
        Ok(Self {
            private: Zeroizing::new(private),
            public,
        })
    }

    /// 从持久化的 32 字节私钥和公钥恢复身份。
    ///
    /// 调用方负责确保存储的公私钥属于同一个 X25519 密钥对；不匹配会导致握手失败。
    pub fn from_parts(private: [u8; STATIC_KEY_BYTES], public: [u8; STATIC_KEY_BYTES]) -> Self {
        Self {
            private: Zeroizing::new(private),
            public,
        }
    }

    /// 返回可以公开和持久化的 X25519 静态公钥。
    pub const fn public_key(&self) -> [u8; STATIC_KEY_BYTES] {
        self.public
    }

    /// 仅向当前 crate 内的 Noise Builder 暴露静态私钥。
    fn private_key(&self) -> &[u8; STATIC_KEY_BYTES] {
        &self.private
    }
}

/// 在 Noise 握手结束、进入 TransportState 前校验远端静态公钥。
pub trait NoiseRemoteKeyVerifier: Send + Sync {
    /// 返回成功表示该公钥允许建立安全会话，失败会永久终止当前连接。
    fn verify(&self, context: &SecurityContext, remote_key: &[u8; 32]) -> Result<()>;
}

/// 只接受一个固定远端静态公钥的校验器。
#[derive(Debug, Clone, Copy)]
pub struct PinnedRemoteKey {
    /// 本地信任配置中预期出现的远端静态公钥。
    expected: [u8; STATIC_KEY_BYTES],
}

impl PinnedRemoteKey {
    /// 创建固定公钥校验器。
    pub const fn new(expected: [u8; STATIC_KEY_BYTES]) -> Self {
        Self { expected }
    }
}

impl NoiseRemoteKeyVerifier for PinnedRemoteKey {
    fn verify(&self, _context: &SecurityContext, remote_key: &[u8; 32]) -> Result<()> {
        if remote_key != &self.expected {
            return Err(Error::Security(
                "Noise remote static key does not match the pinned key".to_owned(),
            ));
        }
        Ok(())
    }
}

/// 显式允许任意远端静态公钥；仅适合测试或注册前的受限连接。
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowUnknownRemoteKey;

impl NoiseRemoteKeyVerifier for AllowUnknownRemoteKey {
    fn verify(&self, _context: &SecurityContext, _remote_key: &[u8; 32]) -> Result<()> {
        Ok(())
    }
}

/// 将同步闭包包装为远端静态公钥校验器。
pub struct CallbackRemoteKeyVerifier<F> {
    /// 执行上层信任策略的同步快速回调。
    callback: F,
}

impl<F> CallbackRemoteKeyVerifier<F> {
    /// 创建回调校验器；回调必须快速完成，禁止执行阻塞数据库或网络操作。
    pub const fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F> NoiseRemoteKeyVerifier for CallbackRemoteKeyVerifier<F>
where
    F: Fn(&SecurityContext, &[u8; 32]) -> Result<()> + Send + Sync,
{
    fn verify(&self, context: &SecurityContext, remote_key: &[u8; 32]) -> Result<()> {
        (self.callback)(context, remote_key)
    }
}

/// 可复用的 Noise 会话工厂；每次 `start` 都创建独立握手和流量密钥。
pub struct NoiseProvider {
    /// 当前 Provider 只允许创建的连接角色。
    role: SecurityRole,
    /// 多连接复用的本地静态身份密钥。
    keypair: NoiseKeypair,
    /// 握手结束后执行的远端静态公钥信任策略。
    verifier: Arc<dyn NoiseRemoteKeyVerifier>,
    /// 是否允许协商 XX 模式。
    enable_xx: bool,
    /// 是否允许协商 IK 模式。
    enable_ik: bool,
    /// IK Client 在握手前必须已知的 Server 静态公钥。
    remote_responder_key: Option<[u8; STATIC_KEY_BYTES]>,
    /// 按本地优先级排列的稳定安全方案名称。
    schemes: Vec<String>,
}

impl fmt::Debug for NoiseProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NoiseProvider")
            .field("role", &self.role)
            .field("local_public_key", &self.keypair.public_key())
            .field("schemes", &self.schemes)
            .finish_non_exhaustive()
    }
}

impl NoiseProvider {
    /// 创建 Provider Builder；必须显式启用 XX、IK 或两者。
    pub fn builder<V>(
        role: SecurityRole,
        keypair: NoiseKeypair,
        verifier: V,
    ) -> NoiseProviderBuilder
    where
        V: NoiseRemoteKeyVerifier + 'static,
    {
        NoiseProviderBuilder {
            role,
            keypair,
            verifier: Arc::new(verifier),
            enable_xx: false,
            enable_ik: false,
            remote_responder_key: None,
        }
    }

    /// 返回当前本地静态公钥。
    pub const fn local_public_key(&self) -> [u8; STATIC_KEY_BYTES] {
        self.keypair.public_key()
    }

    /// 使用指定 record 间隔创建会话，仅用于低阈值 rekey 单元测试。
    #[cfg(test)]
    pub(super) fn start_with_rekey_interval(
        &self,
        context: SecurityContext,
        rekey_interval_records: u64,
    ) -> Result<Arc<NoiseSession>> {
        self.start_session(context, rekey_interval_records)
    }

    /// 完成 Provider 公共配置到单连接 Noise 状态的转换。
    fn start_session(
        &self,
        context: SecurityContext,
        rekey_interval_records: u64,
    ) -> Result<Arc<NoiseSession>> {
        if rekey_interval_records == 0 {
            return Err(Error::Security(
                "Noise rekey interval must be greater than zero".to_owned(),
            ));
        }
        if context.role() != self.role {
            return Err(Error::Security(
                "Noise provider role differs from SecurityContext role".to_owned(),
            ));
        }

        let (params, remote_key) = match context.scheme() {
            NOISE_XX_SCHEME if self.enable_xx => (XX_PARAMS, None),
            NOISE_IK_SCHEME if self.enable_ik => {
                let remote = if self.role == SecurityRole::Client {
                    Some(self.remote_responder_key.ok_or_else(|| {
                        Error::Security(
                            "Noise IK client requires the responder static public key".to_owned(),
                        )
                    })?)
                } else {
                    None
                };
                (IK_PARAMS, remote)
            }
            _ => {
                return Err(Error::Security(format!(
                    "Noise provider does not support negotiated scheme {}",
                    context.scheme()
                )));
            }
        };

        let mut prologue = Vec::with_capacity(PROLOGUE_PREFIX.len() + context.transcript().len());
        prologue.extend_from_slice(PROLOGUE_PREFIX);
        prologue.extend_from_slice(context.transcript());

        let mut builder = Builder::new(parse_params(params)?)
            .local_private_key(self.keypair.private_key())
            .map_err(|error| noise_error("set local static key", error))?
            .prologue(&prologue)
            .map_err(|error| noise_error("set negotiation transcript prologue", error))?;
        if let Some(remote_key) = remote_key.as_ref() {
            builder = builder
                .remote_public_key(remote_key)
                .map_err(|error| noise_error("set IK responder public key", error))?;
        }
        let handshake = match self.role {
            SecurityRole::Client => builder.build_initiator(),
            SecurityRole::Server => builder.build_responder(),
        }
        .map_err(|error| noise_error("create handshake state", error))?;

        Ok(Arc::new(NoiseSession::new(
            context,
            self.verifier.clone(),
            handshake,
            rekey_interval_records,
        )))
    }
}

impl SecurityProvider for NoiseProvider {
    fn supported_schemes(&self) -> &[String] {
        &self.schemes
    }

    fn start(&self, context: SecurityContext) -> Result<Arc<dyn SecuritySession>> {
        Ok(self.start_session(context, NOISE_REKEY_INTERVAL_RECORDS)?)
    }
}

/// NoiseProvider 的显式模式配置器。
pub struct NoiseProviderBuilder {
    /// Provider 创建的连接角色。
    role: SecurityRole,
    /// Provider 使用的本地静态身份。
    keypair: NoiseKeypair,
    /// 远端身份信任策略。
    verifier: Arc<dyn NoiseRemoteKeyVerifier>,
    /// 是否允许 XX。
    enable_xx: bool,
    /// 是否允许 IK。
    enable_ik: bool,
    /// IK Client 预先知道的 Server 静态公钥。
    remote_responder_key: Option<[u8; STATIC_KEY_BYTES]>,
}

impl NoiseProviderBuilder {
    /// 启用无需预先知道远端静态公钥的 XX 模式。
    pub const fn enable_xx(mut self) -> Self {
        self.enable_xx = true;
        self
    }

    /// 启用 IK 模式。Client 必须传入 Server 公钥，Server 必须传入 `None`。
    pub const fn enable_ik(mut self, remote_responder_key: Option<[u8; STATIC_KEY_BYTES]>) -> Self {
        self.enable_ik = true;
        self.remote_responder_key = remote_responder_key;
        self
    }

    /// 校验模式与角色配置并创建可复用 Provider。
    pub fn build(self) -> Result<NoiseProvider> {
        if !self.enable_xx && !self.enable_ik {
            return Err(Error::Security(
                "Noise provider must enable XX, IK, or both".to_owned(),
            ));
        }
        match (self.role, self.enable_ik, self.remote_responder_key) {
            (SecurityRole::Client, true, None) => {
                return Err(Error::Security(
                    "Noise IK client requires the responder static public key".to_owned(),
                ));
            }
            (SecurityRole::Server, true, Some(_)) => {
                return Err(Error::Security(
                    "Noise IK server must not configure a responder public key".to_owned(),
                ));
            }
            _ => {}
        }

        let mut schemes = Vec::with_capacity(2);
        if self.enable_ik {
            schemes.push(NOISE_IK_SCHEME.to_owned());
        }
        if self.enable_xx {
            schemes.push(NOISE_XX_SCHEME.to_owned());
        }
        Ok(NoiseProvider {
            role: self.role,
            keypair: self.keypair,
            verifier: self.verifier,
            enable_xx: self.enable_xx,
            enable_ik: self.enable_ik,
            remote_responder_key: self.remote_responder_key,
            schemes,
        })
    }
}
