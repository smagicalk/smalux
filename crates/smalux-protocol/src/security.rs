//! 可插拔安全层接口、协商上下文和 SessionContent 保护辅助函数。
//!
//! Transport 负责可靠有序传输，`ProtocolSession` 负责消息顺序，本模块负责将最终
//! 协商结果交给具体安全实现，并统一包装/解包 `ProtectedPayload`。

use std::sync::Arc;

use prost::Message;

use crate::{Error, ProtectedPayload, ProtocolVersion, Result, SessionContent};

mod noise;

pub use noise::{
    AllowUnknownRemoteKey, CallbackRemoteKeyVerifier, NOISE_IK_SCHEME,
    NOISE_RECORD_PLAINTEXT_BYTES, NOISE_REKEY_INTERVAL_RECORDS, NOISE_XX_SCHEME, NoiseKeypair,
    NoiseProvider, NoiseProviderBuilder, NoiseRemoteKeyVerifier, PinnedRemoteKey,
};

/// 安全会话在连接中的一方角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityRole {
    /// 主动发起底层连接的一方。
    Client,
    /// 接受底层连接的一方。
    Server,
}

/// 具体安全实现的握手或可用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityState {
    /// 仍需交换握手数据。
    Handshaking,
    /// 已经可以保护和解保护会话内容。
    Ready,
    /// 安全会话已经关闭。
    Closed,
    /// 安全会话发生不可恢复错误。
    Failed,
}

/// 交给安全实现绑定的最终协议协商上下文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityContext {
    /// 当前一方在握手中的角色。
    role: SecurityRole,
    /// Server 最终选择的安全方案名称。
    scheme: String,
    /// 最终使用的协议版本。
    version: ProtocolVersion,
    /// 最终启用的能力集合。
    capabilities: Vec<String>,
    /// 最终协商的单帧上限。
    max_frame_bytes: usize,
    /// 本次连接的 16 字节会话标识。
    session_id: Vec<u8>,
    /// 规范化的 Hello 协商 transcript。
    transcript: Vec<u8>,
}

impl SecurityContext {
    /// 创建只包含已完成协商结果的安全上下文。
    pub(crate) fn new(
        role: SecurityRole,
        scheme: String,
        version: ProtocolVersion,
        capabilities: Vec<String>,
        max_frame_bytes: usize,
        session_id: Vec<u8>,
        transcript: Vec<u8>,
    ) -> Self {
        Self {
            role,
            scheme,
            version,
            capabilities,
            max_frame_bytes,
            session_id,
            transcript,
        }
    }

    /// 返回当前一方的安全握手角色。
    pub const fn role(&self) -> SecurityRole {
        self.role
    }

    /// 返回协商后的安全方案名称。
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// 返回协商后的精确协议版本。
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// 返回协商后启用的能力集合。
    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    /// 返回协商后的单帧上限。
    pub const fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// 返回本次连接的 16 字节会话标识。
    pub fn session_id(&self) -> &[u8] {
        &self.session_id
    }

    /// 返回具体安全实现必须认证的规范化协商 transcript。
    pub fn transcript(&self) -> &[u8] {
        &self.transcript
    }
}

/// 某一步安全握手需要发送给对端的不透明载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeMessage {
    /// 从零开始并严格递增的握手步骤。
    pub step: u32,
    /// 具体安全算法定义的握手字节。
    pub payload: Vec<u8>,
}

/// 可插拔安全算法工厂，可使用内置 Noise Provider 或由上层应用提供其他实现。
pub trait SecurityProvider: Send + Sync {
    /// 返回本实现支持的命名空间安全方案，按本地优先级排序。
    fn supported_schemes(&self) -> &[String];

    /// 根据已协商且不可再修改的上下文创建单连接安全会话。
    fn start(&self, context: SecurityContext) -> Result<Arc<dyn SecuritySession>>;
}

/// 单条连接使用的线程安全状态会话。
pub trait SecuritySession: Send + Sync {
    /// 返回当前实现对应的安全方案名称。
    fn scheme(&self) -> &str;

    /// 返回当前握手、就绪或终止状态。
    fn state(&self) -> SecurityState;

    /// 返回 Ready 后是否要求使用 ProtectedPayload 承载所有 SessionContent。
    fn protects_content(&self) -> bool;

    /// 获取下一条需要发送的握手消息；当前无需发送时返回 `None`。
    fn next_handshake(&self) -> Result<Option<HandshakeMessage>>;

    /// 接收并处理对端的一步握手消息。
    fn receive_handshake(&self, message: HandshakeMessage) -> Result<()>;

    /// 保护已经编码好的 SessionContent。
    fn protect(&self, plaintext: &[u8]) -> Result<Vec<u8>>;

    /// 解保护 ProtectedPayload 中的密文。
    fn unprotect(&self, ciphertext: &[u8]) -> Result<Vec<u8>>;

    /// 返回单次 `protect` 允许接收的最大明文字节数。
    fn max_plaintext_bytes(&self) -> usize {
        usize::MAX
    }

    /// 返回握手确认的远端静态公钥；无静态身份的方案返回 `None`。
    fn remote_static_key(&self) -> Option<Vec<u8>> {
        None
    }

    /// 返回可用于上层通道绑定的握手摘要。
    fn channel_binding(&self) -> Option<Vec<u8>> {
        None
    }

    /// 主动销毁当前会话持有的临时状态和密钥。
    fn close(&self);
}

/// 编码并保护完整 SessionContent，生成可放入方向化 Frame 的密文载荷。
pub fn protect_session_content(
    security: &dyn SecuritySession,
    content: &SessionContent,
) -> Result<ProtectedPayload> {
    let encoded_len = content.encoded_len();
    if encoded_len > security.max_plaintext_bytes() {
        return Err(Error::Security(format!(
            "session content size {encoded_len} exceeds secure plaintext limit {}",
            security.max_plaintext_bytes()
        )));
    }
    let ciphertext = security.protect(&content.encode_to_vec())?;
    if ciphertext.is_empty() {
        return Err(Error::Security(
            "security session returned an empty protected payload".to_owned(),
        ));
    }
    Ok(ProtectedPayload { ciphertext })
}

/// 解保护并解码完整 SessionContent，字段和 sequence 继续交给 ProtocolSession 校验。
pub fn unprotect_session_content(
    security: &dyn SecuritySession,
    payload: &ProtectedPayload,
) -> Result<SessionContent> {
    if payload.ciphertext.is_empty() {
        return Err(Error::Security(
            "protected payload ciphertext is empty".to_owned(),
        ));
    }
    let plaintext = security.unprotect(&payload.ciphertext)?;
    Ok(SessionContent::decode(plaintext.as_slice())?)
}
