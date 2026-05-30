//! WebSocket 客户端配置和 agent 导出配置转换。

use super::auth::WebSocketAuth;
use super::request::redact_url;
use crate::config::model::{ExportAuthMode, ExportConfig, ExportFormat, ExportWireMode};
use crate::export::security::{SecurePskKey, parse_secure_token};
use anyhow::anyhow;
use std::fmt::Debug;

/// 默认心跳间隔，单位秒。
const DEFAULT_HEARTBEAT_SECS: u64 = 30;

/// WebSocket 客户端配置。
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct WebSocketConfig {
    /// WebSocket 服务端基础地址，可以自带 query 参数。
    pub(crate) url: String,
    /// 额外 query 参数，主要用于兼容第三方连接参数。
    pub(crate) query: Vec<(String, String)>,
    /// 握手认证方式。
    pub(crate) auth: WebSocketAuth,
    /// Smalux 自有协议 wire 模式。
    pub(crate) wire_mode: ExportWireMode,
    /// secure_psk 模式下使用的安全 key。
    pub(crate) secure_key: Option<SecurePskKey>,
    /// 是否跳过 TLS 证书校验。
    pub(crate) unsafe_cert: bool,
    /// 心跳间隔，单位为秒；0 表示禁用心跳。
    pub(crate) heartbeat: u64,
}

impl WebSocketConfig {
    /// 使用默认设置创建配置。
    pub(crate) fn new(url: String) -> Self {
        Self {
            url,
            query: Vec::new(),
            auth: WebSocketAuth::None,
            wire_mode: ExportWireMode::BinaryPlain,
            secure_key: None,
            unsafe_cert: false,
            heartbeat: DEFAULT_HEARTBEAT_SECS,
        }
    }

    /// 增加一个 query 参数。
    pub(crate) fn with_query_param(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// 设置握手认证方式。
    pub(crate) fn with_auth(mut self, auth: WebSocketAuth) -> Self {
        self.auth = auth;
        self
    }

    /// 设置 wire 模式和安全 key。
    pub(crate) fn with_wire_security(
        mut self,
        wire_mode: ExportWireMode,
        secure_key: Option<SecurePskKey>,
    ) -> Self {
        self.wire_mode = wire_mode;
        self.secure_key = secure_key;
        self
    }

    /// 设置是否跳过 TLS 证书校验。
    pub(crate) fn with_unsafe_cert(mut self, unsafe_cert: bool) -> Self {
        self.unsafe_cert = unsafe_cert;
        self
    }

    /// 设置心跳间隔，0 表示禁用。
    pub(crate) fn with_heartbeat(mut self, heartbeat: u64) -> Self {
        self.heartbeat = heartbeat;
        self
    }
}

impl TryFrom<&ExportConfig> for WebSocketConfig {
    // 配置转换失败统一返回 anyhow，便于携带具体字段上下文。
    type Error = anyhow::Error;

    /// 把 agent 导出配置转换为 WebSocket 握手配置。
    fn try_from(config: &ExportConfig) -> Result<Self, Self::Error> {
        let effective_wire_mode = match config.format {
            ExportFormat::SmaluxJson => config.wire_mode,
            ExportFormat::Komari => ExportWireMode::BinaryPlain,
        };

        if matches!(effective_wire_mode, ExportWireMode::SecurePsk)
            && !matches!(config.auth_mode, ExportAuthMode::None)
        {
            anyhow::bail!(
                "export.auth_mode must be none when export.wire_mode is secure_psk; token is used only for PSK derivation"
            );
        }

        let url = config.server_url.clone();
        let heartbeat = config.heartbeat.as_secs();
        let auth = match config.auth_mode {
            ExportAuthMode::None => WebSocketAuth::None,
            ExportAuthMode::Query => WebSocketAuth::QueryToken {
                param: config.query_token_param.clone(),
                token: required_token(config)?,
            },
            ExportAuthMode::Bearer => WebSocketAuth::BearerToken {
                token: required_token(config)?,
            },
        };

        let secure_key = match effective_wire_mode {
            ExportWireMode::BinaryPlain => None,
            ExportWireMode::SecurePsk => Some(parse_secure_token(&required_token(config)?)?),
        };

        let mut websocket_config = Self::new(url)
            .with_auth(auth)
            .with_wire_security(effective_wire_mode, secure_key)
            .with_unsafe_cert(config.unsafe_cert)
            .with_heartbeat(heartbeat);

        for (key, value) in &config.query {
            websocket_config = websocket_config.with_query_param(key, value);
        }

        Ok(websocket_config)
    }
}

/// 读取必填 token，并在缺失时返回明确错误。
fn required_token(config: &ExportConfig) -> anyhow::Result<String> {
    config
        .token
        .clone()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| anyhow!("export.token is required for selected auth mode"))
}

impl Debug for WebSocketConfig {
    /// Debug 中 URL 也要脱敏，避免 query token 从调试输出泄露。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketConfig")
            .field("url", &redact_url(&self.url))
            .field("query_count", &self.query.len())
            .field("auth", &self.auth)
            .field("wire_mode", &self.wire_mode.as_str())
            .field(
                "secure_key_id",
                &self.secure_key.as_ref().map(|key| key.key_id.as_str()),
            )
            .field("unsafe_cert", &self.unsafe_cert)
            .field("heartbeat", &self.heartbeat)
            .finish()
    }
}
