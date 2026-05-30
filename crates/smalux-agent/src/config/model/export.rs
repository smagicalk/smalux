//! 导出连接配置模型。

use super::super::defaults::{
    DEFAULT_EXPORT_HEARTBEAT, DEFAULT_EXPORT_RECONNECT_INTERVAL, DEFAULT_QUERY_TOKEN_PARAM,
    DEFAULT_SERVER_URL,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Smalux 自有协议 wire 模式。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExportWireMode {
    /// 二进制包承载明文 JSON bytes，主要用于开发和联调。
    BinaryPlain,
    /// 二进制包承载 Noise PSK 加密后的 JSON bytes。
    SecurePsk,
}

impl ExportWireMode {
    /// wire 模式名称，用于日志。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BinaryPlain => "binary_plain",
            Self::SecurePsk => "secure_psk",
        }
    }
}

impl Default for ExportWireMode {
    /// 默认使用 binary plain，先统一 Smalux 自有协议 wire 形态。
    fn default() -> Self {
        Self::BinaryPlain
    }
}

/// 导出数据编码格式。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExportFormat {
    /// Smalux 默认 JSON frame。
    SmaluxJson,
    /// Komari 兼容格式。
    Komari,
}

impl ExportFormat {
    /// 格式名称，用于结构化日志。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SmaluxJson => "smalux_json",
            Self::Komari => "komari",
        }
    }
}

impl Default for ExportFormat {
    /// 默认使用 smalux 自有 JSON 协议。
    fn default() -> Self {
        Self::SmaluxJson
    }
}

/// 导出认证方式。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExportAuthMode {
    /// 不发送认证信息。
    None,
    /// 使用 query token。
    Query,
    /// 使用 Authorization Bearer token。
    Bearer,
}

impl Default for ExportAuthMode {
    /// 默认不额外发送认证信息，避免无参数启动时必须提供 token。
    fn default() -> Self {
        Self::None
    }
}

/// 导出连接配置。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExportConfig {
    /// server 导出地址。
    pub server_url: String,
    /// 导出数据编码格式。
    pub format: ExportFormat,
    /// Smalux 自有协议 wire 模式；Komari 不使用该字段。
    pub wire_mode: ExportWireMode,
    /// 是否要求 Smalux 自有协议必须使用安全通道。
    pub secure_required: bool,
    /// 认证 token。
    pub token: Option<String>,
    /// 认证方式。
    pub auth_mode: ExportAuthMode,
    /// query token 参数名。
    pub query_token_param: String,
    /// 额外 query 参数，用于兼容第三方 WebSocket 服务。
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    /// 是否跳过 TLS 证书校验。
    pub unsafe_cert: bool,
    /// WebSocket 心跳间隔。
    #[serde(with = "humantime_serde")]
    pub heartbeat: Duration,
    /// 断线或连接失败后的重连间隔。
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
}

impl Default for ExportConfig {
    /// 默认导出配置支持本地开发无参数启动。
    fn default() -> Self {
        Self {
            server_url: DEFAULT_SERVER_URL.to_string(),
            format: ExportFormat::default(),
            wire_mode: ExportWireMode::default(),
            secure_required: false,
            token: None,
            auth_mode: ExportAuthMode::default(),
            query_token_param: DEFAULT_QUERY_TOKEN_PARAM.to_string(),
            query: BTreeMap::new(),
            unsafe_cert: false,
            heartbeat: DEFAULT_EXPORT_HEARTBEAT,
            reconnect_interval: DEFAULT_EXPORT_RECONNECT_INTERVAL,
        }
    }
}

/// 导出连接配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExportConfigPatch {
    /// server 导出地址。
    pub server_url: Option<String>,
    /// 导出数据编码格式。
    pub format: Option<ExportFormat>,
    /// Smalux 自有协议 wire 模式。
    pub wire_mode: Option<ExportWireMode>,
    /// 是否要求 Smalux 自有协议必须使用安全通道。
    pub secure_required: Option<bool>,
    /// 认证 token。
    pub token: Option<String>,
    /// 认证方式。
    pub auth_mode: Option<ExportAuthMode>,
    /// query token 参数名。
    pub query_token_param: Option<String>,
    /// 额外 query 参数；下发后整体替换当前额外 query 集合。
    pub query: Option<BTreeMap<String, String>>,
    /// 是否跳过 TLS 证书校验。
    pub unsafe_cert: Option<bool>,
    /// WebSocket 心跳间隔。
    #[serde(default, with = "humantime_serde")]
    pub heartbeat: Option<Duration>,
    /// 断线或连接失败后的重连间隔。
    #[serde(default, with = "humantime_serde")]
    pub reconnect_interval: Option<Duration>,
}

impl ExportConfigPatch {
    /// 应用导出配置 patch。
    pub(crate) fn apply_to(&self, config: &mut ExportConfig) {
        if let Some(server_url) = self.server_url.clone() {
            config.server_url = server_url;
        }
        if let Some(format) = self.format {
            config.format = format;
        }
        if let Some(wire_mode) = self.wire_mode {
            config.wire_mode = wire_mode;
        }
        if let Some(secure_required) = self.secure_required {
            config.secure_required = secure_required;
        }
        if let Some(token) = self.token.clone() {
            config.token = Some(token);
        }
        if let Some(auth_mode) = self.auth_mode {
            config.auth_mode = auth_mode;
        }
        if let Some(query_token_param) = self.query_token_param.clone() {
            config.query_token_param = query_token_param;
        }
        if let Some(query) = self.query.clone() {
            config.query = query;
        }
        if let Some(unsafe_cert) = self.unsafe_cert {
            config.unsafe_cert = unsafe_cert;
        }
        if let Some(heartbeat) = self.heartbeat {
            config.heartbeat = heartbeat;
        }
        if let Some(reconnect_interval) = self.reconnect_interval {
            config.reconnect_interval = reconnect_interval;
        }
    }
}

#[cfg(test)]
mod tests {
    //! 导出配置模型测试。

    use super::*;

    /// 验证默认导出格式是 smalux_json。
    #[test]
    fn default_export_format_is_smalux_json() {
        let config = ExportConfig::default();

        assert_eq!(config.format, ExportFormat::SmaluxJson);
        assert_eq!(config.format.as_str(), "smalux_json");
    }

    /// 验证导出格式 patch 可以覆盖当前配置。
    #[test]
    fn export_format_patch_updates_config() {
        let mut config = ExportConfig::default();
        let patch = ExportConfigPatch {
            format: Some(ExportFormat::Komari),
            ..ExportConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert_eq!(config.format, ExportFormat::Komari);
        assert_eq!(config.format.as_str(), "komari");
    }
}
