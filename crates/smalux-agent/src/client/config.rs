//! `SmaluxClient` 的连接、认证和后台维护配置。

use std::{env, fmt, time::Duration};

use smalux_core::config::default::{DEFAULT_ADDRESS, DEFAULT_AGENT_PREFIX, DEFAULT_PORT};
use smalux_protocol::tonic_transport::{HeartbeatPolicy, RekeyPolicy, SessionDriverConfig};

use crate::config::default::{
    DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_RECONNECT_INITIAL_DELAY, DEFAULT_RECONNECT_MAX_DELAY,
};

const SERVER_ENDPOINT_ENV: &str = "SMALUX_SERVER_ENDPOINT";
const GRPC_PREFIX_ENV: &str = "SMALUX_GRPC_PREFIX";
const REGISTRATION_TOKEN_ENV: &str = "SMALUX_REGISTRATION_TOKEN";
const HANDSHAKE_TIMEOUT_ENV: &str = "SMALUX_HANDSHAKE_TIMEOUT";
const HEARTBEAT_INTERVAL_ENV: &str = "SMALUX_HEARTBEAT_INTERVAL";
const HEARTBEAT_TIMEOUT_ENV: &str = "SMALUX_HEARTBEAT_TIMEOUT";
const RECONNECT_INITIAL_DELAY_ENV: &str = "SMALUX_RECONNECT_INITIAL_DELAY";
const RECONNECT_MAX_DELAY_ENV: &str = "SMALUX_RECONNECT_MAX_DELAY";

/// 配置中可选的一次性注册 Token。
///
/// 本类型故意不实现 `Display`；`Debug` 也始终脱敏，避免配置整体进入日志时泄露 PSK。
#[derive(Clone)]
pub struct RegistrationToken(String);

impl RegistrationToken {
    /// 接受非空且不含控制字符的 Token；格式和 PSK 长度只在真正需要注册时解析。
    pub fn new(value: impl Into<String>) -> anyhow::Result<Self> {
        let value = value.into();
        if value.is_empty() || value.chars().any(char::is_control) {
            anyhow::bail!("registration token must be non-empty and contain no control characters");
        }
        Ok(Self(value))
    }

    /// 仅供内部 XX 注册分支借用原始 Token。
    pub(super) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RegistrationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegistrationToken(<redacted>)")
    }
}

/// 网络中断后的 IK 重连退避参数。
#[derive(Clone, Copy, Debug)]
pub struct ReconnectPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: DEFAULT_RECONNECT_INITIAL_DELAY,
            max_delay: DEFAULT_RECONNECT_MAX_DELAY,
        }
    }
}

/// 外层 Client 的完整配置；注册 Token 是可选的首次注册材料，不是长期认证信息。
#[derive(Clone, Debug)]
pub struct SmaluxClientConfig {
    pub endpoint: String,
    pub grpc_prefix: Option<String>,
    pub registration_token: Option<RegistrationToken>,
    pub handshake_timeout: Duration,
    pub heartbeat: HeartbeatPolicy,
    pub rekey: RekeyPolicy,
    pub driver: SessionDriverConfig,
    pub reconnect: ReconnectPolicy,
}

impl SmaluxClientConfig {
    /// 使用必要的 Server 地址构造配置，其余参数采用安全默认值。
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let config = Self {
            endpoint: endpoint.into(),
            grpc_prefix: Some(DEFAULT_AGENT_PREFIX.to_owned()),
            registration_token: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            heartbeat: HeartbeatPolicy::default(),
            rekey: RekeyPolicy::default(),
            driver: SessionDriverConfig::default(),
            reconnect: ReconnectPolicy::default(),
        };
        config.validate()?;
        Ok(config)
    }

    /// 从 Agent 环境变量创建运行配置。
    pub fn from_env() -> anyhow::Result<Self> {
        let endpoint = env::var(SERVER_ENDPOINT_ENV)
            .unwrap_or_else(|_| format!("http://{DEFAULT_ADDRESS}:{DEFAULT_PORT}"));
        let mut config = Self::new(endpoint)?;
        if let Some(prefix) = env::var(GRPC_PREFIX_ENV)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            config.grpc_prefix = Some(prefix);
        }
        config.registration_token = env::var(REGISTRATION_TOKEN_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .map(RegistrationToken::new)
            .transpose()?;
        config.handshake_timeout =
            duration_from_env(HANDSHAKE_TIMEOUT_ENV, config.handshake_timeout)?;
        config.heartbeat.interval =
            duration_from_env(HEARTBEAT_INTERVAL_ENV, config.heartbeat.interval)?;
        config.heartbeat.timeout =
            duration_from_env(HEARTBEAT_TIMEOUT_ENV, config.heartbeat.timeout)?;
        config.reconnect.initial_delay =
            duration_from_env(RECONNECT_INITIAL_DELAY_ENV, config.reconnect.initial_delay)?;
        config.reconnect.max_delay =
            duration_from_env(RECONNECT_MAX_DELAY_ENV, config.reconnect.max_delay)?;
        config.validate()?;
        Ok(config)
    }

    /// 设置可选注册 Token；已经存在 `Registered` 状态时 Client 不读取它。
    pub fn set_registration_token(&mut self, token: Option<RegistrationToken>) {
        self.registration_token = token;
    }

    /// 设置反向代理或 Axum 使用的 gRPC 路径前缀。
    pub fn set_grpc_prefix(&mut self, prefix: Option<String>) {
        self.grpc_prefix = prefix;
    }

    /// 校验覆盖环境变量或 CLI 参数后的最终配置。
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.endpoint.trim().is_empty() {
            anyhow::bail!("Server endpoint must not be empty");
        }
        if self.handshake_timeout.is_zero() {
            anyhow::bail!("handshake timeout must be greater than zero");
        }
        if self.heartbeat.interval.is_zero() || self.heartbeat.timeout <= self.heartbeat.interval {
            anyhow::bail!("heartbeat timeout must be greater than its non-zero interval");
        }
        if self.reconnect.initial_delay.is_zero()
            || self.reconnect.max_delay < self.reconnect.initial_delay
        {
            anyhow::bail!("reconnect max delay must be at least the non-zero initial delay");
        }
        Ok(())
    }
}

fn duration_from_env(name: &str, default: Duration) -> anyhow::Result<Duration> {
    let Some(value) = env::var(name).ok().filter(|value| !value.is_empty()) else {
        return Ok(default);
    };
    humantime::parse_duration(&value)
        .map_err(|error| anyhow::anyhow!("invalid {name} duration {value:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RegistrationToken, SmaluxClientConfig};

    #[test]
    fn registration_token_is_redacted_in_config_debug_output() {
        let mut config = SmaluxClientConfig::new("http://127.0.0.1:12345").unwrap();
        let secret = "token-id.0123456789abcdef";
        config.set_registration_token(Some(RegistrationToken::new(secret).unwrap()));

        let debug = format!("{config:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn config_rejects_invalid_heartbeat_and_reconnect_ranges() {
        let mut config = SmaluxClientConfig::new("http://127.0.0.1:12345").unwrap();
        config.heartbeat.timeout = config.heartbeat.interval;
        assert!(config.validate().is_err());

        config.heartbeat.timeout = Duration::from_secs(90);
        config.reconnect.initial_delay = Duration::from_secs(5);
        config.reconnect.max_delay = Duration::from_secs(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_keeps_default_grpc_prefix_without_environment_override() {
        let config = SmaluxClientConfig::new("http://127.0.0.1:12345").unwrap();
        assert_eq!(config.grpc_prefix.as_deref(), Some("/api/v1/grpc"));
    }
}
