//! Agent 配置模块入口。
//!
//! 配置来源优先级：默认值 < 启动参数 < server 运行时下发 patch。

/// 启动参数解析。
pub(crate) mod cli;
/// 默认配置值。
pub(crate) mod defaults;
/// 动态配置管理。
pub(crate) mod manager;
/// 配置模型和 patch 模型。
pub(crate) mod model;

pub(crate) use cli::CliArgs;
pub(crate) use manager::ConfigManager;
pub(crate) use model::{AgentConfig, PublicIpConfig};

#[cfg(test)]
mod tests {
    //! 配置默认值测试。

    use super::*;
    use std::time::Duration;

    /// 验证默认配置满足轻量监控 agent 的启动要求。
    #[test]
    fn default_config_collects_public_ip_without_blocking_first_report() {
        let config = AgentConfig::default();

        assert!(config.core.enabled);
        assert!(config.disk.enabled);
        assert!(config.disk.include_per_device);
        assert!(config.network.enabled);
        assert!(config.network.include_per_interface);
        assert!(config.public_ip.enabled);
        assert!(!config.public_ip.required_for_first_report);
        assert_eq!(
            config.public_ip.refresh_interval,
            Duration::from_secs(24 * 60 * 60)
        );
        assert!(config.report.enabled);
    }
}
