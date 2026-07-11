//! 顶层 server 配置模型，负责聚合 HTTP、数据库、前端和日志配置。

use super::{DatabaseConfig, FrontendConfig, HttpConfig, LogConfig};

/// server 启动后的稳定配置。
///
/// 这里不直接保存 clap 参数类型，方便后续从配置文件、数据库或测试代码构造配置。
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// HTTP 服务配置。
    pub http: HttpConfig,
    /// 数据库连接配置。
    pub database: DatabaseConfig,
    /// 前端静态资源托管配置。
    pub frontend: FrontendConfig,
    /// 日志输出配置。
    pub log: LogConfig,
}
