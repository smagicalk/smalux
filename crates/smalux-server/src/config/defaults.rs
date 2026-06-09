//! server 默认值模块，负责集中保存启动配置和运行限制的默认常量。

#![allow(dead_code)]

/// 默认 HTTP 监听地址，首版只监听本机，避免开发期误暴露到公网。
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:3000";

/// 默认数据库连接地址；运行时也可以改为 postgres:// 或 mysql://。
pub const DEFAULT_DATABASE_URL: &str = "sqlite://smalux-server.db";

/// 默认前端构建产物目录，未启用内置前端资源时使用。
pub const DEFAULT_FRONTEND_DIR: &str = "apps/smalux-web/dist";

/// 默认 server 滚动日志文件路径。
pub const DEFAULT_LOG_FILE: &str = "logs/smalux-server.log";

/// 默认保留的滚动日志文件数量。
pub const DEFAULT_LOG_RETENTION_FILES: usize = 14;

/// 默认单个滚动日志文件最大大小，单位 MB。
pub const DEFAULT_LOG_MAX_SIZE_MB: u64 = 64;
