//! server 默认值模块，负责集中保存启动配置和运行限制的默认常量。

/// 默认 HTTP 监听 IP，首版只监听本机，避免开发期误暴露到公网。
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1";

/// 默认 HTTP 监听端口。
pub const DEFAULT_BIND_PORT: u16 = 3000;

/// 默认数据库驱动，首版默认使用本地 SQLite 文件库。
pub const DEFAULT_DATABASE_DRIVER: &str = "sqlite";

/// 默认 SQLite 文件数据库路径。
pub const DEFAULT_SQLITE_DATABASE_NAME: &str = "smalux-server.db";

/// 默认 PostgreSQL/MySQL 数据库名。
pub const DEFAULT_SERVER_DATABASE_NAME: &str = "smalux";

/// 默认 PostgreSQL/MySQL 数据库主机。
pub const DEFAULT_DATABASE_HOST: &str = "127.0.0.1";

/// 默认 PostgreSQL 端口。
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

/// 默认 MySQL 端口。
pub const DEFAULT_MYSQL_PORT: u16 = 3306;

/// 默认前端构建产物目录，未启用内置前端资源时使用。
pub const DEFAULT_FRONTEND_DIR: &str = "apps/smalux-web/dist";

/// 默认前端槽位模式，优先使用内置资源。
pub const DEFAULT_FRONTEND_SLOT_MODE: &str = "embedded";

/// 默认不由 server 托管前端静态资源。
pub const DEFAULT_SERVE_FRONTEND: bool = false;

/// 默认启用 SPA fallback，适配 React/Vite 这类前端路由。
pub const DEFAULT_FRONTEND_SPA_FALLBACK: bool = true;

/// 默认 server 滚动日志文件路径。
pub const DEFAULT_LOG_FILE: &str = "logs/smalux-server.log";

/// 默认保留的滚动日志文件数量。
pub const DEFAULT_LOG_RETENTION_FILES: usize = 14;

/// 默认单个滚动日志文件最大大小，单位 MB。
pub const DEFAULT_LOG_MAX_SIZE_MB: u64 = 64;
