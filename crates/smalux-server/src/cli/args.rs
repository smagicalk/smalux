//! server CLI 参数模块，负责定义 clap 参数结构和启动输入解析规则。

use std::{collections::BTreeMap, net::IpAddr, path::PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::{Parser, ValueEnum};
use secrecy::SecretString;

use crate::config::defaults::{
    DEFAULT_BIND_ADDR, DEFAULT_BIND_PORT, DEFAULT_DATABASE_DRIVER, DEFAULT_DATABASE_HOST,
    DEFAULT_FRONTEND_DIR, DEFAULT_FRONTEND_SPA_FALLBACK, DEFAULT_LOG_FILE, DEFAULT_LOG_MAX_SIZE_MB,
    DEFAULT_LOG_RETENTION_FILES, DEFAULT_SERVE_FRONTEND,
};
use crate::config::model::{
    DatabaseConfig, DatabaseDriver, FrontendConfig, HttpConfig, LogConfig, ServerConfig,
};

/// CLI 支持的数据库驱动，避免用户输入不可识别的字符串。
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DatabaseDriverArg {
    /// SQLite 文件数据库，适合本地部署和开发环境。
    Sqlite,
    /// PostgreSQL 数据库，适合生产部署和多实例扩展。
    Postgres,
    /// MySQL 数据库，适合已有 MySQL 基础设施的部署场景。
    Mysql,
}

/// CLI 中的敏感字符串，Debug 输出时只显示是否已设置。
#[derive(Clone, Default)]
pub struct SecretArg(pub String);

impl std::fmt::Debug for SecretArg {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretArg")
            .field("is_set", &!self.0.is_empty())
            .finish()
    }
}

/// 解析敏感 CLI 参数，避免在类型层面直接暴露为普通字符串。
fn parse_secret_arg(value: &str) -> Result<SecretArg, String> {
    Ok(SecretArg(value.to_string()))
}

/// 数据库连接 URL 的额外 query 参数。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseParamArg {
    /// query 参数名。
    pub key: String,
    /// query 参数值。
    pub value: String,
}

/// 解析 `KEY=VALUE` 形式的数据库扩展参数。
fn parse_database_param(value: &str) -> Result<DatabaseParamArg, String> {
    let Some((key, param_value)) = value.split_once('=') else {
        return Err("database param must be KEY=VALUE".to_string());
    };

    if key.trim().is_empty() {
        return Err("database param key cannot be empty".to_string());
    }

    Ok(DatabaseParamArg {
        key: key.to_string(),
        value: param_value.to_string(),
    })
}

impl From<DatabaseDriverArg> for DatabaseDriver {
    fn from(driver: DatabaseDriverArg) -> Self {
        match driver {
            DatabaseDriverArg::Sqlite => Self::Sqlite,
            DatabaseDriverArg::Postgres => Self::Postgres,
            DatabaseDriverArg::Mysql => Self::Mysql,
        }
    }
}

/// server 启动参数，负责承接命令行和环境变量中的原始输入。
///
/// CLI 只配置 server 进程启动所需的静态运行环境，不配置 agent token/key 或 agent 执行能力。
/// agent 凭据应在“添加 agent”流程中动态生成，并存储到数据库。
#[derive(Clone, Debug, Parser)]
#[command(
    name = "smalux-server",
    version,
    about = "Smalux monitoring server",
    long_about = "Smalux monitoring server for agent ingestion, REST API, dashboard realtime events, and optional frontend hosting."
)]
pub struct ServerArgs {
    /// HTTP 监听 IP 地址，例如 127.0.0.1 或 0.0.0.0。
    #[arg(short = 'b', long = "bind-addr", env = "SMALUX_SERVER_BIND_ADDR", default_value = DEFAULT_BIND_ADDR)]
    pub bind_addr: String,

    /// HTTP 监听端口。
    #[arg(short = 'p', long = "bind-port", env = "SMALUX_SERVER_BIND_PORT", default_value_t = DEFAULT_BIND_PORT)]
    pub bind_port: u16,

    /// 数据库驱动，可选 sqlite、postgres 或 mysql。
    #[arg(short = 'd', long = "database-driver", env = "SMALUX_SERVER_DATABASE_DRIVER", default_value = DEFAULT_DATABASE_DRIVER)]
    pub database_driver: DatabaseDriverArg,

    /// PostgreSQL/MySQL 数据库主机。
    #[arg(long = "database-host", env = "SMALUX_SERVER_DATABASE_HOST")]
    pub database_host: Option<String>,

    /// PostgreSQL/MySQL 数据库端口。
    #[arg(long = "database-port", env = "SMALUX_SERVER_DATABASE_PORT")]
    pub database_port: Option<u16>,

    /// 数据库目标；SQLite 时表示文件路径，未传默认 smalux-server.db；PostgreSQL/MySQL 时表示数据库名，未传默认 smalux。
    #[arg(long = "database-name", env = "SMALUX_SERVER_DATABASE_NAME")]
    pub database_name: Option<String>,

    /// PostgreSQL/MySQL 用户名。
    #[arg(long = "database-user", env = "SMALUX_SERVER_DATABASE_USER")]
    pub database_user: Option<String>,

    /// PostgreSQL/MySQL 密码，Debug 输出会脱敏。
    #[arg(long = "database-password", env = "SMALUX_SERVER_DATABASE_PASSWORD", value_parser = parse_secret_arg)]
    pub database_password: Option<SecretArg>,

    /// 数据库连接 URL 的额外 query 参数，可重复传入，例如 `sslmode=require` 或 `charset=utf8mb4`。
    #[arg(long = "database-param", value_name = "KEY=VALUE", value_parser = parse_database_param)]
    pub database_params: Vec<DatabaseParamArg>,

    /// 是否由 server 托管前端静态资源；不传时默认只提供 API 和 agent 接入。
    #[arg(
        long = "serve-frontend",
        env = "SMALUX_SERVER_SERVE_FRONTEND",
        default_value_t = DEFAULT_SERVE_FRONTEND,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub serve_frontend: bool,

    /// 未编译内置前端资源时，server 托管前端所使用的静态资源目录。
    #[arg(long = "frontend-dir", env = "SMALUX_SERVER_FRONTEND_DIR", default_value = DEFAULT_FRONTEND_DIR)]
    pub frontend_dir: PathBuf,

    /// 是否为 React/Vite 这类 SPA 启用 index.html fallback，需要显式传入 true 或 false。
    #[arg(
        long = "frontend-spa-fallback",
        env = "SMALUX_SERVER_FRONTEND_SPA_FALLBACK",
        default_value_t = DEFAULT_FRONTEND_SPA_FALLBACK,
        action = clap::ArgAction::Set,
        value_parser = clap::value_parser!(bool)
    )]
    pub frontend_spa_fallback: bool,

    /// 滚动日志文件路径；日志级别仍然只读取 RUST_LOG。
    #[arg(long = "log-file", env = "SMALUX_SERVER_LOG_FILE", default_value = DEFAULT_LOG_FILE)]
    pub log_file: PathBuf,

    /// 滚动日志最多保留的文件数量。
    #[arg(short = 'L', long = "log-retention-files", env = "SMALUX_SERVER_LOG_RETENTION_FILES", default_value_t = DEFAULT_LOG_RETENTION_FILES)]
    pub log_retention_files: usize,

    /// 单个滚动日志文件最大大小，单位 MB。
    #[arg(long = "log-max-size-mb", env = "SMALUX_SERVER_LOG_MAX_SIZE_MB", default_value_t = DEFAULT_LOG_MAX_SIZE_MB)]
    pub log_max_size_mb: u64,
}

impl ServerArgs {
    /// 将 CLI 原始输入转换成 server 稳定运行配置。
    pub fn into_config(self) -> anyhow::Result<ServerConfig> {
        let bind_addr = self
            .bind_addr
            .parse::<IpAddr>()
            .with_context(|| format!("invalid bind address: {}", self.bind_addr))?;
        let database = self.database_config()?;

        Ok(ServerConfig {
            http: HttpConfig {
                bind_addr,
                bind_port: self.bind_port,
            },
            database,
            frontend: FrontendConfig {
                serve_frontend: self.serve_frontend,
                dir: self.frontend_dir,
                spa_fallback: self.frontend_spa_fallback,
            },
            log: LogConfig {
                file: self.log_file,
                retention_files: self.log_retention_files,
                max_size_mb: self.log_max_size_mb,
            },
        })
    }

    /// 构造数据库配置，并在这里补齐与驱动相关的默认值。
    fn database_config(&self) -> anyhow::Result<DatabaseConfig> {
        let driver = DatabaseDriver::from(self.database_driver);
        let name = optional_non_empty(self.database_name.clone(), "database name")?
            .unwrap_or_else(|| driver.default_database_name().to_string());
        let host = if driver.uses_network() {
            optional_non_empty(self.database_host.clone(), "database host")?
                .or_else(|| Some(DEFAULT_DATABASE_HOST.to_string()))
        } else {
            self.database_host.clone()
        };
        let port = if driver.uses_network() {
            self.database_port.or_else(|| driver.default_port())
        } else {
            self.database_port
        };
        let user = optional_non_empty(self.database_user.clone(), "database user")?;
        let password = self
            .database_password
            .clone()
            .map(|secret| SecretString::from(secret.0));
        let params = database_params_to_map(self.database_params.clone())?;

        Ok(DatabaseConfig {
            driver,
            name,
            host,
            port,
            user,
            password,
            params,
        })
    }
}

/// 将可选字符串中的空值提前转换成错误，避免空配置流入运行层。
fn optional_non_empty(value: Option<String>, field_name: &str) -> anyhow::Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };

    if value.trim().is_empty() {
        bail!("{field_name} cannot be empty");
    }

    Ok(Some(value))
}

/// 将 CLI 中可重复传入的数据库参数转换成稳定 map，并拒绝重复 key。
fn database_params_to_map(
    params: Vec<DatabaseParamArg>,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();

    for param in params {
        if map.insert(param.key.clone(), param.value).is_some() {
            return Err(anyhow!("duplicate database param: {}", param.key));
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_database_param_accepts_key_value() {
        let param = parse_database_param("sslmode=require").expect("database param should parse");

        assert_eq!(param.key, "sslmode");
        assert_eq!(param.value, "require");
    }

    #[test]
    fn parse_database_param_rejects_missing_separator() {
        let error = parse_database_param("sslmode").expect_err("missing separator should fail");

        assert!(error.contains("KEY=VALUE"));
    }

    #[test]
    fn parse_database_param_rejects_empty_key() {
        let error = parse_database_param("=require").expect_err("empty key should fail");

        assert!(error.contains("key cannot be empty"));
    }

    #[test]
    fn secret_arg_debug_redacts_value() {
        let secret = SecretArg("password".to_string());
        let debug = format!("{secret:?}");

        assert!(debug.contains("is_set"));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn parse_args_rejects_unknown_database_driver() {
        let error = ServerArgs::try_parse_from(["smalux-server", "-d", "oracle"])
            .expect_err("unknown database driver should fail");

        assert!(error.to_string().contains("oracle"));
    }

    #[test]
    fn parse_args_keeps_database_name_optional() {
        let args = ServerArgs::try_parse_from(["smalux-server"]).expect("args should parse");

        assert_eq!(args.database_name, None);
    }

    #[test]
    fn into_config_uses_sqlite_defaults() {
        let config = ServerArgs::try_parse_from(["smalux-server"])
            .unwrap()
            .into_config()
            .unwrap();

        assert_eq!(config.http.bind_addr.to_string(), "127.0.0.1");
        assert_eq!(config.http.bind_port, 3000);
        assert_eq!(config.http.socket_addr().to_string(), "127.0.0.1:3000");
        assert_eq!(config.database.driver, DatabaseDriver::Sqlite);
        assert_eq!(config.database.name, "smalux-server.db");
        assert_eq!(config.database.host, None);
        assert_eq!(config.database.port, None);
    }

    #[test]
    fn into_config_allows_bind_addr_and_port_overrides() {
        let config = ServerArgs::try_parse_from([
            "smalux-server",
            "--bind-addr",
            "0.0.0.0",
            "--bind-port",
            "8080",
        ])
        .unwrap()
        .into_config()
        .unwrap();

        assert_eq!(config.http.bind_addr.to_string(), "0.0.0.0");
        assert_eq!(config.http.bind_port, 8080);
        assert_eq!(config.http.socket_addr().to_string(), "0.0.0.0:8080");
    }

    #[test]
    fn into_config_rejects_invalid_bind_addr() {
        let error = ServerArgs::try_parse_from(["smalux-server", "--bind-addr", "localhost"])
            .unwrap()
            .into_config()
            .expect_err("hostname should not parse as bind IP");

        assert!(error.to_string().contains("invalid bind address"));
    }

    #[test]
    fn into_config_uses_network_database_defaults() {
        let config = ServerArgs::try_parse_from([
            "smalux-server",
            "-d",
            "postgres",
            "--database-user",
            "smalux",
        ])
        .unwrap()
        .into_config()
        .unwrap();

        assert_eq!(config.database.driver, DatabaseDriver::Postgres);
        assert_eq!(config.database.name, "smalux");
        assert_eq!(config.database.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(config.database.port, Some(5432));
    }

    #[test]
    fn into_config_rejects_empty_database_name() {
        let error = ServerArgs::try_parse_from(["smalux-server", "--database-name", " "])
            .unwrap()
            .into_config()
            .expect_err("empty database name should fail");

        assert!(error.to_string().contains("database name"));
    }

    #[test]
    fn into_config_rejects_duplicate_database_params() {
        let error = ServerArgs::try_parse_from([
            "smalux-server",
            "--database-param",
            "mode=rwc",
            "--database-param",
            "mode=ro",
        ])
        .unwrap()
        .into_config()
        .expect_err("duplicate database param should fail");

        assert!(error.to_string().contains("duplicate"));
    }
}
