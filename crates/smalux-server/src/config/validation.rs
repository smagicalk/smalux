//! server 配置校验模块，负责验证 HTTP、数据库、前端和日志运行配置。

use anyhow::{Result, bail};

use crate::config::model::{DatabaseDriver, ServerConfig};

/// 校验 server 稳定配置。
///
/// CLI 解析只能保证类型正确，不能表达跨字段约束；这些规则统一放在配置层，
/// 后续无论配置来自 CLI、配置文件还是数据库，都可以复用同一套校验逻辑。
pub fn validate_server_config(config: &ServerConfig) -> Result<()> {
    validate_http_config(config)?;
    validate_database_config(config)?;
    validate_frontend_config(config)?;
    validate_log_config(config)?;
    Ok(())
}

/// 校验 HTTP 监听配置。
fn validate_http_config(config: &ServerConfig) -> Result<()> {
    if config.http.bind_port == 0 {
        bail!("bind port must be greater than 0");
    }

    Ok(())
}

/// 校验数据库配置的跨字段约束。
fn validate_database_config(config: &ServerConfig) -> Result<()> {
    let database = &config.database;

    if database.name.trim().is_empty() {
        bail!("database name cannot be empty");
    }

    for key in database.params.keys() {
        if key.trim().is_empty() {
            bail!("database param key cannot be empty");
        }
    }

    match database.driver {
        DatabaseDriver::Sqlite => {
            if database.host.is_some() {
                bail!("database host is not used by sqlite");
            }
            if database.port.is_some() {
                bail!("database port is not used by sqlite");
            }
            if database.user.is_some() {
                bail!("database user is not used by sqlite");
            }
            if database.password.is_some() {
                bail!("database password is not used by sqlite");
            }
        }
        DatabaseDriver::Postgres | DatabaseDriver::Mysql => {
            if database
                .host
                .as_ref()
                .is_none_or(|host| host.trim().is_empty())
            {
                bail!("database host is required");
            }
            if database
                .user
                .as_ref()
                .is_none_or(|user| user.trim().is_empty())
            {
                bail!("database user is required");
            }
        }
    }

    Ok(())
}

/// 校验前端静态资源托管配置。
fn validate_frontend_config(config: &ServerConfig) -> Result<()> {
    if config.frontend.serve_frontend && config.frontend.dir.as_os_str().is_empty() {
        bail!("frontend dir cannot be empty when frontend hosting is enabled");
    }

    Ok(())
}

/// 校验日志滚动配置。
fn validate_log_config(config: &ServerConfig) -> Result<()> {
    if config.log.retention_files == 0 {
        bail!("log retention files must be greater than 0");
    }

    if config.log.max_size_mb == 0 {
        bail!("log max size must be greater than 0");
    }

    if config.log.file.as_os_str().is_empty() {
        bail!("log file cannot be empty");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, net::IpAddr, path::PathBuf};

    use secrecy::SecretString;

    use super::*;
    use crate::config::model::{
        DatabaseConfig, FrontendConfig, HttpConfig, LogConfig, ServerConfig,
    };

    fn base_config(database: DatabaseConfig) -> ServerConfig {
        ServerConfig {
            http: HttpConfig {
                bind_addr: "127.0.0.1".parse::<IpAddr>().unwrap(),
                bind_port: 3000,
            },
            database,
            frontend: FrontendConfig {
                serve_frontend: false,
                dir: PathBuf::from("apps/smalux-web/dist"),
                spa_fallback: true,
            },
            log: LogConfig {
                file: PathBuf::from("logs/smalux-server.log"),
                retention_files: 14,
                max_size_mb: 64,
            },
        }
    }

    fn sqlite_database() -> DatabaseConfig {
        DatabaseConfig {
            driver: DatabaseDriver::Sqlite,
            name: "smalux-server.db".to_string(),
            host: None,
            port: None,
            user: None,
            password: None,
            params: BTreeMap::new(),
        }
    }

    fn postgres_database() -> DatabaseConfig {
        DatabaseConfig {
            driver: DatabaseDriver::Postgres,
            name: "smalux".to_string(),
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            user: Some("smalux".to_string()),
            password: Some(SecretString::from("password")),
            params: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_accepts_sqlite_config() {
        let config = base_config(sqlite_database());

        validate_server_config(&config).expect("sqlite config should be valid");
    }

    #[test]
    fn validate_rejects_sqlite_network_fields() {
        let mut database = sqlite_database();
        database.host = Some("127.0.0.1".to_string());
        let config = base_config(database);

        let error = validate_server_config(&config).expect_err("sqlite host should fail");

        assert!(error.to_string().contains("not used by sqlite"));
    }

    #[test]
    fn validate_rejects_network_database_without_user() {
        let mut database = postgres_database();
        database.user = None;
        let config = base_config(database);

        let error = validate_server_config(&config).expect_err("missing user should fail");

        assert!(error.to_string().contains("user is required"));
    }

    #[test]
    fn validate_rejects_zero_bind_port() {
        let mut config = base_config(sqlite_database());
        config.http.bind_port = 0;

        let error = validate_server_config(&config).expect_err("zero bind port should fail");

        assert!(error.to_string().contains("bind port"));
    }

    #[test]
    fn validate_rejects_zero_log_retention_files() {
        let mut config = base_config(sqlite_database());
        config.log.retention_files = 0;

        let error = validate_server_config(&config).expect_err("zero retention should fail");

        assert!(error.to_string().contains("retention"));
    }

    #[test]
    fn validate_rejects_zero_log_max_size_mb() {
        let mut config = base_config(sqlite_database());
        config.log.max_size_mb = 0;

        let error = validate_server_config(&config).expect_err("zero log size should fail");

        assert!(error.to_string().contains("max size"));
    }
}
