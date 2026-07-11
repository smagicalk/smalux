//! 存储模块入口，负责组织数据库实体、迁移、连接初始化和仓储访问层。

use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use sea_orm::{Database, DatabaseConnection};
use sea_orm_migration::MigratorTrait;
use secrecy::{ExposeSecret, SecretString};
use url::Url;

pub mod entity;
pub mod memory;
pub mod migration;
pub mod repository;

/// 生成数据库连接 URL。
pub fn database_connection_url(
    config: &crate::config::model::DatabaseConfig,
) -> anyhow::Result<String> {
    match config {
        crate::config::model::DatabaseConfig::Sqlite { name, params } => {
            sqlite_connection_url(name, params)
        }
        crate::config::model::DatabaseConfig::Postgres {
            host,
            port,
            name,
            user,
            password,
            params,
        } => network_connection_url(
            crate::config::model::DatabaseDriver::Postgres,
            host,
            *port,
            name,
            user,
            password.as_ref(),
            params,
            false,
        ),
        crate::config::model::DatabaseConfig::Mysql {
            host,
            port,
            name,
            user,
            password,
            params,
        } => network_connection_url(
            crate::config::model::DatabaseDriver::Mysql,
            host,
            *port,
            name,
            user,
            password.as_ref(),
            params,
            false,
        ),
    }
}

/// 生成脱敏后的数据库连接 URL，用于日志和调试输出。
pub fn redacted_database_connection_url(
    config: &crate::config::model::DatabaseConfig,
) -> anyhow::Result<String> {
    match config {
        crate::config::model::DatabaseConfig::Sqlite { name, params } => {
            sqlite_connection_url(name, params)
        }
        crate::config::model::DatabaseConfig::Postgres {
            host,
            port,
            name,
            user,
            password,
            params,
        } => network_connection_url(
            crate::config::model::DatabaseDriver::Postgres,
            host,
            *port,
            name,
            user,
            password.as_ref(),
            params,
            true,
        ),
        crate::config::model::DatabaseConfig::Mysql {
            host,
            port,
            name,
            user,
            password,
            params,
        } => network_connection_url(
            crate::config::model::DatabaseDriver::Mysql,
            host,
            *port,
            name,
            user,
            password.as_ref(),
            params,
            true,
        ),
    }
}

/// 初始化数据库连接并执行 migration。
///
/// 数据库连接、连接 URL 解析后的初始化和 schema 升级都应从这里进入，
/// 避免 `bootstrap.rs` 或 HTTP handler 直接依赖 SeaORM 和 migration 细节。
pub async fn init_database(
    config: &crate::config::model::DatabaseConfig,
) -> anyhow::Result<DatabaseConnection> {
    let connection_url = database_connection_url(config)?;
    let database = Database::connect(connection_url).await?;
    migration::Migrator::up(&database, None).await?;
    Ok(database)
}

fn sqlite_connection_url(name: &str, params: &BTreeMap<String, String>) -> anyhow::Result<String> {
    let mut url = if name == ":memory:" {
        "sqlite::memory:".to_string()
    } else {
        format!("sqlite://{name}")
    };

    append_query_params(&mut url, params);
    Ok(url)
}

fn network_connection_url(
    driver: crate::config::model::DatabaseDriver,
    host: &str,
    port: u16,
    name: &str,
    user: &str,
    password: Option<&SecretString>,
    params: &BTreeMap<String, String>,
    redact_password: bool,
) -> anyhow::Result<String> {
    let mut url = Url::parse(&format!("{}://{}:{}/{}", driver.scheme(), host, port, name))
        .with_context(|| format!("invalid {} database URL parts", driver.scheme()))?;

    url.set_username(user)
        .map_err(|_| anyhow!("database username cannot be applied to URL"))?;

    if let Some(password) = password {
        let value = if redact_password {
            "***"
        } else {
            password.expose_secret()
        };
        url.set_password(Some(value))
            .map_err(|_| anyhow!("database password cannot be applied to URL"))?;
    }

    append_url_query_params(&mut url, params);
    Ok(url.to_string())
}

fn append_query_params(url: &mut String, params: &BTreeMap<String, String>) {
    if params.is_empty() {
        return;
    }

    let query = encode_query_params(params);
    url.push('?');
    url.push_str(&query);
}

fn append_url_query_params(url: &mut Url, params: &BTreeMap<String, String>) {
    if params.is_empty() {
        return;
    }

    let mut pairs = url.query_pairs_mut();
    for (key, value) in params {
        pairs.append_pair(key, value);
    }
}

fn encode_query_params(params: &BTreeMap<String, String>) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::config::model::DatabaseConfig;

    use super::{database_connection_url, redacted_database_connection_url};

    fn params(values: &[(&str, &str)]) -> BTreeMap<String, String> {
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn sqlite_connection_url_uses_file_name() {
        let config = DatabaseConfig::Sqlite {
            name: "smalux-server.db".to_string(),
            params: BTreeMap::new(),
        };

        assert_eq!(
            database_connection_url(&config).unwrap(),
            "sqlite://smalux-server.db"
        );
    }

    #[test]
    fn sqlite_connection_url_keeps_memory_database() {
        let config = DatabaseConfig::Sqlite {
            name: ":memory:".to_string(),
            params: BTreeMap::new(),
        };

        assert_eq!(database_connection_url(&config).unwrap(), "sqlite::memory:");
    }

    #[test]
    fn sqlite_connection_url_adds_query_params() {
        let config = DatabaseConfig::Sqlite {
            name: "smalux-server.db".to_string(),
            params: params(&[("cache", "shared"), ("mode", "rwc")]),
        };

        assert_eq!(
            database_connection_url(&config).unwrap(),
            "sqlite://smalux-server.db?cache=shared&mode=rwc"
        );
    }

    #[test]
    fn postgres_connection_url_uses_default_port_and_encodes_secret_parts() {
        let config = DatabaseConfig::Postgres {
            host: "127.0.0.1".to_string(),
            port: 5432,
            name: "smalux".to_string(),
            user: "user@example".to_string(),
            password: Some("p@ ss".into()),
            params: params(&[("options", "--search_path=public"), ("sslmode", "require")]),
        };

        assert_eq!(
            database_connection_url(&config).unwrap(),
            "postgres://user%40example:p%40%20ss@127.0.0.1:5432/smalux?options=--search_path%3Dpublic&sslmode=require"
        );
    }

    #[test]
    fn mysql_connection_url_uses_default_port() {
        let config = DatabaseConfig::Mysql {
            host: "localhost".to_string(),
            port: 3306,
            name: "smalux".to_string(),
            user: "root".to_string(),
            password: None,
            params: params(&[("charset", "utf8mb4")]),
        };

        assert_eq!(
            database_connection_url(&config).unwrap(),
            "mysql://root@localhost:3306/smalux?charset=utf8mb4"
        );
    }

    #[test]
    fn redacted_connection_url_hides_password() {
        let config = DatabaseConfig::Postgres {
            host: "127.0.0.1".to_string(),
            port: 5432,
            name: "smalux".to_string(),
            user: "user".to_string(),
            password: Some("password".into()),
            params: BTreeMap::new(),
        };

        let url = redacted_database_connection_url(&config).unwrap();

        assert!(url.contains("***"));
        assert!(!url.contains("password"));
    }
}
