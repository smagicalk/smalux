//! server 配置模型模块，负责保存数据库、HTTP、前端和日志等稳定运行配置。

use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, anyhow};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

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

/// HTTP 服务配置。
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// HTTP 监听 IP 地址。
    pub bind_addr: IpAddr,
    /// HTTP 监听端口。
    pub bind_port: u16,
}

/// 数据库连接配置。
///
/// 运行配置层直接按驱动拆成不同变体，避免 SQLite 还携带 host/user/password 这类无效字段。
#[derive(Clone, Debug)]
pub enum DatabaseConfig {
    /// SQLite 文件数据库或内存数据库。
    Sqlite {
        /// 数据库目标；文件模式是路径，内存模式固定为 `:memory:`。
        name: String,
        /// URL query 参数，使用 BTreeMap 保证输出顺序稳定。
        params: BTreeMap<String, String>,
    },
    /// PostgreSQL 数据库连接配置。
    Postgres {
        /// 数据库主机。
        host: String,
        /// 数据库端口。
        port: u16,
        /// 数据库名。
        name: String,
        /// 用户名。
        user: String,
        /// 密码；Debug 输出会由 secrecy 脱敏。
        password: Option<SecretString>,
        /// URL query 参数，使用 BTreeMap 保证输出顺序稳定。
        params: BTreeMap<String, String>,
    },
    /// MySQL 数据库连接配置。
    Mysql {
        /// 数据库主机。
        host: String,
        /// 数据库端口。
        port: u16,
        /// 数据库名。
        name: String,
        /// 用户名。
        user: String,
        /// 密码；Debug 输出会由 secrecy 脱敏。
        password: Option<SecretString>,
        /// URL query 参数，使用 BTreeMap 保证输出顺序稳定。
        params: BTreeMap<String, String>,
    },
}

/// server 支持的数据库驱动。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseDriver {
    /// SQLite 文件数据库。
    Sqlite,
    /// PostgreSQL 数据库。
    Postgres,
    /// MySQL 数据库。
    Mysql,
}

/// 前端静态资源托管配置。
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// 是否启用 server 静态资源托管。
    pub serve_frontend: bool,
    /// 未使用内置前端资源时的静态资源目录。
    pub dir: PathBuf,
    /// 是否启用 SPA fallback。
    pub spa_fallback: bool,
}

/// 日志输出配置。
#[derive(Clone, Debug)]
pub struct LogConfig {
    /// 滚动日志文件路径。
    pub file: PathBuf,
    /// 最多保留的滚动日志文件数量。
    pub retention_files: usize,
    /// 单个滚动日志文件最大大小，单位 MB。
    pub max_size_mb: u64,
}

impl DatabaseDriver {
    /// 返回数据库连接 URL 使用的协议名。
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }

    /// 返回 PostgreSQL/MySQL 默认端口；SQLite 没有网络端口。
    pub fn default_port(self) -> Option<u16> {
        match self {
            Self::Sqlite => None,
            Self::Postgres => Some(crate::config::defaults::DEFAULT_POSTGRES_PORT),
            Self::Mysql => Some(crate::config::defaults::DEFAULT_MYSQL_PORT),
        }
    }

    /// 返回数据库目标的默认名称。
    pub fn default_database_name(self) -> &'static str {
        match self {
            Self::Sqlite => crate::config::defaults::DEFAULT_SQLITE_DATABASE_NAME,
            Self::Postgres | Self::Mysql => crate::config::defaults::DEFAULT_SERVER_DATABASE_NAME,
        }
    }
}

impl HttpConfig {
    /// 合成最终用于 TCP bind 的 SocketAddr。
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_addr, self.bind_port)
    }
}

impl DatabaseConfig {
    /// 生成 SeaORM/SQLx 可以直接使用的数据库连接 URL。
    ///
    /// SQLite 的 `:memory:` 是 SQLx 特殊形式，不能用通用 URL builder 表示；
    /// PostgreSQL/MySQL 使用 `url` crate 处理用户名、密码和 query 编码。
    pub fn connection_url(&self) -> anyhow::Result<String> {
        match self {
            Self::Sqlite { name, params } => sqlite_connection_url(name, params),
            Self::Postgres {
                host,
                port,
                name,
                user,
                password,
                params,
            } => network_connection_url(
                DatabaseDriver::Postgres,
                host,
                *port,
                name,
                user,
                password.as_ref(),
                params,
                false,
            ),
            Self::Mysql {
                host,
                port,
                name,
                user,
                password,
                params,
            } => network_connection_url(
                DatabaseDriver::Mysql,
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
    pub fn redacted_connection_url(&self) -> anyhow::Result<String> {
        match self {
            Self::Sqlite { name, params } => sqlite_connection_url(name, params),
            Self::Postgres {
                host,
                port,
                name,
                user,
                password,
                params,
            } => network_connection_url(
                DatabaseDriver::Postgres,
                host,
                *port,
                name,
                user,
                password.as_ref(),
                params,
                true,
            ),
            Self::Mysql {
                host,
                port,
                name,
                user,
                password,
                params,
            } => network_connection_url(
                DatabaseDriver::Mysql,
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
}

/// 构造 SQLite 连接 URL。
fn sqlite_connection_url(name: &str, params: &BTreeMap<String, String>) -> anyhow::Result<String> {
    let mut url = if name == ":memory:" {
        "sqlite::memory:".to_string()
    } else {
        format!("sqlite://{name}")
    };

    append_query_params(&mut url, params);
    Ok(url)
}

/// 构造 PostgreSQL/MySQL 连接 URL。
fn network_connection_url(
    driver: DatabaseDriver,
    host: &str,
    port: u16,
    name: &str,
    user: &str,
    password: Option<&SecretString>,
    params: &BTreeMap<String, String>,
    redact_password: bool,
) -> anyhow::Result<String> {
    // 先构造最小合法 URL，再通过 url crate 设置用户、密码和 query，避免手写编码。
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

/// 为 `sqlite://...` 这种手工 URL 追加已编码 query 参数。
fn append_query_params(url: &mut String, params: &BTreeMap<String, String>) {
    if params.is_empty() {
        return;
    }

    let query = encode_query_params(params);
    url.push('?');
    url.push_str(&query);
}

/// 为 `url::Url` 追加 query 参数。
fn append_url_query_params(url: &mut Url, params: &BTreeMap<String, String>) {
    if params.is_empty() {
        return;
    }

    let mut pairs = url.query_pairs_mut();
    for (key, value) in params {
        pairs.append_pair(key, value);
    }
}

/// 使用标准 application/x-www-form-urlencoded 规则编码 query 参数。
fn encode_query_params(params: &BTreeMap<String, String>) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            config.connection_url().unwrap(),
            "sqlite://smalux-server.db"
        );
    }

    #[test]
    fn sqlite_connection_url_keeps_memory_database() {
        let config = DatabaseConfig::Sqlite {
            name: ":memory:".to_string(),
            params: BTreeMap::new(),
        };

        assert_eq!(config.connection_url().unwrap(), "sqlite::memory:");
    }

    #[test]
    fn sqlite_connection_url_adds_query_params() {
        let config = DatabaseConfig::Sqlite {
            name: "smalux-server.db".to_string(),
            params: params(&[("cache", "shared"), ("mode", "rwc")]),
        };

        assert_eq!(
            config.connection_url().unwrap(),
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
            config.connection_url().unwrap(),
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
            config.connection_url().unwrap(),
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

        let url = config.redacted_connection_url().unwrap();

        assert!(url.contains("***"));
        assert!(!url.contains("password"));
    }
}
