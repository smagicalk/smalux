//! 数据库配置模型，负责稳定数据库连接参数和驱动级默认值。

use std::collections::BTreeMap;

use secrecy::SecretString;

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
