//! Server 数据库连接池和启动迁移生命周期。

use sea_orm::{
    AccessMode, ConnectOptions, Database, DatabaseConnection, DatabaseTransaction, IsolationLevel,
    TransactionTrait,
};
use sea_orm_migration::MigratorTrait;

use super::{DatabaseError, migration::Migrator};
use crate::config::{DatabaseBackend, DatabaseConfig};

/// Server 进程持有的数据库句柄。
///
/// [`DatabaseConnection`] 是可克隆的连接池句柄，因此持久化适配器可以低成本共享它；
/// 迁移只在 Server 启动阶段执行一次，业务请求不会重复升级 schema。
#[derive(Clone)]
pub struct ServerDatabase {
    connection: DatabaseConnection,
    backend: DatabaseBackend,
}

impl ServerDatabase {
    /// 连接数据库并执行所有待应用迁移。
    pub async fn connect(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        config.validate()?;
        let (url, backend) = config.resolve_connection_url()?;
        tracing::info!(backend = backend.label(), "connecting Server database");

        let mut options = ConnectOptions::new(url);
        config.apply_connect_options(&mut options, backend)?;
        let connection = Database::connect(options).await?;
        let database = Self {
            connection,
            backend,
        };
        database.migrate().await?;

        tracing::info!(
            backend = database.backend.label(),
            "Server database migrations are up to date"
        );
        Ok(database)
    }

    /// 从环境变量读取配置、连接并执行迁移。
    pub async fn connect_from_env() -> Result<Self, DatabaseError> {
        Self::connect(DatabaseConfig::from_env()?).await
    }

    /// 幂等执行当前版本之后的所有迁移。
    pub async fn migrate(&self) -> Result<(), DatabaseError> {
        Migrator::up(&self.connection, None).await?;
        Ok(())
    }

    /// 获取连接池句柄，仅供本 crate 的数据库适配器和测试夹具使用。
    ///
    /// 业务模块应调用 [`ServerDatabase`] 提供的领域持久化方法，不能把此句柄作为新的
    /// 查询入口；收窄为 crate 内可见也避免其他 crate 直接依赖 SeaORM。
    pub(crate) fn connection(&self) -> &DatabaseConnection {
        &self.connection
    }

    /// 多条 SELECT 共用同一快照，避免目录版本与明细来自不同提交。
    /// SQLite 的事务在首次读取时固定快照；PG/MySQL 不能依赖默认 ReadCommitted。
    pub(crate) async fn begin_snapshot_read(&self) -> Result<DatabaseTransaction, DatabaseError> {
        Ok(match self.backend {
            DatabaseBackend::Sqlite => self.connection.begin().await?,
            DatabaseBackend::Postgres | DatabaseBackend::MySql => {
                self.connection
                    .begin_with_config(
                        Some(IsolationLevel::RepeatableRead),
                        Some(AccessMode::ReadOnly),
                    )
                    .await?
            }
        })
    }

    /// 获取当前连接的脱敏后端类型。
    pub fn backend(&self) -> DatabaseBackend {
        self.backend
    }

    /// 获取当前连接的脱敏后端日志标签。
    pub fn backend_label(&self) -> &'static str {
        self.backend.label()
    }
}

/// 并发回归使用文件数据库，避免 SQLite shared-cache 内存库的表级锁升级死锁。
#[cfg(test)]
pub(crate) async fn snapshot_test_database() -> (ServerDatabase, std::path::PathBuf) {
    use super::entity::agent;
    use sea_orm::{EntityTrait, Set};

    let directory = std::env::var_os("PI_SCRATCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("smalux-snapshot-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("snapshot.db");
    let mut url = url::Url::parse("sqlite:///").unwrap();
    url.set_path(
        &path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('%', "%25"),
    );
    url.set_query(Some("mode=rwc"));
    let url = url.to_string();
    #[cfg(windows)]
    let url = url.replacen("sqlite:///", "sqlite:", 1);
    let database = ServerDatabase::connect(DatabaseConfig::new(url))
        .await
        .unwrap();
    agent::Entity::insert(agent::ActiveModel {
        agent_id: Set("agent-a".to_owned()),
        name: Set("Agent A".to_owned()),
        public_key: Set(vec![1; 32]),
        status: Set("active".to_owned()),
        created_at: Set(1),
        updated_at: Set(1),
        revoked_at: Set(None),
    })
    .exec(database.connection())
    .await
    .unwrap();
    (database, directory)
}
