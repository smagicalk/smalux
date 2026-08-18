//! Server 数据库连接池和启动迁移生命周期。

use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use sea_orm_migration::MigratorTrait;

use super::{
    config::{DatabaseBackend, DatabaseConfig, DatabaseError},
    migration::Migrator,
};

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

    /// 获取当前连接的脱敏后端类型。
    pub fn backend(&self) -> DatabaseBackend {
        self.backend
    }

    /// 获取当前连接的脱敏后端日志标签。
    pub fn backend_label(&self) -> &'static str {
        self.backend.label()
    }
}
