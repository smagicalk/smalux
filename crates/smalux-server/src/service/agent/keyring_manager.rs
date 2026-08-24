//! Server Noise 密钥环运行时管理器。
//!
//! `smalux-protocol` 中的 [`smalux_protocol::noise::ServerKeyRing`] 仍然是纯状态机，
//! 不知道数据库、并发和多进程。这个模块把三件运行时工作集中到一个边界：
//!
//! 1. 启动时从数据库读取，缺失时使用数据库唯一键原子创建；
//! 2. 轮换先在候选状态上计算，再用 revision CAS 持久化，成功后才替换内存句柄；
//! 3. 周期性读取更高 revision，使同一个数据库上的其他 Server 进程最终收敛。
//!
//! 握手只需要调用
//! [`ServerKeyRingManager::current_keyring`](crate::service::agent::keyring_manager::ServerKeyRingManager::current_keyring)
//! 取得一个 `Arc`。读句柄
//! 一旦复制出来就不再持有管理器锁，因此数据库轮换和同步不会把 I/O 等待带入握手。

use std::sync::{Arc, RwLock};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::database::{DatabaseError, ServerDatabase};
use smalux_protocol::noise::{NoiseError, NoiseIdentity, ServerKeyRing, ServerRotationPrepared};

/// 跨 Server 进程读取密钥环 revision 的默认周期。
///
/// 轮换不是高频路径；五秒可以让新身份在短时间内传播到其他实例，同时不会为每个
/// Agent 会话创建数据库查询。未来可把它提升为 ServerConfig 的运行参数。
pub const DEFAULT_SERVER_KEYRING_SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// Server 密钥环管理器的稳定错误边界。
#[derive(Debug, Error)]
pub enum ServerKeyRingManagerError {
    /// 数据库读取、CAS 写入或迁移错误。
    #[error("Server keyring database operation failed: {0}")]
    Database(#[from] DatabaseError),
    /// Noise 密钥状态机拒绝了候选状态。
    #[error("Server Noise keyring operation failed: {0}")]
    Noise(#[from] NoiseError),
    /// 进程内状态锁被一个 panic 线程污染；继续运行可能使用不一致密钥。
    #[error("Server keyring manager state lock is poisoned")]
    StatePoisoned,
}

/// 进程内可替换的密钥环句柄和其数据库 revision。
struct RuntimeKeyRingState {
    /// 握手读取的 immutable `Arc`；轮换成功后整体替换，不修改旧对象。
    keyring: Arc<ServerKeyRing>,
    /// 与 `keyring` 一一对应的数据库版本。
    revision: i64,
}

/// Server Noise 密钥环的数据库一致性和进程内并发管理器。
#[derive(Clone)]
pub struct ServerKeyRingManager {
    /// 所有进程共享的持久化来源。
    database: Arc<ServerDatabase>,
    /// 读路径只复制 `Arc`，不克隆私钥状态。
    state: Arc<RwLock<RuntimeKeyRingState>>,
    /// 串行化本进程的轮换和后台同步，避免两个候选快照使用同一 revision。
    mutation_lock: Arc<Mutex<()>>,
}

impl ServerKeyRingManager {
    /// 从数据库加载密钥环；没有记录时使用数据库原子插入初始化。
    pub async fn load_or_create(
        database: Arc<ServerDatabase>,
    ) -> Result<Self, ServerKeyRingManagerError> {
        // 先读可以避免正常重启重复生成身份。
        let record = match database.load_server_keyring_record().await? {
            Some(record) => record,
            None => {
                // 多个进程可能同时走到这里；每个进程都可生成候选，但只有数据库
                // 唯一键允许一个候选插入，insert 方法随后读取最终赢家。
                let identity = NoiseIdentity::generate()?;
                let candidate = ServerKeyRing::new(identity);
                database
                    .insert_server_keyring_if_absent(&candidate.snapshot())
                    .await?
            }
        };

        // 数据库层已经校验字段长度和配对关系；协议层再次恢复，确保状态机不变式成立。
        let keyring = Arc::new(ServerKeyRing::from_snapshot(record.snapshot.clone())?);
        tracing::info!(
            revision = record.revision,
            active_keys = keyring.active_keys().len(),
            "Server Noise keyring manager initialized"
        );

        Ok(Self {
            database,
            state: Arc::new(RwLock::new(RuntimeKeyRingState {
                keyring,
                revision: record.revision,
            })),
            mutation_lock: Arc::new(Mutex::new(())),
        })
    }

    /// 取得当前密钥环的 `Arc`，供一次握手使用。
    ///
    /// 返回后调用方可以跨越异步等待安全使用旧句柄；后续轮换只会替换管理器中的
    /// 新连接句柄，不会修改已经开始的握手所看到的对象。
    pub fn current_keyring(&self) -> Result<Arc<ServerKeyRing>, ServerKeyRingManagerError> {
        let state = self
            .state
            .read()
            .map_err(|_| ServerKeyRingManagerError::StatePoisoned)?;
        Ok(Arc::clone(&state.keyring))
    }

    /// 返回当前活跃身份数量，主要用于路由装配和诊断日志。
    pub fn active_key_count(&self) -> Result<usize, ServerKeyRingManagerError> {
        Ok(self.current_keyring()?.active_keys().len())
    }

    /// 返回当前进程已经应用的数据库 revision。
    pub fn revision(&self) -> Result<i64, ServerKeyRingManagerError> {
        let state = self
            .state
            .read()
            .map_err(|_| ServerKeyRingManagerError::StatePoisoned)?;
        Ok(state.revision)
    }

    /// 读取数据库的最新 revision，并在它更高时替换进程内句柄。
    ///
    /// 返回 `true` 表示本次确实应用了其他进程的更新；返回 `false` 表示数据库没有
    /// 更新，或者本进程已经领先。revision 不允许倒退，防止数据库恢复旧备份时把
    /// 正在服务的新身份悄悄替换掉。
    pub async fn sync_once(&self) -> Result<bool, ServerKeyRingManagerError> {
        // 后台同步与本进程轮换共享同一把异步锁，CAS 成功后安装句柄不会被同步任务
        // 立即覆盖，也不会出现 revision 与 Arc 不匹配的短暂窗口。
        let _mutation_guard = self.mutation_lock.lock().await;
        let Some(record) = self.database.load_server_keyring_record().await? else {
            return Err(DatabaseError::MissingServerKeyring.into());
        };

        let local_revision = self.revision()?;
        if record.revision <= local_revision {
            if record.revision < local_revision {
                tracing::warn!(
                    local_revision,
                    database_revision = record.revision,
                    "ignored older Server Noise keyring revision"
                );
            }
            return Ok(false);
        }

        let keyring = ServerKeyRing::from_snapshot(record.snapshot.clone())?;
        self.install(RuntimeKeyRingState {
            keyring: Arc::new(keyring),
            revision: record.revision,
        })?;
        tracing::info!(
            revision = record.revision,
            "applied Server Noise keyring revision from database"
        );
        Ok(true)
    }

    /// 启动跨实例同步任务。
    ///
    /// 任务只持有管理器的 `Weak` 引用；应用状态释放后，任务会在下一次 tick 结束，
    /// 不会因为后台轮询把数据库和密钥环永久泄漏。调用方可以保存返回的句柄，在测试
    /// 或优雅关闭时显式 `abort`。
    pub fn start_sync_task(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        self.start_sync_task_with_shutdown(interval, CancellationToken::new())
    }

    /// 启动带进程级关闭通知的跨实例同步任务。
    pub fn start_sync_task_with_shutdown(
        self: &Arc<Self>,
        interval: Duration,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let manager = Arc::downgrade(self);
        tokio::spawn(async move {
            if interval.is_zero() {
                tracing::warn!("Server keyring sync interval is zero; background sync is disabled");
                return;
            }

            let mut ticker = tokio::time::interval(interval);
            // interval 的第一次 tick 是立即触发的；先消费它，再按配置周期读取数据库。
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::debug!("Server keyring sync task cancelled before first tick");
                    return;
                }
                _ = ticker.tick() => {}
            }
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::debug!("Server keyring sync task cancelled");
                        return;
                    }
                    _ = ticker.tick() => {}
                }
                let Some(manager) = manager.upgrade() else {
                    tracing::debug!("Server keyring manager dropped; stopping sync task");
                    return;
                };
                match manager.sync_once().await {
                    Ok(true) => tracing::debug!("Server keyring background sync applied an update"),
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "Server keyring background sync failed")
                    }
                }
            }
        })
    }

    /// 生成并持久化下一把 Server 身份。
    pub async fn prepare_rotation(
        &self,
    ) -> Result<ServerRotationPrepared, ServerKeyRingManagerError> {
        self.mutate(|keyring| keyring.prepare_rotation()).await
    }

    /// 将 next 身份提升为 current，并保留 previous 过渡身份。
    pub async fn promote_next(
        &self,
        rotation_id: smalux_protocol::noise::RotationId,
    ) -> Result<(), ServerKeyRingManagerError> {
        self.mutate(|keyring| keyring.promote_next(rotation_id))
            .await
    }

    /// 取消尚未生效的 next 身份。
    pub async fn cancel_rotation(&self) -> Result<(), ServerKeyRingManagerError> {
        self.mutate(ServerKeyRing::cancel_rotation).await
    }

    /// 观察期结束后移除 previous 身份。
    pub async fn retire_previous(&self) -> Result<(), ServerKeyRingManagerError> {
        self.mutate(ServerKeyRing::retire_previous).await
    }

    /// 在候选状态上执行一次协议状态机变更，并用 CAS 提交。
    async fn mutate<T, F>(&self, operation: F) -> Result<T, ServerKeyRingManagerError>
    where
        F: FnOnce(&mut ServerKeyRing) -> Result<T, NoiseError>,
    {
        let _mutation_guard = self.mutation_lock.lock().await;
        let (snapshot, revision) = {
            let state = self
                .state
                .read()
                .map_err(|_| ServerKeyRingManagerError::StatePoisoned)?;
            (state.keyring.snapshot(), state.revision)
        };

        // 候选对象独立于当前握手句柄；即使数据库写入失败，也不会污染正在运行的状态。
        let mut candidate = ServerKeyRing::from_snapshot(snapshot)?;
        let result = operation(&mut candidate)?;
        let record = self
            .database
            .save_server_keyring_if_revision(revision, &candidate.snapshot())
            .await?;

        self.install(RuntimeKeyRingState {
            keyring: Arc::new(candidate),
            revision: record.revision,
        })?;
        tracing::info!(
            revision = record.revision,
            "installed Server Noise keyring mutation"
        );
        Ok(result)
    }

    /// 在已经持有 mutation lock 的操作中安装数据库确认过的运行态状态。
    fn install(&self, state: RuntimeKeyRingState) -> Result<(), ServerKeyRingManagerError> {
        let mut current = self
            .state
            .write()
            .map_err(|_| ServerKeyRingManagerError::StatePoisoned)?;
        *current = state;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::database::{DatabaseConfig, ServerDatabase};

    use super::ServerKeyRingManager;

    async fn shared_database() -> Arc<ServerDatabase> {
        Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .expect("database should connect"),
        )
    }

    #[tokio::test]
    async fn managers_load_the_same_atomically_created_identity() {
        let database = shared_database().await;
        let first = ServerKeyRingManager::load_or_create(Arc::clone(&database))
            .await
            .expect("first manager should initialize");
        let second = ServerKeyRingManager::load_or_create(database)
            .await
            .expect("second manager should load");

        assert_eq!(first.revision().expect("revision should read"), 0);
        assert_eq!(second.revision().expect("revision should read"), 0);
        assert_eq!(
            first
                .current_keyring()
                .expect("keyring should read")
                .active_keys()[0]
                .public_key(),
            second
                .current_keyring()
                .expect("keyring should read")
                .active_keys()[0]
                .public_key()
        );
    }

    #[tokio::test]
    async fn concurrent_manager_startup_converges_on_one_identity() {
        let database = shared_database().await;
        let (first, second) = tokio::join!(
            ServerKeyRingManager::load_or_create(Arc::clone(&database)),
            ServerKeyRingManager::load_or_create(database),
        );
        let first = first.expect("first concurrent manager should initialize");
        let second = second.expect("second concurrent manager should initialize");

        assert_eq!(first.revision().expect("revision should read"), 0);
        assert_eq!(second.revision().expect("revision should read"), 0);
        assert_eq!(
            first
                .current_keyring()
                .expect("first keyring should read")
                .active_keys()[0]
                .public_key(),
            second
                .current_keyring()
                .expect("second keyring should read")
                .active_keys()[0]
                .public_key()
        );
    }

    #[tokio::test]
    async fn rotation_persists_before_manager_installs_new_handle() {
        let database = shared_database().await;
        let manager = ServerKeyRingManager::load_or_create(database.clone())
            .await
            .expect("manager should initialize");
        let prepared = manager
            .prepare_rotation()
            .await
            .expect("rotation should prepare");

        assert_eq!(manager.revision().expect("revision should read"), 1);
        assert_eq!(manager.active_key_count().expect("keys should read"), 2);
        let persisted = database
            .load_server_keyring_record()
            .await
            .expect("keyring should load")
            .expect("keyring row should exist");
        assert_eq!(persisted.revision, 1);
        assert_eq!(
            persisted
                .snapshot
                .next
                .expect("next key should exist")
                .public_key(),
            prepared.next_identity.public_key()
        );
    }

    #[tokio::test]
    async fn sync_once_applies_a_revision_committed_by_another_manager() {
        let database = shared_database().await;
        let first = Arc::new(
            ServerKeyRingManager::load_or_create(Arc::clone(&database))
                .await
                .expect("first manager should initialize"),
        );
        let second = ServerKeyRingManager::load_or_create(Arc::clone(&database))
            .await
            .expect("second manager should initialize");
        first
            .prepare_rotation()
            .await
            .expect("first manager should commit a rotation");
        assert!(second.sync_once().await.expect("sync should succeed"));
        assert_eq!(second.revision().expect("revision should read"), 1);
        assert_eq!(second.active_key_count().expect("keys should read"), 2);
        assert!(
            second
                .current_keyring()
                .expect("keyring should read")
                .active_keys()
                .get(1)
                .is_some()
        );
        assert!(
            !second
                .sync_once()
                .await
                .expect("second sync should be a no-op")
        );
    }

    #[tokio::test]
    async fn stale_manager_rotation_is_rejected_by_database_cas() {
        let database = shared_database().await;
        let first = ServerKeyRingManager::load_or_create(Arc::clone(&database))
            .await
            .expect("first manager should initialize");
        let second = ServerKeyRingManager::load_or_create(Arc::clone(&database))
            .await
            .expect("second manager should initialize");

        first
            .prepare_rotation()
            .await
            .expect("first manager should commit a rotation");
        let error = match second.prepare_rotation().await {
            Ok(_) => panic!("stale manager must not overwrite the database"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("revision conflict"));
        assert_eq!(second.revision().expect("local revision should remain"), 0);
    }
}
