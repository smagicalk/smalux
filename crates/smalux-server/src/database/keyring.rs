//! Server Noise 密钥环的数据库读写。
//!
//! 这里保存的是 `ServerKeyRingSnapshot`，而不是把 `ServerKeyRing` 的内部状态暴露给
//! SeaORM。这样 Noise 协议层仍然只处理密钥状态机，数据库层只负责字节编码、校验和
//! 持久化；两层之间的边界也方便将来替换 SQLite、PostgreSQL 或外部密钥服务。

use std::time::{SystemTime, UNIX_EPOCH};

use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, DbErr, EntityTrait, QueryFilter};
use smalux_protocol::noise::{NoiseIdentity, RotationId, ServerKeyRing, ServerKeyRingSnapshot};

use super::{DatabaseError, ServerDatabase, entity::server_keyring};

/// 当前 Server 唯一的 Noise 密钥环记录 ID。
///
/// 这里不是“多个密钥环中的默认项”：当前数据模型只支持一个 Server 身份，因此使用
/// `_server` 明确表示内部单例记录。未来若支持多租户或多 Server 身份，应为读写方法新增
/// 显式的 keyring ID 参数，而不是重新引入隐式默认值。
pub const SERVER_KEYRING_SINGLETON_ID: &str = "_server";

/// 数据库中的 Server 密钥环完整记录。
///
/// `revision` 与 `snapshot` 必须一起读取；调用方不能只缓存 snapshot 后再用旧版本
/// 覆盖数据库。所有更新都应把这个版本传回 [`ServerDatabase::save_server_keyring_if_revision`]。
#[derive(Clone)]
pub struct ServerKeyRingRecord {
    /// 当前已持久化的 Noise 密钥状态。
    pub snapshot: ServerKeyRingSnapshot,
    /// 数据库乐观锁版本；首次原子创建为 `0`。
    pub revision: i64,
}

impl ServerDatabase {
    /// 从数据库读取 Server 密钥环快照。
    ///
    /// 返回 `Ok(None)` 表示迁移已完成但还没有密钥记录；调用方应在内存中生成新身份，
    /// 然后调用 [`Self::insert_server_keyring_if_absent`]。一旦记录存在，任何字段不完整
    /// 或轮换状态不一致都会返回错误，避免误用错误密钥。
    pub async fn load_server_keyring_record(
        &self,
    ) -> Result<Option<ServerKeyRingRecord>, DatabaseError> {
        let Some(model) = server_keyring::Entity::find_by_id(SERVER_KEYRING_SINGLETON_ID)
            .one(self.connection())
            .await?
        else {
            tracing::debug!("Server Noise keyring is not persisted yet");
            return Ok(None);
        };

        if model.revision < 0 {
            return Err(DatabaseError::InvalidServerKeyring(
                "revision must not be negative".to_owned(),
            ));
        }

        let current = restore_required_identity(
            "current",
            model.current_private_key,
            model.current_public_key,
        )?;
        let next =
            restore_optional_identity("next", model.next_private_key, model.next_public_key)?;
        let previous = restore_optional_identity(
            "previous",
            model.previous_private_key,
            model.previous_public_key,
        )?;
        let rotation_id = model
            .rotation_id
            .map(|bytes| RotationId::from_bytes(&bytes))
            .transpose()?;

        let snapshot = ServerKeyRingSnapshot {
            current,
            next,
            previous,
            rotation_id,
        };

        // from_snapshot 会验证 next 与 rotation_id 必须同时存在或同时缺失。
        ServerKeyRing::from_snapshot(snapshot.clone())?;
        tracing::debug!(
            key_id = ?snapshot.current.key_id(),
            has_next = snapshot.next.is_some(),
            has_previous = snapshot.previous.is_some(),
            "loaded Server Noise keyring from database"
        );
        Ok(Some(ServerKeyRingRecord {
            snapshot,
            revision: model.revision,
        }))
    }

    /// 只在记录不存在时原子创建 Server 密钥环。
    ///
    /// 多个 Server 同时首次启动时，数据库唯一键负责仲裁；输掉竞争的进程不会把自己
    /// 生成的身份覆盖到数据库，而是重新读取已经提交的赢家记录。
    pub async fn insert_server_keyring_if_absent(
        &self,
        snapshot: &ServerKeyRingSnapshot,
    ) -> Result<ServerKeyRingRecord, DatabaseError> {
        ServerKeyRing::from_snapshot(snapshot.clone())?;
        let now = unix_timestamp_micros()?;
        let (next_private_key, next_public_key) = export_optional_identity(&snapshot.next);
        let (previous_private_key, previous_public_key) =
            export_optional_identity(&snapshot.previous);

        let insert = server_keyring::Entity::insert(server_keyring::ActiveModel {
            keyring_id: sea_orm::ActiveValue::Set(SERVER_KEYRING_SINGLETON_ID.to_owned()),
            current_private_key: sea_orm::ActiveValue::Set(
                snapshot.current.export_private_key().as_bytes().to_vec(),
            ),
            current_public_key: sea_orm::ActiveValue::Set(
                snapshot.current.public_key().as_bytes().to_vec(),
            ),
            next_private_key: sea_orm::ActiveValue::Set(next_private_key),
            next_public_key: sea_orm::ActiveValue::Set(next_public_key),
            previous_private_key: sea_orm::ActiveValue::Set(previous_private_key),
            previous_public_key: sea_orm::ActiveValue::Set(previous_public_key),
            rotation_id: sea_orm::ActiveValue::Set(
                snapshot.rotation_id.map(|value| value.as_bytes().to_vec()),
            ),
            revision: sea_orm::ActiveValue::Set(0),
            created_at: sea_orm::ActiveValue::Set(now),
            updated_at: sea_orm::ActiveValue::Set(now),
        })
        .on_conflict(
            OnConflict::column(server_keyring::Column::KeyringId)
                .do_nothing()
                .to_owned(),
        )
        .exec(self.connection())
        .await;

        // SeaORM 在 SQLite 的 `ON CONFLICT DO NOTHING` 路径把“没有插入行”表示为
        // `RecordNotInserted`；这正是另一个进程已经创建记录的结果，不应被当成失败。
        if let Err(error) = insert
            && !matches!(error, DbErr::RecordNotInserted)
        {
            return Err(error.into());
        }

        self.load_server_keyring_record()
            .await?
            .ok_or(DatabaseError::MissingServerKeyring)
    }

    /// 使用 revision 乐观锁保存 Server 密钥环完整快照。
    ///
    /// SQL 更新同时匹配 `keyring_id` 和 `expected_revision`，并在同一条语句中把版本
    /// 加一。没有匹配行时不会写入任何字段，调用方应重新加载数据库记录并决定重试。
    pub async fn save_server_keyring_if_revision(
        &self,
        expected_revision: i64,
        snapshot: &ServerKeyRingSnapshot,
    ) -> Result<ServerKeyRingRecord, DatabaseError> {
        ServerKeyRing::from_snapshot(snapshot.clone())?;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or(DatabaseError::ServerKeyringRevisionOverflow)?;
        let now = unix_timestamp_micros()?;
        let (next_private_key, next_public_key) = export_optional_identity(&snapshot.next);
        let (previous_private_key, previous_public_key) =
            export_optional_identity(&snapshot.previous);
        let rotation_id = snapshot.rotation_id.map(|value| value.as_bytes().to_vec());

        let update = server_keyring::Entity::update_many()
            .col_expr(
                server_keyring::Column::CurrentPrivateKey,
                Expr::value(snapshot.current.export_private_key().as_bytes().to_vec()),
            )
            .col_expr(
                server_keyring::Column::CurrentPublicKey,
                Expr::value(snapshot.current.public_key().as_bytes().to_vec()),
            )
            .col_expr(
                server_keyring::Column::NextPrivateKey,
                Expr::value(next_private_key),
            )
            .col_expr(
                server_keyring::Column::NextPublicKey,
                Expr::value(next_public_key),
            )
            .col_expr(
                server_keyring::Column::PreviousPrivateKey,
                Expr::value(previous_private_key),
            )
            .col_expr(
                server_keyring::Column::PreviousPublicKey,
                Expr::value(previous_public_key),
            )
            .col_expr(server_keyring::Column::RotationId, Expr::value(rotation_id))
            .col_expr(server_keyring::Column::Revision, Expr::value(next_revision))
            .col_expr(server_keyring::Column::UpdatedAt, Expr::value(now))
            .filter(server_keyring::Column::KeyringId.eq(SERVER_KEYRING_SINGLETON_ID))
            .filter(server_keyring::Column::Revision.eq(expected_revision))
            .exec(self.connection())
            .await?;

        if update.rows_affected != 1 {
            let actual = self
                .load_server_keyring_record()
                .await?
                .map(|record| record.revision);
            return Err(DatabaseError::ServerKeyringRevisionConflict {
                expected: expected_revision,
                actual,
            });
        }

        self.load_server_keyring_record()
            .await?
            .ok_or(DatabaseError::MissingServerKeyring)
    }
}

/// 恢复必须存在的身份；current 缺少任一部分都视为数据库损坏。
fn restore_required_identity(
    name: &str,
    private_key: Vec<u8>,
    public_key: Vec<u8>,
) -> Result<NoiseIdentity, DatabaseError> {
    NoiseIdentity::from_parts(&private_key, &public_key).map_err(|error| {
        DatabaseError::InvalidServerKeyring(format!("{name} identity is invalid: {error}"))
    })
}

/// 恢复可选身份；私钥和公钥必须同时存在或同时为空。
fn restore_optional_identity(
    name: &str,
    private_key: Option<Vec<u8>>,
    public_key: Option<Vec<u8>>,
) -> Result<Option<NoiseIdentity>, DatabaseError> {
    match (private_key, public_key) {
        (None, None) => Ok(None),
        (Some(private_key), Some(public_key)) => {
            NoiseIdentity::from_parts(&private_key, &public_key)
                .map(Some)
                .map_err(|error| {
                    DatabaseError::InvalidServerKeyring(format!(
                        "{name} identity is invalid: {error}"
                    ))
                })
        }
        _ => Err(DatabaseError::InvalidServerKeyring(format!(
            "{name} private/public key must be both present or both absent"
        ))),
    }
}

/// 将一个可选身份拆成数据库的两个可空二进制字段。
fn export_optional_identity(
    identity: &Option<NoiseIdentity>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    identity
        .as_ref()
        .map(|identity| {
            (
                Some(identity.export_private_key().as_bytes().to_vec()),
                Some(identity.public_key().as_bytes().to_vec()),
            )
        })
        .unwrap_or((None, None))
}

/// 统一生成数据库记录时间，避免不同数据库使用不同的默认时间函数。
fn unix_timestamp_micros() -> Result<i64, DatabaseError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            DatabaseError::InvalidServerKeyring(format!(
                "system clock is before Unix epoch: {error}"
            ))
        })?;
    duration.as_micros().try_into().map_err(|_| {
        DatabaseError::InvalidServerKeyring("Unix timestamp does not fit in i64".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::{SERVER_KEYRING_SINGLETON_ID, ServerDatabase};
    use crate::database::{DatabaseConfig, DatabaseError};
    use sea_orm::EntityTrait;
    use smalux_protocol::noise::{NoiseIdentity, ServerKeyRing};

    async fn test_database() -> ServerDatabase {
        ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("database should connect")
    }

    #[tokio::test]
    async fn server_keyring_round_trips_current_identity() {
        let database = test_database().await;
        let identity = NoiseIdentity::generate().expect("identity should generate");
        let keyring = ServerKeyRing::new(identity.clone());

        database
            .insert_server_keyring_if_absent(&keyring.snapshot())
            .await
            .expect("keyring should be created");
        let record = database
            .load_server_keyring_record()
            .await
            .expect("keyring should load")
            .expect("keyring row should exist");
        let snapshot = record.snapshot;

        assert_eq!(snapshot.current.public_key(), identity.public_key());
        assert_eq!(
            snapshot.current.export_private_key().as_bytes(),
            identity.export_private_key().as_bytes()
        );
        assert!(snapshot.next.is_none());
        assert!(snapshot.previous.is_none());
        assert!(snapshot.rotation_id.is_none());
        let persisted = crate::database::entity::server_keyring::Entity::find_by_id(
            SERVER_KEYRING_SINGLETON_ID,
        )
        .one(database.connection())
        .await
        .expect("row lookup should work")
        .expect("singleton keyring row should exist");
        // 持久化值以 `_` 表明它是内部单例记录，不能退回容易误解的 `default`。
        assert_eq!(persisted.keyring_id, "_server");
    }

    #[tokio::test]
    async fn server_keyring_round_trips_pending_rotation() {
        let database = test_database().await;
        let mut keyring = ServerKeyRing::new(
            NoiseIdentity::generate().expect("current identity should generate"),
        );
        keyring.prepare_rotation().expect("rotation should prepare");
        database
            .insert_server_keyring_if_absent(&keyring.snapshot())
            .await
            .expect("pending keyring should be created");

        let record = database
            .load_server_keyring_record()
            .await
            .expect("pending keyring should load")
            .expect("keyring row should exist");
        let restored =
            ServerKeyRing::from_snapshot(record.snapshot).expect("snapshot should validate");
        assert_eq!(restored.active_keys().len(), 2);
    }

    #[tokio::test]
    async fn server_keyring_cas_increments_revision() {
        let database = test_database().await;
        let keyring =
            ServerKeyRing::new(NoiseIdentity::generate().expect("identity should generate"));
        let created = database
            .insert_server_keyring_if_absent(&keyring.snapshot())
            .await
            .expect("keyring should be created");
        assert_eq!(created.revision, 0);

        let mut next = ServerKeyRing::from_snapshot(created.snapshot.clone())
            .expect("created snapshot should restore");
        next.prepare_rotation().expect("rotation should prepare");
        let updated = database
            .save_server_keyring_if_revision(created.revision, &next.snapshot())
            .await
            .expect("matching revision should update");
        assert_eq!(updated.revision, 1);
        assert!(updated.snapshot.next.is_some());
    }

    #[tokio::test]
    async fn stale_server_keyring_revision_is_rejected_without_overwrite() {
        let database = test_database().await;
        let first =
            ServerKeyRing::new(NoiseIdentity::generate().expect("identity should generate"));
        let created = database
            .insert_server_keyring_if_absent(&first.snapshot())
            .await
            .expect("keyring should be created");

        let second =
            ServerKeyRing::new(NoiseIdentity::generate().expect("identity should generate"));
        let committed = database
            .save_server_keyring_if_revision(created.revision, &second.snapshot())
            .await
            .expect("first CAS should succeed");
        assert_eq!(committed.revision, 1);

        let stale = match database
            .save_server_keyring_if_revision(created.revision, &first.snapshot())
            .await
        {
            Ok(_) => panic!("stale revision must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            stale,
            DatabaseError::ServerKeyringRevisionConflict {
                expected: 0,
                actual: Some(1)
            }
        ));
        let current = database
            .load_server_keyring_record()
            .await
            .expect("keyring should load")
            .expect("keyring row should exist");
        assert_eq!(current.revision, 1);
        assert_eq!(
            current.snapshot.current.public_key(),
            second.snapshot().current.public_key()
        );
    }

    #[tokio::test]
    async fn atomic_keyring_insert_keeps_the_first_identity() {
        let database = test_database().await;
        let first =
            ServerKeyRing::new(NoiseIdentity::generate().expect("first identity should generate"));
        let second =
            ServerKeyRing::new(NoiseIdentity::generate().expect("second identity should generate"));

        let first_record = database
            .insert_server_keyring_if_absent(&first.snapshot())
            .await
            .expect("first insert should create the row");
        let second_record = database
            .insert_server_keyring_if_absent(&second.snapshot())
            .await
            .expect("second insert should read the existing row");

        assert_eq!(first_record.revision, 0);
        assert_eq!(second_record.revision, 0);
        assert_eq!(
            second_record.snapshot.current.public_key(),
            first.snapshot().current.public_key()
        );
    }
}
