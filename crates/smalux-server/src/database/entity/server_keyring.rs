//! Server Noise 长期密钥环的持久化模型。
//!
//! 表只允许一个逻辑记录（`keyring_id = "default"`）。记录保存 current、next 和
//! previous 三个身份的完整公私钥，以及正在进行的轮换事务 ID。私钥不会写入日志；
//! 生产部署仍应配合数据库文件权限、磁盘加密或外部密钥管理服务保护静态数据。

use sea_orm::entity::prelude::*;

/// `server_keyrings` 表的 SeaORM 实体。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "server_keyrings")]
pub struct Model {
    /// 逻辑密钥环 ID；当前固定为 `default`，便于未来扩展多租户密钥环。
    #[sea_orm(primary_key, auto_increment = false)]
    pub keyring_id: String,
    /// 当前 Server Noise 身份的 32 字节私钥。
    pub current_private_key: Vec<u8>,
    /// 当前 Server Noise 身份的 32 字节公钥。
    pub current_public_key: Vec<u8>,
    /// 轮换期间待提升身份的私钥；没有轮换时为空。
    pub next_private_key: Option<Vec<u8>>,
    /// 轮换期间待提升身份的公钥；没有轮换时为空。
    pub next_public_key: Option<Vec<u8>>,
    /// promote 后为旧身份保留的过渡私钥；观察期结束后清空。
    pub previous_private_key: Option<Vec<u8>>,
    /// promote 后为旧身份保留的过渡公钥；观察期结束后清空。
    pub previous_public_key: Option<Vec<u8>>,
    /// 与 next 对应的 16 字节轮换事务 ID；没有 next 时为空。
    pub rotation_id: Option<Vec<u8>>,
    /// 乐观并发控制版本；每次成功写入快照严格递增一次。
    pub revision: i64,
    /// 首次创建时间，Unix 微秒。
    pub created_at: i64,
    /// 最近一次快照更新时间，Unix 微秒。
    pub updated_at: i64,
}

// SeaORM 要求实体的 ActiveModel 实现默认行为接口。
impl ActiveModelBehavior for ActiveModel {}
