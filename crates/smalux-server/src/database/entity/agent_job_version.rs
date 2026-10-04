//! Agent 远程 Job 的不可变业务版本历史。

use sea_orm::entity::prelude::*;

/// `agent_job_versions` 表的 SeaORM 实体。
///
/// 当前目录表只保存每个 Job 的最新定义，而报告可能在目录更新后才到达；历史表
/// 让 Server 能按 Agent、Job ID 和业务 revision 验证延迟报告的真实归属。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_job_versions")]
pub struct Model {
    /// Agent、Job 和 revision 拼接出的稳定主键。
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_key: String,
    /// 稳定 Agent ID。
    pub agent_id: String,
    /// Job UUID 的 16 字节表示。
    pub job_id: Vec<u8>,
    /// JobDefinition 的业务 revision。
    pub revision: i64,
    /// 固定 Task 或插件 Task 的稳定 kind。
    pub task_kind: String,
    /// 完整 JobDefinition 的 Protobuf 编码。
    pub definition: Vec<u8>,
    /// 首次见到该业务版本的 Unix 微秒时间。
    pub created_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
