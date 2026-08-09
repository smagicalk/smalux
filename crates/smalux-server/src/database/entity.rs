//! SeaORM 实体集合。
//!
//! 实体采用 SeaORM 2 的 `#[sea_orm::model]` 格式：关系直接写在 `Model` 字段上，
//! 不再维护旧版的空 `Relation` 枚举或手写 `Related` 实现。实体只描述数据库表、
//! 字段和关系，不在这里放注册中心业务决策；查询、事务和授权策略由后续
//! repository/service 层组合。

pub mod agent;
pub mod agent_registration;
pub mod registration_token;
pub mod server_keyring;
