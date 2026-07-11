//! 数据库实体模块，负责 SeaORM entity、关系和数据库字段映射定义。
//!
//! 前端双槽位模型接数据库后，建议在这里增加：
//! - frontend_slot_state
//! - frontend_bundle
//! - frontend_bundle_version 或 frontend_activation_log
//!
//! 当前先不设计表结构，只把职责说明固定下来，避免后续把前端状态直接塞进 agent 表或杂项表。
