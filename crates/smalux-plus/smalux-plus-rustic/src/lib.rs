//! Restic 扩展 crate 入口。
//!
//! 当前只预留独立 crate 边界，尚未接入备份实现。后续与 restic 相关的适配、配置和
//! 任务类型应保留在本 crate，避免把可选备份能力耦合进 agent 核心。
