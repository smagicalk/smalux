//! 前端槽位 Web 服务模块，负责站点与管理后台前端来源的业务编排。
//!
//! 当前只定义职责边界，不落数据库实现：
//! - 读取 `site` / `admin` 当前激活来源
//! - 切换 active bundle
//! - 回滚到 previous bundle
//! - 重置到 embedded 来源
//!
//! 后续数据库设计完成后，这里应成为 HTTP API、CLI 强制恢复和路由层之间的唯一编排入口，
//! 避免把“前端槽位状态变更”散落到 `http/`、`storage/` 或 `bootstrap.rs`。
