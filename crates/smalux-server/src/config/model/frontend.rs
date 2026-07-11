//! 前端配置模型，负责 site/admin 双槽位的稳定运行配置。

use std::path::PathBuf;

/// 前端静态资源托管配置。
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// 是否启用 server 静态资源托管。
    pub serve_frontend: bool,
    /// 站点前端槽位配置，负责 `/`。
    pub site: FrontendSlotConfig,
    /// 管理后台前端槽位配置，负责 `/admin`。
    pub admin: FrontendSlotConfig,
    /// 是否启用 SPA fallback。
    pub spa_fallback: bool,
}

/// 单个前端槽位的静态来源配置。
#[derive(Clone, Debug)]
pub struct FrontendSlotConfig {
    /// 槽位来源模式；后续数据库激活状态也应复用这个语义。
    pub mode: FrontendSlotMode,
    /// 目录来源；仅在 `Directory` 模式下有效。
    pub directory: Option<PathBuf>,
    /// 外部部署 URL；仅在 `External` 模式下有效。
    pub external_url: Option<String>,
}

/// 前端槽位来源模式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendSlotMode {
    /// 使用内置资源。
    Embedded,
    /// 使用本地目录。
    Directory,
    /// 前端由外部站点或 CDN 部署，server 不再托管。
    External,
}
