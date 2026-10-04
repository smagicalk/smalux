//! Plus 公共错误类型。
//!
//! 错误枚举覆盖 Worker IPC、插件清单和任务执行的稳定边界。

/// 插件生命周期或执行失败。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PluginError {
    /// 插件清单无法读取或解析。
    #[error("invalid plugin manifest: {0}")]
    InvalidManifest(String),
    /// 插件身份与请求不一致。
    #[error("plugin identity mismatch: expected {expected}, actual {actual}")]
    IdentityMismatch { expected: String, actual: String },
    /// Worker IPC、生命周期或插件执行失败。
    #[error("Plus Worker failed: {0}")]
    ExecutionFailed(String),
}
