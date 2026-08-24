//! Plus 公共错误类型。
//!
//! 错误枚举先覆盖设计阶段已经确定的边界。具体的 `libloading`、TUF 或 protobuf
//! 错误会在对应实现 crate 中转换为这些稳定错误，避免把实现库类型暴露给调用方。

/// 插件生命周期或执行失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginError {
    /// 插件清单无法读取或解析。
    InvalidManifest(String),
    /// 插件身份与请求不一致。
    IdentityMismatch { expected: String, actual: String },
    /// 当前平台没有可用的插件二进制。
    UnsupportedPlatform(String),
    /// 插件要求的 ABI 版本与宿主不兼容。
    AbiMismatch { expected: u32, actual: u32 },
    /// 插件尚未激活，不能执行任务。
    NotActive(String),
    /// 插件正在排空，不能接受新的任务。
    Draining(String),
    /// 插件执行超时。
    ExecutionTimeout(String),
    /// 插件执行失败。
    ExecutionFailed(String),
    /// 插件无法安全卸载。
    UnloadTimeout(String),
    /// 插件文件摘要不匹配。
    IntegrityMismatch { expected: String, actual: String },
    /// 插件版本不可用或已撤销。
    VersionUnavailable { plugin_id: String, version: String },
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PluginError {}
