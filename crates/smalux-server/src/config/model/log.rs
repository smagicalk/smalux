//! 日志配置模型，负责滚动日志的稳定运行配置。

use std::path::PathBuf;

/// 日志输出配置。
#[derive(Clone, Debug)]
pub struct LogConfig {
    /// 滚动日志文件路径。
    pub file: PathBuf,
    /// 最多保留的滚动日志文件数量。
    pub retention_files: usize,
    /// 单个滚动日志文件最大大小，单位 MB。
    pub max_size_mb: u64,
}
