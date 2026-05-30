//! tracing 日志初始化。
//!
//! 正式环境输出到控制台和滚动日志文件；测试环境只输出控制台。
//! 日志级别统一读取 `RUST_LOG`，未设置时按环境使用默认级别。

use std::path::PathBuf;
#[cfg(not(test))]
use std::sync::OnceLock;

use tracing_subscriber::{EnvFilter, prelude::*};

#[cfg(not(test))]
/// 正式环境默认日志级别。
const DEFAULT_PRODUCTION_LOG_LEVEL: &str = "info";

#[cfg(not(test))]
/// 非阻塞文件日志后台线程守卫，必须持有到进程结束。
static FILE_LOG_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

#[cfg(not(test))]
/// 初始化正式环境 tracing。
///
/// `log_path` 是滚动文件前缀，例如 `logs/smalux-agent.log`。
/// `retention_files` 是保留的滚动日志文件数，必须大于 0。
/// `max_size_mb` 是单个日志文件最大大小，达到后会触发大小滚动。
pub fn init_tracing(
    log_path: impl Into<PathBuf>,
    retention_files: usize,
    max_size_mb: u64,
) -> anyhow::Result<()> {
    init_production_tracing(log_path.into(), retention_files, max_size_mb)
}

#[cfg(not(test))]
/// 组装正式环境 subscriber。
fn init_production_tracing(
    log_path: PathBuf,
    retention_files: usize,
    max_size_mb: u64,
) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(log_filter(DEFAULT_PRODUCTION_LOG_LEVEL))
        .with(console_layer())
        .with(file_layer(log_path, retention_files, max_size_mb)?)
        .try_init()
        .ok();

    Ok(())
}

#[cfg(not(test))]
/// 构建文件日志 layer。
fn file_layer<S>(
    log_path: PathBuf,
    retention_files: usize,
    max_size_mb: u64,
) -> anyhow::Result<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber,
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    Ok(tracing_subscriber::fmt::layer()
        .pretty()
        .with_ansi(false)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_writer(rolling_appender(log_path, retention_files, max_size_mb)?)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339()))
}

#[cfg(not(test))]
/// 根据日志路径创建按天和按大小滚动的非阻塞 appender。
fn rolling_appender(
    log_path: PathBuf,
    retention_files: usize,
    max_size_mb: u64,
) -> anyhow::Result<tracing_appender::non_blocking::NonBlocking> {
    let max_size_bytes = log_max_size_bytes(max_size_mb)?;
    let appender = tracing_rolling_file::RollingFileAppenderBase::builder()
        .filename(log_path.to_string_lossy().into_owned())
        .max_filecount(retention_files)
        .condition_daily()
        .condition_max_file_size(max_size_bytes)
        .build()
        .map_err(|err| anyhow::anyhow!("failed to build rolling log appender: {err}"))?;
    let (writer, guard) = appender.get_non_blocking_appender();

    // 初始化 tracing 只应执行一次；重复调用时保留第一个后台线程守卫。
    let _ = FILE_LOG_GUARD.set(guard);
    Ok(writer)
}

/// 将 MB 转换为字节数，避免超大参数在乘法时溢出。
fn log_max_size_bytes(max_size_mb: u64) -> anyhow::Result<u64> {
    max_size_mb
        .checked_mul(1024)
        .and_then(|value| value.checked_mul(1024))
        .ok_or_else(|| anyhow::anyhow!("log max size is too large"))
}

/// 从 `RUST_LOG` 构建过滤器，缺失或解析失败时使用传入默认级别。
fn log_filter(default_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level))
}

/// 构建控制台日志 layer。
fn console_layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber,
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .pretty()
        .with_ansi(true)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
}

#[cfg(test)]
/// 测试环境默认日志级别。
const DEFAULT_TEST_LOG_LEVEL: &str = "debug";

#[cfg(test)]
/// 初始化测试环境 tracing。
pub fn init_tracing(
    log_path: impl Into<PathBuf>,
    retention_files: usize,
    max_size_mb: u64,
) -> anyhow::Result<()> {
    let _ = log_path.into();
    let _ = retention_files;
    let _ = max_size_mb;
    init_test_tracing()
}

#[cfg(test)]
/// 组装测试环境 subscriber。
fn init_test_tracing() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(log_filter(DEFAULT_TEST_LOG_LEVEL))
        .with(console_layer())
        .try_init()
        .ok();

    Ok(())
}

#[cfg(test)]
mod tests {
    //! 日志初始化辅助逻辑测试。

    use super::*;

    /// 验证日志大小配置会从 MB 转换为字节。
    #[test]
    fn log_max_size_bytes_converts_mb_to_bytes() {
        assert_eq!(log_max_size_bytes(64).unwrap(), 64 * 1024 * 1024);
    }

    /// 验证超大日志大小配置不会在乘法时 panic。
    #[test]
    fn log_max_size_bytes_rejects_overflow() {
        assert!(log_max_size_bytes(u64::MAX).is_err());
    }
}
