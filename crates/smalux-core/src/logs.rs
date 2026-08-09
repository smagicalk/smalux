// 日志初始化模块：
// - 非测试环境：控制台 + 滚动文件输出
// - 测试环境：测试输出 writer

#[cfg(not(test))]
use std::{env, path::PathBuf};
#[cfg(not(test))]
use tracing_rolling_file::RollingFileAppenderBase;
#[cfg(not(test))]
use tracing_subscriber::util::SubscriberInitExt;
#[cfg(not(test))]
use tracing_subscriber::{fmt, layer::SubscriberExt};

#[cfg(not(test))]
static LOG_GUARD: once_cell::sync::OnceCell<tracing_appender::non_blocking::WorkerGuard> =
    once_cell::sync::OnceCell::new();

#[cfg(not(test))]
pub fn init_tracing() {
    // 从环境变量读取日志过滤规则；没有则使用默认规则。
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // 默认只输出业务关键流程，避免数据库驱动和底层运行时刷屏。
        // 需要排查细节时可通过 RUST_LOG 覆盖：
        // - RUST_LOG=smalux_server=debug,smalux_agent=debug 查看服务流程
        // - RUST_LOG=smalux_protocol=trace 查看握手和帧级细节
        tracing_subscriber::EnvFilter::new("info,sea_orm=warn,sqlx=warn,tokio=warn")
    });

    let log_path = resolve_log_path();

    // 滚动日志文件：跨日期或达到 10MB 时切换，最多保留 10 个历史文件。
    // 当前文件使用固定路径，历史文件使用 .1、.2 等后缀，便于部署脚本收集和清理。
    let file_appender = RollingFileAppenderBase::builder()
        .filename(log_path.to_string_lossy().into_owned())
        .max_filecount(10)
        .condition_daily()
        .condition_max_file_size(10 * 1024 * 1024)
        .build()
        .expect("log filename must not be empty");
    let (file_writer, guard) = file_appender.get_non_blocking_appender();

    // 控制台日志层。
    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_target(false)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_ansi(true)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_line_number(true)
        .with_file(false)
        .compact();

    // 文件日志层。
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_line_number(true)
        .with_file(true)
        .compact();

    let installed = tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        // 进程内可能已有测试框架或宿主安装了 subscriber；重复调用不应 panic。
        .try_init()
        .is_ok();

    if installed {
        // 保存 guard，避免异步日志在进程退出时丢失。
        let _ = LOG_GUARD.set(guard);
        tracing::info!(log_file = %log_path.display(), "tracing subscriber initialized");
    } else {
        tracing::debug!("tracing subscriber was already initialized");
    }
}

#[cfg(not(test))]
/// 解析公共日志文件路径；目录不可用时退回当前目录并保留 stderr 提示。
fn resolve_log_path() -> PathBuf {
    match crate::config::logs_dir() {
        Ok(logs_dir) => {
            let component_dir = logs_dir.join(log_component());
            if let Err(error) = std::fs::create_dir_all(&component_dir) {
                eprintln!(
                    "[smalux][logging] failed to create log directory {}: {error}; using current directory",
                    component_dir.display()
                );
                PathBuf::from("smalux.log")
            } else {
                component_dir.join("smalux.log")
            }
        }
        Err(error) => {
            eprintln!(
                "[smalux][logging] failed to resolve log directory: {error}; using current directory"
            );
            PathBuf::from("smalux.log")
        }
    }
}

#[cfg(not(test))]
/// 返回安全的日志组件目录名；可由部署环境通过 `SMALUX_LOG_COMPONENT` 覆盖。
fn log_component() -> String {
    let candidate = env::var("SMALUX_LOG_COMPONENT")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env::current_exe().ok().and_then(|path| {
                path.file_stem()
                    .map(|value| value.to_string_lossy().into_owned())
            })
        })
        .unwrap_or_else(|| "smalux".to_owned());
    let sanitized: String = candidate
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
                byte as char
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "smalux".to_owned()
    } else {
        sanitized
    }
}

#[cfg(test)]
pub fn init_tracing() {
    init_test_tracing();
}

/// 为测试进程安装只写测试控制台的全局 subscriber。
///
/// 该函数始终参与编译，而不是依赖 `smalux-core` 自身的 `cfg(test)`。因此 Agent、Server
/// 等下游 crate 的测试调用它时，也不会误用生产文件层或写入用户的正式日志目录。
pub fn init_test_tracing() {
    // 测试环境下允许重复初始化（避免并行测试 panic）。
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // 测试默认不打印 SQL debug；失败时再用 RUST_LOG=debug 打开细节。
        tracing_subscriber::EnvFilter::new("info,sea_orm=warn,sqlx=warn,tokio=warn")
    });

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_target(false)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_ansi(true)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_line_number(true)
        .with_file(false)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use tracing_rolling_file::RollingFileAppenderBase;

    #[test]
    fn rolling_appender_rotates_after_size_limit() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("smalux-log-test-{suffix}"));
        fs::create_dir_all(&directory).expect("test log directory should be created");
        let path = directory.join("smalux.log");
        let mut appender = RollingFileAppenderBase::builder()
            .filename(path.to_string_lossy().into_owned())
            .max_filecount(2)
            .condition_daily()
            .condition_max_file_size(8)
            .build()
            .expect("test log appender should be configured");

        appender
            .write_all(b"12345678")
            .expect("first log write should succeed");
        appender
            .write_all(b"next")
            .expect("second log write should trigger rotation");
        appender.flush().expect("test log appender should flush");

        assert_eq!(
            fs::read_to_string(directory.join("smalux.log.1")).unwrap(),
            "12345678"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "next");
        fs::remove_dir_all(directory).expect("test log directory should be removed");
    }
}
