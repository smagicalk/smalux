//! Agent 配置默认值。

use std::time::Duration;

/// 核心指标默认采样间隔。
pub(crate) const DEFAULT_CORE_INTERVAL: Duration = Duration::from_secs(1);
/// 磁盘指标默认采样间隔。
pub(crate) const DEFAULT_DISK_INTERVAL: Duration = Duration::from_secs(5);
/// 网络指标默认采样间隔。
pub(crate) const DEFAULT_NETWORK_INTERVAL: Duration = Duration::from_secs(5);
/// 进程汇总默认采样间隔。
pub(crate) const DEFAULT_PROCESSES_INTERVAL: Duration = Duration::from_secs(60);
/// 进程 light/details 默认返回条数上限。
pub(crate) const DEFAULT_PROCESSES_LIMIT: usize = 50;
/// Socket 汇总默认采样间隔。
pub(crate) const DEFAULT_SOCKETS_INTERVAL: Duration = Duration::from_secs(60);
/// Socket details 默认返回条数上限。
pub(crate) const DEFAULT_SOCKETS_LIMIT: usize = 200;
/// 默认上报间隔。
pub(crate) const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);
/// 默认 basic info 刷新事件生成间隔。
pub(crate) const DEFAULT_BASIC_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// 默认业务心跳间隔；仅在 report.heartbeat_enabled=true 时生效。
pub(crate) const DEFAULT_REPORT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// 默认完整快照刷新间隔；仅在 report.delta_enabled=true 时生效。
pub(crate) const DEFAULT_REPORT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// 默认 server 强制 snapshot 最小间隔，避免控制消息刷爆完整上报。
pub(crate) const DEFAULT_REPORT_FORCE_SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(10);
/// 公网 IP 单轮外部探测超时。
pub(crate) const DEFAULT_PUBLIC_IP_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
/// 公网 IP 失败重试间隔。
pub(crate) const DEFAULT_PUBLIC_IP_RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// 公网 IP 成功后的低频刷新间隔。
pub(crate) const DEFAULT_PUBLIC_IP_REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// 公网 IP 探测默认并发数。
pub(crate) const DEFAULT_PUBLIC_IP_MAX_CONCURRENCY: usize = 2;
/// 默认 WebSocket 心跳间隔。
pub(crate) const DEFAULT_EXPORT_HEARTBEAT: Duration = Duration::from_secs(30);
/// WebSocket 断线或连接失败后的默认重连间隔。
pub(crate) const DEFAULT_EXPORT_RECONNECT_INTERVAL: Duration = Duration::from_secs(5);
/// 默认 server 根地址，adapter 会根据导出格式派生具体 endpoint。
pub(crate) const DEFAULT_BASE_URL: &str = "http://127.0.0.1:9000";
/// 默认 token query 参数名。
pub(crate) const DEFAULT_QUERY_TOKEN_PARAM: &str = "token";
/// 默认日志文件前缀；滚动策略会按日期和序号生成实际文件。
pub(crate) const DEFAULT_LOG_FILE: &str = "logs/smalux-agent.log";
/// 默认保留的滚动日志文件数，必须大于 0。
pub(crate) const DEFAULT_LOG_RETENTION_FILES: usize = 14;
/// 默认单个日志文件最大大小，单位 MB。
pub(crate) const DEFAULT_LOG_MAX_SIZE_MB: u64 = 64;
/// 默认最大远程 shell 会话数。
pub(crate) const DEFAULT_REMOTE_SHELL_MAX_SESSIONS: usize = 1;
/// 默认远程 shell 空闲超时。
pub(crate) const DEFAULT_REMOTE_SHELL_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// 默认远程 shell 总会话超时。
pub(crate) const DEFAULT_REMOTE_SHELL_SESSION_TIMEOUT: Duration = Duration::from_secs(60 * 60);
/// 默认最大远程任务并发数。
pub(crate) const DEFAULT_REMOTE_TASK_MAX_CONCURRENT: usize = 1;
/// 默认远程任务最大运行时间。
pub(crate) const DEFAULT_REMOTE_TASK_TIMEOUT: Duration = Duration::from_secs(30);
/// 默认远程任务 stdout 最大回传字节数。
pub(crate) const DEFAULT_REMOTE_TASK_MAX_STDOUT_BYTES: usize = 64 * 1024;
/// 默认远程任务 stderr 最大回传字节数。
pub(crate) const DEFAULT_REMOTE_TASK_MAX_STDERR_BYTES: usize = 64 * 1024;
/// 默认远程探测单次超时。
pub(crate) const DEFAULT_REMOTE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// 默认远程探测全局最小启动间隔。
pub(crate) const DEFAULT_REMOTE_PROBE_GLOBAL_MIN_INTERVAL: Duration = Duration::from_millis(500);
/// 默认同一目标远程探测最小启动间隔。
pub(crate) const DEFAULT_REMOTE_PROBE_TARGET_MIN_INTERVAL: Duration = Duration::from_secs(10);
