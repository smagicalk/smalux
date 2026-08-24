//! Noise 单端口示例的 Server/Client 公共配置。

// 此文件分别编译进两个独立 example；每个二进制都会有另一端专用的常量。
#![allow(dead_code)]

use std::{env, fmt, time::Duration};

use smalux_protocol::tonic_transport::{HeartbeatPolicy, HeartbeatStats};
use tracing::{debug, info, warn};

/// 示例业务会话的运行方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExampleMode {
    /// 调用方逐步调用 `send`、`receive_event` 和维护方法，便于阅读协议流程。
    Manual,
    /// `SessionDriver` 独占会话并自动处理心跳、rekey 与事件派发。
    Driver,
}

impl ExampleMode {
    /// 从 `--mode manual|driver` 读取模式；未指定时使用便于调试的 manual。
    pub fn from_args() -> Result<Self, ExampleModeError> {
        let mut arguments = env::args().skip(1);
        let mut mode = Self::Manual;
        while let Some(argument) = arguments.next() {
            if argument != "--mode" {
                warn!(argument, "unknown example command-line argument");
                return Err(ExampleModeError(format!("unknown argument: {argument}")));
            }
            mode = match arguments.next().as_deref() {
                Some("manual") => Self::Manual,
                Some("driver") => Self::Driver,
                Some(value) => {
                    warn!(value, "unknown example session mode");
                    return Err(ExampleModeError(format!("unknown mode: {value}")));
                }
                None => {
                    warn!("example --mode argument is missing a value");
                    return Err(ExampleModeError("--mode requires manual or driver".into()));
                }
            };
        }
        debug!(?mode, "parsed example session mode");
        Ok(mode)
    }
}

#[derive(Debug)]
pub struct ExampleModeError(String);

impl fmt::Display for ExampleModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ExampleModeError {}

/// Server 默认监听地址。
pub const DEFAULT_ADDRESS: &str = "127.0.0.1:8080";
/// Client 默认连接地址；使用 `https://` 即可经过 Cloudflare 或 Nginx。
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8080";
/// 示例数据默认写入 Cargo target，避免污染源码目录。
pub const DEFAULT_SERVER_DATA_DIR: &str = "target/smalux-noise-server";
pub const DEFAULT_AGENT_DATA_DIR: &str = "target/smalux-noise-agent";
pub const ADDRESS_ENV: &str = "SMALUX_EXAMPLE_ADDR";
pub const ENDPOINT_ENV: &str = "SMALUX_EXAMPLE_ENDPOINT";
pub const SERVER_DATA_DIR_ENV: &str = "SMALUX_EXAMPLE_SERVER_DATA_DIR";
pub const AGENT_DATA_DIR_ENV: &str = "SMALUX_EXAMPLE_AGENT_DATA_DIR";
pub const REVOKE_AGENT_ENV: &str = "SMALUX_EXAMPLE_REVOKE_AGENT";
/// 可选的 Server TLS 证书链 PEM 路径；不设置时 Server 使用 h2c。
pub const TLS_CERT_ENV: &str = "SMALUX_EXAMPLE_TLS_CERT";
/// 可选的 Server TLS 私钥 PEM 路径，必须与证书变量同时设置。
pub const TLS_KEY_ENV: &str = "SMALUX_EXAMPLE_TLS_KEY";
pub const GRPC_PREFIX: &str = "/api/v1/grpc";
pub const HEALTH_PATH: &str = "/api/v1/health";
/// 普通 REST 状态接口，演示与 gRPC 共用 Router、端口和版本前缀。
pub const STATUS_PATH: &str = "/api/v1/status";
/// 浏览器或普通 Client 使用的 WebSocket Echo 接口。
pub const WEBSOCKET_PATH: &str = "/api/v1/ws";

/// 示例心跳发送间隔；生产环境应按业务上报频率和网络质量单独配置。
pub const EXAMPLE_HEARTBEAT_INTERVAL_SECS: u64 = 1;
/// 示例心跳失联判定时间；必须大于发送间隔，避免正常抖动被立即判定为断线。
pub const EXAMPLE_HEARTBEAT_TIMEOUT_SECS: u64 = 5;
/// 示例在业务消息完成后继续保持会话的时间，用来观察至少一次 Ping/Pong。
pub const EXAMPLE_HEARTBEAT_OBSERVE_SECS: u64 = 2;

/// 返回 Client 与 Server 共用的示例心跳策略。
pub fn example_heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        interval: Duration::from_secs(EXAMPLE_HEARTBEAT_INTERVAL_SECS),
        timeout: Duration::from_secs(EXAMPLE_HEARTBEAT_TIMEOUT_SECS),
    }
}

/// 返回业务消息发送完成后用于观察心跳的等待窗口。
pub fn example_heartbeat_observe_window() -> Duration {
    Duration::from_secs(EXAMPLE_HEARTBEAT_OBSERVE_SECS)
}

/// 统一输出示例两端的心跳累计值和最近一次完整 RTT 样本。
pub fn print_heartbeat_stats(side: &str, stats: HeartbeatStats) {
    info!(
        side,
        sent = stats.sent_count,
        received = stats.received_count,
        lost = stats.lost_count,
        consecutive_failures = stats.consecutive_failures,
        "example heartbeat statistics"
    );
    println!(
        "[{side}][heartbeat] sent={} received={} lost={} consecutive_failures={} min_rtt={:?} max_rtt={:?}",
        stats.sent_count,
        stats.received_count,
        stats.lost_count,
        stats.consecutive_failures,
        stats.min_rtt,
        stats.max_rtt,
    );
    if let Some(sample) = stats.last_sample {
        debug!(side, nonce = sample.nonce, rtt = ?sample.rtt, "example heartbeat sample");
        println!(
            "[{side}][heartbeat] sample nonce={} rtt={:?} sent_at={} responder_received_at={} responder_sent_at={} received_at={}",
            sample.nonce,
            sample.rtt,
            sample.sent_at_unix_micros,
            sample.responder_received_at_unix_micros,
            sample.responder_sent_at_unix_micros,
            sample.received_at_unix_micros,
        );
    } else {
        warn!(side, "example has no matching heartbeat Pong");
        println!("[{side}][heartbeat] no matching Pong received");
    }
}
