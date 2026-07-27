//! Noise 单端口示例的 Server/Client 公共配置。

// 此文件分别编译进两个独立 example；每个二进制都会有另一端专用的常量。
#![allow(dead_code)]

use std::{env, fmt};

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
                return Err(ExampleModeError(format!("unknown argument: {argument}")));
            }
            mode = match arguments.next().as_deref() {
                Some("manual") => Self::Manual,
                Some("driver") => Self::Driver,
                Some(value) => return Err(ExampleModeError(format!("unknown mode: {value}"))),
                None => return Err(ExampleModeError("--mode requires manual or driver".into())),
            };
        }
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
pub const REGISTRATION_TOKEN_ENV: &str = "SMALUX_EXAMPLE_REGISTRATION_TOKEN";
pub const REVOKE_AGENT_ENV: &str = "SMALUX_EXAMPLE_REVOKE_AGENT";
/// 仅供本地手工测试使用的固定 256 位注册 Token。
///
/// 生产环境绝不能内置固定 Token：应使用密码学安全随机数生成、设置短有效期，并在注册成功后
/// 立即作废。这里固定它只是为了反复运行示例时不必每次复制新的值。
pub const FIXED_REGISTRATION_TOKEN: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
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
