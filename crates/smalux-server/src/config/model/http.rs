//! HTTP 配置模型，负责监听地址和端口的稳定配置。

use std::net::{IpAddr, SocketAddr};

/// HTTP 服务配置。
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// HTTP 监听 IP 地址。
    pub bind_addr: IpAddr,
    /// HTTP 监听端口。
    pub bind_port: u16,
}

impl HttpConfig {
    /// 合成最终用于 TCP bind 的 SocketAddr。
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_addr, self.bind_port)
    }
}
