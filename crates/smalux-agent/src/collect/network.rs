//! 网络采集模块入口。
//!
//! 本地网卡流量/IP 映射和公网 IP 外部探测分别放在独立子模块。

mod local;
mod public_ip;

pub(crate) use local::{
    build_network_info, build_network_info_with_elapsed_and_filter, interface_public_ip_candidate,
    local_ips,
};
pub(crate) use public_ip::resolve_public_ip;
#[cfg(test)]
pub(crate) use public_ip::{
    fetch_public_network, get_public_network, get_public_network_v4, get_public_network_v6,
};
