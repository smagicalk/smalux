//! agent 身份信息模型。

use super::{Ip, PublicIpInfo};
use serde::{Deserialize, Serialize};

/// 本机身份信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IdentityInfo {
    /// agent 实例 ID。
    pub agent_id: String,
    /// 主机名。
    pub hostname: String,
    /// 公网 IP 获取结果。
    pub public_ip: PublicIpInfo,
    /// 本地网卡 IP 列表。
    pub local_ips: Vec<Ip>,
}
