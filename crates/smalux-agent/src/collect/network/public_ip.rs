//! 公网 IP 外部服务探测。

use crate::collect::unix_timestamp_secs;
use crate::config::PublicIpConfig;
use futures_util::FutureExt;
use futures_util::stream::StreamExt;
use serde_json_path::JsonPath;
use smalux_core::model::info::{NetworkInfo, PublicIpInfo, PublicIpSource};
use std::net::IpAddr;
use std::str::FromStr;

/// 获取公网 IP，优先使用网卡公网候选地址。
pub(crate) async fn resolve_public_ip(
    network_info: &NetworkInfo,
    config: &PublicIpConfig,
) -> PublicIpInfo {
    if !config.enabled {
        return PublicIpInfo::disabled();
    }

    let attempted_at = unix_timestamp_secs();

    if config.prefer_interface_candidate
        && let Some(ip) = super::interface_public_ip_candidate(network_info)
    {
        tracing::info!(ip = %ip, "Public IP resolved from interface candidate");
        let sampled_at = unix_timestamp_secs();
        let mut public_ip =
            PublicIpInfo::ready(ip, PublicIpSource::InterfaceCandidate, sampled_at, None);

        if config.verify_interface_candidate
            && let Ok(verified_ip) = lookup_external_public_ip(config).await
        {
            if Some(verified_ip) != public_ip.ip {
                tracing::info!(
                    interface_ip = %ip,
                    verified_ip = %verified_ip,
                    "Interface public IP candidate replaced by external verification"
                );
                public_ip = PublicIpInfo::ready(
                    verified_ip,
                    PublicIpSource::ExternalHttp,
                    sampled_at,
                    Some(unix_timestamp_secs()),
                );
            } else {
                public_ip.verified_at = Some(unix_timestamp_secs());
            }
        }

        return public_ip;
    }

    match lookup_external_public_ip(config).await {
        Ok(ip) => {
            let sampled_at = unix_timestamp_secs();
            tracing::info!(ip = %ip, "Public IP resolved from external service");
            PublicIpInfo::ready(
                ip,
                PublicIpSource::ExternalHttp,
                sampled_at,
                Some(sampled_at),
            )
        }
        Err(err) => {
            tracing::warn!(error = ?err, "Public IP lookup failed");
            PublicIpInfo::failed(err.to_string(), attempted_at)
        }
    }
}

/// 通过外部服务获取公网 IP。
async fn lookup_external_public_ip(config: &PublicIpConfig) -> anyhow::Result<IpAddr> {
    let (v4, v6) = tokio::time::timeout(config.lookup_timeout, async {
        futures_util::future::join(
            get_public_network_v4_with_concurrency(config.max_concurrency),
            get_public_network_v6_with_concurrency(config.max_concurrency),
        )
        .await
    })
    .await
    .map_err(|_| anyhow::anyhow!("Public IP lookup timed out"))?;

    v4.or(v6)
        .map_err(|err| anyhow::anyhow!("Public IP lookup failed: {err}"))
}

/// 并发获取公网 IPv4 和 IPv6，成功的地址会被加入结果。
#[cfg(test)]
pub(crate) async fn get_public_network() -> anyhow::Result<Vec<IpAddr>> {
    let mut ips = vec![];
    let (v4, v6) = futures_util::join!(get_public_network_v4(), get_public_network_v6());
    match v4 {
        Ok(ipv4) => {
            ips.push(ipv4);
        }
        Err(_) => {
            // 单个 IP 协议栈失败不影响另一个协议栈的结果。
        }
    }

    match v6 {
        Ok(ipv6) => {
            ips.push(ipv6);
        }
        Err(_) => {
            // 单个 IP 协议栈失败不影响另一个协议栈的结果。
        }
    }
    Ok(ips)
}

/// 访问一个公网 IP 服务，并按需要从 JSON 响应中提取 IP。
pub async fn fetch_public_network(
    client: reqwest::Client,
    url: &str,
    json_path: Option<JsonPath>,
) -> anyhow::Result<IpAddr> {
    let body = client.get(url).send().await?.text().await?;
    match json_path {
        // 纯文本服务直接把响应体解析为 IP 地址。
        None => Ok(IpAddr::from_str(&body)?),
        Some(ip_json_path) => {
            // JSON 服务通过调用方提供的 JsonPath 定位 IP 字段。
            let ip_json = serde_json::from_str(body.as_str())?;
            let ip_node = ip_json_path.query(&ip_json);
            match ip_node.first() {
                None => {
                    anyhow::bail!("No node found with given IP address");
                }
                Some(ip_node) => match ip_node.as_str() {
                    None => {
                        anyhow::bail!("{} conversion to ip error", ip_node);
                    }
                    Some(ip_node_str) => Ok(IpAddr::from_str(ip_node_str)?),
                },
            }
        }
    }
}

/// 按指定并发访问多个公网 IP 服务，任意一个成功即返回。
pub async fn fetch_public_networks_with_concurrency(
    verify_url: Vec<(&str, Option<JsonPath>)>,
    max_concurrency: usize,
) -> anyhow::Result<IpAddr> {
    let client = reqwest::Client::builder().build()?;
    let max_concurrency = max_concurrency.max(1);

    // 使用 FuturesOrdered 控制并发请求数量，降低外部服务压力。
    let mut in_flight = futures_util::stream::FuturesOrdered::new();

    let mut verify_url_iter = verify_url.iter();
    for _ in 0..max_concurrency {
        if let Some((url, json_path)) = verify_url_iter.next() {
            in_flight
                .push_back(fetch_public_network(client.clone(), url, json_path.clone()).boxed());
        }
    }

    // 轮询当前并发，成功则返回，否则继续补充新的请求。
    while let Some(res) = in_flight.next().await {
        match res {
            Ok(ip) => {
                // 一旦成功直接返回，并取消剩余 futures。
                return Ok(ip);
            }
            Err(err) => {
                tracing::debug!(error = ?err, "Public network fetch failed");
            }
        }

        // 补充一个新的任务到并发中。
        if let Some((url, json_path)) = verify_url_iter.next() {
            let c = client.clone();
            in_flight.push_back(fetch_public_network(c, url, json_path.clone()).boxed());
        }
    }
    anyhow::bail!("Failed to fetch public networks");
}

/// 通过多个 IPv4 服务获取公网 IPv4 地址。
#[cfg(test)]
pub(crate) async fn get_public_network_v4() -> anyhow::Result<IpAddr> {
    get_public_network_v4_with_concurrency(2).await
}

/// 通过多个 IPv4 服务获取公网 IPv4 地址。
pub(crate) async fn get_public_network_v4_with_concurrency(
    max_concurrency: usize,
) -> anyhow::Result<IpAddr> {
    let v4_verify_url = vec![
        ("https://api.ipify.org", None),
        ("https://4.ipconfig.com", None),
        ("https://ifconfig.me/ip", None),
        ("https://4.ident.me", None),
        ("https://api.myip.la", None),
        ("https://api64.ipify.org", None),
        ("https://ipv4.ip.sb", None),
    ];
    fetch_public_networks_with_concurrency(v4_verify_url, max_concurrency).await
}

/// 通过多个 IPv6 服务获取公网 IPv6 地址。
#[cfg(test)]
pub(crate) async fn get_public_network_v6() -> anyhow::Result<IpAddr> {
    get_public_network_v6_with_concurrency(2).await
}

/// 通过多个 IPv6 服务获取公网 IPv6 地址。
pub(crate) async fn get_public_network_v6_with_concurrency(
    max_concurrency: usize,
) -> anyhow::Result<IpAddr> {
    let v6_verify_url = vec![
        ("https://api6.ipify.org", None),
        ("https://6.ipconfig.com", None),
        ("https://6.ident.me", None),
        ("https://ipv6.ip.sb", None),
    ];
    fetch_public_networks_with_concurrency(v6_verify_url, max_concurrency).await
}
