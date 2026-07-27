//! 公网 IPv4/IPv6 多端点查询、响应解析和错误汇总。

use std::{net::IpAddr, time::Duration};

use anyhow::{Context, ensure};
use futures_util::stream::{self, StreamExt};

use super::{PublicIpSnapshot, PublicIpState, PublicIpStatus, public_ip_not_requested};

/// 单个地址族同时进行的公网 IP 请求数。
///
/// IPv4 与 IPv6 会各自使用该上限，因此全局最多同时存在四个请求。
const PUBLIC_IP_CONCURRENCY_PER_FAMILY: usize = 2;

/// IPv4 公网地址查询端点，以有限并发方式获取首个合法结果。
const IPV4_ENDPOINTS: &[PublicIpEndpoint] = &[
    PublicIpEndpoint::json("https://api.ipify.org?format=json", IpFamily::V4, "root.ip"),
    PublicIpEndpoint::text("https://api-ipv4.ip.sb/ip", IpFamily::V4),
    PublicIpEndpoint::text("https://v4.ident.me", IpFamily::V4),
    PublicIpEndpoint::text("https://ipv4.icanhazip.com", IpFamily::V4),
];

/// IPv6 公网地址查询端点，以有限并发方式获取首个合法结果。
const IPV6_ENDPOINTS: &[PublicIpEndpoint] = &[
    PublicIpEndpoint::json(
        "https://api6.ipify.org?format=json",
        IpFamily::V6,
        "root.ip",
    ),
    PublicIpEndpoint::text("https://api-ipv6.ip.sb/ip", IpFamily::V6),
    PublicIpEndpoint::text("https://v6.ident.me", IpFamily::V6),
    PublicIpEndpoint::text("https://ipv6.icanhazip.com", IpFamily::V6),
];

/// 公网 IP 地址族，用于校验端点没有返回错误的地址类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpFamily {
    /// 仅接受 IPv4。
    V4,
    /// 仅接受 IPv6。
    V6,
}

/// 公网 IP 响应解析模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicIpParseMode {
    /// 响应正文就是 IP 地址。
    Text,
    /// 按 `root.field.child` 形式读取 JSON 对象字段。
    JsonPath(&'static str),
}

/// 一个内置公网 IP 服务端点及其响应契约。
#[derive(Debug, Clone, Copy)]
struct PublicIpEndpoint {
    /// HTTP GET 地址。
    url: &'static str,
    /// 端点必须返回的地址族。
    family: IpFamily,
    /// 响应正文的解析方式。
    parse_mode: PublicIpParseMode,
}

impl PublicIpEndpoint {
    /// 定义返回纯文本 IP 地址的内置端点。
    const fn text(url: &'static str, family: IpFamily) -> Self {
        Self {
            url,
            family,
            parse_mode: PublicIpParseMode::Text,
        }
    }

    /// 定义按简单 root 字段路径读取 JSON 的内置端点。
    const fn json(url: &'static str, family: IpFamily, path: &'static str) -> Self {
        Self {
            url,
            family,
            parse_mode: PublicIpParseMode::JsonPath(path),
        }
    }
}

/// 使用内置端点分别查询公网 IPv4 和 IPv6。
///
/// 两个地址族并行执行，每个地址族内部以固定上限并发访问端点。IPv6 不可用不会
/// 影响 IPv4 结果，反之亦然。
pub async fn fetch_public_ips(
    timeout: Duration,
) -> (anyhow::Result<IpAddr>, anyhow::Result<IpAddr>) {
    let client = reqwest::Client::new();
    tokio::join!(
        fetch_from_endpoints(&client, IPV4_ENDPOINTS, timeout),
        fetch_from_endpoints(&client, IPV6_ENDPOINTS, timeout)
    )
}

/// 查询并转换公网 IPv4 与 IPv6 状态。
///
/// # 示例
///
/// ```ignore
/// let snapshot = collect_public(Duration::from_secs(3)).await;
/// match snapshot.ipv4 {
///     Some(state) if state.status == PublicIpStatus::Resolved as i32 => {
///         println!("{}", state.address.as_deref().unwrap_or_default());
///     }
///     Some(state) if state.status == PublicIpStatus::Failed as i32 => {
///         eprintln!("{}", state.message.as_deref().unwrap_or_default());
///     }
///     _ => {}
/// }
/// ```
pub async fn collect_public(timeout: Duration) -> PublicIpSnapshot {
    collect_public_families(timeout, true, true).await
}

pub(crate) async fn collect_public_families(
    timeout: Duration,
    request_ipv4: bool,
    request_ipv6: bool,
) -> PublicIpSnapshot {
    let client = reqwest::Client::new();
    match (request_ipv4, request_ipv6) {
        (true, true) => {
            let (ipv4, ipv6) = tokio::join!(
                fetch_from_endpoints(&client, IPV4_ENDPOINTS, timeout),
                fetch_from_endpoints(&client, IPV6_ENDPOINTS, timeout)
            );
            PublicIpSnapshot {
                ipv4: Some(into_public_ip_state(ipv4)),
                ipv6: Some(into_public_ip_state(ipv6)),
            }
        }
        (true, false) => PublicIpSnapshot {
            ipv4: Some(into_public_ip_state(
                fetch_from_endpoints(&client, IPV4_ENDPOINTS, timeout).await,
            )),
            ipv6: Some(public_ip_not_requested()),
        },
        (false, true) => PublicIpSnapshot {
            ipv4: Some(public_ip_not_requested()),
            ipv6: Some(into_public_ip_state(
                fetch_from_endpoints(&client, IPV6_ENDPOINTS, timeout).await,
            )),
        },
        (false, false) => PublicIpSnapshot {
            ipv4: Some(public_ip_not_requested()),
            ipv6: Some(public_ip_not_requested()),
        },
    }
}

/// 把端点查询结果转换为可序列化的稳定状态模型。
fn into_public_ip_state(result: anyhow::Result<IpAddr>) -> PublicIpState {
    match result {
        Ok(address) => PublicIpState {
            status: PublicIpStatus::Resolved as i32,
            address: Some(address.to_string()),
            message: None,
        },
        Err(error) => PublicIpState {
            status: PublicIpStatus::Failed as i32,
            address: None,
            message: Some(error.to_string()),
        },
    }
}

/// 以有限并发访问同一地址族端点，并返回首个合法响应。
async fn fetch_from_endpoints(
    client: &reqwest::Client,
    endpoints: &[PublicIpEndpoint],
    timeout: Duration,
) -> anyhow::Result<IpAddr> {
    let requests = stream::iter(endpoints.iter().copied())
        .map(|endpoint| async move {
            let result = fetch_from_endpoint(client, endpoint, timeout).await;
            (endpoint, result)
        })
        .buffer_unordered(PUBLIC_IP_CONCURRENCY_PER_FAMILY);
    futures_util::pin_mut!(requests);
    let mut failures = Vec::with_capacity(endpoints.len());

    while let Some((endpoint, result)) = requests.next().await {
        match result {
            Ok(address) => return Ok(address),
            Err(error) => failures.push(format!("{}: {error:#}", endpoint.url)),
        }
    }

    anyhow::bail!("all public IP endpoints failed: {}", failures.join("; "))
}

/// 请求单个端点、检查 HTTP 状态并按端点契约解析正文。
async fn fetch_from_endpoint(
    client: &reqwest::Client,
    endpoint: PublicIpEndpoint,
    timeout: Duration,
) -> anyhow::Result<IpAddr> {
    let body = client
        .get(endpoint.url)
        .timeout(timeout)
        .send()
        .await
        .context("request failed")?
        .error_for_status()
        .context("endpoint returned an error status")?
        .text()
        .await
        .context("response body could not be read")?;

    parse_public_ip(&body, endpoint.parse_mode, endpoint.family)
}

/// 按解析模式读取 IP，并严格校验返回地址族。
fn parse_public_ip(
    body: &str,
    parse_mode: PublicIpParseMode,
    family: IpFamily,
) -> anyhow::Result<IpAddr> {
    let raw_address = match parse_mode {
        PublicIpParseMode::Text => body.trim().to_owned(),
        PublicIpParseMode::JsonPath(path) => parse_json_path(body, path)?,
    };
    let address = raw_address
        .parse::<IpAddr>()
        .context("public IP value is invalid")?;

    ensure!(
        matches!(
            (family, address),
            (IpFamily::V4, IpAddr::V4(_)) | (IpFamily::V6, IpAddr::V6(_))
        ),
        "public IP endpoint returned the wrong address family"
    );
    Ok(address)
}

/// 解析 `root.field.child` 形式的受限 JSON 字段路径。
fn parse_json_path(body: &str, path: &str) -> anyhow::Result<String> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("public IP response is not valid JSON")?;
    let mut segments = path.split('.');
    ensure!(
        segments.next() == Some("root"),
        "JSON path must start with `root`"
    );

    let mut current = &value;
    for segment in segments {
        ensure!(!segment.is_empty(), "JSON path contains an empty field");
        current = current
            .get(segment)
            .with_context(|| format!("JSON path field `{segment}` does not exist"))?;
    }

    current
        .as_str()
        .map(str::to_owned)
        .context("JSON path result is not a string")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unrequested_public_ip_families_do_not_start_requests() {
        let snapshot = collect_public_families(Duration::from_secs(1), false, false).await;

        assert_eq!(
            snapshot.ipv4.expect("IPv4 state").status,
            PublicIpStatus::NotRequested as i32
        );
        assert_eq!(
            snapshot.ipv6.expect("IPv6 state").status,
            PublicIpStatus::NotRequested as i32
        );
    }

    #[test]
    fn public_ip_parser_supports_text_and_nested_json_paths() {
        assert_eq!(
            parse_public_ip("203.0.113.10\n", PublicIpParseMode::Text, IpFamily::V4).unwrap(),
            "203.0.113.10".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            parse_public_ip(
                r#"{"ip":"2001:db8::1"}"#,
                PublicIpParseMode::JsonPath("root.ip"),
                IpFamily::V6,
            )
            .unwrap(),
            "2001:db8::1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            parse_public_ip(
                r#"{"data":{"ip":"198.51.100.8"}}"#,
                PublicIpParseMode::JsonPath("root.data.ip"),
                IpFamily::V4,
            )
            .unwrap(),
            "198.51.100.8".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn public_ip_parser_rejects_invalid_paths_values_and_address_families() {
        assert!(
            parse_public_ip(
                r#"{"data":{}}"#,
                PublicIpParseMode::JsonPath("root.data.ip"),
                IpFamily::V4,
            )
            .is_err()
        );
        assert!(
            parse_public_ip(
                r#"{"ip":42}"#,
                PublicIpParseMode::JsonPath("root.ip"),
                IpFamily::V4,
            )
            .is_err()
        );
        assert!(parse_public_ip("not-an-ip", PublicIpParseMode::Text, IpFamily::V4).is_err());
        assert!(parse_public_ip("2001:db8::1", PublicIpParseMode::Text, IpFamily::V4).is_err());
        assert!(parse_public_ip("198.51.100.8", PublicIpParseMode::Text, IpFamily::V6).is_err());
    }
}
