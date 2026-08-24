//! 网络探测配置、校验错误和输出快照。

use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use serde::Serialize;
use smalux_protocol::agent::v1::{ProbeTaskConfig as ProtoProbeTaskConfig, probe_node_config};

/// 单个 Task 允许配置的最大探测节点数。
pub const MAX_PROBE_NODES: usize = 256;
/// 单次 Task 运行允许并发探测的最大节点数。
pub const MAX_PROBE_CONCURRENCY: usize = 64;
/// 单个节点在一次 Task 运行中的最大尝试次数。
pub const MAX_PROBE_ATTEMPTS: u16 = 20;
/// 单次探测允许配置的最大超时。
pub const MAX_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
/// 同一节点两次尝试之间允许配置的最大间隔。
pub const MAX_PROBE_INTERVAL: Duration = Duration::from_secs(60);
/// UDP 单次请求或期望响应前缀允许的最大字节数。
pub const MAX_UDP_PAYLOAD_BYTES: usize = 4 * 1024;

/// 节点使用的网络探测方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeTarget {
    /// 使用 ICMP Echo 探测主机可达性。
    IcmpEcho,
    /// 通过建立 TCP 连接测量指定端口的可达性和握手延迟。
    TcpConnect {
        /// 目标 TCP 端口；零端口无法构造。
        port: NonZeroU16,
    },
    /// 发送 UDP 数据报，并等待同一目标返回满足前缀条件的数据报。
    UdpRequest {
        /// 目标 UDP 端口；零端口无法构造。
        port: NonZeroU16,
        /// 每次尝试发送的原始请求载荷。
        request_payload: Vec<u8>,
        /// 成功响应必须具有的前缀；`None` 表示接受任意响应数据报。
        expected_response_prefix: Option<Vec<u8>>,
    },
    /// 发送 HTTP GET 请求并按状态码范围判断成功。
    Http {
        /// 完整的 HTTP 或 HTTPS URL。
        url: reqwest::Url,
        /// 被视为成功的 HTTP 状态码范围。
        expected_status: HttpStatusRange,
        /// 是否允许跟随最多十次 HTTP 重定向。
        follow_redirects: bool,
    },
}

/// HTTP 探测接受的状态码闭区间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpStatusRange {
    /// 成功状态码下界。
    pub min: u16,
    /// 成功状态码上界。
    pub max: u16,
}

impl HttpStatusRange {
    /// 判断一个 HTTP 状态码是否位于配置区间内。
    pub const fn contains(self, status: u16) -> bool {
        status >= self.min && status <= self.max
    }
}

/// 探测结果中记录的稳定协议类型。
///
/// 使用枚举而不是任意字符串，避免输出方需要猜测 `ping`、`tcp_ping` 等历史名称。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeProtocol {
    /// ICMP Echo 请求与响应探测。
    IcmpEcho,
    /// TCP 三次握手连接探测。
    TcpConnect,
    /// UDP 请求与响应数据报探测。
    UdpRequest,
    /// HTTP GET 响应探测。
    Http,
}

/// 单个探测节点的运行时配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeNodeConfig {
    /// 节点稳定名称；同一 Task 内必须唯一。
    pub name: String,
    /// IP 地址或可由系统 DNS 解析的主机名。
    pub host: String,
    /// ICMP、TCP、UDP 或 HTTP 探测方式。
    pub target: ProbeTarget,
    /// 一次 Task 运行中对该节点执行的尝试次数。
    pub attempts: NonZeroU16,
    /// 每次协议交互的总等待上限。
    pub timeout: Duration,
    /// 同一节点相邻尝试之间的等待时间；零表示立即进行下一次。
    pub interval: Duration,
}

/// 多节点网络探测 Task 的完整配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeTaskConfig {
    /// 单次 Task 运行中允许同时探测的节点数量。
    pub concurrency: NonZeroUsize,
    /// 按配置顺序输出结果的探测节点。
    pub nodes: Vec<ProbeNodeConfig>,
}

impl ProbeTaskConfig {
    /// 校验节点数量、唯一名称和各项资源限制。
    pub fn validate(&self) -> Result<(), ProbeConfigError> {
        if self.nodes.is_empty() {
            return Err(ProbeConfigError::EmptyNodes);
        }
        if self.nodes.len() > MAX_PROBE_NODES {
            return Err(ProbeConfigError::TooManyNodes {
                actual: self.nodes.len(),
                maximum: MAX_PROBE_NODES,
            });
        }
        if self.concurrency.get() > MAX_PROBE_CONCURRENCY {
            return Err(ProbeConfigError::ConcurrencyTooHigh {
                actual: self.concurrency.get(),
                maximum: MAX_PROBE_CONCURRENCY,
            });
        }

        let mut names = HashSet::with_capacity(self.nodes.len());
        for node in &self.nodes {
            if node.name.trim().is_empty() {
                return Err(ProbeConfigError::EmptyName);
            }
            if !names.insert(node.name.as_str()) {
                return Err(ProbeConfigError::DuplicateName(node.name.clone()));
            }
            if node.host.trim().is_empty() {
                return Err(ProbeConfigError::EmptyHost {
                    name: node.name.clone(),
                });
            }
            if let ProbeTarget::Http {
                url,
                expected_status,
                ..
            } = &node.target
            {
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(ProbeConfigError::InvalidHttpUrl {
                        name: node.name.clone(),
                    });
                }
                if expected_status.min < 100
                    || expected_status.max > 599
                    || expected_status.min > expected_status.max
                {
                    return Err(ProbeConfigError::InvalidHttpStatusRange {
                        name: node.name.clone(),
                        min: expected_status.min,
                        max: expected_status.max,
                    });
                }
            }
            if let ProbeTarget::UdpRequest {
                request_payload,
                expected_response_prefix,
                ..
            } = &node.target
            {
                if request_payload.is_empty() {
                    return Err(ProbeConfigError::EmptyUdpRequestPayload {
                        name: node.name.clone(),
                    });
                }
                if request_payload.len() > MAX_UDP_PAYLOAD_BYTES {
                    return Err(ProbeConfigError::UdpPayloadTooLarge {
                        name: node.name.clone(),
                        field: "request_payload",
                        actual: request_payload.len(),
                        maximum: MAX_UDP_PAYLOAD_BYTES,
                    });
                }
                if let Some(prefix) = expected_response_prefix
                    && prefix.len() > MAX_UDP_PAYLOAD_BYTES
                {
                    return Err(ProbeConfigError::UdpPayloadTooLarge {
                        name: node.name.clone(),
                        field: "expected_response_prefix",
                        actual: prefix.len(),
                        maximum: MAX_UDP_PAYLOAD_BYTES,
                    });
                }
            }
            if node.attempts.get() > MAX_PROBE_ATTEMPTS {
                return Err(ProbeConfigError::AttemptsTooHigh {
                    name: node.name.clone(),
                    actual: node.attempts.get(),
                    maximum: MAX_PROBE_ATTEMPTS,
                });
            }
            if node.timeout.is_zero() || node.timeout > MAX_PROBE_TIMEOUT {
                return Err(ProbeConfigError::InvalidTimeout {
                    name: node.name.clone(),
                    maximum: MAX_PROBE_TIMEOUT,
                });
            }
            if node.interval > MAX_PROBE_INTERVAL {
                return Err(ProbeConfigError::IntervalTooHigh {
                    name: node.name.clone(),
                    maximum: MAX_PROBE_INTERVAL,
                });
            }
        }
        Ok(())
    }
}

/// Probe Task 配置不满足安全边界时返回的错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProbeConfigError {
    /// 并发数必须显式设置为正整数。
    #[error("probe concurrency must be greater than zero")]
    InvalidConcurrency,
    /// 节点尝试次数必须显式设置为正整数且不超过 u16。
    #[error("probe node '{name}' attempts must be within 1..={maximum}")]
    InvalidAttempts {
        /// 配置错误的节点名称。
        name: String,
        /// 允许的最大尝试次数。
        maximum: u16,
    },
    /// 节点没有配置 ICMP、TCP、UDP 或 HTTP target。
    #[error("probe node '{name}' target is required")]
    MissingTarget {
        /// 配置错误的节点名称。
        name: String,
    },
    /// TCP 端口不是 1 到 65535。
    #[error("probe node '{name}' TCP port must be within 1..=65535")]
    InvalidTcpPort {
        /// 配置错误的节点名称。
        name: String,
    },
    /// UDP 端口不是 1 到 65535。
    #[error("probe node '{name}' UDP port must be within 1..=65535")]
    InvalidUdpPort {
        /// 配置错误的节点名称。
        name: String,
    },
    /// UDP 请求载荷为空，无法验证目标是否会响应实际请求。
    #[error("probe node '{name}' UDP request payload cannot be empty")]
    EmptyUdpRequestPayload {
        /// 配置错误的节点名称。
        name: String,
    },
    /// UDP 请求载荷或响应前缀超过内存和网络安全边界。
    #[error("probe node '{name}' UDP {field} size {actual} exceeds limit {maximum}")]
    UdpPayloadTooLarge {
        /// 配置错误的节点名称。
        name: String,
        /// 超过限制的字段名。
        field: &'static str,
        /// 实际字节数。
        actual: usize,
        /// 允许的最大字节数。
        maximum: usize,
    },
    /// 未配置任何探测节点。
    #[error("at least one probe node is required")]
    EmptyNodes,
    /// 节点数量超过单 Task 上限。
    #[error("probe node count {actual} exceeds limit {maximum}")]
    TooManyNodes {
        /// 实际节点数量。
        actual: usize,
        /// 允许的最大节点数量。
        maximum: usize,
    },
    /// 节点并发超过安全上限。
    #[error("probe concurrency {actual} exceeds limit {maximum}")]
    ConcurrencyTooHigh {
        /// 实际并发数。
        actual: usize,
        /// 允许的最大并发数。
        maximum: usize,
    },
    /// 节点名称为空或只有空白字符。
    #[error("probe node name cannot be empty")]
    EmptyName,
    /// 同一 Task 内出现重复节点名称。
    #[error("duplicate probe node name '{0}'")]
    DuplicateName(String),
    /// 节点目标为空或只有空白字符。
    #[error("probe node '{name}' host cannot be empty")]
    EmptyHost {
        /// 配置错误的节点名称。
        name: String,
    },
    /// HTTP URL 不是带主机名的 `http` 或 `https` URL。
    #[error("probe node '{name}' requires an absolute HTTP or HTTPS URL")]
    InvalidHttpUrl {
        /// 配置错误的节点名称。
        name: String,
    },
    /// HTTP 成功状态范围不在 100 到 599 内，或下界大于上界。
    #[error("probe node '{name}' HTTP status range {min}..={max} is invalid")]
    InvalidHttpStatusRange {
        /// 配置错误的节点名称。
        name: String,
        /// 配置的状态码下界。
        min: u16,
        /// 配置的状态码上界。
        max: u16,
    },
    /// 单节点尝试次数超过安全上限。
    #[error("probe node '{name}' attempts {actual} exceeds limit {maximum}")]
    AttemptsTooHigh {
        /// 配置错误的节点名称。
        name: String,
        /// 实际尝试次数。
        actual: u16,
        /// 允许的最大尝试次数。
        maximum: u16,
    },
    /// 探测超时为零或超过上限。
    #[error("probe node '{name}' timeout must be within (0, {maximum:?}]")]
    InvalidTimeout {
        /// 配置错误的节点名称。
        name: String,
        /// 允许的最大超时。
        maximum: Duration,
    },
    /// 尝试间隔超过上限。
    #[error("probe node '{name}' interval exceeds limit {maximum:?}")]
    IntervalTooHigh {
        /// 配置错误的节点名称。
        name: String,
        /// 允许的最大间隔。
        maximum: Duration,
    },
}

/// 将可反序列化的 Proto 配置编译成执行器使用的强类型配置。
pub(super) fn compile_config(
    config: &ProtoProbeTaskConfig,
) -> Result<ProbeTaskConfig, ProbeConfigError> {
    let concurrency = NonZeroUsize::new(config.concurrency as usize)
        .ok_or(ProbeConfigError::InvalidConcurrency)?;
    let nodes = config
        .nodes
        .iter()
        .map(|node| {
            let attempts = u16::try_from(node.attempts)
                .ok()
                .and_then(NonZeroU16::new)
                .ok_or_else(|| ProbeConfigError::InvalidAttempts {
                    name: node.name.clone(),
                    maximum: MAX_PROBE_ATTEMPTS,
                })?;
            let timeout = proto_duration(node.timeout.as_ref()).ok_or_else(|| {
                ProbeConfigError::InvalidTimeout {
                    name: node.name.clone(),
                    maximum: MAX_PROBE_TIMEOUT,
                }
            })?;
            let interval = match node.interval.as_ref() {
                Some(value) => proto_duration(Some(value)).ok_or_else(|| {
                    ProbeConfigError::IntervalTooHigh {
                        name: node.name.clone(),
                        maximum: MAX_PROBE_INTERVAL,
                    }
                })?,
                None => Duration::ZERO,
            };
            let target = match node.target.as_ref() {
                Some(probe_node_config::Target::IcmpEcho(_)) => ProbeTarget::IcmpEcho,
                Some(probe_node_config::Target::TcpConnect(target)) => {
                    let port = u16::try_from(target.port)
                        .ok()
                        .and_then(NonZeroU16::new)
                        .ok_or_else(|| ProbeConfigError::InvalidTcpPort {
                            name: node.name.clone(),
                        })?;
                    ProbeTarget::TcpConnect { port }
                }
                Some(probe_node_config::Target::UdpRequest(target)) => {
                    let port = u16::try_from(target.port)
                        .ok()
                        .and_then(NonZeroU16::new)
                        .ok_or_else(|| ProbeConfigError::InvalidUdpPort {
                            name: node.name.clone(),
                        })?;
                    ProbeTarget::UdpRequest {
                        port,
                        request_payload: target.request_payload.clone(),
                        expected_response_prefix: target
                            .expected_response_prefix
                            .clone()
                            .filter(|prefix| !prefix.is_empty()),
                    }
                }
                Some(probe_node_config::Target::Http(target)) => ProbeTarget::Http {
                    url: reqwest::Url::parse(&target.url).map_err(|_| {
                        ProbeConfigError::InvalidHttpUrl {
                            name: node.name.clone(),
                        }
                    })?,
                    expected_status: HttpStatusRange {
                        min: u16::try_from(target.expected_status_min).unwrap_or(u16::MAX),
                        max: u16::try_from(target.expected_status_max).unwrap_or(u16::MAX),
                    },
                    follow_redirects: target.follow_redirects,
                },
                None => {
                    return Err(ProbeConfigError::MissingTarget {
                        name: node.name.clone(),
                    });
                }
            };
            Ok(ProbeNodeConfig {
                name: node.name.clone(),
                host: node.host.clone(),
                target,
                attempts,
                timeout,
                interval,
            })
        })
        .collect::<Result<Vec<_>, ProbeConfigError>>()?;
    let compiled = ProbeTaskConfig { concurrency, nodes };
    compiled.validate()?;
    Ok(compiled)
}

fn proto_duration(value: Option<&prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    if value.seconds < 0 || !(0..1_000_000_000).contains(&value.nanos) {
        return None;
    }
    Some(Duration::new(value.seconds as u64, value.nanos as u32))
}

/// 一次 ICMP Echo、TCP Connect、UDP Request 或 HTTP 尝试的结果。
#[derive(Debug, Clone, Serialize)]
pub struct ProbeAttemptSnapshot {
    /// 从 1 开始的尝试序号。
    pub sequence: u16,
    /// 本次尝试是否成功。
    pub success: bool,
    /// 成功时的往返或连接耗时。
    pub latency_ms: Option<f64>,
    /// HTTP 响应状态码；ICMP、TCP、UDP 或未收到响应头时为 `None`。
    pub status_code: Option<u16>,
    /// 失败时的稳定错误摘要。
    pub error: Option<String>,
}

/// 单个节点在一次 Task 运行中的汇总结果。
#[derive(Debug, Clone, Serialize)]
pub struct ProbeNodeSnapshot {
    /// 配置中的稳定节点名称。
    pub name: String,
    /// 原始主机名或 IP 地址。
    pub host: String,
    /// 节点使用的探测协议。
    pub protocol: ProbeProtocol,
    /// TCP Connect 或 UDP Request 的目标端口；其他协议为 `None`。
    pub port: Option<u16>,
    /// HTTP 探测 URL 的脱敏形式；凭据、查询参数和片段会被移除。
    pub url: Option<String>,
    /// 本次运行解析并实际使用的 IP 地址。
    pub resolved_ip: Option<String>,
    /// 执行的尝试次数。
    pub attempted: u16,
    /// 成功的尝试次数。
    pub succeeded: u16,
    /// 未满足该协议成功条件的尝试比例，范围为 0 到 100。
    ///
    /// 对 ICMP/UDP 这通常等同于丢包率；对 TCP/HTTP 则表示连接或健康检查失败率。
    pub failure_percent: f64,
    /// 成功样本的最小耗时。
    pub min_latency_ms: Option<f64>,
    /// 成功样本的平均耗时。
    pub avg_latency_ms: Option<f64>,
    /// 成功样本的最大耗时。
    pub max_latency_ms: Option<f64>,
    /// 按执行顺序记录的全部尝试。
    pub attempts: Vec<ProbeAttemptSnapshot>,
}

/// 多节点网络探测的完整快照。
#[derive(Debug, Clone, Serialize)]
pub struct ProbeSnapshot {
    /// 配置的节点总数。
    pub total_nodes: usize,
    /// 至少有一次探测满足成功条件的健康节点数。
    pub healthy_nodes: usize,
    /// 按配置顺序排列的节点结果。
    pub nodes: Vec<ProbeNodeSnapshot>,
}

impl ProbeNodeSnapshot {
    pub(super) fn from_attempts(
        node: &ProbeNodeConfig,
        resolved_ip: Option<String>,
        attempts: Vec<ProbeAttemptSnapshot>,
    ) -> Self {
        let succeeded = attempts.iter().filter(|attempt| attempt.success).count() as u16;
        let attempted = attempts.len() as u16;
        let latencies = attempts
            .iter()
            .filter(|attempt| attempt.success)
            .filter_map(|attempt| attempt.latency_ms)
            .collect::<Vec<_>>();
        let min_latency_ms = latencies.iter().copied().reduce(f64::min);
        let max_latency_ms = latencies.iter().copied().reduce(f64::max);
        let avg_latency_ms =
            (!latencies.is_empty()).then(|| latencies.iter().sum::<f64>() / latencies.len() as f64);
        let (protocol, port, url) = match &node.target {
            ProbeTarget::IcmpEcho => (ProbeProtocol::IcmpEcho, None, None),
            ProbeTarget::TcpConnect { port } => (ProbeProtocol::TcpConnect, Some(port.get()), None),
            ProbeTarget::UdpRequest { port, .. } => {
                (ProbeProtocol::UdpRequest, Some(port.get()), None)
            }
            ProbeTarget::Http { url, .. } => {
                (ProbeProtocol::Http, None, Some(sanitized_http_url(url)))
            }
        };
        Self {
            name: node.name.clone(),
            host: node.host.clone(),
            protocol,
            port,
            url,
            resolved_ip,
            attempted,
            succeeded,
            failure_percent: if attempted > 0 {
                (attempted - succeeded) as f64 * 100.0 / attempted as f64
            } else {
                0.0
            },
            min_latency_ms,
            avg_latency_ms,
            max_latency_ms,
            attempts,
        }
    }

    pub(super) fn failed(node: &ProbeNodeConfig, error: impl Into<String>) -> Self {
        let error = error.into();
        let attempts = (1..=node.attempts.get())
            .map(|sequence| ProbeAttemptSnapshot {
                sequence,
                success: false,
                latency_ms: None,
                status_code: None,
                error: Some(error.clone()),
            })
            .collect();
        Self::from_attempts(node, None, attempts)
    }
}

fn sanitized_http_url(url: &reqwest::Url) -> String {
    let mut sanitized = url.clone();
    let _ = sanitized.set_username("");
    let _ = sanitized.set_password(None);
    sanitized.set_query(None);
    sanitized.set_fragment(None);
    sanitized.into()
}

pub(super) fn latency_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_config_compiles_udp_request_target() {
        let config = ProtoProbeTaskConfig {
            concurrency: 1,
            nodes: vec![smalux_protocol::agent::v1::ProbeNodeConfig {
                name: "dns".to_owned(),
                host: "127.0.0.1".to_owned(),
                attempts: 1,
                timeout: Some(prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                }),
                interval: None,
                target: Some(probe_node_config::Target::UdpRequest(
                    smalux_protocol::agent::v1::UdpRequestTarget {
                        port: 53,
                        request_payload: b"request".to_vec(),
                        expected_response_prefix: Some(b"response".to_vec()),
                    },
                )),
            }],
        };

        let compiled = compile_config(&config).unwrap();

        assert!(matches!(
            &compiled.nodes[0].target,
            ProbeTarget::UdpRequest {
                port,
                request_payload,
                expected_response_prefix: Some(expected),
            } if port.get() == 53
                && request_payload == b"request"
                && expected == b"response"
        ));
    }

    fn node(name: &str) -> ProbeNodeConfig {
        ProbeNodeConfig {
            name: name.to_owned(),
            host: "127.0.0.1".to_owned(),
            target: ProbeTarget::IcmpEcho,
            attempts: NonZeroU16::new(1).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        }
    }

    #[test]
    fn config_rejects_empty_and_duplicate_nodes() {
        let empty = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: Vec::new(),
        };
        assert_eq!(empty.validate(), Err(ProbeConfigError::EmptyNodes));

        let duplicate = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![node("edge"), node("edge")],
        };
        assert_eq!(
            duplicate.validate(),
            Err(ProbeConfigError::DuplicateName("edge".to_owned()))
        );
    }

    #[test]
    fn config_rejects_invalid_node_limits() {
        let mut invalid = node("edge");
        invalid.timeout = Duration::ZERO;
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::InvalidTimeout { .. })
        ));

        let mut invalid = node("edge");
        invalid.attempts = NonZeroU16::new(MAX_PROBE_ATTEMPTS + 1).unwrap();
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::AttemptsTooHigh { .. })
        ));

        let mut invalid = node("edge");
        invalid.interval = MAX_PROBE_INTERVAL + Duration::from_millis(1);
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::IntervalTooHigh { .. })
        ));

        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(MAX_PROBE_CONCURRENCY + 1).unwrap(),
            nodes: vec![node("edge")],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::ConcurrencyTooHigh { .. })
        ));
    }

    #[test]
    fn config_rejects_invalid_udp_payloads() {
        let mut invalid = node("udp");
        invalid.target = ProbeTarget::UdpRequest {
            port: NonZeroU16::new(53).unwrap(),
            request_payload: Vec::new(),
            expected_response_prefix: None,
        };
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::EmptyUdpRequestPayload { .. })
        ));

        let mut invalid = node("udp");
        invalid.target = ProbeTarget::UdpRequest {
            port: NonZeroU16::new(53).unwrap(),
            request_payload: vec![0; MAX_UDP_PAYLOAD_BYTES + 1],
            expected_response_prefix: None,
        };
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::UdpPayloadTooLarge {
                field: "request_payload",
                ..
            })
        ));
    }

    #[test]
    fn config_rejects_blank_name_and_host() {
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![node("   ")],
        };
        assert_eq!(config.validate(), Err(ProbeConfigError::EmptyName));

        let mut invalid = node("edge");
        invalid.host = " ".to_owned();
        let config = ProbeTaskConfig {
            concurrency: NonZeroUsize::new(1).unwrap(),
            nodes: vec![invalid],
        };
        assert!(matches!(
            config.validate(),
            Err(ProbeConfigError::EmptyHost { .. })
        ));
    }

    #[test]
    fn node_snapshot_aggregates_successful_latencies_and_loss() {
        let node = node("edge");
        let snapshot = ProbeNodeSnapshot::from_attempts(
            &node,
            Some("127.0.0.1".to_owned()),
            vec![
                ProbeAttemptSnapshot {
                    sequence: 1,
                    success: true,
                    latency_ms: Some(10.0),
                    status_code: None,
                    error: None,
                },
                ProbeAttemptSnapshot {
                    sequence: 2,
                    success: false,
                    latency_ms: None,
                    status_code: None,
                    error: Some("timeout".to_owned()),
                },
                ProbeAttemptSnapshot {
                    sequence: 3,
                    success: true,
                    latency_ms: Some(30.0),
                    status_code: None,
                    error: None,
                },
            ],
        );

        assert_eq!(snapshot.attempted, 3);
        assert_eq!(snapshot.succeeded, 2);
        assert!((snapshot.failure_percent - 100.0 / 3.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.min_latency_ms, Some(10.0));
        assert_eq!(snapshot.avg_latency_ms, Some(20.0));
        assert_eq!(snapshot.max_latency_ms, Some(30.0));
    }

    #[test]
    fn probe_protocol_serializes_to_stable_canonical_names() {
        assert_eq!(
            serde_json::to_string(&ProbeProtocol::IcmpEcho).unwrap(),
            r#""icmp_echo""#
        );
        assert_eq!(
            serde_json::to_string(&ProbeProtocol::TcpConnect).unwrap(),
            r#""tcp_connect""#
        );
        assert_eq!(
            serde_json::to_string(&ProbeProtocol::UdpRequest).unwrap(),
            r#""udp_request""#
        );
        assert_eq!(
            serde_json::to_string(&ProbeProtocol::Http).unwrap(),
            r#""http""#
        );
    }

    #[test]
    fn http_snapshot_redacts_credentials_query_and_fragment() {
        let node = ProbeNodeConfig {
            name: "private-health".to_owned(),
            host: "example.com".to_owned(),
            target: ProbeTarget::Http {
                url: reqwest::Url::parse(
                    "https://agent:secret@example.com/health?token=private#details",
                )
                .unwrap(),
                expected_status: HttpStatusRange { min: 200, max: 299 },
                follow_redirects: false,
            },
            attempts: NonZeroU16::new(1).unwrap(),
            timeout: Duration::from_secs(1),
            interval: Duration::ZERO,
        };

        let snapshot = ProbeNodeSnapshot::failed(&node, "not executed");

        assert_eq!(snapshot.url.as_deref(), Some("https://example.com/health"));
    }
}
