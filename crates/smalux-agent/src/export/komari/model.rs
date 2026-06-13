//! Komari 兼容协议模型。
//!
//! 这些结构体用于和 Komari 风格 WebSocket 上报格式兼容，字段命名需要保持协议要求。

use serde::{Deserialize, Serialize};
use smalux_core::model::info::{AgentReport, PublicIpStatus};
use smalux_protocol::{RemoteProbeId, RemoteProbeResult, RemoteTaskResult, RemoteTaskStatus};
use std::net::IpAddr;
use std::time::{Duration, SystemTime};

/// Komari 基础机器信息。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BasicInfo {
    /// CPU 架构。
    pub arch: String,
    /// CPU 核心数。
    pub cpu_cores: i64,
    /// CPU 名称。
    pub cpu_name: String,
    /// 磁盘总量，单位字节。
    pub disk_total: i64,
    /// GPU 名称。
    pub gpu_name: String,
    /// 公网或主网卡 IPv4。
    pub ipv4: String,
    /// 公网或主网卡 IPv6。
    pub ipv6: String,
    /// 内存总量，单位字节。
    pub mem_total: i64,
    /// 操作系统名称。
    pub os: String,
    /// 内核版本。
    pub kernel_version: String,
    /// swap 总量，单位字节。
    pub swap_total: i64,
    /// agent 版本。
    pub version: String,
    /// 虚拟化环境。
    pub virtualization: String,
}

impl BasicInfo {
    /// 从内部 AgentReport 映射 Komari 基础机器信息。
    pub fn from_agent_report(report: &AgentReport) -> Self {
        let memory = report
            .core
            .as_ref()
            .map(|core| &core.value.memory)
            .cloned()
            .unwrap_or_default();
        let disk = report
            .disk
            .as_ref()
            .map(|disk| &disk.value)
            .cloned()
            .unwrap_or_default();
        let cpu_name = report
            .core
            .as_ref()
            .and_then(|core| core.value.cpu.cpus.first())
            .map(|cpu| cpu.brand.clone())
            .unwrap_or_default();
        let (ipv4, ipv6) = public_ip_strings(report);

        Self {
            arch: report.system.cpu_arch.clone(),
            cpu_cores: usize_to_i64_saturating(report.system.core_num),
            cpu_name,
            disk_total: u64_to_i64_saturating(disk.total_space),
            gpu_name: String::new(),
            ipv4,
            ipv6,
            mem_total: u64_to_i64_saturating(memory.memory_total),
            os: display_os_name(report),
            kernel_version: report.system.kernel_version.clone(),
            swap_total: u64_to_i64_saturating(memory.swap_total),
            version: report.meta.agent_version.clone(),
            virtualization: String::new(),
        }
    }
}

/// Komari CPU 使用率。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cpu {
    /// CPU 使用率百分比。
    pub usage: f64,
}

/// Komari 内存状态。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ram {
    /// 内存总量，单位字节。
    pub total: i64,
    /// 已使用内存，单位字节。
    pub used: i64,
}

/// Komari swap 状态。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Swap {
    /// swap 总量，单位字节。
    pub total: i64,
    /// 已使用 swap，单位字节。
    pub used: i64,
}

/// Komari 系统负载。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Load {
    /// 1 分钟平均负载。
    pub load1: f64,
    /// 5 分钟平均负载。
    pub load5: f64,
    /// 15 分钟平均负载。
    pub load15: f64,
}

/// Komari 磁盘状态。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Disk {
    /// 磁盘总量，单位字节。
    pub total: i64,
    /// 已使用磁盘空间，单位字节。
    pub used: i64,
}

/// Komari 网络速率和累计流量。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    /// 当前上传速率。
    pub up: i64,
    /// 当前下载速率。
    pub down: i64,
    /// 累计上传流量。
    #[serde(rename = "totalUp")]
    pub total_up: i64,
    /// 累计下载流量。
    #[serde(rename = "totalDown")]
    pub total_down: i64,
}

/// Komari 连接数统计。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Connections {
    /// TCP 连接数。
    pub tcp: i64,
    /// UDP 连接数。
    pub udp: i64,
}

/// Komari 实时上报消息。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Report {
    /// CPU 状态。
    pub cpu: Cpu,
    /// 内存状态。
    pub ram: Ram,
    /// swap 状态。
    pub swap: Swap,
    /// 系统负载。
    pub load: Load,
    /// 磁盘状态。
    pub disk: Disk,
    /// 网络状态。
    pub network: Network,
    /// 连接数状态。
    pub connections: Connections,
    /// 系统运行时长，单位秒。
    pub uptime: i64,
    /// 进程数量。
    pub process: i64,
    /// 附加消息。
    pub message: String,
}

impl Report {
    /// 从内部 AgentReport 映射 Komari 实时上报。
    pub fn from_agent_report(report: &AgentReport) -> Self {
        let core = report.core.as_ref().map(|core| &core.value).cloned();
        let memory = core
            .as_ref()
            .map(|core| core.memory.clone())
            .unwrap_or_default();
        let load = core.map(|core| core.load_avg).unwrap_or_default();
        let disk = report
            .disk
            .as_ref()
            .map(|disk| disk.value.clone())
            .unwrap_or_default();
        let network = report
            .network
            .as_ref()
            .map(|network| network.value.clone())
            .unwrap_or_default();
        let sockets = report.sockets.as_ref().map(|sockets| &sockets.value);
        let process_count = report
            .processes
            .as_ref()
            .map(|processes| processes.value.count)
            .unwrap_or_default();

        Self {
            cpu: Cpu {
                usage: report
                    .core
                    .as_ref()
                    .map(|core| f32_to_f64(core.value.cpu.cpu_usage))
                    .unwrap_or_default(),
            },
            ram: Ram {
                total: u64_to_i64_saturating(memory.memory_total),
                used: u64_to_i64_saturating(memory.memory_usage),
            },
            swap: Swap {
                total: u64_to_i64_saturating(memory.swap_total),
                used: u64_to_i64_saturating(memory.swap_usage),
            },
            load: Load {
                load1: finite_f64(load.one),
                load5: finite_f64(load.five),
                load15: finite_f64(load.fifteen),
            },
            disk: Disk {
                total: u64_to_i64_saturating(disk.total_space),
                used: u64_to_i64_saturating(disk.total_space.saturating_sub(disk.available_space)),
            },
            network: Network {
                up: f64_to_i64_saturating(network.transmitted_bytes_per_sec),
                down: f64_to_i64_saturating(network.received_bytes_per_sec),
                total_up: u64_to_i64_saturating(network.total_transmitted),
                total_down: u64_to_i64_saturating(network.total_received),
            },
            connections: Connections {
                tcp: u64_to_i64_saturating(sockets.map(|sockets| sockets.tcp).unwrap_or_default()),
                udp: u64_to_i64_saturating(sockets.map(|sockets| sockets.udp).unwrap_or_default()),
            },
            uptime: u64_to_i64_saturating(report.system.uptime),
            process: u64_to_i64_saturating(process_count),
            message: String::new(),
        }
    }
}

/// Komari 远程任务结果。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResult {
    /// Komari server 下发的任务 ID。
    pub task_id: String,
    /// 命令输出结果；Komari 只有一个 result 字段，这里合并 stdout/stderr/error。
    pub result: String,
    /// 退出码；无法启动、超时或拒绝执行时使用 -1。
    pub exit_code: i32,
    /// 完成时间，RFC3339 字符串。
    pub finished_at: String,
}

impl TaskResult {
    /// 从内部远程任务结果映射为 Komari task/result 请求体。
    pub fn from_remote_task_result(result: &RemoteTaskResult) -> Self {
        Self {
            task_id: result.task_id.clone(),
            result: komari_task_result_text(result),
            exit_code: komari_exit_code(result),
            finished_at: unix_secs_to_rfc3339(result.finished_at),
        }
    }
}

/// Komari 网络探测结果。
#[derive(Debug, Serialize, Clone)]
pub struct PingResult {
    /// Komari WebSocket 消息类型。
    #[serde(rename = "type")]
    pub message_type: &'static str,
    /// Komari server 下发的 ping task ID，保持字符串或整数语义。
    pub task_id: RemoteProbeId,
    /// 探测类型。
    pub ping_type: String,
    /// 延迟毫秒数；失败、禁用或限频时为 -1。
    pub value: i64,
    /// 完成时间，RFC3339 字符串。
    pub finished_at: String,
}

impl PingResult {
    /// 从内部 probe job 结果映射为 Komari ping_result WebSocket 消息。
    pub fn from_probe_job_result(result: &RemoteProbeResult) -> Self {
        Self {
            message_type: "ping_result",
            task_id: result.komari_task_id(),
            ping_type: result.probe_type.as_str().to_string(),
            value: result.komari_value(),
            finished_at: unix_secs_to_rfc3339(result.finished_at),
        }
    }
}

/// 合并 stdout、stderr 和错误信息，适配 Komari 单一 result 字段。
fn komari_task_result_text(result: &RemoteTaskResult) -> String {
    let mut text = String::new();
    append_task_output(&mut text, &result.stdout);
    append_task_output(&mut text, &result.stderr);
    if let Some(error) = &result.error {
        append_task_output(&mut text, error);
    }
    text
}

/// 追加任务输出片段，保留不同来源之间的换行边界。
fn append_task_output(target: &mut String, value: &str) {
    if value.is_empty() {
        return;
    }
    if !target.is_empty() && !target.ends_with('\n') {
        target.push('\n');
    }
    target.push_str(value);
}

/// 把内部任务状态映射成 Komari exit_code。
fn komari_exit_code(result: &RemoteTaskResult) -> i32 {
    if let Some(exit_code) = result.exit_code {
        return exit_code;
    }
    match result.status {
        RemoteTaskStatus::Success => 0,
        RemoteTaskStatus::Failed => 1,
        RemoteTaskStatus::TimedOut | RemoteTaskStatus::Rejected => -1,
    }
}

/// 把 Unix 秒转换成 RFC3339 字符串。
fn unix_secs_to_rfc3339(timestamp: u64) -> String {
    let time = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(timestamp))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    humantime::format_rfc3339_seconds(time).to_string()
}

/// 把 usize 饱和转换为 i64，匹配 Komari 服务端整数类型。
fn usize_to_i64_saturating(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

/// 把 u64 饱和转换为 i64，避免超大值导致第三方服务解析失败。
fn u64_to_i64_saturating(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// 把浮点速率转换成 Komari 需要的整数 byte/s。
fn f64_to_i64_saturating(value: f64) -> i64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }

    value.round().min(i64::MAX as f64) as i64
}

/// 把 f32 转成安全的 f64。
fn f32_to_f64(value: f32) -> f64 {
    finite_f64(value as f64)
}

/// 过滤 NaN、无穷大和负数，避免第三方服务收到异常数字。
fn finite_f64(value: f64) -> f64 {
    if value.is_finite() && value >= 0.0 {
        value
    } else {
        0.0
    }
}

/// 返回展示用操作系统名称。
fn display_os_name(report: &AgentReport) -> String {
    if !report.system.long_os_version.trim().is_empty() {
        return report.system.long_os_version.clone();
    }
    if !report.system.os_version.trim().is_empty() {
        return report.system.os_version.clone();
    }
    report.system.name.clone()
}

/// 返回 Komari basic info 使用的公网 IPv4 / IPv6 字符串。
fn public_ip_strings(report: &AgentReport) -> (String, String) {
    let public_ip = &report.identity.public_ip;
    if !matches!(
        public_ip.status,
        PublicIpStatus::Ready | PublicIpStatus::Stale
    ) {
        return (String::new(), String::new());
    }

    match public_ip.ip {
        Some(IpAddr::V4(ip)) => (ip.to_string(), String::new()),
        Some(IpAddr::V6(ip)) => (String::new(), ip.to_string()),
        None => (String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    //! Komari 协议映射测试。

    use super::*;
    use smalux_core::model::info::{
        CoreInfo, DiskInfo, IdentityInfo, LoadAverageInfo, MemoryInfo, NetworkInfo, ProcessInfo,
        PublicIpInfo, PublicIpSource, ReportMeta, SocketAccuracy, SocketInfo, SocketSource,
        Stamped, SystemInfo,
    };
    use smalux_protocol::{RemoteProbeResult, RemoteProbeType, RemoteTaskResult, RemoteTaskStatus};
    use std::net::{IpAddr, Ipv4Addr};

    /// 构造带核心指标的测试 report。
    fn report_for_mapping() -> AgentReport {
        AgentReport {
            meta: ReportMeta {
                agent_version: "0.1.0-test".to_string(),
                ..Default::default()
            },
            identity: IdentityInfo {
                public_ip: PublicIpInfo::ready(
                    IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                    PublicIpSource::ExternalHttp,
                    100,
                    Some(100),
                ),
                ..Default::default()
            },
            system: SystemInfo {
                cpu_arch: "x86_64".to_string(),
                core_num: 8,
                long_os_version: "Windows 11 Pro".to_string(),
                kernel_version: "10.0.26100".to_string(),
                uptime: 3600,
                ..Default::default()
            },
            core: Some(Stamped {
                sampled_at: 100,
                value: CoreInfo {
                    memory: MemoryInfo {
                        memory_total: 16,
                        memory_usage: 8,
                        swap_total: 4,
                        swap_usage: 1,
                        ..Default::default()
                    },
                    load_avg: LoadAverageInfo {
                        one: 0.1,
                        five: 0.2,
                        fifteen: 0.3,
                        supported: true,
                    },
                    ..Default::default()
                },
            }),
            disk: Some(Stamped {
                sampled_at: 100,
                value: DiskInfo {
                    total_space: 100,
                    available_space: 40,
                    ..Default::default()
                },
            }),
            network: Some(Stamped {
                sampled_at: 100,
                value: NetworkInfo {
                    transmitted_bytes_per_sec: 12.0,
                    received_bytes_per_sec: 34.0,
                    total_transmitted: 120,
                    total_received: 340,
                    ..Default::default()
                },
            }),
            processes: Some(Stamped {
                sampled_at: 100,
                value: ProcessInfo::ready(42),
            }),
            sockets: Some(Stamped {
                sampled_at: 100,
                value: SocketInfo::ready(
                    10,
                    3,
                    SocketSource::SocketTable,
                    SocketAccuracy::SocketTable,
                ),
            }),
        }
    }

    /// 验证 basic info 会映射基础系统字段。
    #[test]
    fn basic_info_maps_agent_report() {
        let info = BasicInfo::from_agent_report(&report_for_mapping());

        assert_eq!(info.arch, "x86_64");
        assert_eq!(info.cpu_cores, 8);
        assert_eq!(info.disk_total, 100);
        assert_eq!(info.ipv4, "203.0.113.10");
        assert_eq!(info.mem_total, 16);
        assert_eq!(info.swap_total, 4);
        assert_eq!(info.version, "0.1.0-test");
    }

    /// 验证实时 report 会映射 Komari 字段名。
    #[test]
    fn report_serializes_komari_network_field_names() {
        let report = Report::from_agent_report(&report_for_mapping());
        let json = serde_json::to_value(report).unwrap();

        assert_eq!(json["ram"]["total"], 16);
        assert_eq!(json["disk"]["used"], 60);
        assert_eq!(json["network"]["up"].as_i64(), Some(12));
        assert_eq!(json["network"]["down"].as_i64(), Some(34));
        assert_eq!(json["network"]["totalUp"], 120);
        assert_eq!(json["network"]["totalDown"], 340);
        assert_eq!(json["connections"]["tcp"], 10);
        assert_eq!(json["connections"]["udp"], 3);
        assert_eq!(json["process"], 42);
    }

    /// 验证 Komari 服务端要求的整数字段不会序列化成浮点数。
    #[test]
    fn report_serializes_integer_fields_as_integers() {
        let report = Report::from_agent_report(&report_for_mapping());
        let json = serde_json::to_value(report).unwrap();

        assert_eq!(json["ram"]["used"].as_i64(), Some(8));
        assert_eq!(json["swap"]["used"].as_i64(), Some(1));
        assert_eq!(json["disk"]["used"].as_i64(), Some(60));
        assert_eq!(json["network"]["up"].as_i64(), Some(12));
        assert_eq!(json["network"]["totalDown"].as_i64(), Some(340));
        assert_eq!(json["uptime"].as_i64(), Some(3600));
        assert!(json["cpu"]["usage"].is_f64());
    }

    /// 验证远程任务结果会映射为 Komari task/result 字段。
    #[test]
    fn task_result_maps_remote_task_result() {
        let result = RemoteTaskResult {
            task_id: "task-1".to_string(),
            status: RemoteTaskStatus::Success,
            exit_code: Some(0),
            stdout: "hello".to_string(),
            stderr: "warn".to_string(),
            started_at: 99,
            finished_at: 100,
            duration_ms: 1000,
            timed_out: false,
            stdout_truncated: false,
            stderr_truncated: false,
            error: None,
        };

        let body = serde_json::to_value(TaskResult::from_remote_task_result(&result)).unwrap();

        assert_eq!(body["task_id"], "task-1");
        assert_eq!(body["result"], "hello\nwarn");
        assert_eq!(body["exit_code"], 0);
        assert_eq!(body["finished_at"], "1970-01-01T00:01:40Z");
    }

    /// 验证 rejected / timed out 这类无退出码结果会用 -1 表达失败。
    #[test]
    fn task_result_uses_negative_exit_code_without_process_exit_code() {
        let result = RemoteTaskResult {
            task_id: "task-rejected".to_string(),
            status: RemoteTaskStatus::Rejected,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: 100,
            finished_at: 100,
            duration_ms: 0,
            timed_out: false,
            stdout_truncated: false,
            stderr_truncated: false,
            error: Some("remote task is disabled".to_string()),
        };

        let body = serde_json::to_value(TaskResult::from_remote_task_result(&result)).unwrap();

        assert_eq!(body["exit_code"], -1);
        assert_eq!(body["result"], "remote task is disabled");
    }

    /// 验证 Komari ping_result 会保持 task_id 的原始 JSON 类型。
    #[test]
    fn ping_result_maps_probe_job_result() {
        let result = RemoteProbeResult {
            run_id: "probe-run-1".to_string(),
            source: smalux_protocol::RemoteProbeResultSource::Once,
            point_id: Some(RemoteProbeId::from("point-123")),
            request_id: Some(RemoteProbeId::from(123)),
            job_id: None,
            probe_type: RemoteProbeType::Tcp,
            target: "example.com:443".to_string(),
            status: smalux_protocol::RemoteProbeResultStatus::Success,
            latency_ms: Some(13),
            started_at: 99,
            finished_at: 100,
            duration_ms: 13,
            error: None,
        };

        let body = serde_json::to_value(PingResult::from_probe_job_result(&result)).unwrap();

        assert_eq!(body["type"], "ping_result");
        assert_eq!(body["task_id"], 123);
        assert_eq!(body["ping_type"], "tcp");
        assert_eq!(body["value"], 13);
        assert_eq!(body["finished_at"], "1970-01-01T00:01:40Z");
    }
}
