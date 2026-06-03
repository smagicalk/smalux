//! CLI 枚举值和运行时配置枚举的转换。

use super::super::model::{ExportAuthMode, ExportFormat, ExportWireMode};
use clap::builder::PossibleValue;
use smalux_core::model::info::MetricLevel;

/// CLI 导出数据编码格式。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliExportFormat {
    /// Smalux 默认 JSON frame。
    SmaluxJson,
    /// Komari 兼容格式。
    Komari,
}

impl clap::ValueEnum for CliExportFormat {
    /// 当前可用的导出格式。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::SmaluxJson, Self::Komari]
    }

    /// 使用配置里的 snake_case 名称，避免 CLI 和 JSON patch 命名不一致。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::SmaluxJson => Some(PossibleValue::new("smalux_json")),
            Self::Komari => Some(PossibleValue::new("komari")),
        }
    }
}

impl From<CliExportFormat> for ExportFormat {
    /// 转换为运行时导出格式。
    fn from(value: CliExportFormat) -> Self {
        match value {
            CliExportFormat::SmaluxJson => Self::SmaluxJson,
            CliExportFormat::Komari => Self::Komari,
        }
    }
}

/// CLI Smalux 自有协议 wire 模式。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliWireMode {
    /// 二进制明文 JSON bytes。
    BinaryPlain,
    /// Noise PSK 安全通道。
    SecurePsk,
}

impl clap::ValueEnum for CliWireMode {
    /// 当前可用的 wire 模式。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::BinaryPlain, Self::SecurePsk]
    }

    /// 使用配置里的 snake_case 名称，避免 CLI 和 JSON patch 命名不一致。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::BinaryPlain => Some(PossibleValue::new("binary_plain")),
            Self::SecurePsk => Some(PossibleValue::new("secure_psk")),
        }
    }
}

impl From<CliWireMode> for ExportWireMode {
    /// 转换为运行时 wire 模式。
    fn from(value: CliWireMode) -> Self {
        match value {
            CliWireMode::BinaryPlain => Self::BinaryPlain,
            CliWireMode::SecurePsk => Self::SecurePsk,
        }
    }
}

/// CLI 指标采集级别。
#[derive(Debug, Clone, Copy)]
pub(crate) enum CliMetricLevel {
    /// 只采集总数。
    Count,
    /// 采集轻量信息。
    Light,
    /// 采集完整明细。
    Details,
}

impl clap::ValueEnum for CliMetricLevel {
    /// 当前可用的采集级别。
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Count, Self::Light, Self::Details]
    }

    /// 使用配置里的 snake_case 名称。
    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::Count => Some(PossibleValue::new("count")),
            Self::Light => Some(PossibleValue::new("light")),
            Self::Details => Some(PossibleValue::new("details")),
        }
    }
}

impl From<CliMetricLevel> for MetricLevel {
    /// 转换为运行时采集级别。
    fn from(value: CliMetricLevel) -> Self {
        match value {
            CliMetricLevel::Count => Self::Count,
            CliMetricLevel::Light => Self::Light,
            CliMetricLevel::Details => Self::Details,
        }
    }
}

/// CLI 认证方式。
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CliAuthMode {
    /// 不发送认证信息。
    None,
    /// 使用 query token。
    Query,
    /// 使用 Authorization Bearer token。
    Bearer,
}

impl From<CliAuthMode> for ExportAuthMode {
    /// 转换为运行时认证方式。
    fn from(value: CliAuthMode) -> Self {
        match value {
            CliAuthMode::None => Self::None,
            CliAuthMode::Query => Self::Query,
            CliAuthMode::Bearer => Self::Bearer,
        }
    }
}
