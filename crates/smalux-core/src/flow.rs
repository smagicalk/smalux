//! 流量和容量单位换算工具。
//!
//! 内部统一先换算为 bytes，再转换到目标单位，避免不同单位之间直接互转产生重复逻辑。

use std::fmt;

/// 容量单位。
///
/// `KiB/MiB/GiB/TiB` 使用 1024 进制，`KB/MB/GB/TB` 使用 1000 进制。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Unit {
    /// 字节。
    Bytes,
    /// 1024 字节。
    KiB,
    /// 1024^2 字节。
    MiB,
    /// 1024^3 字节。
    GiB,
    /// 1024^4 字节。
    TiB,
    /// 1000 字节。
    KB,
    /// 1000^2 字节。
    MB,
    /// 1000^3 字节。
    GB,
    /// 1000^4 字节。
    TB,
}

/// 带单位的容量值。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Flow {
    /// 字节值。
    BYTES(f64),
    /// KiB 值。
    KIB(f64),
    /// MiB 值。
    MIB(f64),
    /// GiB 值。
    GIB(f64),
    /// TiB 值。
    TIB(f64),
    /// KB 值。
    KB(f64),
    /// MB 值。
    MB(f64),
    /// GB 值。
    GB(f64),
    /// TB 值。
    TB(f64),
}

impl Unit {
    /// 该单位对应多少 bytes
    pub fn bytes_factor(self) -> f64 {
        match self {
            Unit::Bytes => 1.0,

            // IEC 单位使用 1024 进制。
            Unit::KiB => 1024.0,
            Unit::MiB => 1024.0_f64.powi(2),
            Unit::GiB => 1024.0_f64.powi(3),
            Unit::TiB => 1024.0_f64.powi(4),

            // SI 单位使用 1000 进制。
            Unit::KB => 1000.0,
            Unit::MB => 1000.0_f64.powi(2),
            Unit::GB => 1000.0_f64.powi(3),
            Unit::TB => 1000.0_f64.powi(4),
        }
    }

    /// 返回展示用单位名称。
    pub fn format_name(self) -> &'static str {
        match self {
            Unit::Bytes => "B",
            Unit::KiB => "KiB",
            Unit::MiB => "MiB",
            Unit::GiB => "GiB",
            Unit::TiB => "TiB",
            Unit::KB => "KB",
            Unit::MB => "MB",
            Unit::GB => "GB",
            Unit::TB => "TB",
        }
    }
}

impl Flow {
    /// Pretty 打印：自动换算成 IEC 单位并带 2 位小数
    pub fn pretty(&self) -> String {
        let bytes = self.as_bytes();
        let f = Flow::human_iec(bytes);

        match f {
            Flow::BYTES(v) => format!("{:.0} B", v),
            Flow::KIB(v) => format!("{:.2} KiB", v),
            Flow::MIB(v) => format!("{:.2} MiB", v),
            Flow::GIB(v) => format!("{:.2} GiB", v),
            Flow::TIB(v) => format!("{:.2} TiB", v),
            // human_iec 不会走到 SI 单位，这里做兜底。
            Flow::KB(v) => format!("{:.2} KB", v),
            Flow::MB(v) => format!("{:.2} MB", v),
            Flow::GB(v) => format!("{:.2} GB", v),
            Flow::TB(v) => format!("{:.2} TB", v),
        }
    }

    /// 返回当前值的单位。
    pub fn unit(&self) -> Unit {
        match self {
            Flow::BYTES(_) => Unit::Bytes,
            Flow::KIB(_) => Unit::KiB,
            Flow::MIB(_) => Unit::MiB,
            Flow::GIB(_) => Unit::GiB,
            Flow::TIB(_) => Unit::TiB,
            Flow::KB(_) => Unit::KB,
            Flow::MB(_) => Unit::MB,
            Flow::GB(_) => Unit::GB,
            Flow::TB(_) => Unit::TB,
        }
    }

    /// 返回当前值的数值部分，不做单位换算。
    pub fn value(&self) -> f64 {
        match *self {
            Flow::BYTES(v)
            | Flow::KIB(v)
            | Flow::MIB(v)
            | Flow::GIB(v)
            | Flow::TIB(v)
            | Flow::KB(v)
            | Flow::MB(v)
            | Flow::GB(v)
            | Flow::TB(v) => v,
        }
    }

    /// 转换成 bytes（统一内部单位）
    pub fn as_bytes(&self) -> f64 {
        self.value() * self.unit().bytes_factor()
    }

    /// 从 bytes 转换到目标单位（返回 f64）
    pub fn to_unit_value(&self, target: Unit) -> f64 {
        self.as_bytes() / target.bytes_factor()
    }

    /// 转换到目标单位（返回 Flow）
    pub fn to_unit(&self, target: Unit) -> Flow {
        let v = self.to_unit_value(target);
        match target {
            Unit::Bytes => Flow::BYTES(v),
            Unit::KiB => Flow::KIB(v),
            Unit::MiB => Flow::MIB(v),
            Unit::GiB => Flow::GIB(v),
            Unit::TiB => Flow::TIB(v),
            Unit::KB => Flow::KB(v),
            Unit::MB => Flow::MB(v),
            Unit::GB => Flow::GB(v),
            Unit::TB => Flow::TB(v),
        }
    }

    /// 自动选择最合适 IEC 单位（KiB/MiB/GiB/TiB）
    pub fn human_iec(bytes: f64) -> Flow {
        let abs = bytes.abs();

        if abs >= 1024.0_f64.powi(4) {
            Flow::TIB(bytes / 1024.0_f64.powi(4))
        } else if abs >= 1024.0_f64.powi(3) {
            Flow::GIB(bytes / 1024.0_f64.powi(3))
        } else if abs >= 1024.0_f64.powi(2) {
            Flow::MIB(bytes / 1024.0_f64.powi(2))
        } else if abs >= 1024.0 {
            Flow::KIB(bytes / 1024.0)
        } else {
            Flow::BYTES(bytes)
        }
    }
}

/// 默认 Display 也做 pretty 输出
impl fmt::Display for Flow {
    // Display 和 pretty 保持一致，避免两套容量展示格式。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pretty())
    }
}

#[cfg(test)]
mod tests {
    //! Flow 单位换算测试。

    use super::*;

    /// 验证 1024 进制 bytes 到 KiB 的转换。
    #[test]
    fn test_bytes_to_kib() {
        let f = Flow::BYTES(2048.0);
        let kib = f.to_unit(Unit::KiB);
        assert_eq!(kib, Flow::KIB(2.0));
    }

    /// 验证 1000 进制 bytes 到 KB 的转换。
    #[test]
    fn test_bytes_to_kb() {
        let f = Flow::BYTES(2048.0);
        let kb = f.to_unit(Unit::KB);
        assert_eq!(kb, Flow::KB(2.048));
    }

    /// 验证 Display 和 pretty 输出一致。
    #[test]
    fn test_pretty() {
        let f = Flow::BYTES(2048.0);
        assert_eq!(f.pretty(), "2.00 KiB");
        assert_eq!(format!("{f}"), "2.00 KiB");
    }
}
