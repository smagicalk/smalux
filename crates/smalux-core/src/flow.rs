use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Unit {
    Bytes,
    KiB,
    MiB,
    GiB,
    TiB,
    KB,
    MB,
    GB,
    TB,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Flow {
    BYTES(f64),
    KIB(f64),
    MIB(f64),
    GIB(f64),
    TIB(f64),
    KB(f64),
    MB(f64),
    GB(f64),
    TB(f64),
}

impl Unit {
    /// 该单位对应多少 bytes
    pub fn bytes_factor(self) -> f64 {
        match self {
            Unit::Bytes => 1.0,

            // IEC (1024)
            Unit::KiB => 1024.0,
            Unit::MiB => 1024.0_f64.powi(2),
            Unit::GiB => 1024.0_f64.powi(3),
            Unit::TiB => 1024.0_f64.powi(4),

            // SI (1000)
            Unit::KB => 1000.0,
            Unit::MB => 1000.0_f64.powi(2),
            Unit::GB => 1000.0_f64.powi(3),
            Unit::TB => 1000.0_f64.powi(4),
        }
    }

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
            // human_iec 不会走到 SI 单位，这里兜底
            Flow::KB(v) => format!("{:.2} KB", v),
            Flow::MB(v) => format!("{:.2} MB", v),
            Flow::GB(v) => format!("{:.2} GB", v),
            Flow::TB(v) => format!("{:.2} TB", v),
        }
    }

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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pretty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_kib() {
        let f = Flow::BYTES(2048.0);
        let kib = f.to_unit(Unit::KiB);
        assert_eq!(kib, Flow::KIB(2.0));
    }

    #[test]
    fn test_bytes_to_kb() {
        let f = Flow::BYTES(2048.0);
        let kb = f.to_unit(Unit::KB);
        assert_eq!(kb, Flow::KB(2.048));
    }

    #[test]
    fn test_pretty() {
        let f = Flow::BYTES(2048.0);
        assert_eq!(f.pretty(), "2.00 KiB");
        assert_eq!(format!("{f}"), "2.00 KiB");
    }
}
