//! 系统平均负载采集。

use serde::Serialize;

/// 1、5、15 分钟平均负载。
#[derive(Debug, Clone, Serialize)]
pub struct LoadSnapshot {
    /// Windows 等不提供 Unix load average 语义的平台返回 `false`。
    pub supported: bool,
    /// 最近 1 分钟平均负载。
    pub one: f64,
    /// 最近 5 分钟平均负载。
    pub five: f64,
    /// 最近 15 分钟平均负载。
    pub fifteen: f64,
}

/// 采集系统平均负载。
pub(crate) fn collect() -> LoadSnapshot {
    let load = sysinfo::System::load_average();
    LoadSnapshot {
        supported: !cfg!(target_os = "windows"),
        one: load.one,
        five: load.five,
        fifteen: load.fifteen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_values_are_finite() {
        let snapshot = collect();
        assert!(snapshot.one.is_finite());
        assert!(snapshot.five.is_finite());
        assert!(snapshot.fifteen.is_finite());
    }
}
