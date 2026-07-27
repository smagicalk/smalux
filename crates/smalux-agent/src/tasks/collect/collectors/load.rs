//! 系统平均负载采集。

pub use smalux_protocol::agent::v1::LoadSnapshot;

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
