//! Memory and disk alarms: pause publishers before the broker runs out of resources.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use firemq_core::Broker;
use sysinfo::{Disks, ProcessesToUpdate, System};
use tokio::task::JoinHandle;

/// When to raise alarms.
#[derive(Debug, Clone)]
pub struct AlarmConfig {
    /// Fraction of system memory the broker may use before publishers are paused.
    pub memory_high_watermark: f64,
    /// Absolute memory limit in bytes; overrides the watermark when set.
    pub memory_limit: Option<u64>,
    /// Free disk space (bytes) below which publishers are paused. Only checked with a data
    /// directory.
    pub disk_free_limit: u64,
    pub interval: Duration,
}

impl Default for AlarmConfig {
    fn default() -> Self {
        Self {
            memory_high_watermark: 0.4,
            memory_limit: None,
            disk_free_limit: 50 * 1000 * 1000,
            interval: Duration::from_secs(1),
        }
    }
}

/// Parses sizes such as `512MB`, `2GiB`, `1000000` or `0.4` (a fraction, for memory).
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "kib" | "k" => 1024.0,
        "mib" | "m" => 1024.0 * 1024.0,
        "gib" | "g" => 1024.0 * 1024.0 * 1024.0,
        "tib" | "t" => 1024f64.powi(4),
        _ => return None,
    };
    Some((n * mult) as u64)
}

/// Starts the monitor. It samples process memory and free disk space every interval and
/// blocks or unblocks publishers on the broker.
pub fn spawn(broker: Broker, cfg: AlarmConfig, data_dir: Option<PathBuf>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sys = System::new();
        sys.refresh_memory();
        let limit = cfg
            .memory_limit
            .unwrap_or_else(|| (sys.total_memory() as f64 * cfg.memory_high_watermark) as u64);
        let pid = sysinfo::get_current_pid().ok();
        let dir = data_dir.and_then(|d| std::fs::canonicalize(d).ok());
        let stats = broker.node_stats();
        stats.mem_limit.store(limit, Ordering::Relaxed);
        stats
            .disk_limit
            .store(cfg.disk_free_limit, Ordering::Relaxed);
        let mut disks = Disks::new_with_refreshed_list();
        let mut tick = tokio::time::interval(cfg.interval);
        loop {
            tick.tick().await;
            let used = match pid {
                Some(pid) => {
                    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
                    sys.process(pid).map(|p| p.memory()).unwrap_or(0)
                }
                None => 0,
            };
            stats.mem_used.store(used, Ordering::Relaxed);
            let mem_alarm = limit > 0 && used > limit;
            let mut disk_alarm = false;
            if let Some(dir) = &dir {
                disks.refresh(true);
                let free = disks
                    .list()
                    .iter()
                    .filter(|d| dir.starts_with(d.mount_point()))
                    .max_by_key(|d| d.mount_point().as_os_str().len())
                    .map(|d| d.available_space());
                if let Some(free) = free {
                    stats.disk_free.store(free, Ordering::Relaxed);
                    disk_alarm = free < cfg.disk_free_limit;
                }
            }
            if mem_alarm != stats.mem_alarm.swap(mem_alarm, Ordering::Relaxed) {
                if mem_alarm {
                    tracing::warn!(
                        used,
                        limit,
                        "memory alarm: publishers are paused until memory use drops"
                    );
                } else {
                    tracing::info!(used, limit, "memory alarm cleared");
                }
            }
            if disk_alarm != stats.disk_alarm.swap(disk_alarm, Ordering::Relaxed) {
                if disk_alarm {
                    tracing::warn!(
                        limit = cfg.disk_free_limit,
                        "disk alarm: free space is low; publishers are paused"
                    );
                } else {
                    tracing::info!("disk alarm cleared");
                }
            }
            broker.set_blocked(mem_alarm || disk_alarm);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn sizes_parse() {
        assert_eq!(parse_size("1000"), Some(1000));
        assert_eq!(parse_size("50MB"), Some(50_000_000));
        assert_eq!(parse_size("2GiB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1.5 kb"), Some(1500));
        assert_eq!(parse_size("lots"), None);
    }
}
