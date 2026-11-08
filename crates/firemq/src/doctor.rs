//! `firemq doctor`: check the environment and a running broker.

use std::net::TcpListener;
use std::path::PathBuf;

use clap::Args;

#[derive(Args, Debug)]
pub struct DoctorArgs {
    /// Data directory to check.
    #[arg(long, env = "FIREMQ_DATA_DIR", default_value = "firemq-data")]
    pub data_dir: PathBuf,
    /// Management URL of a running broker.
    #[arg(
        long,
        env = "FIREMQ_HTTP_URL",
        default_value = "http://localhost:15672"
    )]
    pub url: String,
}

struct Report {
    problems: usize,
}

impl Report {
    fn ok(&self, what: &str, detail: impl AsRef<str>) {
        println!("  ok    {what:<22} {}", detail.as_ref());
    }

    fn warn(&mut self, what: &str, detail: impl AsRef<str>) {
        self.problems += 1;
        println!("  warn  {what:<22} {}", detail.as_ref());
    }

    fn info(&self, what: &str, detail: impl AsRef<str>) {
        println!("  info  {what:<22} {}", detail.as_ref());
    }
}

/// Soft and hard open-files limits.
#[allow(clippy::unnecessary_cast)] // rlim_t is u32 on some platforms
fn fd_limit() -> Option<(u64, u64)> {
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            return Some((lim.rlim_cur as u64, lim.rlim_max as u64));
        }
    }
    None
}

fn limit_text(n: u64) -> String {
    if n >= 1 << 40 {
        "unlimited".into()
    } else {
        n.to_string()
    }
}

pub fn run(a: DoctorArgs) -> Result<(), String> {
    let mut r = Report { problems: 0 };
    println!("FireMQ {} doctor\n", env!("CARGO_PKG_VERSION"));
    r.info(
        "platform",
        format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
    );
    r.info(
        "cpus",
        std::thread::available_parallelism()
            .map(|n| n.get().to_string())
            .unwrap_or_else(|_| "unknown".into()),
    );
    match fd_limit() {
        Some((_, hard)) if hard < 4096 => r.warn(
            "open files limit",
            format!("hard limit {hard}; raise it to at least 65536 for many connections or queues"),
        ),
        Some((soft, hard)) => r.ok(
            "open files limit",
            format!(
                "soft {}, hard {} (firemq raises soft to hard at start)",
                limit_text(soft),
                limit_text(hard)
            ),
        ),
        None => r.info("open files limit", "unknown on this platform"),
    }
    let dir = &a.data_dir;
    if dir.exists() {
        let probe = dir.join(".doctor-probe");
        match std::fs::write(&probe, b"ok") {
            Ok(()) => {
                let _ = std::fs::remove_file(&probe);
                r.ok("data directory", format!("{} is writable", dir.display()));
            }
            Err(e) => r.warn(
                "data directory",
                format!("{} is not writable: {e}", dir.display()),
            ),
        }
        if dir.join("firemq.lock").exists() {
            r.info(
                "data directory",
                "has a lock file (fine; a running broker holds it)",
            );
        }
    } else {
        r.info(
            "data directory",
            format!(
                "{} does not exist yet; FireMQ creates it on start",
                dir.display()
            ),
        );
    }
    let ports = [(5672, "AMQP"), (5673, "FireMQ"), (15672, "HTTP")];
    let mut in_use = Vec::new();
    for (port, name) in ports {
        match TcpListener::bind(("0.0.0.0", port)) {
            Ok(_) => r.ok(&format!("port {port}"), format!("free for {name}")),
            Err(_) => in_use.push((port, name)),
        }
    }
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(std::time::Duration::from_secs(2)))
        .build()
        .into();
    let health = agent
        .get(format!("{}/healthz", a.url.trim_end_matches('/')))
        .call()
        .ok()
        .filter(|r| r.status().is_success());
    let running = health.is_some();
    for (port, name) in in_use {
        if running {
            r.ok(
                &format!("port {port}"),
                format!("used by the running broker ({name})"),
            );
        } else {
            r.warn(
                &format!("port {port}"),
                format!("in use by another process; RabbitMQ may be running. Stop it, or start FireMQ with --{} off or another port", name.to_lowercase().replace("firemq", "fire")),
            );
        }
    }
    if running {
        r.ok("broker", format!("reachable at {}", a.url));
    } else {
        r.info(
            "broker",
            format!("not running at {} (start it with `firemq`)", a.url),
        );
    }
    println!();
    if r.problems == 0 {
        println!("No problems found.");
        Ok(())
    } else {
        Err(format!("{} problem(s) found", r.problems))
    }
}
