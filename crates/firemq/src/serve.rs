//! `firemq serve`: run the broker.

use std::path::PathBuf;
use std::time::Instant;

use clap::Args;
use firemq_server::{AlarmConfig, Server, ServerConfig, TlsConfig};

use crate::config::{self, FileConfig};

#[derive(Args, Debug, Default, Clone)]
pub struct ServeArgs {
    /// Settings file. Defaults to ./firemq.toml when it exists.
    #[arg(long, short, env = "FIREMQ_CONFIG")]
    pub config: Option<PathBuf>,
    /// AMQP 0-9-1 listener (host:port, port, or off). Default 0.0.0.0:5672.
    #[arg(long, env = "FIREMQ_AMQP")]
    pub amqp: Option<String>,
    /// AMQP over TLS listener. Default 0.0.0.0:5671 when a certificate is set.
    #[arg(long, env = "FIREMQ_AMQPS")]
    pub amqps: Option<String>,
    /// FireMQ protocol listener. Default 0.0.0.0:5673.
    #[arg(long, env = "FIREMQ_FIRE")]
    pub fire: Option<String>,
    /// FireMQ protocol over TLS listener. Default 0.0.0.0:5674 when a certificate is set.
    #[arg(long, env = "FIREMQ_FIRES")]
    pub fires: Option<String>,
    /// HTTP listener for the UI, API, metrics and WebSocket. Default 0.0.0.0:15672.
    #[arg(long, env = "FIREMQ_HTTP")]
    pub http: Option<String>,
    /// TLS certificate chain (PEM). Enables the TLS listeners.
    #[arg(long, env = "FIREMQ_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// TLS private key (PEM).
    #[arg(long, env = "FIREMQ_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Directory for durable queues, messages and definitions. Default ./firemq-data.
    #[arg(long, env = "FIREMQ_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// Keep everything in memory; nothing survives a restart.
    #[arg(long, env = "FIREMQ_EPHEMERAL")]
    pub ephemeral: bool,
    /// Development mode: in memory, guest allowed from any host, debug logs.
    #[arg(long, env = "FIREMQ_DEV")]
    pub dev: bool,
    /// Sync persistent messages to disk before confirming them. Default true.
    #[arg(long, env = "FIREMQ_FSYNC", action = clap::ArgAction::Set)]
    pub fsync: Option<bool>,
    /// Pause publishers above this fraction of system memory. Default 0.4.
    #[arg(long, env = "FIREMQ_MEMORY_HIGH_WATERMARK")]
    pub memory_high_watermark: Option<f64>,
    /// Absolute memory limit such as 2GiB; overrides the watermark.
    #[arg(long, env = "FIREMQ_MEMORY_LIMIT")]
    pub memory_limit: Option<String>,
    /// Pause publishers when free disk drops below this, such as 1GB. Default 50MB.
    #[arg(long, env = "FIREMQ_DISK_FREE_LIMIT")]
    pub disk_free_limit: Option<String>,
    /// Users who may only connect from localhost, comma-separated. Default guest.
    #[arg(long, env = "FIREMQ_LOOPBACK_USERS")]
    pub loopback_users: Option<String>,
    /// User created on first start when there are no users. Default guest.
    #[arg(long, env = "FIREMQ_DEFAULT_USER")]
    pub default_user: Option<String>,
    /// Password of the default user. Default guest.
    #[arg(long, env = "FIREMQ_DEFAULT_PASS")]
    pub default_pass: Option<String>,
    /// Log level: error, warn, info, debug or trace. Default info.
    #[arg(long, env = "FIREMQ_LOG")]
    pub log_level: Option<String>,
    /// Log format: text or json. Default text.
    #[arg(long, env = "FIREMQ_LOG_FORMAT")]
    pub log_format: Option<String>,
}

fn size(s: &str, what: &str) -> Result<u64, String> {
    firemq_server::alarms::parse_size(s).ok_or_else(|| {
        format!("invalid {what} '{s}'; use a number with a unit such as 512MB or 2GiB")
    })
}

/// Settings after merging flags, environment, file and defaults.
pub struct Resolved {
    pub server: ServerConfig,
    pub log_level: String,
    pub log_format: String,
    pub data_dir: Option<PathBuf>,
}

pub fn resolve(a: &ServeArgs, f: FileConfig) -> Result<Resolved, String> {
    let mut c = ServerConfig::default();
    let addr = |flag: &Option<String>, file: &Option<String>, default: &str| {
        let v = flag
            .clone()
            .or_else(|| file.clone())
            .unwrap_or_else(|| default.to_string());
        config::listener(&v)
    };
    c.amqp = addr(&a.amqp, &f.listeners.amqp, "0.0.0.0:5672")?;
    c.amqps = addr(&a.amqps, &f.listeners.amqps, "0.0.0.0:5671")?;
    c.fire = addr(&a.fire, &f.listeners.fire, "0.0.0.0:5673")?;
    c.fires = addr(&a.fires, &f.listeners.fires, "0.0.0.0:5674")?;
    c.http = addr(&a.http, &f.listeners.http, "0.0.0.0:15672")?;
    c.tls = match (&a.tls_cert, &a.tls_key, &f.tls) {
        (Some(cert), Some(key), _) => Some(TlsConfig {
            cert: cert.clone(),
            key: key.clone(),
        }),
        (_, _, Some(t)) => Some(TlsConfig {
            cert: t.cert.clone(),
            key: t.key.clone(),
        }),
        _ => None,
    };
    let ephemeral = a.dev || a.ephemeral || f.storage.ephemeral.unwrap_or(false);
    let data_dir = a
        .data_dir
        .clone()
        .or(f.storage.data_dir.clone())
        .unwrap_or_else(|| PathBuf::from("firemq-data"));
    c.data_dir = (!ephemeral).then(|| data_dir.clone());
    c.store.fsync = a.fsync.or(f.storage.fsync).unwrap_or(true);
    let memory_limit = match a.memory_limit.clone().or(f.limits.memory_limit.clone()) {
        Some(s) => Some(size(&s, "memory limit")?),
        None => None,
    };
    let disk = a
        .disk_free_limit
        .clone()
        .or(f.limits.disk_free_limit.clone())
        .unwrap_or_else(|| "50MB".into());
    c.alarms = Some(AlarmConfig {
        memory_high_watermark: a
            .memory_high_watermark
            .or(f.limits.memory_high_watermark)
            .unwrap_or(0.4),
        memory_limit,
        disk_free_limit: size(&disk, "disk free limit")?,
        ..Default::default()
    });
    c.broker.loopback_users = if a.dev {
        Vec::new()
    } else {
        match &a.loopback_users {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .map(String::from)
                .collect(),
            None => f
                .auth
                .loopback_users
                .clone()
                .unwrap_or_else(|| vec!["guest".into()]),
        }
    };
    c.default_user = a
        .default_user
        .clone()
        .or(f.auth.default_user.clone())
        .unwrap_or_else(|| "guest".into());
    c.default_pass = a
        .default_pass
        .clone()
        .or(f.auth.default_pass.clone())
        .unwrap_or_else(|| "guest".into());
    let log_level = a
        .log_level
        .clone()
        .or(f.log.level.clone())
        .unwrap_or_else(|| if a.dev { "debug".into() } else { "info".into() });
    let log_format = a
        .log_format
        .clone()
        .or(f.log.format.clone())
        .unwrap_or_else(|| "text".into());
    Ok(Resolved {
        server: c,
        log_level,
        log_format,
        data_dir: (!ephemeral).then_some(data_dir),
    })
}

fn init_logging(level: &str, format: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if format == "json" {
        builder.json().init();
    } else {
        builder.init();
    }
}

/// Raises the open-files limit to the hard limit; each connection and segment uses one.
fn raise_fd_limit() {
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            // macOS rejects values above OPEN_MAX even when the hard limit is "unlimited".
            let target = if cfg!(target_os = "macos") {
                lim.rlim_max.min(10240)
            } else {
                lim.rlim_max
            };
            lim.rlim_cur = target.max(lim.rlim_cur);
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

pub fn run(args: ServeArgs) -> Result<(), String> {
    let started = Instant::now();
    let file = config::load(args.config.as_deref())?;
    let r = resolve(&args, file)?;
    init_logging(&r.log_level, &r.log_format);
    raise_fd_limit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("firemq")
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let loopback_only = r
            .server
            .broker
            .loopback_users
            .contains(&r.server.default_user);
        let server = Server::start(r.server.clone())
            .await
            .map_err(|e| format!("cannot start: {e}"))?;
        banner(&server, &r, loopback_only, started);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate() => {}
        }
        tracing::info!("shutting down");
        server.shutdown().await;
        Ok(())
    })
}

#[cfg(unix)]
async fn terminate() {
    if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        s.recv().await;
    } else {
        std::future::pending::<()>().await
    }
}

#[cfg(not(unix))]
async fn terminate() {
    std::future::pending::<()>().await
}

fn banner(server: &Server, r: &Resolved, loopback_only: bool, started: Instant) {
    let a = server.addresses();
    let user = &r.server.default_user;
    let pass = &r.server.default_pass;
    let port = |x: Option<std::net::SocketAddr>| x.map(|x| x.port());
    println!(
        "\nFireMQ {} is ready in {} ms\n",
        env!("CARGO_PKG_VERSION"),
        started.elapsed().as_millis()
    );
    if let Some(p) = port(a.amqp) {
        println!("  AMQP 0-9-1   amqp://{user}:{pass}@localhost:{p}");
    }
    if let Some(p) = port(a.amqps) {
        println!("  AMQPS        amqps://{user}:{pass}@localhost:{p}");
    }
    if let Some(p) = port(a.fire) {
        println!("  FireMQ       fire://{user}:{pass}@localhost:{p}");
    }
    if let Some(p) = port(a.fires) {
        println!("  FireMQ TLS   fires://{user}:{pass}@localhost:{p}");
    }
    if let Some(p) = port(a.http) {
        println!("  WebSocket    ws://localhost:{p}/fire");
        println!("  Management   http://localhost:{p}");
        println!("  Metrics      http://localhost:{p}/metrics");
    }
    match &r.data_dir {
        Some(d) => {
            let rec = server.recovery();
            println!(
                "  Data         {} ({} queues, {} messages recovered)",
                d.display(),
                rec.queues,
                rec.messages
            );
        }
        None => println!("  Data         in memory; nothing survives a restart"),
    }
    if loopback_only {
        println!(
            "\n  '{user}' can only connect from this machine. Add users, or set FIREMQ_LOOPBACK_USERS= to allow it elsewhere."
        );
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_override_file_and_defaults() {
        let file: FileConfig = toml::from_str(
            "[listeners]\namqp = \"6000\"\nfire = \"off\"\n[storage]\nfsync = false\n[auth]\nloopback_users = []",
        )
        .unwrap();
        let args = ServeArgs {
            amqp: Some("7000".into()),
            ..Default::default()
        };
        let r = resolve(&args, file).unwrap();
        assert_eq!(r.server.amqp.unwrap().port(), 7000);
        assert!(r.server.fire.is_none());
        assert_eq!(r.server.http.unwrap().port(), 15672);
        assert!(!r.server.store.fsync);
        assert!(r.server.broker.loopback_users.is_empty());
        assert_eq!(r.server.data_dir, Some(PathBuf::from("firemq-data")));
    }

    #[test]
    fn dev_mode_is_ephemeral_and_open() {
        let args = ServeArgs {
            dev: true,
            ..Default::default()
        };
        let r = resolve(&args, FileConfig::default()).unwrap();
        assert!(r.server.data_dir.is_none());
        assert!(r.server.broker.loopback_users.is_empty());
        assert_eq!(r.log_level, "debug");
    }
}
