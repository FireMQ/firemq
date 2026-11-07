//! `firemq.toml`: optional settings file. Flags and `FIREMQ_*` variables override it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub listeners: Listeners,
    pub tls: Option<Tls>,
    pub storage: Storage,
    pub limits: Limits,
    pub auth: Auth,
    pub log: Log,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Listeners {
    pub amqp: Option<String>,
    pub amqps: Option<String>,
    pub fire: Option<String>,
    pub fires: Option<String>,
    pub http: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    pub data_dir: Option<PathBuf>,
    pub ephemeral: Option<bool>,
    pub fsync: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub memory_high_watermark: Option<f64>,
    pub memory_limit: Option<String>,
    pub disk_free_limit: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Auth {
    pub default_user: Option<String>,
    pub default_pass: Option<String>,
    pub loopback_users: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Log {
    pub level: Option<String>,
    pub format: Option<String>,
}

/// Reads a config file. A missing default file is fine; a missing explicit file is not.
pub fn load(path: Option<&Path>) -> Result<FileConfig, String> {
    let (path, explicit) = match path {
        Some(p) => (p.to_path_buf(), true),
        None => (PathBuf::from("firemq.toml"), false),
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !explicit => {
            Ok(FileConfig::default())
        }
        Err(e) => Err(format!("cannot read {}: {e}", path.display())),
    }
}

/// Parses a listener address. `off` (or an empty string) disables the listener; a bare
/// port means all interfaces.
pub fn listener(value: &str) -> Result<Option<SocketAddr>, String> {
    let v = value.trim();
    if v.is_empty() || v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    if let Ok(port) = v.trim_start_matches(':').parse::<u16>() {
        return Ok(Some(SocketAddr::from(([0, 0, 0, 0], port))));
    }
    v.parse()
        .map(Some)
        .map_err(|_| format!("invalid listen address '{v}'; use host:port, a port, or off"))
}

/// The file `firemq init` writes.
pub const TEMPLATE: &str = r#"# FireMQ configuration. Every setting is optional; the values below are the defaults.
# Command-line flags and FIREMQ_* environment variables override this file.

[listeners]
# Use "off" to disable a listener, a port such as "5672", or an address such as "127.0.0.1:5672".
amqp = "0.0.0.0:5672"     # AMQP 0-9-1, for RabbitMQ clients
amqps = "0.0.0.0:5671"    # AMQP over TLS; needs [tls]
fire = "0.0.0.0:5673"     # FireMQ protocol, for FireMQ SDKs
fires = "0.0.0.0:5674"    # FireMQ protocol over TLS; needs [tls]
http = "0.0.0.0:15672"    # management UI and API, /metrics, WebSocket at /fire

# [tls]
# cert = "/etc/firemq/cert.pem"   # certificate chain, PEM
# key = "/etc/firemq/key.pem"     # private key, PEM

[storage]
data_dir = "firemq-data"  # durable queues, messages and definitions
ephemeral = false         # true keeps everything in memory
fsync = true              # sync persistent messages to disk before confirming them

[limits]
memory_high_watermark = 0.4   # pause publishers above this fraction of system memory
# memory_limit = "2GiB"       # or an absolute limit
disk_free_limit = "50MB"      # pause publishers when free disk space drops below this

[auth]
default_user = "guest"        # created on first start when there are no users
default_pass = "guest"
loopback_users = ["guest"]    # users who may only connect from localhost

[log]
level = "info"                # error, warn, info, debug or trace
format = "text"               # text or json
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_template_parses() {
        let c: FileConfig = toml::from_str(TEMPLATE).unwrap();
        assert_eq!(c.listeners.amqp.as_deref(), Some("0.0.0.0:5672"));
        assert_eq!(c.storage.fsync, Some(true));
        assert_eq!(c.auth.loopback_users.unwrap(), vec!["guest".to_string()]);
    }

    #[test]
    fn typos_are_reported() {
        let err = toml::from_str::<FileConfig>("[storage]\ndatadir = \"x\"").unwrap_err();
        assert!(err.to_string().contains("datadir"));
    }

    #[test]
    fn listener_values() {
        assert_eq!(listener("off").unwrap(), None);
        assert_eq!(listener("5672").unwrap().unwrap().port(), 5672);
        assert_eq!(
            listener("127.0.0.1:1").unwrap().unwrap().to_string(),
            "127.0.0.1:1"
        );
        assert!(listener("nope").is_err());
    }
}
