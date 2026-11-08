use std::fmt;

/// How a client reaches the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    Tls,
    Ws,
    Wss,
}

/// A parsed `fire://`, `fires://`, `ws://` or `wss://` connection URL.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectUrl {
    pub transport: Transport,
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub user: String,
    pub pass: String,
    /// WebSocket endpoint path, `/fire` by default.
    pub path: String,
}

impl fmt::Debug for ConnectUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectUrl")
            .field("transport", &self.transport)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("pass", &"***")
            .field("path", &self.path)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UrlError {
    #[error("invalid URL: {0}")]
    Invalid(String),
    #[error("unsupported scheme {0:?}; use fire, fires, ws or wss")]
    Scheme(String),
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(v) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl ConnectUrl {
    /// Parses a connection URL, filling in defaults: user and password `guest`, vhost `/`,
    /// ports 5673 (`fire`), 5674 (`fires`), 15672 (`ws`) and 15671 (`wss`).
    ///
    /// For `fire` and `fires` the path is the vhost (`fire://host/my-vhost`). For `ws` and
    /// `wss` the path is the endpoint and the vhost is the `vhost` query parameter.
    pub fn parse(input: &str) -> Result<ConnectUrl, UrlError> {
        let url = url::Url::parse(input).map_err(|e| UrlError::Invalid(e.to_string()))?;
        let (transport, default_port) = match url.scheme() {
            "fire" => (Transport::Tcp, crate::DEFAULT_PORT),
            "fires" => (Transport::Tls, crate::DEFAULT_TLS_PORT),
            "ws" => (Transport::Ws, 15672),
            "wss" => (Transport::Wss, 15671),
            other => return Err(UrlError::Scheme(other.to_string())),
        };
        let host = url
            .host_str()
            .filter(|h| !h.is_empty())
            .unwrap_or("localhost")
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = url.port().unwrap_or(default_port);
        let user = match url.username() {
            "" => "guest".to_string(),
            u => percent_decode(u),
        };
        let pass = url
            .password()
            .map(percent_decode)
            .unwrap_or_else(|| "guest".to_string());
        let query_vhost = url
            .query_pairs()
            .find(|(k, _)| k == "vhost")
            .map(|(_, v)| v.into_owned());
        let (vhost, path) = match transport {
            Transport::Tcp | Transport::Tls => {
                let raw = url.path().trim_start_matches('/');
                let vhost = if raw.is_empty() {
                    query_vhost.unwrap_or_else(|| "/".to_string())
                } else {
                    percent_decode(raw)
                };
                (vhost, String::new())
            }
            Transport::Ws | Transport::Wss => {
                let path = match url.path() {
                    "" | "/" => "/fire".to_string(),
                    p => p.to_string(),
                };
                (query_vhost.unwrap_or_else(|| "/".to_string()), path)
            }
        };
        Ok(ConnectUrl {
            transport,
            host,
            port,
            vhost,
            user,
            pass,
            path,
        })
    }

    /// `host:port`, with IPv6 hosts bracketed.
    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fill_in() {
        let u = ConnectUrl::parse("fire://localhost").unwrap();
        assert_eq!(u.transport, Transport::Tcp);
        assert_eq!(u.port, 5673);
        assert_eq!(u.vhost, "/");
        assert_eq!((u.user.as_str(), u.pass.as_str()), ("guest", "guest"));
    }

    #[test]
    fn credentials_and_vhost_are_decoded() {
        let u = ConnectUrl::parse("fires://app:p%40ss@mq.example.com:7000/prod%2Feu").unwrap();
        assert_eq!(u.transport, Transport::Tls);
        assert_eq!(u.host, "mq.example.com");
        assert_eq!(u.port, 7000);
        assert_eq!(u.user, "app");
        assert_eq!(u.pass, "p@ss");
        assert_eq!(u.vhost, "prod/eu");
    }

    #[test]
    fn websocket_urls_use_query_vhost() {
        let u = ConnectUrl::parse("ws://localhost:15672/fire?vhost=dev").unwrap();
        assert_eq!(u.transport, Transport::Ws);
        assert_eq!(u.path, "/fire");
        assert_eq!(u.vhost, "dev");
        let u = ConnectUrl::parse("wss://mq.example.com").unwrap();
        assert_eq!((u.port, u.path.as_str()), (15671, "/fire"));
    }

    #[test]
    fn other_schemes_are_rejected() {
        assert!(matches!(
            ConnectUrl::parse("amqp://localhost"),
            Err(UrlError::Scheme(_))
        ));
    }

    #[test]
    fn ipv6_hosts_are_bracketed_in_authority() {
        let u = ConnectUrl::parse("fire://[::1]:5673").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.authority(), "[::1]:5673");
    }
}
