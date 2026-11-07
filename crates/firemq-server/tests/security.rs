//! TLS listeners and permission checks.

use std::net::SocketAddr;
use std::sync::Arc;

use firemq_client::{Client, ConnectOptions, QueueOptions};
use firemq_core::Permission;
use firemq_server::{Server, ServerConfig, TlsConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn local() -> Option<SocketAddr> {
    Some("127.0.0.1:0".parse().unwrap())
}

struct Certs {
    _dir: tempfile::TempDir,
    tls: TlsConfig,
    cert_pem: String,
}

fn certs() -> Certs {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.key_pair.serialize_pem()).unwrap();
    Certs {
        _dir: dir,
        tls: TlsConfig { cert, key },
        cert_pem: ck.cert.pem(),
    }
}

async fn start_tls(c: &Certs) -> Server {
    Server::start(ServerConfig {
        amqp: local(),
        amqps: local(),
        fire: local(),
        fires: local(),
        http: None,
        tls: Some(c.tls.clone()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn native_protocol_over_tls() {
    let c = certs();
    let s = start_tls(&c).await;
    let port = s.addresses().fires.unwrap().port();
    let mut opts = ConnectOptions::from_url(&format!("fires://localhost:{port}")).unwrap();
    opts.tls_ca_pem = Some(c.cert_pem.clone().into_bytes());
    let client = Client::connect_with(opts).await.unwrap();
    client
        .declare_queue("secure", QueueOptions::default())
        .await
        .unwrap();
    client.publish("", "secure", "over tls").await.unwrap();
    let m = client.get("secure", true).await.unwrap().unwrap();
    assert_eq!(m.body_str(), Some("over tls"));

    // An untrusted certificate is refused unless verification is explicitly disabled.
    let strict = Client::connect(&format!("fires://localhost:{port}")).await;
    assert!(strict.is_err());
    let mut insecure = ConnectOptions::from_url(&format!("fires://localhost:{port}")).unwrap();
    insecure.tls_insecure = true;
    Client::connect_with(insecure).await.unwrap();
}

#[tokio::test]
async fn amqps_listener_speaks_amqp() {
    use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
    let c = certs();
    let s = start_tls(&c).await;
    let addr = s.addresses().amqps.unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(c.cert_pem.as_bytes()).unwrap())
        .unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    tls.write_all(b"AMQP\x00\x00\x09\x01").await.unwrap();
    let mut buf = [0u8; 11];
    tls.read_exact(&mut buf).await.unwrap();
    // A method frame on channel 0 carrying connection.start (class 10, method 10).
    assert_eq!(buf[0], 1);
    assert_eq!(&buf[7..11], &[0, 10, 0, 10]);
}

#[tokio::test]
async fn permissions_limit_what_a_user_can_touch() {
    let s = Server::start(ServerConfig {
        amqp: None,
        amqps: None,
        fire: local(),
        fires: None,
        http: None,
        ..Default::default()
    })
    .await
    .unwrap();
    let users = s.broker().users();
    users.put("app", Some("secret"), vec![]);
    users
        .set_permission(
            "app",
            "/",
            Permission::new("^app\\.", "^(app\\.|amq\\.default)", "^app\\.").unwrap(),
        )
        .unwrap();
    let url = format!("fire://app:secret@{}", s.fire_addr().unwrap());
    let c = Client::connect(&url).await.unwrap();
    c.declare_queue("app.jobs", QueueOptions::default())
        .await
        .unwrap();
    let err = c
        .declare_queue("billing", QueueOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(403));
    assert!(err.to_string().contains("refused for user 'app'"), "{err}");
    c.publish("", "app.jobs", "ok").await.unwrap();
    let err = c.get("billing", true).await.unwrap_err();
    assert_eq!(err.code(), Some(403));

    // A user without access to the vhost cannot connect.
    users.put("nobody", Some("pw"), vec![]);
    let err = Client::connect(&format!("fire://nobody:pw@{}", s.fire_addr().unwrap()))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Some(530));
}
