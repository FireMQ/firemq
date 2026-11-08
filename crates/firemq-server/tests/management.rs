//! The management HTTP API, exercised over real HTTP.

use std::net::SocketAddr;

use firemq_client::{Client, QueueOptions};
use firemq_server::{Server, ServerConfig};
use serde_json::{Value, json};

fn local() -> Option<SocketAddr> {
    Some("127.0.0.1:0".parse().unwrap())
}

async fn start() -> Server {
    Server::start(ServerConfig {
        amqp: local(),
        amqps: None,
        fire: local(),
        fires: None,
        http: local(),
        alarms: None,
        ..Default::default()
    })
    .await
    .unwrap()
}

/// A blocking HTTP client with Basic auth, run off the async runtime.
#[derive(Clone)]
struct Api {
    base: String,
    auth: Option<String>,
}

struct Reply {
    status: u16,
    body: Value,
    location: Option<String>,
}

impl Api {
    fn new(s: &Server, user: &str, pass: &str) -> Api {
        use base64::Engine;
        Api {
            base: format!("http://{}", s.http_addr().unwrap()),
            auth: Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
            )),
        }
    }

    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Reply {
        let api = self.clone();
        let method = method.to_string();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .http_status_as_error(false)
                .build()
                .into();
            let url = format!("{}{}", api.base, path);
            let builder = ureq::http::Request::builder()
                .method(method.as_str())
                .uri(&url);
            let builder = match &api.auth {
                Some(a) => builder.header("Authorization", a),
                None => builder,
            };
            let mut resp = match body {
                Some(b) => agent
                    .run(
                        builder
                            .header("Content-Type", "application/json")
                            .body(b.to_string())
                            .unwrap(),
                    )
                    .unwrap(),
                None => agent.run(builder.body(()).unwrap()).unwrap(),
            };
            let status = resp.status().as_u16();
            let location = resp
                .headers()
                .get("location")
                .map(|v| v.to_str().unwrap().to_string());
            let text = resp.body_mut().read_to_string().unwrap_or_default();
            let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
            Reply {
                status,
                body,
                location,
            }
        })
        .await
        .unwrap()
    }

    async fn get(&self, path: &str) -> Reply {
        self.call("GET", path, None).await
    }
}

#[tokio::test]
async fn overview_and_auth() {
    let s = start().await;
    let api = Api::new(&s, "guest", "guest");
    let r = api.get("/api/overview").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["product_name"], "FireMQ");
    assert!(r.body["listeners"].as_array().unwrap().len() >= 3);
    let anonymous = Api {
        auth: None,
        ..api.clone()
    };
    assert_eq!(anonymous.get("/api/overview").await.status, 401);
    let wrong = Api::new(&s, "guest", "nope");
    assert_eq!(wrong.get("/api/queues").await.status, 401);
    assert_eq!(api.get("/api/whoami").await.body["name"], "guest");
    assert_eq!(anonymous.get("/healthz").await.status, 200);
}

#[tokio::test]
async fn queues_exchanges_bindings_publish_and_get() {
    let s = start().await;
    let api = Api::new(&s, "guest", "guest");
    let r = api
        .call(
            "PUT",
            "/api/queues/%2F/orders",
            Some(json!({"durable": true, "arguments": {"x-max-length": 100}})),
        )
        .await;
    assert_eq!(r.status, 201);
    assert_eq!(
        api.call(
            "PUT",
            "/api/queues/%2F/orders",
            Some(json!({"durable": true, "arguments": {"x-max-length": 100}}))
        )
        .await
        .status,
        204
    );
    // rabbitmqadmin sends booleans as strings.
    let r = api
        .call(
            "PUT",
            "/api/exchanges/%2F/shop",
            Some(json!({"type": "topic", "durable": "true"})),
        )
        .await;
    assert_eq!(r.status, 201);
    let r = api
        .call(
            "POST",
            "/api/bindings/%2F/e/shop/q/orders",
            Some(json!({"routing_key": "order.*"})),
        )
        .await;
    assert_eq!(r.status, 201);
    let props_key = r.location.unwrap();
    let r = api.get("/api/bindings/%2F/e/shop/q/orders").await;
    assert_eq!(r.body.as_array().unwrap().len(), 1);
    assert_eq!(r.body[0]["routing_key"], "order.*");

    let r = api
        .call(
            "POST",
            "/api/exchanges/%2F/shop/publish",
            Some(json!({"routing_key": "order.created", "payload": "{\"id\":1}", "payload_encoding": "string", "properties": {"content_type": "application/json", "headers": {"source": "api"}}})),
        )
        .await;
    assert_eq!(r.body["routed"], true);
    let r = api
        .call("POST", "/api/exchanges/%2F/amq.default/publish", Some(json!({"routing_key": "orders", "payload": "aGk=", "payload_encoding": "base64", "properties": {}})))
        .await;
    assert_eq!(r.body["routed"], true);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let q = api.get("/api/queues/%2F/orders").await.body;
    assert_eq!(q["messages_ready"], 2);
    assert_eq!(q["arguments"]["x-max-length"], 100);

    let got = api
        .call(
            "POST",
            "/api/queues/%2F/orders/get",
            Some(json!({"count": 5, "ackmode": "ack_requeue_true", "encoding": "auto"})),
        )
        .await
        .body;
    assert_eq!(got.as_array().unwrap().len(), 2);
    assert_eq!(got[0]["payload"], "{\"id\":1}");
    assert_eq!(got[0]["properties"]["headers"]["source"], "api");
    assert_eq!(got[1]["payload"], "hi");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // ack_requeue_true leaves the messages in place.
    assert_eq!(
        api.get("/api/queues/%2F/orders").await.body["messages_ready"],
        2
    );
    assert_eq!(
        api.call("DELETE", "/api/queues/%2F/orders/contents", None)
            .await
            .status,
        204
    );

    let r = api
        .call(
            "DELETE",
            &format!("/api/bindings/%2F/e/shop/q/orders/{props_key}"),
            None,
        )
        .await;
    assert_eq!(r.status, 204);
    assert_eq!(
        api.get("/api/bindings/%2F/e/shop/q/orders").await.body,
        json!([])
    );
    assert_eq!(
        api.call("DELETE", "/api/queues/%2F/orders", None)
            .await
            .status,
        204
    );
    assert_eq!(api.get("/api/queues/%2F/orders").await.status, 404);

    let names: Vec<String> = api
        .get("/api/exchanges?columns=name,type")
        .await
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            assert!(
                e.get("durable").is_none(),
                "columns filter keeps only requested keys"
            );
            e["name"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(names.contains(&"shop".to_string()) && names.contains(&"amq.topic".to_string()));
}

#[tokio::test]
async fn users_permissions_and_tags() {
    let s = start().await;
    let admin = Api::new(&s, "guest", "guest");
    let r = admin
        .call(
            "PUT",
            "/api/users/monitor",
            Some(json!({"password": "pw", "tags": "monitoring"})),
        )
        .await;
    assert_eq!(r.status, 201);
    let r = admin
        .call(
            "PUT",
            "/api/permissions/%2F/monitor",
            Some(json!({"configure": "", "write": "", "read": ".*"})),
        )
        .await;
    assert_eq!(r.status, 201);
    let perms = admin.get("/api/users/monitor/permissions").await.body;
    assert_eq!(perms[0]["read"], ".*");

    let monitor = Api::new(&s, "monitor", "pw");
    assert_eq!(monitor.get("/api/queues").await.status, 200);
    let r = monitor
        .call("PUT", "/api/queues/%2F/sneaky", Some(json!({})))
        .await;
    assert_eq!(r.status, 403);

    admin
        .call(
            "PUT",
            "/api/users/plain",
            Some(json!({"password": "pw", "tags": ""})),
        )
        .await;
    assert_eq!(
        Api::new(&s, "plain", "pw")
            .get("/api/overview")
            .await
            .status,
        403
    );
    assert_eq!(
        admin.call("DELETE", "/api/users/plain", None).await.status,
        204
    );
    assert_eq!(
        Api::new(&s, "plain", "pw")
            .get("/api/overview")
            .await
            .status,
        401
    );
}

#[tokio::test]
async fn definitions_round_trip_between_brokers() {
    let a = start().await;
    let api_a = Api::new(&a, "guest", "guest");
    api_a.call("PUT", "/api/vhosts/staging", None).await;
    api_a
        .call(
            "PUT",
            "/api/exchanges/staging/events",
            Some(json!({"type": "fanout", "durable": true})),
        )
        .await;
    api_a
        .call(
            "PUT",
            "/api/queues/staging/audit",
            Some(json!({"durable": true})),
        )
        .await;
    api_a
        .call(
            "POST",
            "/api/bindings/staging/e/events/q/audit",
            Some(json!({"routing_key": ""})),
        )
        .await;
    api_a
        .call("PUT", "/api/policies/staging/ttl", Some(json!({"pattern": "^audit$", "definition": {"message-ttl": 1000}, "apply-to": "queues"})))
        .await;
    let defs = api_a.get("/api/definitions").await.body;
    assert_eq!(defs["queues"].as_array().unwrap().len(), 1);

    let b = start().await;
    let api_b = Api::new(&b, "guest", "guest");
    assert_eq!(
        api_b
            .call("POST", "/api/definitions", Some(defs))
            .await
            .status,
        204
    );
    let q = api_b.get("/api/queues/staging/audit").await;
    assert_eq!(q.status, 200);
    assert_eq!(q.body["policy"], "ttl");
    let bindings = api_b
        .get("/api/bindings/staging/e/events/q/audit")
        .await
        .body;
    assert_eq!(bindings.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn connections_consumers_metrics_and_health() {
    let s = start().await;
    let api = Api::new(&s, "guest", "guest");
    let c = Client::connect(&format!("fire://{}", s.fire_addr().unwrap()))
        .await
        .unwrap();
    c.declare_queue("work", QueueOptions::default())
        .await
        .unwrap();
    let _sub = c.subscribe("work", Default::default()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let conns = api.get("/api/connections").await.body;
    assert_eq!(conns.as_array().unwrap().len(), 1);
    assert!(conns[0]["protocol"].as_str().unwrap().starts_with("FireMQ"));
    let consumers = api.get("/api/consumers").await.body;
    assert_eq!(consumers[0]["queue"]["name"], "work");
    assert_eq!(consumers[0]["prefetch_count"], 100);

    let metrics = api.get("/metrics").await.body;
    let text = metrics.as_str().unwrap();
    assert!(text.contains("rabbitmq_queues 1"), "{text}");
    assert!(text.contains("firemq_queue_consumers{vhost=\"/\",queue=\"work\"} 1"));

    assert_eq!(
        api.get("/api/aliveness-test/%2F").await.body["status"],
        "ok"
    );
    assert_eq!(api.get("/api/health/checks/alarms").await.status, 200);
    let port = s.amqp_addr().unwrap().port();
    assert_eq!(
        api.get(&format!("/api/health/checks/port-listener/{port}"))
            .await
            .status,
        200
    );
    assert_eq!(
        api.get("/api/health/checks/port-listener/1").await.status,
        503
    );

    let name = conns[0]["name"]
        .as_str()
        .unwrap()
        .replace(' ', "%20")
        .replace('>', "%3E");
    assert_eq!(
        api.call("DELETE", &format!("/api/connections/{name}"), None)
            .await
            .status,
        204
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(c.is_closed());
}
