//! The HTTP listener: management API, metrics, health probe, web UI, and the FireMQ
//! protocol over WebSocket at `/fire`.

use std::net::SocketAddr;

use axum::Router;
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::response::{IntoResponse, Json};
use axum::routing::get;

pub(crate) use crate::mgmt::AppState;
use crate::native::serve_ws;
use crate::{metrics, mgmt};

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/fire", get(fire_ws))
        .route("/metrics", get(metrics::metrics))
        .route("/healthz", get(healthz))
        .with_state(state.clone())
        .merge(mgmt::router(state))
}

async fn fire_ws(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.protocols(["firemq.v1"])
        .on_upgrade(move |socket| serve_ws(s.broker, s.native, socket, peer, s.local, s.tls))
}

/// Unauthenticated liveness probe for containers and load balancers.
async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}
