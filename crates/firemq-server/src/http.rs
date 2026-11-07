//! The HTTP listener: the FireMQ protocol over WebSocket at `/fire`, and health checks.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use firemq_core::Broker;

use crate::native::{NativeConfig, serve_ws};

#[derive(Clone)]
pub(crate) struct AppState {
    pub broker: Broker,
    pub native: Arc<NativeConfig>,
    pub local: SocketAddr,
    pub tls: bool,
}

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/fire", get(fire_ws))
        .route("/api/health/checks/alive", get(alive))
        .with_state(state)
}

async fn fire_ws(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.protocols(["firemq.v1"])
        .on_upgrade(move |socket| serve_ws(s.broker, s.native, socket, peer, s.local, s.tls))
}

async fn alive() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}
