//! HTTP Basic authentication for the management API, with RabbitMQ's user tags.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use firemq_core::User;

use super::{ApiError, AppState};

/// The authenticated user, available to handlers as an extension.
#[derive(Clone)]
pub struct Caller(pub Arc<User>);

const READ_TAGS: [&str; 4] = ["administrator", "monitoring", "management", "policymaker"];

fn credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("basic "))?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

fn unauthorized(reason: &str) -> Response {
    let mut r = ApiError::new(StatusCode::UNAUTHORIZED, "not_authorised", reason).into_response();
    r.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        "Basic realm=\"FireMQ Management\""
            .parse()
            .expect("static header"),
    );
    r
}

pub async fn require_user(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some((name, pass)) = credentials(req.headers()) else {
        return unauthorized("Login with an administrator, management or monitoring user");
    };
    let user = match state.broker.users().authenticate(&name, &pass, "PLAIN") {
        Ok(u) => u,
        Err(_) => return unauthorized("Login failed"),
    };
    if state.broker.is_loopback_only(&user.name) && !peer.ip().is_loopback() {
        return unauthorized(&format!(
            "User '{}' can only log in via localhost",
            user.name
        ));
    }
    if !READ_TAGS.iter().any(|t| user.has_tag(t)) {
        return ApiError::new(
            StatusCode::FORBIDDEN,
            "not_authorised",
            "Not management user: add the 'management' or 'administrator' tag",
        )
        .into_response();
    }
    let writes = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    if writes && !user.is_admin() {
        let path = req.uri().path();
        let policymaker = user.has_tag("policymaker")
            && (path.starts_with("/api/policies") || path.starts_with("/api/parameters"));
        // Monitoring tools may read messages; only administrators change state.
        let allowed = policymaker || (path.ends_with("/get") && user.has_tag("management"));
        if !allowed {
            return ApiError::new(
                StatusCode::FORBIDDEN,
                "not_authorised",
                "Administrator tag required for this operation",
            )
            .into_response();
        }
    }
    req.extensions_mut().insert(Caller(user));
    next.run(req).await
}
