use std::collections::HashMap;

use axum::{
    Json,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use sentinelmesh_core::ControlMessage;
use tracing::{info, warn};

use crate::{AppError, AppState};

/// Extract the authentication token from the request headers or query parameters.
/// Returns `Some(token)` if a token is found, `None` otherwise.
pub fn extract_token<'a>(
    headers: &'a HeaderMap,
    params: &'a HashMap<String, String>,
) -> Option<&'a str> {
    headers
        .get("x-sentinelmesh-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| params.get("token").map(String::as_str))
}

/// Validate a control-plane token against the configured API keys.
/// Returns `true` if the token is present and matches one of the `api_keys`.
pub fn validate_control_token(
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    api_keys: &[String],
) -> bool {
    match extract_token(headers, params) {
        Some(t) => api_keys.iter().any(|k| k == t),
        None => false,
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if validate_control_token(&headers, &params, &state.api_keys) {
        info!(result = "authenticated", "control plane connection attempt");
        let tx = state.control_tx.clone();
        Ok(ws.on_upgrade(move |socket| async move { handle_socket(socket, tx) }))
    } else {
        warn!(result = "rejected", "control plane connection attempt");
        Err(AppError::unauthorized(
            "invalid or missing control plane token",
        ))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn handle_socket(socket: WebSocket, control_tx: tokio::sync::broadcast::Sender<ControlMessage>) {
    let (mut sender, mut _receiver) = socket.split();
    let mut rx = control_tx.subscribe();

    info!("agent connected to control plane websocket");

    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json.into())).await.is_err() {
                    warn!("failed to send control message to agent, connection closed");
                    break;
                }
            }
        }
    });
}

pub async fn admin_broadcast(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(message): Json<ControlMessage>,
) -> Result<Json<&'static str>, AppError> {
    crate::authorize(&headers, state.api_keys.as_slice())?;

    if state.control_tx.send(message).is_err() {
        warn!("broadcast failed: no agents connected");
    } else {
        info!("control message broadcasted to connected agents");
    }
    Ok(Json("broadcasted"))
}


#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Generate an arbitrary non-empty ASCII token string (1..64 chars).
    fn arb_token() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_\\-]{1,64}".prop_map(String::from)
    }

    /// Generate a small set of API keys (1..5 keys).
    fn arb_api_keys() -> impl Strategy<Value = Vec<String>> {
        prop::collection::vec(arb_token(), 1..5)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// **Validates: Requirements 5.1, 5.2**
        #[test]
        fn prop_ws_auth_valid_token_accepted_invalid_rejected(
            // Feature: sentinelmesh-comprehensive-upgrade, Property 6: Autenticação WebSocket — token válido aceita, inválido rejeita
            api_keys in arb_api_keys(),
            token_index in any::<prop::sample::Index>(),
            invalid_token in arb_token(),
            use_header in any::<bool>(),
        ) {
            // --- Valid token: pick one from the api_keys list ---
            let valid_token = token_index.get(&api_keys).clone();

            let (valid_headers, valid_params) = if use_header {
                let mut h = HeaderMap::new();
                h.insert("x-sentinelmesh-api-key", valid_token.parse().unwrap());
                (h, HashMap::new())
            } else {
                let mut p = HashMap::new();
                p.insert("token".to_string(), valid_token.clone());
                (HeaderMap::new(), p)
            };

            let result = validate_control_token(&valid_headers, &valid_params, &api_keys);
            prop_assert!(result, "valid token '{}' should be accepted", valid_token);

            // --- Invalid token: ensure it's not in the api_keys list ---
            if !api_keys.contains(&invalid_token) {
                let (inv_headers, inv_params) = if use_header {
                    let mut h = HeaderMap::new();
                    h.insert("x-sentinelmesh-api-key", invalid_token.parse().unwrap());
                    (h, HashMap::new())
                } else {
                    let mut p = HashMap::new();
                    p.insert("token".to_string(), invalid_token.clone());
                    (HeaderMap::new(), p)
                };

                let result = validate_control_token(&inv_headers, &inv_params, &api_keys);
                prop_assert!(!result, "invalid token '{}' should be rejected", invalid_token);
            }

            // --- Missing token: no header, no query param ---
            let result = validate_control_token(&HeaderMap::new(), &HashMap::new(), &api_keys);
            prop_assert!(!result, "missing token should be rejected");
        }
    }
}
