use axum::{
    Json,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::{sink::SinkExt, stream::StreamExt};
use sentinelmesh_core::ControlMessage;
use tracing::{info, warn};

use crate::{AppError, AppState};

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    let tx = state.control_tx.clone();
    ws.on_upgrade(move |socket| async move { handle_socket(socket, tx) })
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
