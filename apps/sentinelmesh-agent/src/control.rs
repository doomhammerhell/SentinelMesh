use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use sentinelmesh_core::ControlMessage;
use std::sync::Arc;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::AgentStatus;

pub async fn run_control_plane(
    control_url: String,
    _status: Arc<RwLock<AgentStatus>>,
    endpoints: Arc<RwLock<Vec<sentinelmesh_core::RpcEndpointConfig>>>,
) {
    let mut retry_interval = tokio::time::interval(std::time::Duration::from_secs(10));

    loop {
        retry_interval.tick().await;

        match connect_async(&control_url).await {
            Ok((ws_stream, _)) => {
                info!(url = %control_url, "connected to aggregator control plane");
                let (mut write, mut read) = ws_stream.split();

                let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));
                ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if write.send(tokio_tungstenite::tungstenite::Message::Ping(vec![].into())).await.is_err() {
                                warn!("failed to send ping to control plane, connection dead");
                                break;
                            }
                        }
                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                warn!("control plane connection closed by server (eof)");
                                break;
                            };
                            match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                                    if let Ok(control_msg) = serde_json::from_str::<ControlMessage>(&text) {
                                        handle_control_message(control_msg, &endpoints);
                                    } else {
                                        warn!("received malformed control message: {}", text);
                                    }
                                }
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                                    warn!("control plane connection closed by server");
                                    break;
                                }
                                Err(e) => {
                                    error!(error = %e, "error reading from control plane websocket");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, url = %control_url, "failed to connect to control plane, retrying in 10s");
            }
        }
    }
}

fn handle_control_message(
    msg: ControlMessage,
    endpoints: &Arc<RwLock<Vec<sentinelmesh_core::RpcEndpointConfig>>>,
) {
    match msg {
        ControlMessage::UpdateEndpoints {
            endpoints: new_endpoints,
        } => {
            endpoints.write().clone_from(&new_endpoints);
            info!(
                count = new_endpoints.len(),
                "received full endpoints update from control plane"
            );
        }
        ControlMessage::AddEndpoint { endpoint } => {
            let mut writer = endpoints.write();
            if !writer.iter().any(|e| e.id == endpoint.id) {
                info!(id = %endpoint.id, url = %endpoint.rpc_url, "added new endpoint from control plane");
                writer.push(endpoint);
            }
        }
        ControlMessage::RemoveEndpoint { id } => {
            let mut writer = endpoints.write();
            if let Some(pos) = writer.iter().position(|e| e.id == id) {
                writer.remove(pos);
                info!(id = %id, "removed endpoint by control plane command");
            }
        }
    }
}
