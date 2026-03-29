//! Mock RPC server for integration tests.
//!
//! Provides a lightweight HTTP server that returns pre-defined JSON-RPC
//! responses, allowing tests to exercise Agent probe logic without a real
//! Solana endpoint.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::post};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

/// A single canned JSON-RPC response keyed by method name.
#[derive(Clone, Debug)]
pub struct CannedResponse {
    pub result: serde_json::Value,
}

/// Shared state for the mock RPC server.
#[derive(Clone)]
pub struct MockRpcState {
    /// Map from JSON-RPC method name to canned response.
    responses: Arc<RwLock<HashMap<String, CannedResponse>>>,
    /// Counter of requests received per method.
    pub request_counts: Arc<RwLock<HashMap<String, usize>>>,
}

impl MockRpcState {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(RwLock::new(HashMap::new())),
            request_counts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a canned response for a given JSON-RPC method.
    pub fn on_method(&self, method: &str, result: serde_json::Value) {
        self.responses.write().insert(
            method.to_string(),
            CannedResponse { result },
        );
    }

    /// Return how many times a method was called.
    pub fn call_count(&self, method: &str) -> usize {
        self.request_counts.read().get(method).copied().unwrap_or(0)
    }
}

impl Default for MockRpcState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize)]
struct JsonRpcRequest {
    method: String,
    id: serde_json::Value,
    #[allow(dead_code)]
    params: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<serde_json::Value>,
}

async fn handle_rpc(
    State(state): State<MockRpcState>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    // Increment call counter
    *state
        .request_counts
        .write()
        .entry(req.method.clone())
        .or_insert(0) += 1;

    let responses = state.responses.read();
    if let Some(canned) = responses.get(&req.method) {
        Json(JsonRpcResponse {
            jsonrpc: "2.0",
            id: req.id,
            result: Some(canned.result.clone()),
            error: None,
        })
    } else {
        Json(JsonRpcResponse {
            jsonrpc: "2.0",
            id: req.id,
            result: None,
            error: Some(serde_json::json!({
                "code": -32601,
                "message": format!("Method not found: {}", req.method)
            })),
        })
    }
}

/// A running mock RPC server handle.
pub struct MockRpcServer {
    pub addr: SocketAddr,
    pub state: MockRpcState,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl MockRpcServer {
    /// Start a mock RPC server on a random available port.
    /// Returns a handle that can be used to configure responses and get the address.
    pub async fn start() -> anyhow::Result<Self> {
        let state = MockRpcState::new();
        Self::start_with_state(state).await
    }

    /// Start a mock RPC server with pre-configured state.
    pub async fn start_with_state(state: MockRpcState) -> anyhow::Result<Self> {
        let app = Router::new()
            .route("/", post(handle_rpc))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        Ok(Self {
            addr,
            state,
            shutdown_tx,
        })
    }

    /// Get the base URL for this mock server.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Shut down the mock server.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}
