//! Mock ClickHouse for integration tests.
//!
//! Provides a lightweight HTTP server that accepts ClickHouse-style queries
//! and returns pre-defined data, allowing tests to verify storage logic
//! without a real ClickHouse instance.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, extract::State, routing::post};
use parking_lot::RwLock;
use tokio::net::TcpListener;

/// Shared state for the mock ClickHouse server.
#[derive(Clone)]
pub struct MockClickHouseState {
    /// Map from query pattern (substring match) to canned response body.
    responses: Arc<RwLock<HashMap<String, String>>>,
    /// All queries received, in order.
    pub queries: Arc<RwLock<Vec<String>>>,
    /// If true, all queries will return an error.
    pub fail_mode: Arc<RwLock<bool>>,
}

impl MockClickHouseState {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(RwLock::new(HashMap::new())),
            queries: Arc::new(RwLock::new(Vec::new())),
            fail_mode: Arc::new(RwLock::new(false)),
        }
    }

    /// Register a canned response for queries containing the given substring.
    pub fn on_query_containing(&self, pattern: &str, response: &str) {
        self.responses
            .write()
            .insert(pattern.to_string(), response.to_string());
    }

    /// Enable or disable fail mode (all queries return HTTP 500).
    pub fn set_fail_mode(&self, fail: bool) {
        *self.fail_mode.write() = fail;
    }

    /// Return all queries received so far.
    pub fn received_queries(&self) -> Vec<String> {
        self.queries.read().clone()
    }

    /// Return how many queries were received.
    pub fn query_count(&self) -> usize {
        self.queries.read().len()
    }
}

impl Default for MockClickHouseState {
    fn default() -> Self {
        Self::new()
    }
}

async fn handle_query(
    State(state): State<MockClickHouseState>,
    body: String,
) -> Result<String, (axum::http::StatusCode, String)> {
    // Record the query
    state.queries.write().push(body.clone());

    // Check fail mode
    if *state.fail_mode.read() {
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Mock ClickHouse: fail mode enabled".to_string(),
        ));
    }

    // Find a matching canned response
    let responses = state.responses.read();
    for (pattern, response) in responses.iter() {
        if body.contains(pattern) {
            return Ok(response.clone());
        }
    }

    // Default: return empty success for INSERT/CREATE queries
    if body.to_uppercase().starts_with("INSERT") || body.to_uppercase().starts_with("CREATE") {
        return Ok(String::new());
    }

    // Default: return empty result set for SELECT queries
    Ok(String::new())
}

/// A running mock ClickHouse server handle.
pub struct MockClickHouseServer {
    pub addr: SocketAddr,
    pub state: MockClickHouseState,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl MockClickHouseServer {
    /// Start a mock ClickHouse server on a random available port.
    pub async fn start() -> anyhow::Result<Self> {
        let state = MockClickHouseState::new();
        Self::start_with_state(state).await
    }

    /// Start a mock ClickHouse server with pre-configured state.
    pub async fn start_with_state(state: MockClickHouseState) -> anyhow::Result<Self> {
        let app = Router::new()
            .route("/", post(handle_query))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_clickhouse_accepts_insert() {
        let server = MockClickHouseServer::start().await.unwrap();

        let client = reqwest::Client::new();
        let resp = client
            .post(&server.url())
            .body("INSERT INTO probe_batches FORMAT JSONEachRow {}")
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        assert_eq!(server.state.query_count(), 1);
        server.shutdown();
    }

    #[tokio::test]
    async fn mock_clickhouse_returns_canned_response() {
        let server = MockClickHouseServer::start().await.unwrap();
        server
            .state
            .on_query_containing("SELECT", r#"[{"count": 42}]"#);

        let client = reqwest::Client::new();
        let resp = client
            .post(&server.url())
            .body("SELECT count() FROM probe_batches")
            .send()
            .await
            .unwrap();

        let body = resp.text().await.unwrap();
        assert!(body.contains("42"));
        server.shutdown();
    }

    #[tokio::test]
    async fn mock_clickhouse_fail_mode() {
        let server = MockClickHouseServer::start().await.unwrap();
        server.state.set_fail_mode(true);

        let client = reqwest::Client::new();
        let resp = client
            .post(&server.url())
            .body("SELECT 1")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 500);
        server.shutdown();
    }
}
