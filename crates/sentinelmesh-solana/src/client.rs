use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use sentinelmesh_core::RpcEndpointConfig;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone)]
pub(crate) struct SolanaRpcClient {
    client: Client,
}

impl SolanaRpcClient {
    pub(crate) fn new(request_timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(request_timeout)
            .timeout(request_timeout)
            .user_agent("sentinelmesh/0.1")
            .build()
            .context("failed to build reqwest client")?;

        Ok(Self { client })
    }

    pub(crate) async fn call<T>(
        &self,
        endpoint: &RpcEndpointConfig,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = self
            .client
            .post(endpoint.rpc_url.as_str())
            .json(&JsonRpcRequest {
                jsonrpc: "2.0",
                id: 1_u8,
                method,
                params,
            })
            .send()
            .await
            .with_context(|| format!("rpc transport failure for endpoint {}", endpoint.label))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<body unavailable>".to_owned());
            bail!(
                "rpc endpoint {} returned HTTP {} with body {}",
                endpoint.label,
                status,
                body
            );
        }

        let envelope: JsonRpcEnvelope<T> = response
            .json()
            .await
            .with_context(|| format!("failed to decode rpc payload from {}", endpoint.label))?;

        match (envelope.result, envelope.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(anyhow!(
                "rpc error from {}: code={} message={} data={}",
                endpoint.label,
                error.code,
                error.message,
                error
                    .data
                    .map_or_else(|| "null".to_owned(), |value| value.to_string())
            )),
            _ => Err(anyhow!(
                "rpc response from {} was missing both result and error",
                endpoint.label
            )),
        }
    }
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u8,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct JsonRpcEnvelope<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    data: Option<serde_json::Value>,
}
