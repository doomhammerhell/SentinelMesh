use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::{StreamExt, stream};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::RwLock;
use reqwest::{Certificate, Client, Identity};
use sentinelmesh_core::{
    AgentConfig, BatchAuth, HealthResponse, ProbeBatch, ProbeEnvelope, SigningMaterial,
    telemetry::init_tracing,
};
use sentinelmesh_solana::SolanaProbe;
use serde::Serialize;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "sentinelmesh-agent")]
#[command(about = "Distributed Solana RPC probe and telemetry forwarder")]
struct Cli {
    #[arg(long, env = "SENTINELMESH_CONFIG")]
    config: PathBuf,
}

#[derive(Clone)]
struct AppState {
    status: Arc<RwLock<AgentStatus>>,
    metrics: PrometheusHandle,
}

#[derive(Clone, Debug)]
struct RuntimeState {
    canary_signatures: Arc<RwLock<VecDeque<String>>>,
}

#[derive(Clone, Debug, Serialize)]
struct AgentStatus {
    sentinel_id: String,
    location: String,
    configured_endpoints: usize,
    tracked_accounts: usize,
    tracked_signatures: usize,
    publish_key_id: Option<String>,
    last_batch_id: Option<String>,
    last_batch_at: Option<DateTime<Utc>>,
    last_publish_success_at: Option<DateTime<Utc>>,
    last_canary_signature: Option<String>,
    last_error: Option<String>,
}

impl AgentStatus {
    fn from_config(config: &AgentConfig) -> Self {
        Self {
            sentinel_id: config.runtime.sentinel_id.clone(),
            location: config.runtime.location.clone(),
            configured_endpoints: config.endpoints.len(),
            tracked_accounts: config.tracked_accounts.len(),
            tracked_signatures: config.tracked_signatures.len(),
            publish_key_id: config
                .publish
                .auth
                .signing
                .as_ref()
                .map(|signing| signing.key_id.clone()),
            last_batch_id: None,
            last_batch_at: None,
            last_publish_success_at: None,
            last_canary_signature: None,
            last_error: None,
        }
    }
}

struct BatchPublisher {
    client: Client,
    ingestion_url: String,
    api_key: Option<String>,
    signing_material: Option<SigningMaterial>,
}

impl BatchPublisher {
    fn new(config: &AgentConfig) -> Result<Self> {
        let mut client_builder = Client::builder()
            .connect_timeout(config.publish.timeout)
            .timeout(config.publish.timeout)
            .user_agent("sentinelmesh-agent/0.1");

        if let Some(tls) = &config.publish.tls {
            if let Some(ca_cert_path) = &tls.ca_cert_path {
                let pem = std::fs::read(ca_cert_path)
                    .with_context(|| format!("failed to read CA cert {ca_cert_path}"))?;
                client_builder = client_builder.add_root_certificate(
                    Certificate::from_pem(&pem).context("failed to decode client CA cert PEM")?,
                );
            }

            if let (Some(client_cert_path), Some(client_key_path)) =
                (&tls.client_cert_path, &tls.client_key_path)
            {
                let cert = std::fs::read(client_cert_path).with_context(|| {
                    format!("failed to read mTLS client cert {client_cert_path}")
                })?;
                let key = std::fs::read(client_key_path)
                    .with_context(|| format!("failed to read mTLS client key {client_key_path}"))?;
                let mut pem = cert;
                pem.extend_from_slice(&key);
                client_builder = client_builder.identity(
                    Identity::from_pem(&pem)
                        .context("failed to decode mTLS client identity PEM")?,
                );
            }
        }

        let client = client_builder
            .build()
            .context("failed to construct publish client")?;

        let signing_material = config
            .publish
            .auth
            .signing
            .as_ref()
            .map(|signing| {
                SigningMaterial::from_base64(
                    signing.signer_id.clone(),
                    signing.key_id.clone(),
                    &signing.private_key_base64,
                )
            })
            .transpose()?;

        Ok(Self {
            client,
            ingestion_url: config.publish.ingestion_url.clone(),
            api_key: config.publish.auth.api_key.clone(),
            signing_material,
        })
    }

    async fn publish(&self, batch: ProbeBatch) -> Result<()> {
        let auth = self
            .signing_material
            .as_ref()
            .map(|signing| signing.sign(&batch, Utc::now()))
            .transpose()?;
        let envelope = ProbeEnvelope { batch, auth };

        let mut request = self
            .client
            .post(self.ingestion_url.as_str())
            .json(&envelope);
        if let Some(api_key) = &self.api_key {
            request = request.header("x-sentinelmesh-api-key", api_key);
        }
        if let Some(BatchAuth { key_id, .. }) = &envelope.auth {
            request = request.header("x-sentinelmesh-key-id", key_id);
        }

        let response = request
            .send()
            .await
            .context("failed to deliver batch to aggregator")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<body unavailable>".to_owned());
            bail!("aggregator rejected batch with status {status}: {body}");
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config: AgentConfig = sentinelmesh_core::load_from_path(&cli.config)
        .with_context(|| format!("failed to load {}", cli.config.display()))?;

    if config.endpoints.is_empty() {
        bail!("agent config must declare at least one endpoint");
    }

    init_tracing(
        "sentinelmesh-agent",
        &config.log_filter,
        &config.observability,
    )?;

    let metrics = PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install Prometheus recorder")?;
    let status = Arc::new(RwLock::new(AgentStatus::from_config(&config)));
    let state = AppState {
        status: Arc::clone(&status),
        metrics: metrics.clone(),
    };
    let runtime_state = RuntimeState {
        canary_signatures: Arc::new(RwLock::new(VecDeque::with_capacity(32))),
    };

    let probe = SolanaProbe::new(config.runtime.request_timeout)?;
    let publisher = BatchPublisher::new(&config)?;

    let admin_listener = tokio::net::TcpListener::bind(config.admin.bind_address)
        .await
        .with_context(|| {
            format!(
                "failed to bind admin listener on {}",
                config.admin.bind_address
            )
        })?;
    let admin_app = Router::new()
        .route("/", get(agent_index))
        .route("/healthz", get(agent_health))
        .route("/v1/status", get(agent_status))
        .route("/metrics", get(agent_metrics))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tokio::spawn(async move {
        if let Err(error) = axum::serve(admin_listener, admin_app).await {
            error!("agent admin server terminated: {error:#}");
        }
    });

    if config.canary.enabled {
        tokio::spawn(run_canary_loop(
            config.clone(),
            runtime_state.clone(),
            Arc::clone(&status),
        ));
    }

    info!(
        sentinel_id = %config.runtime.sentinel_id,
        bind_address = %config.admin.bind_address,
        endpoints = config.endpoints.len(),
        "agent started"
    );

    run_collection_loop(config, probe, publisher, status, runtime_state).await
}

async fn run_collection_loop(
    config: AgentConfig,
    probe: SolanaProbe,
    publisher: BatchPublisher,
    status: Arc<RwLock<AgentStatus>>,
    runtime_state: RuntimeState,
) -> Result<()> {
    let mut ticker = tokio::time::interval(config.runtime.sample_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let loop_started_at = std::time::Instant::now();
                let tracked_signatures = merged_signatures(&config, &runtime_state);
                let observations =
                    collect_observations(&config, &probe, &tracked_signatures).await;

                let mut sorted_observations = observations;
                sorted_observations.sort_by(|left, right| left.endpoint.id.cmp(&right.endpoint.id));

                let batch = ProbeBatch {
                    schema_version: 2,
                    batch_id: Uuid::new_v4(),
                    sampled_at: Utc::now(),
                    sentinel_id: config.runtime.sentinel_id.clone(),
                    sentinel_location: config.runtime.location.clone(),
                    endpoints: sorted_observations,
                };

                counter!("sentinelmesh_agent_batches_total").increment(1);
                gauge!("sentinelmesh_agent_endpoints_last_batch")
                    .set(usize_to_f64(batch.endpoints.len()));
                gauge!("sentinelmesh_agent_tracked_signatures")
                    .set(usize_to_f64(tracked_signatures.len()));

                let publish_result = publisher.publish(batch.clone()).await;
                let loop_duration_ms = loop_started_at.elapsed().as_secs_f64() * 1000.0;
                histogram!("sentinelmesh_agent_publish_cycle_ms").record(loop_duration_ms);

                {
                    let mut current = status.write();
                    current.last_batch_id = Some(batch.batch_id.to_string());
                    current.last_batch_at = Some(batch.sampled_at);
                    current.tracked_signatures = tracked_signatures.len();
                }

                match publish_result {
                    Ok(()) => {
                        counter!("sentinelmesh_agent_publish_success_total").increment(1);
                        let mut current = status.write();
                        current.last_publish_success_at = Some(Utc::now());
                        current.last_error = None;
                        info!(
                            batch_id = %batch.batch_id,
                            endpoints = batch.endpoints.len(),
                            duration_ms = loop_duration_ms,
                            "published probe batch"
                        );
                    }
                    Err(error) => {
                        counter!("sentinelmesh_agent_publish_failure_total").increment(1);
                        status.write().last_error = Some(error.to_string());
                        warn!(
                            batch_id = %batch.batch_id,
                            endpoints = batch.endpoints.len(),
                            error = %error,
                            "failed to publish probe batch"
                        );
                    }
                }
            }
            signal = shutdown_signal() => {
                signal?;
                info!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

async fn collect_observations(
    config: &AgentConfig,
    probe: &SolanaProbe,
    tracked_signatures: &[String],
) -> Vec<sentinelmesh_core::EndpointObservation> {
    let endpoints = config.endpoints.clone();
    let tracked_accounts = config.tracked_accounts.clone();
    let tracked_signatures = tracked_signatures.to_vec();
    let validator_probes = config.validator_probes.clone();
    let concurrency = config.runtime.max_concurrency.max(1);

    stream::iter(endpoints.into_iter().map(|endpoint| {
        let probe = probe.clone();
        let tracked_accounts = tracked_accounts.clone();
        let tracked_signatures = tracked_signatures.clone();
        let validator_probes = validator_probes.clone();

        async move {
            probe
                .observe_endpoint(
                    endpoint,
                    &tracked_accounts,
                    &tracked_signatures,
                    &validator_probes,
                )
                .await
        }
    }))
    .buffer_unordered(concurrency)
    .collect()
    .await
}

async fn run_canary_loop(
    config: AgentConfig,
    runtime_state: RuntimeState,
    status: Arc<RwLock<AgentStatus>>,
) {
    let mut ticker = tokio::time::interval(config.canary.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        match emit_canary_signature(&config).await {
            Ok(Some(signature)) => {
                let mut signatures = runtime_state.canary_signatures.write();
                if signatures.len() >= 32 {
                    signatures.pop_back();
                }
                signatures.push_front(signature.clone());
                drop(signatures);

                counter!("sentinelmesh_agent_canary_success_total").increment(1);
                status.write().last_canary_signature = Some(signature.clone());
                info!(signature = %signature, "registered fresh canary signature");
            }
            Ok(None) => {}
            Err(error) => {
                counter!("sentinelmesh_agent_canary_failure_total").increment(1);
                warn!(error = %error, "failed to emit canary transaction");
            }
        }
    }
}

async fn emit_canary_signature(config: &AgentConfig) -> Result<Option<String>> {
    if !config.canary.enabled {
        return Ok(None);
    }

    match &config.canary.mode {
        sentinelmesh_core::CanaryMode::Disabled => Ok(None),
        sentinelmesh_core::CanaryMode::CliTransfer(canary) => {
            let amount = format!("{:.9}", canary.amount_sol);
            let output = tokio::process::Command::new(&canary.solana_cli_path)
                .args([
                    "transfer",
                    &canary.recipient_pubkey,
                    &amount,
                    "--keypair",
                    &canary.sender_keypair_path,
                    "--url",
                    &canary.rpc_url,
                    "--allow-unfunded-recipient",
                    "--output",
                    "json-compact",
                ])
                .output()
                .await
                .with_context(|| {
                    format!(
                        "failed to execute {} for canary transfer",
                        canary.solana_cli_path
                    )
                })?;

            if !output.status.success() {
                bail!(
                    "canary transfer command failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            parse_canary_signature(stdout.as_ref()).map(Some)
        }
    }
}

fn parse_canary_signature(output: &str) -> Result<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(output) {
        if let Some(signature) = value.get("signature").and_then(serde_json::Value::as_str) {
            return Ok(signature.to_owned());
        }
    }

    output
        .split_whitespace()
        .find(|token| token.len() >= 64 && token.chars().all(is_base58_character))
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("failed to parse canary signature from solana CLI output"))
}

fn is_base58_character(character: char) -> bool {
    matches!(
        character,
        '1'..='9'
            | 'A'..='H'
            | 'J'..='N'
            | 'P'..='Z'
            | 'a'..='k'
            | 'm'..='z'
    )
}

fn merged_signatures(config: &AgentConfig, runtime_state: &RuntimeState) -> Vec<String> {
    let mut signatures = config.tracked_signatures.clone();
    for signature in runtime_state.canary_signatures.read().iter() {
        if !signatures.contains(signature) {
            signatures.push(signature.clone());
        }
    }
    signatures
}

async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| anyhow!("failed while awaiting ctrl-c signal: {error}"))
}

async fn agent_index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>SentinelMesh Agent</title>
    <style>
      :root { color-scheme: dark; font-family: "IBM Plex Sans", sans-serif; }
      body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: radial-gradient(circle at top, #12314a, #07141e 60%); color: #ebf4ff; }
      main { max-width: 720px; padding: 32px; background: rgba(7, 20, 30, 0.75); border: 1px solid rgba(125, 211, 252, 0.2); border-radius: 20px; backdrop-filter: blur(12px); }
      h1 { margin-top: 0; font-size: 2rem; }
      p, li { line-height: 1.6; color: #cbd5e1; }
      code { color: #7dd3fc; }
    </style>
  </head>
  <body>
    <main>
      <h1>SentinelMesh Agent</h1>
      <p>This node exposes local health, runtime status, Prometheus metrics and canary transaction state.</p>
      <ul>
        <li><code>GET /healthz</code></li>
        <li><code>GET /v1/status</code></li>
        <li><code>GET /metrics</code></li>
      </ul>
    </main>
  </body>
</html>"#,
    )
}

async fn agent_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "sentinelmesh-agent",
        generated_at: Utc::now(),
    })
}

async fn agent_status(State(state): State<AppState>) -> Json<AgentStatus> {
    Json(state.status.read().clone())
}

async fn agent_metrics(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}
