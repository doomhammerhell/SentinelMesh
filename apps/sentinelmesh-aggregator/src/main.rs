#![allow(clippy::cast_precision_loss)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::must_use_candidate)]
#![allow(dead_code)]

mod alert;
pub mod committer;
mod control;

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as HyperBuilder,
    service::TowerToHyperService,
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::RwLock;
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use sentinelmesh_analysis::MeshStore;
use sentinelmesh_core::{
    AccountDivergence, AggregatorConfig, BatchVerifier, HealthResponse, IdentityChangeEvent,
    IngestionResponse, NetworkSnapshot, ProbeEnvelope, ProviderStatus, ServerSecurityConfig,
    SignaturePropagation, TrustedSigner, telemetry::init_tracing,
};
use sentinelmesh_storage::StorageEngine;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "sentinelmesh-aggregator")]
#[command(about = "SentinelMesh aggregation plane and public observability API")]
struct Cli {
    #[arg(long, env = "SENTINELMESH_CONFIG")]
    config: PathBuf,
}

#[derive(Clone)]
pub struct AppState {
    store: Arc<RwLock<MeshStore>>,
    storage: Arc<StorageEngine>,
    api_keys: Arc<Vec<String>>,
    verifier: Arc<BatchVerifier>,
    require_signed_batches: bool,
    metrics: PrometheusHandle,
    alert_sink: Option<alert::AlertSink>,
    control_tx: tokio::sync::broadcast::Sender<sentinelmesh_core::ControlMessage>,
    agent_whitelist: Arc<Option<Vec<String>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config: AggregatorConfig = sentinelmesh_core::load_from_path(&cli.config)
        .with_context(|| format!("failed to load {}", cli.config.display()))?;

    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing(
        "sentinelmesh-aggregator",
        &config.log_filter,
        &config.observability,
    )?;

    let metrics_handle = PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install Prometheus recorder")?;
    let store = Arc::new(RwLock::new(MeshStore::new(
        config.analysis.retention,
        config.analysis.freshness_window,
    )));

    let storage = Arc::new(StorageEngine::connect(&config.storage).await?);
    storage.ensure_schema().await?;
    storage.replay_from_log().await?;
    let bootstrap_samples = storage
        .hydrate_recent_samples(config.analysis.retention)
        .await?;
    store.write().replace_samples(bootstrap_samples);

    let verifier = config
        .ingestion
        .auth
        .trusted_signers
        .iter()
        .map(|trusted_signer| {
            TrustedSigner::from_base64(
                trusted_signer.signer_id.clone(),
                trusted_signer.key_id.clone(),
                &trusted_signer.public_key_base64,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let alert_sink = config.alerts.clone().map(alert::AlertSink::new);

    let (control_tx, _) = tokio::sync::broadcast::channel(100);

    let state = AppState {
        store: Arc::clone(&store),
        storage: Arc::clone(&storage),
        api_keys: Arc::new(config.ingestion.auth.api_keys.clone()),
        verifier: Arc::new(BatchVerifier::new(verifier)),
        require_signed_batches: config.ingestion.auth.require_signed_batches,
        metrics: metrics_handle.clone(),
        alert_sink: alert_sink.clone(),
        control_tx,
        agent_whitelist: Arc::new(config.agent_whitelist),
    };

    if let Some(committer_config) = config.committer {
        tokio::spawn(committer::start_committer_loop(
            committer_config,
            Arc::clone(&storage),
        ));
    }

    tokio::spawn(refresh_from_storage_loop(
        Arc::clone(&storage),
        Arc::clone(&store),
        config.storage.clickhouse.refresh_interval,
        std::time::Duration::from_secs(config.storage.clickhouse.max_refresh_interval_secs),
        config.analysis.retention,
        alert_sink,
    ));

    let app = Router::new()
        .route("/", get(index))
        .route("/healthz", get(health))
        .route("/metrics", get(render_metrics))
        .route("/v1/ingest", post(ingest))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/providers", get(providers))
        .route("/v1/signatures", get(signatures))
        .route("/v1/accounts", get(accounts))
        .route("/v1/validator-history", get(validator_history))
        .route("/v1/ws/control", get(control::ws_handler))
        .route("/v1/admin/broadcast", post(control::admin_broadcast))
        .layer(DefaultBodyLimit::max(config.ingestion.max_batch_bytes))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = TcpListener::bind(config.server.bind_address)
        .await
        .with_context(|| format!("failed to bind {}", config.server.bind_address))?;

    let tls_config = build_tls_server_config(&config.security)?;
    if let Some(tls_config) = tls_config {
        info!(
            bind_address = %config.server.bind_address,
            client_cert_required = config.security.require_client_cert,
            "aggregator started with native TLS"
        );
        serve_tls(listener, app, tls_config).await
    } else {
        info!(
            bind_address = %config.server.bind_address,
            database = %config.storage.clickhouse.database,
            "aggregator started without native TLS"
        );
        axum::serve(listener, app)
            .await
            .context("aggregator server terminated unexpectedly")
    }
}

async fn refresh_from_storage_loop(
    storage: Arc<StorageEngine>,
    store: Arc<RwLock<MeshStore>>,
    refresh_interval: std::time::Duration,
    max_refresh_interval: std::time::Duration,
    retention: std::time::Duration,
    alert_sink: Option<alert::AlertSink>,
) {
    let mut adaptive = AdaptiveRefresh::new(refresh_interval, max_refresh_interval);

    loop {
        tokio::time::sleep(adaptive.current_interval).await;

        match storage.hydrate_recent_samples(retention).await {
            Ok(samples) => {
                let sample_count = samples.len();
                let snapshot = {
                    let mut w = store.write();
                    w.replace_samples(samples);
                    w.snapshot()
                };
                adaptive.on_refresh(sample_count);
                gauge!("sentinelmesh_aggregator_refresh_interval_ms")
                    .set(adaptive.current_interval.as_millis() as f64);

                if let Some(sink) = &alert_sink {
                    sink.dispatch(snapshot.anomalies);
                }
            }
            Err(error) => warn!(error = %error, "failed to refresh in-memory state from storage"),
        }
    }
}

// ---------------------------------------------------------------------------
// Adaptive refresh backoff
// ---------------------------------------------------------------------------

/// Adaptive refresh interval that increases when no new data arrives and
/// resets to the base interval when new envelopes are ingested.
pub struct AdaptiveRefresh {
    pub base_interval: std::time::Duration,
    pub current_interval: std::time::Duration,
    pub max_interval: std::time::Duration,
    pub last_sample_count: usize,
}

impl AdaptiveRefresh {
    pub fn new(base_interval: std::time::Duration, max_interval: std::time::Duration) -> Self {
        Self {
            base_interval,
            current_interval: base_interval,
            max_interval,
            last_sample_count: 0,
        }
    }

    /// Called after each refresh cycle. If the sample count hasn't changed,
    /// increase the interval by 50% (capped at max). Otherwise, keep current.
    pub fn on_refresh(&mut self, new_sample_count: usize) {
        if new_sample_count == self.last_sample_count {
            self.current_interval = self.current_interval.mul_f64(1.5).min(self.max_interval);
        }
        self.last_sample_count = new_sample_count;
        // Ensure we never go below base
        if self.current_interval < self.base_interval {
            self.current_interval = self.base_interval;
        }
    }

    /// Called when a new envelope is ingested. Resets the interval to base.
    pub fn on_new_envelope(&mut self) {
        self.current_interval = self.base_interval;
    }
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    tls_config: Arc<ServerConfig>,
) -> Result<()> {
    let tls_acceptor = TlsAcceptor::from(tls_config);

    loop {
        let (stream, remote_addr) = listener
            .accept()
            .await
            .context("failed to accept TCP connection")?;
        let tls_acceptor = tls_acceptor.clone();
        let app = app.clone();

        tokio::spawn(async move {
            let tls_stream = match tls_acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    warn!(remote_addr = %remote_addr, error = %error, "TLS handshake failed");
                    return;
                }
            };

            let io = TokioIo::new(tls_stream);
            let builder = HyperBuilder::new(TokioExecutor::new());
            let service = TowerToHyperService::new(app);
            if let Err(error) = builder.serve_connection(io, service).await {
                warn!(remote_addr = %remote_addr, error = %error, "TLS connection terminated with error");
            }
        });
    }
}

fn build_tls_server_config(security: &ServerSecurityConfig) -> Result<Option<Arc<ServerConfig>>> {
    let (Some(cert_path), Some(key_path)) = (
        security.server_cert_path.as_deref(),
        security.server_key_path.as_deref(),
    ) else {
        if security.require_client_cert || security.trusted_client_ca_path.is_some() {
            bail!("native TLS requires both server_cert_path and server_key_path");
        }
        return Ok(None);
    };

    let cert_chain = load_certificates(cert_path)?;
    let private_key = load_private_key(key_path)?;

    let builder = ServerConfig::builder();
    let mut server_config = if let Some(client_ca_path) = security.trusted_client_ca_path.as_deref()
    {
        let roots = Arc::new(load_root_store(client_ca_path)?);
        let verifier = if security.require_client_cert {
            WebPkiClientVerifier::builder(roots)
                .build()
                .map_err(|error| anyhow!("failed to build mandatory client verifier: {error}"))?
        } else {
            WebPkiClientVerifier::builder(roots)
                .allow_unauthenticated()
                .build()
                .map_err(|error| anyhow!("failed to build optional client verifier: {error}"))?
        };

        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)
            .context("failed to build rustls server config with client auth")?
    } else if security.require_client_cert {
        bail!("trusted_client_ca_path is required when require_client_cert=true");
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .context("failed to build rustls server config")?
    };

    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Some(Arc::new(server_config)))
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    use rustls::pki_types::pem::PemObject;
    CertificateDer::pem_file_iter(path)
        .with_context(|| format!("failed to open certificate file {path}"))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse certificate PEM {path}"))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    use rustls::pki_types::pem::PemObject;
    PrivateKeyDer::from_pem_file(path)
        .with_context(|| format!("failed to parse private key PEM {path}"))
}

fn load_root_store(path: &str) -> Result<RootCertStore> {
    let certificates = load_certificates(path)?;
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| anyhow!("failed to add CA certificate from {path}: {error}"))?;
    }
    Ok(roots)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "sentinelmesh-aggregator",
        generated_at: chrono::Utc::now(),
    })
}

async fn render_metrics(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(envelope): Json<ProbeEnvelope>,
) -> Result<Json<IngestionResponse>, AppError> {
    authorize(&headers, state.api_keys.as_slice())?;
    verify_envelope(&state, &envelope)?;

    let batch_id = envelope.batch.batch_id;
    let endpoints_received = envelope.batch.endpoints.len();
    let started_at = std::time::Instant::now();

    let persisted = state
        .storage
        .persist_envelope(&envelope)
        .await
        .map_err(AppError::internal)?;
    if persisted {
        state.store.write().ingest(envelope.batch.clone());
    }

    let snapshot = state.store.write().snapshot();

    if let Some(sink) = &state.alert_sink {
        sink.dispatch(snapshot.anomalies.clone());
    }

    gauge!("sentinelmesh_active_endpoints").set(usize_to_f64(snapshot.active_endpoints));
    gauge!("sentinelmesh_rpc_consistency_index").set(snapshot.rpc_consistency_index);
    gauge!("sentinelmesh_provider_hhi").set(snapshot.infrastructure_concentration.provider_hhi);

    counter!("sentinelmesh_ingested_batches_total").increment(1);
    counter!("sentinelmesh_ingested_endpoints_total").increment(endpoints_received as u64);
    histogram!("sentinelmesh_ingest_handler_ms")
        .record(started_at.elapsed().as_secs_f64() * 1000.0);

    Ok(Json(IngestionResponse {
        accepted: true,
        batch_id,
        endpoints_received,
        received_at: chrono::Utc::now(),
        persisted,
    }))
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

async fn snapshot(State(state): State<AppState>) -> Json<NetworkSnapshot> {
    Json(state.store.write().snapshot())
}

async fn providers(State(state): State<AppState>) -> Json<Vec<ProviderStatus>> {
    Json(state.store.read().provider_statuses())
}

async fn signatures(State(state): State<AppState>) -> Json<Vec<SignaturePropagation>> {
    Json(state.store.read().signature_propagation())
}

async fn accounts(State(state): State<AppState>) -> Json<Vec<AccountDivergence>> {
    Json(state.store.read().account_divergences())
}

async fn validator_history(
    State(state): State<AppState>,
) -> Json<BTreeMap<String, Vec<IdentityChangeEvent>>> {
    Json(state.store.read().validator_history().clone())
}

pub fn authorize(headers: &HeaderMap, api_keys: &[String]) -> Result<(), AppError> {
    if api_keys.is_empty() {
        return Ok(());
    }

    let provided = headers
        .get("x-sentinelmesh-api-key")
        .and_then(|value| value.to_str().ok());

    if api_keys
        .iter()
        .any(|candidate| Some(candidate.as_str()) == provided)
    {
        Ok(())
    } else {
        Err(AppError::unauthorized(
            "missing or invalid x-sentinelmesh-api-key",
        ))
    }
}

fn verify_envelope(state: &AppState, envelope: &ProbeEnvelope) -> Result<(), AppError> {
    if let Some(auth) = &envelope.auth {
        if let Some(whitelist) = state.agent_whitelist.as_ref() {
            if !whitelist.contains(&auth.signer_id) {
                return Err(AppError::unauthorized("signer_id not in whitelist"));
            }
        }
    }

    match (&envelope.auth, state.require_signed_batches) {
        (Some(auth), _) => state
            .verifier
            .verify(&envelope.batch, auth)
            .map_err(AppError::unauthorized),
        (None, true) => Err(AppError::unauthorized("signed batch required")),
        (None, false) => Ok(()),
    }
}

#[derive(Debug)]
pub struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn unauthorized(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: error.to_string(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!(status = %self.status, message = %self.message, "request failed");
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: sentinelmesh-comprehensive-upgrade, Property 15: Comportamento do backoff adaptativo
    // **Validates: Requirements 11.1, 11.2, 11.4**
    //
    // For any sequence of refresh cycles where the sample count doesn't change,
    // the refresh_interval must increase by 50% each cycle, never exceeding
    // max_refresh_interval. When a new envelope arrives, the interval must
    // reset to the base value. The interval must never be below the base.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_adaptive_backoff_behaviour(
            base_ms in 100_u64..=5000,
            max_ms_extra in 1_u64..=60000,
            // Sequence of events: true = no change (backoff), false = new envelope (reset)
            events in proptest::collection::vec(proptest::bool::ANY, 1..50),
        ) {
            let base = std::time::Duration::from_millis(base_ms);
            let max = std::time::Duration::from_millis(base_ms + max_ms_extra);
            let mut adaptive = AdaptiveRefresh::new(base, max);

            // Initial state checks
            prop_assert_eq!(adaptive.current_interval, base);

            let mut expected_interval = base;

            for event in &events {
                if *event {
                    // No change in sample count → backoff by 50%
                    let same_count = adaptive.last_sample_count;
                    adaptive.on_refresh(same_count);
                    expected_interval = expected_interval.mul_f64(1.5).min(max);
                } else {
                    // New envelope → reset to base
                    adaptive.on_new_envelope();
                    expected_interval = base;
                    // Then a refresh with new data
                    adaptive.on_refresh(adaptive.last_sample_count + 1);
                    // Sample count changed, so no backoff — interval stays
                }

                // Invariant 1: interval never exceeds max
                prop_assert!(adaptive.current_interval <= max,
                    "interval {:?} exceeded max {:?}",
                    adaptive.current_interval, max);

                // Invariant 2: interval never below base
                prop_assert!(adaptive.current_interval >= base,
                    "interval {:?} below base {:?}",
                    adaptive.current_interval, base);
            }
        }

        #[test]
        fn prop_adaptive_backoff_monotonic_increase(
            base_ms in 100_u64..=2000,
            max_factor in 2_u64..=20,
            num_idle_cycles in 1_usize..=30,
        ) {
            let base = std::time::Duration::from_millis(base_ms);
            let max = std::time::Duration::from_millis(base_ms * max_factor);
            let mut adaptive = AdaptiveRefresh::new(base, max);

            let mut prev_interval = adaptive.current_interval;

            for _ in 0..num_idle_cycles {
                let same_count = adaptive.last_sample_count;
                adaptive.on_refresh(same_count);

                // Interval should be >= previous (monotonically non-decreasing during idle)
                prop_assert!(adaptive.current_interval >= prev_interval,
                    "interval decreased from {:?} to {:?} during idle",
                    prev_interval, adaptive.current_interval);

                prev_interval = adaptive.current_interval;
            }

            // After reset, interval should be back to base
            adaptive.on_new_envelope();
            prop_assert_eq!(adaptive.current_interval, base);
        }
    }
}
