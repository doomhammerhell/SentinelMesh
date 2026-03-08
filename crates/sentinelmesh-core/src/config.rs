use std::{collections::BTreeMap, fs, net::SocketAddr, path::Path, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigLoadError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode yaml config {path}: {source}")]
    Yaml {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("failed to decode json config {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("unsupported config extension for {0}; expected .yaml, .yml or .json")]
    UnsupportedExtension(String),
}

pub fn load_from_path<T>(path: impl AsRef<Path>) -> Result<T, ConfigLoadError>
where
    T: DeserializeOwned,
{
    let path = path.as_ref();
    let path_str = path.display().to_string();
    let raw = fs::read_to_string(path).map_err(|source| ConfigLoadError::Io {
        path: path_str.clone(),
        source,
    })?;

    match path.extension().and_then(|extension| extension.to_str()) {
        Some("yaml" | "yml") => {
            serde_yaml::from_str(&raw).map_err(|source| ConfigLoadError::Yaml {
                path: path_str,
                source,
            })
        }
        Some("json") => serde_json::from_str(&raw).map_err(|source| ConfigLoadError::Json {
            path: path_str,
            source,
        }),
        _ => Err(ConfigLoadError::UnsupportedExtension(
            path.display().to_string(),
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentConfig {
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
    pub runtime: AgentRuntimeConfig,
    pub publish: PublishConfig,
    pub admin: AdminServerConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub canary: CanaryConfig,
    #[serde(default)]
    pub validator_probes: ValidatorProbeConfig,
    pub endpoints: Vec<RpcEndpointConfig>,
    #[serde(default)]
    pub tracked_accounts: Vec<TrackedAccountConfig>,
    #[serde(default)]
    pub tracked_signatures: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentRuntimeConfig {
    pub sentinel_id: String,
    pub location: String,
    #[serde(default = "default_sample_interval", with = "humantime_serde")]
    pub sample_interval: Duration,
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PublishConfig {
    pub ingestion_url: String,
    #[serde(default)]
    pub auth: PublishAuthConfig,
    #[serde(default = "default_publish_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PublishAuthConfig {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub signing: Option<SigningKeyConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SigningKeyConfig {
    pub signer_id: String,
    pub key_id: String,
    pub private_key_base64: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TlsClientConfig {
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub client_cert_path: Option<String>,
    #[serde(default)]
    pub client_key_path: Option<String>,
    #[serde(default)]
    pub domain_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AdminServerConfig {
    #[serde(default = "default_agent_bind_address")]
    pub bind_address: SocketAddr,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CanaryConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_canary_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default)]
    pub mode: CanaryMode,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanaryMode {
    #[default]
    Disabled,
    CliTransfer(CanaryCliTransferConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CanaryCliTransferConfig {
    #[serde(default = "default_solana_cli_path")]
    pub solana_cli_path: String,
    pub rpc_url: String,
    pub sender_keypair_path: String,
    pub recipient_pubkey: String,
    #[serde(default = "default_canary_amount_sol")]
    pub amount_sol: f64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ValidatorProbeConfig {
    #[serde(default = "default_bool_true")]
    pub include_identity: bool,
    #[serde(default = "default_bool_true")]
    pub include_vote_accounts: bool,
    #[serde(default = "default_bool_true")]
    pub include_cluster_nodes: bool,
    #[serde(default = "default_bool_true")]
    pub include_leader_schedule: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AggregatorConfig {
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
    pub server: AggregatorServerConfig,
    pub ingestion: IngestionConfig,
    pub analysis: AnalysisConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub security: ServerSecurityConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AggregatorServerConfig {
    #[serde(default = "default_aggregator_bind_address")]
    pub bind_address: SocketAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IngestionConfig {
    #[serde(default)]
    pub auth: IngestionAuthConfig,
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct IngestionAuthConfig {
    #[serde(default)]
    pub api_keys: Vec<String>,
    #[serde(default)]
    pub trusted_signers: Vec<TrustedSignerConfig>,
    #[serde(default)]
    pub require_signed_batches: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrustedSignerConfig {
    pub key_id: String,
    pub public_key_base64: String,
    #[serde(default)]
    pub signer_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AnalysisConfig {
    #[serde(default = "default_retention", with = "humantime_serde")]
    pub retention: Duration,
    #[serde(default = "default_freshness_window", with = "humantime_serde")]
    pub freshness_window: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub replay_log: Option<ReplayLogConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub postgres_url: String,
    #[serde(default = "default_max_db_connections")]
    pub max_connections: u32,
    #[serde(default = "default_bootstrap_window", with = "humantime_serde")]
    pub bootstrap_window: Duration,
    #[serde(default = "default_refresh_interval", with = "humantime_serde")]
    pub refresh_interval: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplayLogConfig {
    pub path: String,
    #[serde(default = "default_bool_true")]
    pub replay_on_startup: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub otlp: Option<OtlpConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OtlpConfig {
    pub endpoint: String,
    #[serde(default = "default_otlp_service_name")]
    pub service_name: String,
    #[serde(default = "default_environment")]
    pub environment: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ServerSecurityConfig {
    #[serde(default)]
    pub server_cert_path: Option<String>,
    #[serde(default)]
    pub server_key_path: Option<String>,
    #[serde(default)]
    pub trusted_client_ca_path: Option<String>,
    #[serde(default)]
    pub require_client_cert: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RpcEndpointConfig {
    pub id: String,
    pub label: String,
    pub provider: String,
    pub region: String,
    pub rpc_url: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedAccountConfig {
    pub pubkey: String,
    #[serde(default = "default_commitment")]
    pub commitment: String,
}

const fn default_sample_interval() -> Duration {
    Duration::from_secs(15)
}

const fn default_request_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn default_publish_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn default_retention() -> Duration {
    Duration::from_secs(600)
}

const fn default_freshness_window() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_concurrency() -> usize {
    32
}

const fn default_max_batch_bytes() -> usize {
    1_048_576
}

const fn default_max_db_connections() -> u32 {
    20
}

const fn default_bootstrap_window() -> Duration {
    Duration::from_secs(600)
}

const fn default_refresh_interval() -> Duration {
    Duration::from_secs(10)
}

const fn default_canary_interval() -> Duration {
    Duration::from_secs(45)
}

const fn default_bool_true() -> bool {
    true
}

const fn default_canary_amount_sol() -> f64 {
    0.000_001
}

fn default_log_filter() -> String {
    "info,sentinelmesh_agent=debug,sentinelmesh_aggregator=debug".to_owned()
}

fn default_agent_bind_address() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9490))
}

fn default_aggregator_bind_address() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 9480))
}

fn default_commitment() -> String {
    "confirmed".to_owned()
}

fn default_otlp_service_name() -> String {
    "sentinelmesh".to_owned()
}

fn default_environment() -> String {
    "development".to_owned()
}

fn default_solana_cli_path() -> String {
    "solana".to_owned()
}

#[cfg(test)]
mod tests {
    use super::{AgentConfig, AggregatorConfig};

    #[test]
    fn deserializes_agent_config() {
        let raw = r"
log_filter: info
runtime:
  sentinel_id: sentinel-scl-01
  location: santiago-cl
  sample_interval: 15s
  request_timeout: 5s
  max_concurrency: 8
publish:
  ingestion_url: http://127.0.0.1:9480/v1/ingest
  timeout: 5s
  auth:
    api_key: dev-token
admin:
  bind_address: 127.0.0.1:9490
canary:
  enabled: false
  interval: 45s
  mode:
    type: disabled
validator_probes:
  include_identity: true
  include_vote_accounts: true
  include_cluster_nodes: true
  include_leader_schedule: true
endpoints:
  - id: local
    label: local
    provider: test
    region: local
    rpc_url: http://127.0.0.1:8899
tracked_accounts:
  - pubkey: 11111111111111111111111111111111
    commitment: confirmed
tracked_signatures:
  - signature-1
";

        let config: AgentConfig = serde_yaml::from_str(raw).expect("agent config should parse");
        assert_eq!(config.runtime.sentinel_id, "sentinel-scl-01");
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.tracked_accounts.len(), 1);
        assert_eq!(config.tracked_signatures.len(), 1);
        assert_eq!(config.publish.auth.api_key.as_deref(), Some("dev-token"));
    }

    #[test]
    fn deserializes_aggregator_config() {
        let raw = r"
log_filter: debug
server:
  bind_address: 0.0.0.0:9480
ingestion:
  auth:
    api_keys:
      - sentinelmesh-dev-token
    require_signed_batches: false
  max_batch_bytes: 1048576
analysis:
  retention: 10m
  freshness_window: 60s
storage:
  database:
    postgres_url: postgres://sentinelmesh:sentinelmesh@localhost:5432/sentinelmesh
    max_connections: 10
    bootstrap_window: 10m
    refresh_interval: 10s
";

        let config: AggregatorConfig =
            serde_yaml::from_str(raw).expect("aggregator config should parse");
        assert_eq!(config.analysis.retention.as_secs(), 600);
        assert_eq!(
            config.ingestion.auth.api_keys.first().map(String::as_str),
            Some("sentinelmesh-dev-token")
        );
        assert_eq!(config.storage.database.max_connections, 10);
    }
}
