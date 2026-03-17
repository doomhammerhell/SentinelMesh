use crate::config::RpcEndpointConfig;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeBatch {
    pub schema_version: u16,
    pub batch_id: Uuid,
    pub sampled_at: DateTime<Utc>,
    pub sentinel_id: String,
    pub sentinel_location: String,
    #[serde(default)]
    pub asn: Option<u32>,
    pub endpoints: Vec<EndpointObservation>,
}

impl ProbeBatch {
    #[must_use]
    pub fn into_samples(self) -> Vec<EndpointSample> {
        self.endpoints
            .into_iter()
            .map(|observation| EndpointSample {
                batch_id: self.batch_id,
                sampled_at: self.sampled_at,
                sentinel_id: self.sentinel_id.clone(),
                sentinel_location: self.sentinel_location.clone(),
                asn: self.asn,
                observation,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeEnvelope {
    pub batch: ProbeBatch,
    #[serde(default)]
    pub auth: Option<BatchAuth>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchAuth {
    pub signer_id: String,
    pub key_id: String,
    pub signed_at: DateTime<Utc>,
    pub batch_hash: String,
    pub signature_b64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointSample {
    pub batch_id: Uuid,
    pub sampled_at: DateTime<Utc>,
    pub sentinel_id: String,
    pub sentinel_location: String,
    #[serde(default)]
    pub asn: Option<u32>,
    pub observation: EndpointObservation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointObservation {
    pub endpoint: RpcEndpointConfig,
    pub overall_latency_ms: u64,
    pub health: ProbeValue<String>,
    pub slot: ProbeValue<u64>,
    pub block_height: ProbeValue<u64>,
    pub latest_blockhash: ProbeValue<BlockhashObservation>,
    pub version: ProbeValue<String>,
    pub identity: ProbeValue<IdentityObservation>,
    pub vote_accounts: ProbeValue<VoteAccountsObservation>,
    pub cluster_nodes: ProbeValue<ClusterNodesObservation>,
    pub leader_schedule: ProbeValue<LeaderScheduleObservation>,
    #[serde(default)]
    pub accounts: Vec<AccountObservation>,
    #[serde(default)]
    pub signatures: Vec<SignatureObservation>,
    #[serde(default)]
    pub probe_errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeValue<T> {
    pub value: Option<T>,
    pub latency_ms: u64,
    #[serde(default)]
    pub error: Option<String>,
}

impl<T> ProbeValue<T> {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            value: None,
            latency_ms: 0,
            error: None,
        }
    }

    #[must_use]
    pub fn ok(value: T, latency_ms: u64) -> Self {
        Self {
            value: Some(value),
            latency_ms,
            error: None,
        }
    }

    #[must_use]
    pub fn err(error: impl Into<String>, latency_ms: u64) -> Self {
        Self {
            value: None,
            latency_ms,
            error: Some(error.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockhashObservation {
    pub blockhash: String,
    pub last_valid_block_height: u64,
    pub context_slot: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityObservation {
    pub identity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VoteAccountsObservation {
    pub current_vote_accounts: usize,
    pub delinquent_vote_accounts: usize,
    pub current_activated_stake: u64,
    pub delinquent_activated_stake: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterNodesObservation {
    pub nodes: usize,
    pub rpc_nodes: usize,
    pub tpu_nodes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderScheduleObservation {
    pub validators: usize,
    pub total_leader_slots: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountObservation {
    pub pubkey: String,
    pub commitment: String,
    pub slot: Option<u64>,
    pub state_hash: Option<String>,
    pub lamports: Option<u64>,
    pub owner: Option<String>,
    pub executable: Option<bool>,
    pub rent_epoch: Option<u64>,
    pub data_len: Option<usize>,
    pub latency_ms: u64,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignatureObservation {
    pub signature: String,
    pub latency_ms: u64,
    pub status: Option<SignatureStatusObservation>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignatureStatusObservation {
    pub slot: u64,
    pub confirmation_status: Option<String>,
    pub confirmations: Option<usize>,
    pub finalized: bool,
    pub err: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkSnapshot {
    pub generated_at: DateTime<Utc>,
    pub freshness_window_seconds: u64,
    pub active_sentinels: usize,
    pub active_endpoints: usize,
    pub rpc_consistency_index: f64,
    pub asn_hhi: f64,
    pub validator_state_divergence: ValidatorStateDivergence,
    pub infrastructure_concentration: InfrastructureConcentration,
    pub propagation: PropagationSummary,
    #[serde(default)]
    pub anomalies: Vec<Anomaly>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorStateDivergence {
    pub slot_spread: u64,
    pub block_height_spread: u64,
    pub blockhash_disagreement_ratio: f64,
    pub account_divergence_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InfrastructureConcentration {
    pub provider_hhi: f64,
    #[serde(default)]
    pub provider_shares: Vec<ProviderShare>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderShare {
    pub provider: String,
    pub sample_share: f64,
    pub samples: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropagationSummary {
    pub tracked_signatures: usize,
    pub observed_signatures: usize,
    pub p50_window_ms: Option<u64>,
    pub p95_window_ms: Option<u64>,
    pub max_window_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderStatus {
    pub endpoint_id: String,
    pub label: String,
    pub provider: String,
    pub region: String,
    pub rpc_url: String,
    pub sentinel_id: String,
    pub sentinel_location: String,
    pub sampled_at: DateTime<Utc>,
    pub overall_latency_ms: u64,
    pub healthy: bool,
    pub slot: Option<u64>,
    pub block_height: Option<u64>,
    pub latest_blockhash: Option<String>,
    pub validator_identity: Option<String>,
    pub vote_accounts_current: Option<usize>,
    pub cluster_nodes: Option<usize>,
    pub slot_lag_from_max: Option<u64>,
    pub block_height_lag_from_max: Option<u64>,
    #[serde(default)]
    pub probe_errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignaturePropagation {
    pub signature: String,
    pub seen_by: usize,
    pub total_endpoints: usize,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub propagation_window_ms: u64,
    pub highest_observed_slot: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountStateVariant {
    pub pubkey: String,
    pub state_hash: String,
    pub endpoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountDivergence {
    pub pubkey: String,
    pub variants: Vec<AccountStateVariant>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Anomaly {
    pub severity: AnomalySeverity,
    pub code: String,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AnomalySeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestionResponse {
    pub accepted: bool,
    pub batch_id: Uuid,
    pub endpoints_received: usize,
    pub received_at: DateTime<Utc>,
    pub persisted: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ControlMessage {
    UpdateEndpoints { endpoints: Vec<RpcEndpointConfig> },
    AddEndpoint { endpoint: RpcEndpointConfig },
    RemoveEndpoint { id: String },
}
