use crate::config::RpcEndpointConfig;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProbeEnvelope {
    pub batch: ProbeBatch,
    #[serde(default)]
    pub auth: Option<BatchAuth>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
    #[serde(default)]
    pub transaction_order: Vec<TransactionOrderObservation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BlockhashObservation {
    pub blockhash: String,
    pub last_valid_block_height: u64,
    pub context_slot: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IdentityObservation {
    pub identity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VoteAccountsObservation {
    pub current_vote_accounts: usize,
    pub delinquent_vote_accounts: usize,
    pub current_activated_stake: u64,
    pub delinquent_activated_stake: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterNodesObservation {
    pub nodes: usize,
    pub rpc_nodes: usize,
    pub tpu_nodes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LeaderScheduleObservation {
    pub validators: usize,
    pub total_leader_slots: usize,
    #[serde(default)]
    pub schedule: Option<BTreeMap<String, Vec<u64>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignatureObservation {
    pub signature: String,
    pub latency_ms: u64,
    pub status: Option<SignatureStatusObservation>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
    #[serde(default)]
    pub mev_audit: Option<MevAuditSummary>,
    #[serde(default)]
    pub z_scores: Option<ZScoreReport>,
    #[serde(default)]
    pub leader_schedule_anomalies: usize,
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
    #[serde(default)]
    pub p99_window_ms: Option<u64>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TransactionOrderObservation {
    pub slot: u64,
    pub transaction_signatures: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MevAuditSummary {
    pub slots_analyzed: usize,
    pub slots_with_reordering: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZScoreReport {
    pub slot_spread_z: Option<f64>,
    pub block_height_spread_z: Option<f64>,
    pub avg_latency_z: Option<f64>,
    pub provider_hhi_z: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityChangeEvent {
    pub endpoint_id: String,
    pub timestamp: DateTime<Utc>,
    pub previous_identity: String,
    pub new_identity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CircuitBreakerStatus {
    pub endpoint_id: String,
    pub state: String,
    pub consecutive_failures: u32,
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: sentinelmesh-comprehensive-upgrade, Property 22: Round-trip de serialização ProbeEnvelope
    // **Validates: Requirements 18.6**

    // --- Strategy helpers ---

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        // Generate timestamps in a reasonable range (2020-2030)
        (1_577_836_800i64..1_893_456_000i64)
            .prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap_or_else(Utc::now))
    }

    fn arb_short_string() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_]{1,16}"
    }

    fn arb_hex_string() -> impl Strategy<Value = String> {
        "[0-9a-f]{8,64}"
    }

    fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
        // Exclude Null because Option<Value> treats Some(Null) as None after
        // JSON round-trip, which is correct serde behavior but breaks PartialEq.
        prop_oneof![
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i32>().prop_map(|n| serde_json::Value::Number(n.into())),
            arb_short_string().prop_map(serde_json::Value::String),
        ]
    }

    fn arb_rpc_endpoint_config() -> impl Strategy<Value = RpcEndpointConfig> {
        (
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            "https?://[a-z]{3,8}\\.[a-z]{2,4}",
            prop::collection::btree_map(arb_short_string(), arb_short_string(), 0..3),
        )
            .prop_map(
                |(id, label, provider, region, rpc_url, tags)| RpcEndpointConfig {
                    id,
                    label,
                    provider,
                    region,
                    rpc_url,
                    tags,
                },
            )
    }

    fn arb_blockhash_observation() -> impl Strategy<Value = BlockhashObservation> {
        (arb_hex_string(), any::<u64>(), any::<u64>()).prop_map(
            |(blockhash, last_valid_block_height, context_slot)| BlockhashObservation {
                blockhash,
                last_valid_block_height,
                context_slot,
            },
        )
    }

    fn arb_identity_observation() -> impl Strategy<Value = IdentityObservation> {
        arb_short_string().prop_map(|identity| IdentityObservation { identity })
    }

    fn arb_vote_accounts_observation() -> impl Strategy<Value = VoteAccountsObservation> {
        (any::<usize>(), any::<usize>(), any::<u64>(), any::<u64>()).prop_map(
            |(
                current_vote_accounts,
                delinquent_vote_accounts,
                current_activated_stake,
                delinquent_activated_stake,
            )| {
                VoteAccountsObservation {
                    current_vote_accounts,
                    delinquent_vote_accounts,
                    current_activated_stake,
                    delinquent_activated_stake,
                }
            },
        )
    }

    fn arb_cluster_nodes_observation() -> impl Strategy<Value = ClusterNodesObservation> {
        (any::<usize>(), any::<usize>(), any::<usize>()).prop_map(
            |(nodes, rpc_nodes, tpu_nodes)| ClusterNodesObservation {
                nodes,
                rpc_nodes,
                tpu_nodes,
            },
        )
    }

    fn arb_leader_schedule_observation() -> impl Strategy<Value = LeaderScheduleObservation> {
        (
            any::<usize>(),
            any::<usize>(),
            prop::option::of(prop::collection::btree_map(
                arb_short_string(),
                prop::collection::vec(any::<u64>(), 0..4),
                0..3,
            )),
        )
            .prop_map(|(validators, total_leader_slots, schedule)| {
                LeaderScheduleObservation {
                    validators,
                    total_leader_slots,
                    schedule,
                }
            })
    }

    fn arb_probe_value<T: std::fmt::Debug + 'static>(
        inner: impl Strategy<Value = T> + 'static,
    ) -> impl Strategy<Value = ProbeValue<T>> {
        (
            prop::option::of(inner),
            0..10_000u64,
            prop::option::of(arb_short_string()),
        )
            .prop_map(|(value, latency_ms, error)| ProbeValue {
                value,
                latency_ms,
                error,
            })
    }

    fn arb_account_observation() -> impl Strategy<Value = AccountObservation> {
        (
            arb_short_string(),
            arb_short_string(),
            prop::option::of(any::<u64>()),
            prop::option::of(arb_hex_string()),
            prop::option::of(any::<u64>()),
            prop::option::of(arb_short_string()),
            prop::option::of(any::<bool>()),
            prop::option::of(any::<u64>()),
            prop::option::of(any::<usize>()),
            0..10_000u64,
            prop::option::of(arb_short_string()),
        )
            .prop_map(
                |(
                    pubkey,
                    commitment,
                    slot,
                    state_hash,
                    lamports,
                    owner,
                    executable,
                    rent_epoch,
                    data_len,
                    latency_ms,
                    error,
                )| {
                    AccountObservation {
                        pubkey,
                        commitment,
                        slot,
                        state_hash,
                        lamports,
                        owner,
                        executable,
                        rent_epoch,
                        data_len,
                        latency_ms,
                        error,
                    }
                },
            )
    }

    fn arb_signature_status_observation() -> impl Strategy<Value = SignatureStatusObservation> {
        (
            any::<u64>(),
            prop::option::of(arb_short_string()),
            prop::option::of(any::<usize>()),
            any::<bool>(),
            prop::option::of(arb_json_value()),
        )
            .prop_map(
                |(slot, confirmation_status, confirmations, finalized, err)| {
                    SignatureStatusObservation {
                        slot,
                        confirmation_status,
                        confirmations,
                        finalized,
                        err,
                    }
                },
            )
    }

    fn arb_signature_observation() -> impl Strategy<Value = SignatureObservation> {
        (
            arb_hex_string(),
            0..10_000u64,
            prop::option::of(arb_signature_status_observation()),
            prop::option::of(arb_short_string()),
        )
            .prop_map(
                |(signature, latency_ms, status, error)| SignatureObservation {
                    signature,
                    latency_ms,
                    status,
                    error,
                },
            )
    }

    fn arb_transaction_order_observation() -> impl Strategy<Value = TransactionOrderObservation> {
        (any::<u64>(), prop::collection::vec(arb_hex_string(), 0..5)).prop_map(
            |(slot, transaction_signatures)| TransactionOrderObservation {
                slot,
                transaction_signatures,
            },
        )
    }

    fn arb_endpoint_observation() -> impl Strategy<Value = EndpointObservation> {
        // Split into two groups to stay within proptest's 12-element tuple limit
        let probes = (
            arb_rpc_endpoint_config(),
            0..10_000u64,
            arb_probe_value(arb_short_string()),
            arb_probe_value(any::<u64>()),
            arb_probe_value(any::<u64>()),
            arb_probe_value(arb_blockhash_observation()),
            arb_probe_value(arb_short_string()),
            arb_probe_value(arb_identity_observation()),
            arb_probe_value(arb_vote_accounts_observation()),
            arb_probe_value(arb_cluster_nodes_observation()),
            arb_probe_value(arb_leader_schedule_observation()),
        );
        let collections = (
            prop::collection::vec(arb_account_observation(), 0..3),
            prop::collection::vec(arb_signature_observation(), 0..3),
            prop::collection::vec(arb_short_string(), 0..3),
            prop::collection::vec(arb_transaction_order_observation(), 0..3),
        );
        (probes, collections).prop_map(
            |(
                (
                    endpoint,
                    overall_latency_ms,
                    health,
                    slot,
                    block_height,
                    latest_blockhash,
                    version,
                    identity,
                    vote_accounts,
                    cluster_nodes,
                    leader_schedule,
                ),
                (accounts, signatures, probe_errors, transaction_order),
            )| {
                EndpointObservation {
                    endpoint,
                    overall_latency_ms,
                    health,
                    slot,
                    block_height,
                    latest_blockhash,
                    version,
                    identity,
                    vote_accounts,
                    cluster_nodes,
                    leader_schedule,
                    accounts,
                    signatures,
                    probe_errors,
                    transaction_order,
                }
            },
        )
    }

    fn arb_batch_auth() -> impl Strategy<Value = BatchAuth> {
        (
            arb_short_string(),
            arb_short_string(),
            arb_datetime(),
            arb_hex_string(),
            arb_hex_string(),
        )
            .prop_map(
                |(signer_id, key_id, signed_at, batch_hash, signature_b64)| BatchAuth {
                    signer_id,
                    key_id,
                    signed_at,
                    batch_hash,
                    signature_b64,
                },
            )
    }

    fn arb_probe_batch() -> impl Strategy<Value = ProbeBatch> {
        (
            any::<u16>(),
            arb_short_string().prop_map(|_| Uuid::new_v4()),
            arb_datetime(),
            arb_short_string(),
            arb_short_string(),
            prop::option::of(any::<u32>()),
            prop::collection::vec(arb_endpoint_observation(), 0..3),
        )
            .prop_map(
                |(
                    schema_version,
                    batch_id,
                    sampled_at,
                    sentinel_id,
                    sentinel_location,
                    asn,
                    endpoints,
                )| {
                    ProbeBatch {
                        schema_version,
                        batch_id,
                        sampled_at,
                        sentinel_id,
                        sentinel_location,
                        asn,
                        endpoints,
                    }
                },
            )
    }

    fn arb_probe_envelope() -> impl Strategy<Value = ProbeEnvelope> {
        (arb_probe_batch(), prop::option::of(arb_batch_auth()))
            .prop_map(|(batch, auth)| ProbeEnvelope { batch, auth })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_probe_envelope_json_round_trip(envelope in arb_probe_envelope()) {
            // Feature: sentinelmesh-comprehensive-upgrade, Property 22: Round-trip de serialização ProbeEnvelope
            let json = serde_json::to_string(&envelope).expect("serialization should succeed");
            let deserialized: ProbeEnvelope =
                serde_json::from_str(&json).expect("deserialization should succeed");
            prop_assert_eq!(envelope, deserialized);
        }
    }
}
