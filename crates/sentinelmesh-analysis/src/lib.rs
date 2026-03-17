use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use sentinelmesh_core::{
    AccountDivergence, AccountStateVariant, Anomaly, AnomalySeverity, EndpointSample,
    InfrastructureConcentration, NetworkSnapshot, ProbeBatch, PropagationSummary, ProviderShare,
    ProviderStatus, SignaturePropagation, ValidatorStateDivergence,
};

pub struct MeshStore {
    retention: Duration,
    freshness_window: Duration,
    samples: Vec<EndpointSample>,
}

impl MeshStore {
    #[must_use]
    pub fn new(retention: Duration, freshness_window: Duration) -> Self {
        Self {
            retention,
            freshness_window,
            samples: Vec::new(),
        }
    }

    pub fn ingest(&mut self, batch: ProbeBatch) {
        self.samples.extend(batch.into_samples());
        self.prune_expired();
    }

    pub fn replace_samples(&mut self, samples: Vec<EndpointSample>) {
        self.samples = samples;
        self.prune_expired();
    }

    #[must_use]
    pub fn snapshot(&self) -> NetworkSnapshot {
        let now = Utc::now();
        let active = self.active_samples(now);
        let provider_statuses = self.provider_statuses();
        let account_divergences = Self::account_divergences_from(&active);
        let signature_propagation = self.signature_propagation();

        let slot_values: Vec<u64> = active
            .iter()
            .filter_map(|sample| sample.observation.slot.value)
            .collect();
        let block_height_values: Vec<u64> = active
            .iter()
            .filter_map(|sample| sample.observation.block_height.value)
            .collect();
        let blockhash_values: Vec<String> = active
            .iter()
            .filter_map(|sample| {
                sample
                    .observation
                    .latest_blockhash
                    .value
                    .as_ref()
                    .map(|blockhash| blockhash.blockhash.clone())
            })
            .collect();
        let account_scores: Vec<f64> = account_divergences
            .iter()
            .filter_map(|divergence| {
                categorical_agreement(
                    divergence
                        .variants
                        .iter()
                        .map(|variant| (variant.state_hash.clone(), variant.endpoints.len())),
                )
            })
            .collect();

        let mut consistency_scores = Vec::new();
        if let Some(score) = numeric_agreement(&slot_values) {
            consistency_scores.push(score);
        }
        if let Some(score) = numeric_agreement(&block_height_values) {
            consistency_scores.push(score);
        }
        if let Some(score) =
            categorical_agreement(blockhash_values.into_iter().map(|value| (value, 1_usize)))
        {
            consistency_scores.push(score);
        }
        if !account_scores.is_empty() {
            let average = account_scores.iter().sum::<f64>() / count_as_f64(account_scores.len());
            consistency_scores.push(average);
        }

        let rpc_consistency_index = if consistency_scores.is_empty() {
            0.0
        } else {
            consistency_scores.iter().sum::<f64>() / count_as_f64(consistency_scores.len())
        };

        let slot_spread = spread(&slot_values);
        let block_height_spread = spread(&block_height_values);
        let blockhash_disagreement_ratio = disagreement_ratio(
            provider_statuses.len(),
            mode_frequency(
                provider_statuses
                    .iter()
                    .filter_map(|provider| provider.latest_blockhash.clone()),
            ),
        );

        let infrastructure_concentration = Self::infrastructure_concentration(&active);
        let asn_hhi = Self::asn_concentration(&active);
        let propagation = summarize_propagation(&signature_propagation);
        let anomalies = build_anomalies(
            rpc_consistency_index,
            slot_spread,
            block_height_spread,
            blockhash_disagreement_ratio,
            account_divergences.len(),
            infrastructure_concentration.provider_hhi,
            asn_hhi,
            propagation.max_window_ms,
        );

        NetworkSnapshot {
            generated_at: now,
            freshness_window_seconds: self.freshness_window.as_secs(),
            active_sentinels: active
                .iter()
                .map(|sample| sample.sentinel_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            active_endpoints: active.len(),
            rpc_consistency_index,
            asn_hhi,
            validator_state_divergence: ValidatorStateDivergence {
                slot_spread,
                block_height_spread,
                blockhash_disagreement_ratio,
                account_divergence_count: account_divergences.len(),
            },
            infrastructure_concentration,
            propagation,
            anomalies,
        }
    }

    #[must_use]
    pub fn provider_statuses(&self) -> Vec<ProviderStatus> {
        let now = Utc::now();
        let active = self.active_samples(now);
        let max_slot = active
            .iter()
            .filter_map(|sample| sample.observation.slot.value)
            .max();
        let max_block_height = active
            .iter()
            .filter_map(|sample| sample.observation.block_height.value)
            .max();

        let mut providers: Vec<ProviderStatus> = active
            .into_iter()
            .map(|sample| ProviderStatus {
                endpoint_id: sample.observation.endpoint.id.clone(),
                label: sample.observation.endpoint.label.clone(),
                provider: sample.observation.endpoint.provider.clone(),
                region: sample.observation.endpoint.region.clone(),
                rpc_url: sample.observation.endpoint.rpc_url.clone(),
                sentinel_id: sample.sentinel_id.clone(),
                sentinel_location: sample.sentinel_location.clone(),
                sampled_at: sample.sampled_at,
                overall_latency_ms: sample.observation.overall_latency_ms,
                healthy: sample.observation.health.value.as_deref() == Some("ok")
                    && sample.observation.probe_errors.is_empty(),
                slot: sample.observation.slot.value,
                block_height: sample.observation.block_height.value,
                latest_blockhash: sample
                    .observation
                    .latest_blockhash
                    .value
                    .as_ref()
                    .map(|value| value.blockhash.clone()),
                validator_identity: sample
                    .observation
                    .identity
                    .value
                    .as_ref()
                    .map(|identity| identity.identity.clone()),
                vote_accounts_current: sample
                    .observation
                    .vote_accounts
                    .value
                    .as_ref()
                    .map(|vote_accounts| vote_accounts.current_vote_accounts),
                cluster_nodes: sample
                    .observation
                    .cluster_nodes
                    .value
                    .as_ref()
                    .map(|cluster_nodes| cluster_nodes.nodes),
                slot_lag_from_max: max_slot
                    .zip(sample.observation.slot.value)
                    .map(|(max, value)| max - value),
                block_height_lag_from_max: max_block_height
                    .zip(sample.observation.block_height.value)
                    .map(|(max, value)| max - value),
                probe_errors: sample.observation.probe_errors.clone(),
            })
            .collect();

        providers.sort_by(|left, right| {
            right
                .slot_lag_from_max
                .cmp(&left.slot_lag_from_max)
                .then_with(|| left.provider.cmp(&right.provider))
                .then_with(|| left.label.cmp(&right.label))
        });
        providers
    }

    #[must_use]
    pub fn signature_propagation(&self) -> Vec<SignaturePropagation> {
        let active_endpoint_count = self.active_samples(Utc::now()).len();
        let mut signatures: BTreeMap<String, SignatureAccumulator> = BTreeMap::new();

        for sample in &self.samples {
            let endpoint_key = sample_key(sample);
            for signature in &sample.observation.signatures {
                if let Some(status) = &signature.status {
                    let entry = signatures.entry(signature.signature.clone()).or_default();
                    entry.first_seen_at = Some(
                        entry
                            .first_seen_at
                            .map_or(sample.sampled_at, |current| current.min(sample.sampled_at)),
                    );
                    entry.last_seen_at = Some(
                        entry
                            .last_seen_at
                            .map_or(sample.sampled_at, |current| current.max(sample.sampled_at)),
                    );
                    entry.seen_by.insert(endpoint_key.clone());
                    entry.highest_observed_slot = Some(
                        entry
                            .highest_observed_slot
                            .map_or(status.slot, |current| current.max(status.slot)),
                    );
                }
            }
        }

        let mut propagation: Vec<SignaturePropagation> = signatures
            .into_iter()
            .filter_map(|(signature, accumulator)| {
                Some(SignaturePropagation {
                    signature,
                    seen_by: accumulator.seen_by.len(),
                    total_endpoints: active_endpoint_count,
                    first_seen_at: accumulator.first_seen_at?,
                    last_seen_at: accumulator.last_seen_at?,
                    propagation_window_ms: saturating_window_ms(
                        accumulator
                            .last_seen_at?
                            .signed_duration_since(accumulator.first_seen_at?),
                    ),
                    highest_observed_slot: accumulator.highest_observed_slot,
                })
            })
            .collect();

        propagation.sort_by(|left, right| {
            right
                .propagation_window_ms
                .cmp(&left.propagation_window_ms)
                .then_with(|| left.signature.cmp(&right.signature))
        });
        propagation
    }

    #[must_use]
    pub fn account_divergences(&self) -> Vec<AccountDivergence> {
        let active = self.active_samples(Utc::now());
        Self::account_divergences_from(&active)
    }

    fn account_divergences_from(active: &[&EndpointSample]) -> Vec<AccountDivergence> {
        let mut accounts: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();

        for sample in active {
            let endpoint_label = format!(
                "{}:{}@{}",
                sample.observation.endpoint.provider,
                sample.observation.endpoint.label,
                sample.sentinel_location
            );
            for account in &sample.observation.accounts {
                if let Some(state_hash) = &account.state_hash {
                    accounts
                        .entry(account.pubkey.clone())
                        .or_default()
                        .entry(state_hash.clone())
                        .or_default()
                        .push(endpoint_label.clone());
                }
            }
        }

        let mut divergences = Vec::new();
        for (pubkey, variants) in accounts {
            if variants.len() <= 1 {
                continue;
            }

            let variants = variants
                .into_iter()
                .map(|(state_hash, endpoints)| AccountStateVariant {
                    pubkey: pubkey.clone(),
                    state_hash,
                    endpoints,
                })
                .collect();

            divergences.push(AccountDivergence { pubkey, variants });
        }

        divergences
    }

    fn infrastructure_concentration(active: &[&EndpointSample]) -> InfrastructureConcentration {
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for sample in active {
            *counts
                .entry(sample.observation.endpoint.provider.clone())
                .or_default() += 1;
        }

        let total = active.len();
        let mut provider_shares = Vec::with_capacity(counts.len());
        let mut provider_hhi = 0.0;
        for (provider, samples) in counts {
            let share = if total == 0 {
                0.0
            } else {
                ratio_usize(samples, total)
            };
            provider_hhi += share * share;
            provider_shares.push(ProviderShare {
                provider,
                sample_share: share,
                samples,
            });
        }

        provider_shares.sort_by(|left, right| right.samples.cmp(&left.samples));

        InfrastructureConcentration {
            provider_hhi,
            provider_shares,
        }
    }

    fn asn_concentration(active: &[&EndpointSample]) -> f64 {
        let mut counts: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
        let mut total_with_asn = 0;
        for sample in active {
            if let Some(asn) = sample.asn {
                *counts.entry(asn).or_default() += 1;
                total_with_asn += 1;
            }
        }
        if total_with_asn == 0 {
            return 0.0;
        }
        let mut asn_hhi = 0.0;
        for count in counts.values() {
            let share = ratio_usize(*count, total_with_asn);
            asn_hhi += share * share;
        }
        asn_hhi
    }

    fn active_samples(&self, now: DateTime<Utc>) -> Vec<&EndpointSample> {
        let cutoff = now - chrono_duration(self.freshness_window);
        let mut latest: BTreeMap<String, &EndpointSample> = BTreeMap::new();

        for sample in self
            .samples
            .iter()
            .filter(|sample| sample.sampled_at >= cutoff)
        {
            let key = sample_key(sample);
            if let Some(current) = latest.get(&key) {
                if sample.sampled_at > current.sampled_at {
                    latest.insert(key, sample);
                }
            } else {
                latest.insert(key, sample);
            }
        }

        latest.into_values().collect()
    }

    fn prune_expired(&mut self) {
        let cutoff = Utc::now() - chrono_duration(self.retention);
        self.samples.retain(|sample| sample.sampled_at >= cutoff);
    }
}

#[derive(Default)]
struct SignatureAccumulator {
    first_seen_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    seen_by: BTreeSet<String>,
    highest_observed_slot: Option<u64>,
}

fn summarize_propagation(propagation: &[SignaturePropagation]) -> PropagationSummary {
    let mut windows: Vec<u64> = propagation
        .iter()
        .map(|sample| sample.propagation_window_ms)
        .collect();
    windows.sort_unstable();

    PropagationSummary {
        tracked_signatures: propagation.len(),
        observed_signatures: propagation
            .iter()
            .filter(|sample| sample.seen_by > 0)
            .count(),
        p50_window_ms: percentile(&windows, 0.50),
        p95_window_ms: percentile(&windows, 0.95),
        max_window_ms: windows.last().copied(),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_anomalies(
    rpc_consistency_index: f64,
    slot_spread: u64,
    block_height_spread: u64,
    blockhash_disagreement_ratio: f64,
    account_divergence_count: usize,
    provider_hhi: f64,
    asn_hhi: f64,
    max_propagation_window_ms: Option<u64>,
) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();

    let is_topologically_blind = asn_hhi >= 0.90; // Over 90% concentration in a single ASN

    if rpc_consistency_index < 0.85 {
        anomalies.push(Anomaly {
            severity: if is_topologically_blind { AnomalySeverity::Warning } else { AnomalySeverity::Critical },
            code: "rpc_consistency_degraded".to_owned(),
            summary: format!(
                "RPC consistency index degraded to {rpc_consistency_index:.3};{}.",
                if is_topologically_blind { " High ASN concentration detected, downgrading severity as possible topological blindness" } else { " independent verification should inspect provider skew" }
            ),
        });
    }

    if slot_spread > 8 {
        anomalies.push(Anomaly {
            severity: if slot_spread > 32 && !is_topologically_blind {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            },
            code: "slot_spread_high".to_owned(),
            summary: format!(
                "Observed slot spread reached {slot_spread} slots across active endpoints."
            ),
        });
    }

    if block_height_spread > 8 {
        anomalies.push(Anomaly {
            severity: if block_height_spread > 32 && !is_topologically_blind {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            },
            code: "block_height_spread_high".to_owned(),
            summary: format!(
                "Observed block height spread reached {block_height_spread} blocks across active endpoints."
            ),
        });
    }

    if blockhash_disagreement_ratio > 0.20 {
        anomalies.push(Anomaly {
            severity: AnomalySeverity::Warning,
            code: "blockhash_disagreement".to_owned(),
            summary: format!(
                "Blockhash disagreement ratio is {:.2}% across active endpoints.",
                blockhash_disagreement_ratio * 100.0
            ),
        });
    }

    if account_divergence_count > 0 {
        anomalies.push(Anomaly {
            severity: AnomalySeverity::Warning,
            code: "account_state_divergence".to_owned(),
            summary: format!(
                "Detected {account_divergence_count} tracked account(s) with divergent state hashes."
            ),
        });
    }

    if provider_hhi >= 0.35 {
        anomalies.push(Anomaly {
            severity: AnomalySeverity::Info,
            code: "provider_concentration".to_owned(),
            summary: format!(
                "Provider concentration HHI is {provider_hhi:.3}, indicating elevated infrastructure centralization."
            ),
        });
    }

    if let Some(window_ms) = max_propagation_window_ms {
        if window_ms > 3_000 {
            anomalies.push(Anomaly {
                severity: if window_ms > 10_000 {
                    AnomalySeverity::Critical
                } else {
                    AnomalySeverity::Warning
                },
                code: "transaction_propagation_slow".to_owned(),
                summary: format!(
                    "Maximum observed transaction propagation window reached {window_ms} ms."
                ),
            });
        }
    }

    anomalies
}

fn sample_key(sample: &EndpointSample) -> String {
    format!("{}::{}", sample.sentinel_id, sample.observation.endpoint.id)
}

fn chrono_duration(duration: Duration) -> chrono::Duration {
    chrono::Duration::from_std(duration).unwrap_or_else(|_| {
        let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
        chrono::Duration::seconds(seconds)
    })
}

fn numeric_agreement(values: &[u64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    // Low sample counts skip the BFT trimming
    if values.len() <= 2 {
        let max = *values.iter().max()?;
        let min = *values.iter().min()?;
        if max == 0 {
            return Some(1.0);
        }
        return Some((1.0 - ratio_u64(max - min, max)).clamp(0.0, 1.0));
    }

    // BFT Implementation: Trimmed means removing the 10% bottom and top tails
    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let trim_count = (sorted.len() as f64 * 0.1).floor() as usize;
    let trimmed = &sorted[trim_count..sorted.len() - trim_count];

    let max = *trimmed.last()?;
    let min = *trimmed.first()?;

    if max == 0 {
        Some(1.0)
    } else {
        Some((1.0 - ratio_u64(max - min, max)).clamp(0.0, 1.0))
    }
}

fn categorical_agreement<I>(values: I) -> Option<f64>
where
    I: IntoIterator<Item = (String, usize)>,
{
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0_usize;
    for (value, count) in values {
        total += count;
        *counts.entry(value).or_default() += count;
    }

    let mode = counts.values().copied().max()?;
    Some(ratio_usize(mode, total))
}

fn spread(values: &[u64]) -> u64 {
    match (values.iter().min(), values.iter().max()) {
        (Some(min), Some(max)) => max - min,
        _ => 0,
    }
}

fn mode_frequency<I>(values: I) -> usize
where
    I: IntoIterator<Item = String>,
{
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts.values().copied().max().unwrap_or(0)
}

fn disagreement_ratio(total: usize, mode_count: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        1.0 - ratio_usize(mode_count, total)
    }
}

fn percentile(sorted_values: &[u64], quantile: f64) -> Option<u64> {
    if sorted_values.is_empty() {
        return None;
    }

    let last_index = sorted_values.len().saturating_sub(1);
    let index = quantile_index(last_index, quantile);
    sorted_values.get(index).copied()
}

fn saturating_window_ms(duration: chrono::Duration) -> u64 {
    let millis = duration.num_milliseconds();
    if millis < 0 {
        0
    } else {
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

#[allow(clippy::cast_precision_loss)]
fn count_as_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn ratio_usize(numerator: usize, denominator: usize) -> f64 {
    numerator as f64 / denominator as f64
}

#[allow(clippy::cast_precision_loss)]
fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn quantile_index(last_index: usize, quantile: f64) -> usize {
    ((count_as_f64(last_index)) * quantile.clamp(0.0, 1.0)).round() as usize
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Duration as ChronoDuration;
    use sentinelmesh_core::{
        AccountObservation, BlockhashObservation, EndpointObservation, ProbeBatch, ProbeValue,
        RpcEndpointConfig, SignatureObservation, SignatureStatusObservation,
    };
    use uuid::Uuid;

    use super::MeshStore;

    #[test]
    fn snapshot_reports_perfect_consistency_for_identical_observations() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );
        store.ingest(batch(
            base,
            "sentinel-a",
            "us-east-1",
            vec![
                endpoint("rpc-a", "ProviderA", 100, 90, "hash-1"),
                endpoint("rpc-b", "ProviderB", 100, 90, "hash-1"),
            ],
        ));

        let snapshot = store.snapshot();
        assert_eq!(snapshot.active_endpoints, 2);
        assert!((snapshot.rpc_consistency_index - 1.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.validator_state_divergence.slot_spread, 0);
        assert!(!snapshot.anomalies.iter().any(|anomaly| matches!(
            anomaly.severity,
            sentinelmesh_core::AnomalySeverity::Critical
        )));
    }

    #[test]
    fn signature_propagation_tracks_detection_window() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(300),
        );
        store.ingest(batch(
            base,
            "sentinel-a",
            "us-east-1",
            vec![endpoint_with_signature(
                "rpc-a",
                "ProviderA",
                100,
                90,
                "hash-1",
                Some("sig-1"),
            )],
        ));
        store.ingest(batch(
            base + ChronoDuration::milliseconds(900),
            "sentinel-b",
            "eu-west-1",
            vec![endpoint_with_signature(
                "rpc-b",
                "ProviderB",
                100,
                90,
                "hash-1",
                Some("sig-1"),
            )],
        ));

        let propagation = store.signature_propagation();
        assert_eq!(propagation.len(), 1);
        assert_eq!(propagation[0].seen_by, 2);
        assert_eq!(propagation[0].propagation_window_ms, 900);
    }

    fn batch(
        sampled_at: chrono::DateTime<chrono::Utc>,
        sentinel_id: &str,
        location: &str,
        endpoints: Vec<EndpointObservation>,
    ) -> ProbeBatch {
        ProbeBatch {
            schema_version: 1,
            batch_id: Uuid::new_v4(),
            sampled_at,
            sentinel_id: sentinel_id.to_owned(),
            sentinel_location: location.to_owned(),
            asn: None,
            endpoints,
        }
    }

    fn endpoint(
        id: &str,
        provider: &str,
        slot: u64,
        block_height: u64,
        blockhash: &str,
    ) -> EndpointObservation {
        endpoint_with_signature(id, provider, slot, block_height, blockhash, None)
    }

    fn endpoint_with_signature(
        id: &str,
        provider: &str,
        slot: u64,
        block_height: u64,
        blockhash: &str,
        signature: Option<&str>,
    ) -> EndpointObservation {
        EndpointObservation {
            endpoint: RpcEndpointConfig {
                id: id.to_owned(),
                label: id.to_owned(),
                provider: provider.to_owned(),
                region: "global".to_owned(),
                rpc_url: format!("https://{id}.example.com"),
                tags: BTreeMap::default(),
            },
            overall_latency_ms: 25,
            health: ProbeValue::ok("ok".to_owned(), 2),
            slot: ProbeValue::ok(slot, 3),
            block_height: ProbeValue::ok(block_height, 3),
            latest_blockhash: ProbeValue::ok(
                BlockhashObservation {
                    blockhash: blockhash.to_owned(),
                    last_valid_block_height: block_height + 150,
                    context_slot: slot,
                },
                3,
            ),
            version: ProbeValue::ok("2.2.1".to_owned(), 3),
            identity: ProbeValue::empty(),
            vote_accounts: ProbeValue::empty(),
            cluster_nodes: ProbeValue::empty(),
            leader_schedule: ProbeValue::empty(),
            accounts: vec![AccountObservation {
                pubkey: "account-1".to_owned(),
                commitment: "confirmed".to_owned(),
                slot: Some(slot),
                state_hash: Some("state-hash-1".to_owned()),
                lamports: Some(42),
                owner: Some("11111111111111111111111111111111".to_owned()),
                executable: Some(false),
                rent_epoch: Some(0),
                data_len: Some(0),
                latency_ms: 3,
                error: None,
            }],
            signatures: signature
                .map(|signature| {
                    vec![SignatureObservation {
                        signature: signature.to_owned(),
                        latency_ms: 4,
                        status: Some(SignatureStatusObservation {
                            slot,
                            confirmation_status: Some("confirmed".to_owned()),
                            confirmations: Some(1),
                            finalized: false,
                            err: None,
                        }),
                        error: None,
                    }]
                })
                .unwrap_or_default(),
            probe_errors: Vec::new(),
        }
    }
}
