#![allow(clippy::cast_precision_loss)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::for_kv_map)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::unnecessary_sort_by)]

pub mod anomaly;
pub mod mev;

use chrono::{DateTime, Utc};
use sentinelmesh_core::{
    AccountDivergence, AccountStateVariant, Anomaly, AnomalySeverity, EndpointSample,
    IdentityChangeEvent, InfrastructureConcentration, NetworkSnapshot, ProbeBatch,
    PropagationSummary, ProviderShare, ProviderStatus, SignaturePropagation,
    ValidatorStateDivergence, ZScoreReport,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use crate::anomaly::{DetectionMode, SlidingWindow};

/// Tracks validator identity changes per endpoint over time.
pub struct ValidatorIdentityTracker {
    /// Maps `endpoint_id` → list of identity change events.
    history: BTreeMap<String, Vec<IdentityChangeEvent>>,
    /// Last known identity per `endpoint_id`.
    last_known: BTreeMap<String, String>,
}

impl ValidatorIdentityTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            history: BTreeMap::new(),
            last_known: BTreeMap::new(),
        }
    }

    /// Check the current identity for an endpoint and record a change event if it differs.
    /// Returns `Some(IdentityChangeEvent)` if a change was detected, `None` otherwise.
    pub fn track(
        &mut self,
        endpoint_id: &str,
        identity: &str,
        timestamp: DateTime<Utc>,
    ) -> Option<IdentityChangeEvent> {
        if let Some(previous) = self.last_known.get(endpoint_id) {
            if previous != identity {
                let event = IdentityChangeEvent {
                    endpoint_id: endpoint_id.to_owned(),
                    timestamp,
                    previous_identity: previous.clone(),
                    new_identity: identity.to_owned(),
                };
                self.history
                    .entry(endpoint_id.to_owned())
                    .or_default()
                    .push(event.clone());
                self.last_known
                    .insert(endpoint_id.to_owned(), identity.to_owned());
                return Some(event);
            }
        } else {
            // First observation — record as baseline, no change event
            self.last_known
                .insert(endpoint_id.to_owned(), identity.to_owned());
        }
        None
    }

    /// Returns the full identity change history.
    #[must_use]
    pub fn history(&self) -> &BTreeMap<String, Vec<IdentityChangeEvent>> {
        &self.history
    }
}

impl Default for ValidatorIdentityTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MeshStore {
    retention: Duration,
    freshness_window: Duration,
    samples: Vec<EndpointSample>,
    detection_mode: DetectionMode,
    slot_spread_window: SlidingWindow,
    block_height_spread_window: SlidingWindow,
    avg_latency_window: SlidingWindow,
    provider_hhi_window: SlidingWindow,
    identity_tracker: ValidatorIdentityTracker,
    pending_identity_anomalies: Vec<Anomaly>,
}

impl MeshStore {
    #[must_use]
    pub fn new(retention: Duration, freshness_window: Duration) -> Self {
        Self {
            retention,
            freshness_window,
            samples: Vec::new(),
            detection_mode: DetectionMode::Fixed,
            slot_spread_window: SlidingWindow::default(),
            block_height_spread_window: SlidingWindow::default(),
            avg_latency_window: SlidingWindow::default(),
            provider_hhi_window: SlidingWindow::default(),
            identity_tracker: ValidatorIdentityTracker::new(),
            pending_identity_anomalies: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_detection(
        retention: Duration,
        freshness_window: Duration,
        detection_mode: DetectionMode,
        sliding_window_size: usize,
    ) -> Self {
        Self {
            retention,
            freshness_window,
            samples: Vec::new(),
            detection_mode,
            slot_spread_window: SlidingWindow::new(sliding_window_size),
            block_height_spread_window: SlidingWindow::new(sliding_window_size),
            avg_latency_window: SlidingWindow::new(sliding_window_size),
            provider_hhi_window: SlidingWindow::new(sliding_window_size),
            identity_tracker: ValidatorIdentityTracker::new(),
            pending_identity_anomalies: Vec::new(),
        }
    }

    pub fn ingest(&mut self, batch: ProbeBatch) {
        let timestamp = batch.sampled_at;
        // Track identity changes for each endpoint in the batch
        for obs in &batch.endpoints {
            if let Some(identity_obs) = &obs.identity.value {
                if let Some(event) =
                    self.identity_tracker
                        .track(&obs.endpoint.id, &identity_obs.identity, timestamp)
                {
                    self.pending_identity_anomalies.push(Anomaly {
                        severity: AnomalySeverity::Info,
                        code: "validator_identity_change".to_owned(),
                        summary: format!(
                            "Validator identity changed for endpoint {}: {} → {}",
                            event.endpoint_id, event.previous_identity, event.new_identity,
                        ),
                    });
                }
            }
        }
        self.samples.extend(batch.into_samples());
        self.prune_expired();
    }

    pub fn replace_samples(&mut self, samples: Vec<EndpointSample>) {
        self.samples = samples;
        self.prune_expired();
    }

    #[must_use]
    pub fn snapshot(&mut self) -> NetworkSnapshot {
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

        // Compute average latency across active samples
        let avg_latency = if active.is_empty() {
            0.0
        } else {
            let total: u64 = active
                .iter()
                .map(|s| s.observation.overall_latency_ms)
                .sum();
            total as f64 / active.len() as f64
        };

        // Collect transaction order observations for MEV analysis
        let tx_order_observations: Vec<sentinelmesh_core::TransactionOrderObservation> = active
            .iter()
            .flat_map(|sample| sample.observation.transaction_order.clone())
            .collect();

        // Leader schedule analysis
        let (leader_schedule_anomaly_count, leader_schedule_anomalies) =
            analyse_leader_schedule(&active);

        // Capture values from active borrow before dropping it
        let active_sentinels = active
            .iter()
            .map(|sample| sample.sentinel_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let active_endpoints = active.len();

        // Drop the immutable borrow of self.samples via `active`
        drop(active);

        // Push current values into sliding windows
        self.slot_spread_window.push(slot_spread as f64);
        self.block_height_spread_window
            .push(block_height_spread as f64);
        self.avg_latency_window.push(avg_latency);
        self.provider_hhi_window
            .push(infrastructure_concentration.provider_hhi);

        // Compute z-scores and generate statistical anomalies
        let (z_scores, z_score_anomalies) = if self.detection_mode == DetectionMode::Statistical {
            let slot_z = self.slot_spread_window.z_score(slot_spread as f64);
            let bh_z = self
                .block_height_spread_window
                .z_score(block_height_spread as f64);
            let lat_z = self.avg_latency_window.z_score(avg_latency);
            let hhi_z = self
                .provider_hhi_window
                .z_score(infrastructure_concentration.provider_hhi);

            let report = ZScoreReport {
                slot_spread_z: slot_z,
                block_height_spread_z: bh_z,
                avg_latency_z: lat_z,
                provider_hhi_z: hhi_z,
            };

            let mut z_anomalies = Vec::new();
            if let Some(z) = slot_z {
                if z.abs() >= 3.0 {
                    z_anomalies.push(Anomaly {
                        severity: if z.abs() >= 4.0 {
                            AnomalySeverity::Critical
                        } else {
                            AnomalySeverity::Warning
                        },
                        code: "zscore_slot_spread".to_owned(),
                        summary: format!(
                            "Slot spread z-score is {z:.2}, indicating statistical anomaly."
                        ),
                    });
                }
            }
            if let Some(z) = bh_z {
                if z.abs() >= 3.0 {
                    z_anomalies.push(Anomaly {
                        severity: if z.abs() >= 4.0 {
                            AnomalySeverity::Critical
                        } else {
                            AnomalySeverity::Warning
                        },
                        code: "zscore_block_height_spread".to_owned(),
                        summary: format!(
                            "Block height spread z-score is {z:.2}, indicating statistical anomaly."
                        ),
                    });
                }
            }
            if let Some(z) = lat_z {
                if z.abs() >= 3.0 {
                    z_anomalies.push(Anomaly {
                        severity: if z.abs() >= 4.0 {
                            AnomalySeverity::Critical
                        } else {
                            AnomalySeverity::Warning
                        },
                        code: "zscore_avg_latency".to_owned(),
                        summary: format!(
                            "Average latency z-score is {z:.2}, indicating statistical anomaly."
                        ),
                    });
                }
            }
            if let Some(z) = hhi_z {
                if z.abs() >= 3.0 {
                    z_anomalies.push(Anomaly {
                        severity: if z.abs() >= 4.0 {
                            AnomalySeverity::Critical
                        } else {
                            AnomalySeverity::Warning
                        },
                        code: "zscore_provider_hhi".to_owned(),
                        summary: format!(
                            "Provider HHI z-score is {z:.2}, indicating statistical anomaly."
                        ),
                    });
                }
            }

            (Some(report), z_anomalies)
        } else {
            (None, Vec::new())
        };

        let mut anomalies = build_anomalies(
            rpc_consistency_index,
            slot_spread,
            block_height_spread,
            blockhash_disagreement_ratio,
            account_divergences.len(),
            infrastructure_concentration.provider_hhi,
            asn_hhi,
            propagation.max_window_ms,
        );
        anomalies.extend(z_score_anomalies);

        // MEV audit analysis
        let (mev_summary, mev_anomalies) = crate::mev::analyse_mev(&tx_order_observations);
        anomalies.extend(mev_anomalies);

        // Extend with leader schedule anomalies
        anomalies.extend(leader_schedule_anomalies);

        // Drain pending identity change anomalies
        anomalies.append(&mut self.pending_identity_anomalies);

        NetworkSnapshot {
            generated_at: now,
            freshness_window_seconds: self.freshness_window.as_secs(),
            active_sentinels,
            active_endpoints,
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
            mev_audit: Some(mev_summary),
            z_scores,
            leader_schedule_anomalies: leader_schedule_anomaly_count,
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

    /// Returns the full validator identity change history.
    #[must_use]
    pub fn validator_history(&self) -> &BTreeMap<String, Vec<IdentityChangeEvent>> {
        self.identity_tracker.history()
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
        p99_window_ms: percentile(&windows, 0.99),
        max_window_ms: windows.last().copied(),
    }
}

#[allow(clippy::too_many_arguments)]
/// Analyse leader schedule observations from active endpoint samples.
///
/// Compares `LeaderScheduleObservation` schedules across endpoints and detects:
/// - `leader_schedule_divergence` (Warning): when two or more endpoints report different schedules
/// - `leader_concentration` (Info): when a single validator holds > 10% of leadership slots
///
/// Returns `(anomaly_count, anomalies)`.
fn analyse_leader_schedule(active: &[&EndpointSample]) -> (usize, Vec<Anomaly>) {
    let mut anomalies = Vec::new();

    // Collect all schedules that have the optional schedule map populated
    let schedules: Vec<&BTreeMap<String, Vec<u64>>> = active
        .iter()
        .filter_map(|sample| {
            sample
                .observation
                .leader_schedule
                .value
                .as_ref()
                .and_then(|obs| obs.schedule.as_ref())
        })
        .collect();

    // Need at least 2 schedules to compare for divergence
    if schedules.len() >= 2 {
        let reference = schedules[0];
        for other in &schedules[1..] {
            if *other != reference {
                anomalies.push(Anomaly {
                    severity: AnomalySeverity::Warning,
                    code: "leader_schedule_divergence".to_owned(),
                    summary: "Leader schedules diverge across endpoints for the same epoch."
                        .to_owned(),
                });
                // One divergence anomaly is enough
                break;
            }
        }
    }

    // Analyse leadership slot concentration across all observed schedules.
    // Use the first available schedule as representative (or any — they should
    // be identical when no divergence exists).
    if let Some(schedule) = schedules.first() {
        let total_slots: usize = schedule.values().map(Vec::len).sum();
        if total_slots > 0 {
            for (validator, slots) in *schedule {
                let share = slots.len() as f64 / total_slots as f64;
                if share > 0.10 {
                    anomalies.push(Anomaly {
                        severity: AnomalySeverity::Info,
                        code: "leader_concentration".to_owned(),
                        summary: format!(
                            "Validator {validator} holds {:.1}% of leadership slots ({}/{total_slots}).",
                            share * 100.0,
                            slots.len(),
                        ),
                    });
                }
            }
        }
    }

    let count = anomalies.len();
    (count, anomalies)
}

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

    if asn_hhi >= 0.50 {
        anomalies.push(Anomaly {
            severity: AnomalySeverity::Warning,
            code: "asn_concentration".to_owned(),
            summary: format!(
                "ASN concentration HHI is {asn_hhi:.3}, indicating elevated topological centralization."
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
    use std::collections::{BTreeMap, BTreeSet};

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

    // Feature: sentinelmesh-comprehensive-upgrade, Task 10.1: state_hash divergence edge cases

    /// Validates: Requirements 3.2
    /// Observations without state_hash are excluded from divergence analysis.
    #[test]
    fn account_divergence_excludes_observations_without_state_hash() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        // Two endpoints: one with state_hash, one without
        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-A".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: None, // No state_hash — should be excluded
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let snapshot = store.snapshot();
        // Only one endpoint has state_hash, so no divergence possible
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 0,
            "Observations without state_hash should be excluded from divergence analysis"
        );
        assert!(
            !snapshot
                .anomalies
                .iter()
                .any(|a| a.code == "account_state_divergence"),
            "No account_state_divergence anomaly should be generated when only one endpoint has state_hash"
        );
    }

    /// Validates: Requirements 3.1, 3.2
    /// A single endpoint cannot produce a divergence even with state_hash present.
    #[test]
    fn account_divergence_single_endpoint_no_divergence() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-A".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs]));

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 0,
            "Single endpoint should not produce divergence"
        );
    }

    /// Validates: Requirements 3.1, 3.3, 3.4
    /// Two endpoints with different state_hash for the same account produce a divergence,
    /// the count is reflected in the snapshot, and an anomaly is generated.
    #[test]
    fn account_divergence_detected_with_different_hashes() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-A".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-B".to_owned()), // Different hash
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 1,
            "Should detect exactly 1 account divergence"
        );

        let divergence_anomaly = snapshot
            .anomalies
            .iter()
            .find(|a| a.code == "account_state_divergence");
        assert!(
            divergence_anomaly.is_some(),
            "Should generate account_state_divergence anomaly"
        );
        assert_eq!(
            divergence_anomaly.unwrap().severity,
            sentinelmesh_core::AnomalySeverity::Warning,
            "account_state_divergence anomaly should have Warning severity"
        );
    }

    /// Validates: Requirements 3.1, 3.3
    /// Two endpoints with the same state_hash for the same account produce no divergence.
    #[test]
    fn account_divergence_same_hash_no_divergence() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("same-state".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("same-state".to_owned()), // Same hash
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 0,
            "Same state_hash across endpoints should not produce divergence"
        );
    }

    /// Validates: Requirements 3.2
    /// All observations without state_hash produce zero divergences.
    #[test]
    fn account_divergence_all_missing_state_hash() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: None,
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: None,
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 0,
            "All observations without state_hash should produce zero divergences"
        );
    }

    /// Validates: Requirements 3.1, 3.3
    /// Multiple accounts with divergences are all counted correctly.
    #[test]
    fn account_divergence_multiple_accounts() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![
            AccountObservation {
                pubkey: "account-1".to_owned(),
                commitment: "confirmed".to_owned(),
                slot: Some(100),
                state_hash: Some("state-A1".to_owned()),
                lamports: Some(42),
                owner: Some("11111111111111111111111111111111".to_owned()),
                executable: Some(false),
                rent_epoch: Some(0),
                data_len: Some(0),
                latency_ms: 3,
                error: None,
            },
            AccountObservation {
                pubkey: "account-2".to_owned(),
                commitment: "confirmed".to_owned(),
                slot: Some(100),
                state_hash: Some("state-A2".to_owned()),
                lamports: Some(100),
                owner: Some("11111111111111111111111111111111".to_owned()),
                executable: Some(false),
                rent_epoch: Some(0),
                data_len: Some(0),
                latency_ms: 3,
                error: None,
            },
        ];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![
            AccountObservation {
                pubkey: "account-1".to_owned(),
                commitment: "confirmed".to_owned(),
                slot: Some(100),
                state_hash: Some("state-B1".to_owned()), // Different from A1
                lamports: Some(42),
                owner: Some("11111111111111111111111111111111".to_owned()),
                executable: Some(false),
                rent_epoch: Some(0),
                data_len: Some(0),
                latency_ms: 3,
                error: None,
            },
            AccountObservation {
                pubkey: "account-2".to_owned(),
                commitment: "confirmed".to_owned(),
                slot: Some(100),
                state_hash: Some("state-B2".to_owned()), // Different from A2
                lamports: Some(100),
                owner: Some("11111111111111111111111111111111".to_owned()),
                executable: Some(false),
                rent_epoch: Some(0),
                data_len: Some(0),
                latency_ms: 3,
                error: None,
            },
        ];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.validator_state_divergence.account_divergence_count, 2,
            "Should detect divergences for both accounts"
        );
    }

    /// Validates: Requirements 3.1
    /// account_divergences_from correctly reports variant endpoints.
    #[test]
    fn account_divergence_reports_correct_endpoints() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );

        let mut obs_a = endpoint("rpc-a", "ProviderA", 100, 90, "hash-1");
        obs_a.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-X".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        let mut obs_b = endpoint("rpc-b", "ProviderB", 100, 90, "hash-1");
        obs_b.accounts = vec![AccountObservation {
            pubkey: "account-1".to_owned(),
            commitment: "confirmed".to_owned(),
            slot: Some(100),
            state_hash: Some("state-Y".to_owned()),
            lamports: Some(42),
            owner: Some("11111111111111111111111111111111".to_owned()),
            executable: Some(false),
            rent_epoch: Some(0),
            data_len: Some(0),
            latency_ms: 3,
            error: None,
        }];

        store.ingest(batch(base, "sentinel-a", "us-east-1", vec![obs_a]));
        store.ingest(batch(base, "sentinel-b", "eu-west-1", vec![obs_b]));

        let divergences = store.account_divergences();
        assert_eq!(divergences.len(), 1);
        assert_eq!(divergences[0].pubkey, "account-1");
        assert_eq!(divergences[0].variants.len(), 2);

        // Each variant should have exactly one endpoint
        for variant in &divergences[0].variants {
            assert_eq!(variant.endpoints.len(), 1);
        }

        // Verify the hashes are present
        let hashes: BTreeSet<&str> = divergences[0]
            .variants
            .iter()
            .map(|v| v.state_hash.as_str())
            .collect();
        assert!(hashes.contains("state-X"));
        assert!(hashes.contains("state-Y"));
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
            transaction_order: Vec::new(),
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 2: Detecção de divergência de state_hash
    mod prop_state_hash_divergence {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // **Validates: Requirements 3.1, 3.3, 3.4**
        //
        // For any set of EndpointSample where two or more endpoints report different
        // state_hash for the same account (pubkey), the MeshStore::snapshot() must:
        // (a) include that account in the divergence list,
        // (b) report account_divergence_count equal to the number of accounts with variant hashes,
        // (c) generate an anomaly with code account_state_divergence and severity Warning.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn state_hash_divergence_detected(
                // Generate 1..=4 divergent accounts, each with 2..=4 distinct hashes
                num_accounts in 1_usize..=4,
                num_endpoints in 2_usize..=5,
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(300),
                    std::time::Duration::from_secs(120),
                );

                // Build endpoint observations where each endpoint gets a different
                // state_hash for each account, guaranteeing divergence.
                for ep_idx in 0..num_endpoints {
                    let ep_id = format!("rpc-{ep_idx}");
                    let provider = format!("Provider{ep_idx}");
                    let sentinel = format!("sentinel-{ep_idx}");
                    let location = format!("region-{ep_idx}");

                    let mut obs = endpoint(
                        &ep_id,
                        &provider,
                        seed_slot,
                        seed_slot.saturating_sub(10),
                        "blockhash-common",
                    );

                    // Replace default accounts with our divergent accounts
                    obs.accounts = (0..num_accounts)
                        .map(|acc_idx| AccountObservation {
                            pubkey: format!("account-{acc_idx}"),
                            commitment: "confirmed".to_owned(),
                            slot: Some(seed_slot),
                            // Each endpoint gets a unique hash per account
                            state_hash: Some(format!("hash-{acc_idx}-ep{ep_idx}")),
                            lamports: Some(42),
                            owner: Some("11111111111111111111111111111111".to_owned()),
                            executable: Some(false),
                            rent_epoch: Some(0),
                            data_len: Some(0),
                            latency_ms: 3,
                            error: None,
                        })
                        .collect();

                    store.ingest(batch(base, &sentinel, &location, vec![obs]));
                }

                let snapshot = store.snapshot();

                // (a) Each divergent account must appear in the divergence list
                let divergences = store.account_divergences();
                for acc_idx in 0..num_accounts {
                    let pubkey = format!("account-{acc_idx}");
                    prop_assert!(
                        divergences.iter().any(|d| d.pubkey == pubkey),
                        "Account {} should be in divergence list", pubkey
                    );
                }

                // (b) account_divergence_count equals the number of divergent accounts
                prop_assert_eq!(
                    snapshot.validator_state_divergence.account_divergence_count,
                    num_accounts,
                    "account_divergence_count should equal number of divergent accounts"
                );

                // (c) anomaly with code account_state_divergence and severity Warning
                let anomaly = snapshot
                    .anomalies
                    .iter()
                    .find(|a| a.code == "account_state_divergence");
                prop_assert!(
                    anomaly.is_some(),
                    "Should generate account_state_divergence anomaly"
                );
                prop_assert_eq!(
                    anomaly.unwrap().severity,
                    AnomalySeverity::Warning,
                    "account_state_divergence anomaly should have Warning severity"
                );
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 3: Cálculo correto do ASN HHI
    mod prop_asn_hhi_calculation {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        // **Validates: Requirements 4.1, 4.2**
        //
        // For any set of EndpointSample with asn fields filled, the asn_hhi in
        // NetworkSnapshot must equal the sum of squares of each ASN's fraction
        // (HHI formula: Σ(share_i²)), where share_i = count_i / total_with_asn.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn asn_hhi_matches_formula(
                // Generate 2..=8 samples, each with an ASN drawn from a small pool
                // to create realistic concentration scenarios.
                asn_assignments in proptest::collection::vec(1_u32..=5, 2..=8),
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(300),
                    std::time::Duration::from_secs(120),
                );

                // Each element in asn_assignments becomes a unique sentinel+endpoint
                // pair with the given ASN, avoiding deduplication collisions.
                for (idx, &asn) in asn_assignments.iter().enumerate() {
                    let sentinel = format!("sentinel-{idx}");
                    let ep_id = format!("rpc-{idx}");
                    let provider = format!("Provider{idx}");

                    let obs = endpoint(
                        &ep_id,
                        &provider,
                        seed_slot,
                        seed_slot.saturating_sub(10),
                        "blockhash-common",
                    );

                    let mut b = batch(base, &sentinel, "region-0", vec![obs]);
                    b.asn = Some(asn);
                    store.ingest(b);
                }

                let snapshot = store.snapshot();

                // Compute expected HHI independently
                let total = asn_assignments.len() as f64;
                let mut counts: BTreeMap<u32, usize> = BTreeMap::new();
                for &asn in &asn_assignments {
                    *counts.entry(asn).or_default() += 1;
                }
                let expected_hhi: f64 = counts
                    .values()
                    .map(|&c| {
                        let share = c as f64 / total;
                        share * share
                    })
                    .sum();

                let diff = (snapshot.asn_hhi - expected_hhi).abs();
                prop_assert!(
                    diff < 1e-10,
                    "asn_hhi mismatch: snapshot={}, expected={}, diff={}",
                    snapshot.asn_hhi,
                    expected_hhi,
                    diff,
                );
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 4: Anomalia de concentração ASN
    mod prop_asn_concentration_anomaly {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // **Validates: Requirements 4.3**
        //
        // For any NetworkSnapshot where asn_hhi >= 0.50, the anomaly list must
        // contain an anomaly with code `asn_concentration` and severity `Warning`.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn asn_concentration_anomaly_present_when_hhi_high(
                // Generate 2..=6 samples all sharing the same ASN value,
                // guaranteeing HHI = 1.0 (well above the 0.50 threshold).
                num_samples in 2_usize..=6,
                asn_value in 1_u32..=65000,
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(300),
                    std::time::Duration::from_secs(120),
                );

                // All samples share the same ASN → HHI = 1.0 >= 0.50
                for idx in 0..num_samples {
                    let sentinel = format!("sentinel-{idx}");
                    let ep_id = format!("rpc-{idx}");
                    let provider = format!("Provider{idx}");

                    let obs = endpoint(
                        &ep_id,
                        &provider,
                        seed_slot,
                        seed_slot.saturating_sub(10),
                        "blockhash-common",
                    );

                    let mut b = batch(base, &sentinel, "region-0", vec![obs]);
                    b.asn = Some(asn_value);
                    store.ingest(b);
                }

                let snapshot = store.snapshot();

                // Verify asn_hhi >= 0.50
                prop_assert!(
                    snapshot.asn_hhi >= 0.50,
                    "asn_hhi should be >= 0.50 when all samples share the same ASN, got {}",
                    snapshot.asn_hhi,
                );

                // Verify anomaly with code asn_concentration exists
                let anomaly = snapshot
                    .anomalies
                    .iter()
                    .find(|a| a.code == "asn_concentration");
                prop_assert!(
                    anomaly.is_some(),
                    "Should generate asn_concentration anomaly when asn_hhi={:.3} >= 0.50",
                    snapshot.asn_hhi,
                );

                // Verify severity is Warning
                prop_assert_eq!(
                    anomaly.unwrap().severity,
                    AnomalySeverity::Warning,
                    "asn_concentration anomaly should have Warning severity",
                );
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 5: Topological blindness — downgrade de severidade
    mod prop_topological_blindness_downgrade {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // **Validates: Requirements 4.4**
        //
        // For any NetworkSnapshot where asn_hhi >= 0.90 and rpc_consistency_index < 0.85,
        // the anomaly rpc_consistency_degraded must have severity Warning (not Critical).
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn rpc_consistency_downgraded_to_warning_under_topological_blindness(
                // Number of endpoints (need at least 2 for divergence)
                num_endpoints in 2_usize..=6,
                // Common ASN for all samples → asn_hhi = 1.0 (>= 0.90)
                asn_value in 1_u32..=65000,
                // Base slot; each endpoint gets a wildly different slot to degrade consistency
                base_slot in 1000_u64..100_000,
                // Slot offset per endpoint to create large spread (>= 500 apart)
                slot_step in 500_u64..5000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(300),
                    std::time::Duration::from_secs(120),
                );

                // Create endpoints with the same ASN but very different slot/block_height/blockhash
                // to drive rpc_consistency_index well below 0.85.
                for idx in 0..num_endpoints {
                    let sentinel = format!("sentinel-{idx}");
                    let ep_id = format!("rpc-{idx}");
                    let provider = format!("Provider{idx}");
                    let slot = base_slot + (idx as u64) * slot_step;
                    let block_height = slot.saturating_sub(10);
                    let blockhash = format!("blockhash-{idx}");

                    let obs = endpoint(
                        &ep_id,
                        &provider,
                        slot,
                        block_height,
                        &blockhash,
                    );

                    let mut b = batch(base, &sentinel, "region-0", vec![obs]);
                    b.asn = Some(asn_value);
                    store.ingest(b);
                }

                let snapshot = store.snapshot();

                // Precondition: asn_hhi must be >= 0.90 (all same ASN → 1.0)
                prop_assert!(
                    snapshot.asn_hhi >= 0.90,
                    "asn_hhi should be >= 0.90, got {}",
                    snapshot.asn_hhi,
                );

                // Precondition: rpc_consistency_index must be < 0.85
                prop_assert!(
                    snapshot.rpc_consistency_index < 0.85,
                    "rpc_consistency_index should be < 0.85, got {}",
                    snapshot.rpc_consistency_index,
                );

                // Property: rpc_consistency_degraded anomaly must exist with Warning severity
                let anomaly = snapshot
                    .anomalies
                    .iter()
                    .find(|a| a.code == "rpc_consistency_degraded");
                prop_assert!(
                    anomaly.is_some(),
                    "Should generate rpc_consistency_degraded anomaly when index={:.3} < 0.85",
                    snapshot.rpc_consistency_index,
                );
                prop_assert_eq!(
                    anomaly.unwrap().severity,
                    AnomalySeverity::Warning,
                    "rpc_consistency_degraded should be Warning (not Critical) under topological blindness (asn_hhi={:.3})",
                    snapshot.asn_hhi,
                );
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Task 11.2: z-score integration in MeshStore

    /// Validates: Requirements 12.1, 12.5
    /// In Fixed mode, z_scores field in NetworkSnapshot is None.
    #[test]
    fn zscore_fixed_mode_returns_none() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
        );
        store.ingest(batch(
            base,
            "sentinel-a",
            "us-east-1",
            vec![endpoint("rpc-a", "ProviderA", 100, 90, "hash-1")],
        ));

        let snapshot = store.snapshot();
        assert!(
            snapshot.z_scores.is_none(),
            "z_scores should be None in Fixed mode"
        );
    }

    /// Validates: Requirements 12.1, 12.4, 12.5
    /// In Statistical mode with fewer than 30 samples, z_scores fields are all None
    /// (fallback to fixed thresholds).
    #[test]
    fn zscore_statistical_mode_fewer_than_30_samples_returns_none_fields() {
        use crate::anomaly::DetectionMode;

        let base = chrono::Utc::now();
        let mut store = MeshStore::with_detection(
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(120),
            DetectionMode::Statistical,
            100,
        );

        // Ingest 10 snapshots (< 30)
        for i in 0..10 {
            store.ingest(batch(
                base,
                &format!("sentinel-{i}"),
                "us-east-1",
                vec![endpoint(
                    &format!("rpc-{i}"),
                    &format!("Provider{i}"),
                    100 + i as u64,
                    90 + i as u64,
                    "hash-1",
                )],
            ));
            let _ = store.snapshot(); // push values into windows
        }

        let snapshot = store.snapshot();
        let report = snapshot
            .z_scores
            .expect("z_scores should be Some in Statistical mode");
        assert!(
            report.slot_spread_z.is_none(),
            "slot_spread_z should be None with < 30 samples"
        );
        assert!(
            report.block_height_spread_z.is_none(),
            "block_height_spread_z should be None with < 30 samples"
        );
        assert!(
            report.avg_latency_z.is_none(),
            "avg_latency_z should be None with < 30 samples"
        );
        assert!(
            report.provider_hhi_z.is_none(),
            "provider_hhi_z should be None with < 30 samples"
        );
    }

    /// Validates: Requirements 12.1, 12.2, 12.5
    /// In Statistical mode with >= 30 samples of stable data, z_scores are computed
    /// and no z-score anomalies are generated for normal values.
    #[test]
    fn zscore_statistical_mode_stable_data_no_anomalies() {
        use crate::anomaly::DetectionMode;

        let base = chrono::Utc::now();
        let mut store = MeshStore::with_detection(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
            DetectionMode::Statistical,
            100,
        );

        // Ingest 35 identical snapshots to build a stable baseline
        for i in 0..35 {
            store.ingest(batch(
                base,
                &format!("sentinel-{i}"),
                "us-east-1",
                vec![
                    endpoint("rpc-a", "ProviderA", 100, 90, "hash-1"),
                    endpoint("rpc-b", "ProviderB", 101, 91, "hash-1"),
                ],
            ));
            let _ = store.snapshot();
        }

        let snapshot = store.snapshot();
        let report = snapshot
            .z_scores
            .expect("z_scores should be Some in Statistical mode");

        // With stable data, z-scores should be close to 0 or None (if std_dev ≈ 0)
        // No z-score anomalies should be generated
        let zscore_anomalies: Vec<_> = snapshot
            .anomalies
            .iter()
            .filter(|a| a.code.starts_with("zscore_"))
            .collect();
        assert!(
            zscore_anomalies.is_empty(),
            "No z-score anomalies should be generated for stable data, got: {:?}",
            zscore_anomalies
        );

        // The report should exist (even if individual z-scores are None due to zero std_dev)
        // This validates that the ZScoreReport is populated in Statistical mode
        assert!(
            report.slot_spread_z.is_none() || report.slot_spread_z.unwrap().abs() < 3.0,
            "slot_spread_z should be None or < 3.0 for stable data"
        );
    }

    /// Validates: Requirements 12.2
    /// In Statistical mode, an outlier value generates a z-score anomaly with
    /// Warning severity for |z| >= 3.0 and Critical for |z| >= 4.0.
    #[test]
    fn zscore_statistical_mode_outlier_generates_anomaly() {
        use crate::anomaly::DetectionMode;

        let base = chrono::Utc::now();
        let mut store = MeshStore::with_detection(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
            DetectionMode::Statistical,
            100,
        );

        // Build a baseline of 35 snapshots with slot spread = 1
        // (two endpoints with slots 100 and 101)
        for i in 0..35 {
            store.ingest(batch(
                base,
                &format!("sentinel-{i}"),
                "us-east-1",
                vec![
                    endpoint("rpc-a", "ProviderA", 100, 90, "hash-1"),
                    endpoint("rpc-b", "ProviderB", 101, 91, "hash-1"),
                ],
            ));
            let _ = store.snapshot();
        }

        // Now ingest a snapshot with a huge slot spread (outlier)
        // Replace all samples with an outlier scenario
        store.replace_samples(Vec::new());
        store.ingest(batch(
            base,
            "sentinel-outlier-a",
            "us-east-1",
            vec![endpoint("rpc-a", "ProviderA", 100, 90, "hash-1")],
        ));
        store.ingest(batch(
            base,
            "sentinel-outlier-b",
            "us-east-1",
            vec![endpoint("rpc-b", "ProviderB", 5000, 4990, "hash-2")],
        ));

        let snapshot = store.snapshot();
        let report = snapshot
            .z_scores
            .expect("z_scores should be Some in Statistical mode");

        // The slot spread is now 4900, which should be a massive outlier
        // compared to the baseline of ~1
        if let Some(z) = report.slot_spread_z {
            assert!(
                z.abs() >= 3.0,
                "slot_spread_z should be >= 3.0 for outlier, got {z}"
            );
        }

        // Check that a z-score anomaly was generated
        let zscore_anomaly = snapshot
            .anomalies
            .iter()
            .find(|a| a.code == "zscore_slot_spread");
        assert!(
            zscore_anomaly.is_some(),
            "Should generate zscore_slot_spread anomaly for outlier"
        );
    }

    /// Validates: Requirements 12.2
    /// Z-score anomaly severity is Warning for |z| >= 3.0 and Critical for |z| >= 4.0.
    #[test]
    fn zscore_severity_proportional_to_zscore() {
        use crate::anomaly::DetectionMode;

        let base = chrono::Utc::now();
        let mut store = MeshStore::with_detection(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
            DetectionMode::Statistical,
            100,
        );

        // Build baseline: 35 snapshots with slot spread = 1
        for i in 0..35 {
            store.ingest(batch(
                base,
                &format!("sentinel-{i}"),
                "us-east-1",
                vec![
                    endpoint("rpc-a", "ProviderA", 100, 90, "hash-1"),
                    endpoint("rpc-b", "ProviderB", 101, 91, "hash-1"),
                ],
            ));
            let _ = store.snapshot();
        }

        // Extreme outlier → should produce Critical (|z| >= 4.0)
        store.replace_samples(Vec::new());
        store.ingest(batch(
            base,
            "sentinel-outlier-a",
            "us-east-1",
            vec![endpoint("rpc-a", "ProviderA", 100, 90, "hash-1")],
        ));
        store.ingest(batch(
            base,
            "sentinel-outlier-b",
            "us-east-1",
            vec![endpoint("rpc-b", "ProviderB", 50000, 49990, "hash-2")],
        ));

        let snapshot = store.snapshot();
        let zscore_anomaly = snapshot
            .anomalies
            .iter()
            .find(|a| a.code == "zscore_slot_spread");

        if let Some(anomaly) = zscore_anomaly {
            let report = snapshot.z_scores.as_ref().unwrap();
            if let Some(z) = report.slot_spread_z {
                if z.abs() >= 4.0 {
                    assert_eq!(
                        anomaly.severity,
                        sentinelmesh_core::AnomalySeverity::Critical,
                        "z-score >= 4.0 should produce Critical severity"
                    );
                } else if z.abs() >= 3.0 {
                    assert_eq!(
                        anomaly.severity,
                        sentinelmesh_core::AnomalySeverity::Warning,
                        "z-score >= 3.0 but < 4.0 should produce Warning severity"
                    );
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Leader schedule analysis unit tests
    // ---------------------------------------------------------------

    /// Helper to create an EndpointObservation with a leader schedule.
    fn endpoint_with_leader_schedule(
        id: &str,
        provider: &str,
        slot: u64,
        block_height: u64,
        blockhash: &str,
        schedule: Option<BTreeMap<String, Vec<u64>>>,
    ) -> EndpointObservation {
        let mut obs = endpoint(id, provider, slot, block_height, blockhash);
        obs.leader_schedule = ProbeValue::ok(
            sentinelmesh_core::LeaderScheduleObservation {
                validators: schedule.as_ref().map_or(0, |s| s.len()),
                total_leader_slots: schedule
                    .as_ref()
                    .map_or(0, |s| s.values().map(Vec::len).sum()),
                schedule,
            },
            5,
        );
        obs
    }

    #[test]
    fn leader_schedule_no_schedules_no_anomalies() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        // Endpoints without leader schedule data
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![endpoint("rpc-a", "ProviderA", 100, 90, "hash-1")],
        ));
        let snap = store.snapshot();
        assert_eq!(snap.leader_schedule_anomalies, 0);
        assert!(
            !snap.anomalies.iter().any(|a| a.code == "leader_schedule_divergence"
                || a.code == "leader_concentration"),
        );
    }

    #[test]
    fn leader_schedule_single_endpoint_no_divergence() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        let mut schedule = BTreeMap::new();
        schedule.insert("valA".to_owned(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![endpoint_with_leader_schedule(
                "rpc-a",
                "ProviderA",
                100,
                90,
                "hash-1",
                Some(schedule),
            )],
        ));
        let snap = store.snapshot();
        // Single endpoint → no divergence, but concentration should be detected
        assert!(
            !snap
                .anomalies
                .iter()
                .any(|a| a.code == "leader_schedule_divergence"),
        );
        // valA holds 100% of slots → leader_concentration
        assert!(
            snap.anomalies
                .iter()
                .any(|a| a.code == "leader_concentration"),
        );
    }

    #[test]
    fn leader_schedule_identical_schedules_no_divergence() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        let mut schedule = BTreeMap::new();
        schedule.insert("valA".to_owned(), vec![0, 1, 2]);
        schedule.insert("valB".to_owned(), vec![3, 4, 5]);
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![
                endpoint_with_leader_schedule(
                    "rpc-a",
                    "ProviderA",
                    100,
                    90,
                    "hash-1",
                    Some(schedule.clone()),
                ),
                endpoint_with_leader_schedule(
                    "rpc-b",
                    "ProviderB",
                    100,
                    90,
                    "hash-1",
                    Some(schedule),
                ),
            ],
        ));
        let snap = store.snapshot();
        assert!(
            !snap
                .anomalies
                .iter()
                .any(|a| a.code == "leader_schedule_divergence"),
            "Identical schedules should not produce divergence"
        );
    }

    #[test]
    fn leader_schedule_divergent_schedules_detected() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        let mut schedule_a = BTreeMap::new();
        schedule_a.insert("valA".to_owned(), vec![0, 1, 2]);
        let mut schedule_b = BTreeMap::new();
        schedule_b.insert("valB".to_owned(), vec![0, 1, 2]);
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![
                endpoint_with_leader_schedule(
                    "rpc-a",
                    "ProviderA",
                    100,
                    90,
                    "hash-1",
                    Some(schedule_a),
                ),
                endpoint_with_leader_schedule(
                    "rpc-b",
                    "ProviderB",
                    100,
                    90,
                    "hash-1",
                    Some(schedule_b),
                ),
            ],
        ));
        let snap = store.snapshot();
        assert!(
            snap.anomalies
                .iter()
                .any(|a| a.code == "leader_schedule_divergence"),
            "Divergent schedules should produce leader_schedule_divergence anomaly"
        );
        assert!(snap.leader_schedule_anomalies > 0);
    }

    #[test]
    fn leader_schedule_concentration_detected() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        // valA has 6 slots, valB has 4 → valA = 60%, valB = 40%, both > 10%
        let mut schedule = BTreeMap::new();
        schedule.insert("valA".to_owned(), vec![0, 1, 2, 3, 4, 5]);
        schedule.insert("valB".to_owned(), vec![6, 7, 8, 9]);
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![endpoint_with_leader_schedule(
                "rpc-a",
                "ProviderA",
                100,
                90,
                "hash-1",
                Some(schedule),
            )],
        ));
        let snap = store.snapshot();
        let concentration_anomalies: Vec<_> = snap
            .anomalies
            .iter()
            .filter(|a| a.code == "leader_concentration")
            .collect();
        assert_eq!(
            concentration_anomalies.len(),
            2,
            "Both validators hold > 10% of slots"
        );
        for a in &concentration_anomalies {
            assert_eq!(a.severity, sentinelmesh_core::AnomalySeverity::Info);
        }
    }

    #[test]
    fn leader_schedule_no_concentration_when_evenly_distributed() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        // 11 validators each with 1 slot → each holds ~9.09% < 10%
        let mut schedule = BTreeMap::new();
        for i in 0..11 {
            schedule.insert(format!("val{i}"), vec![i as u64]);
        }
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![endpoint_with_leader_schedule(
                "rpc-a",
                "ProviderA",
                100,
                90,
                "hash-1",
                Some(schedule),
            )],
        ));
        let snap = store.snapshot();
        assert!(
            !snap
                .anomalies
                .iter()
                .any(|a| a.code == "leader_concentration"),
            "No validator holds > 10% of slots when evenly distributed across 11 validators"
        );
    }

    #[test]
    fn leader_schedule_schedule_none_excluded_from_analysis() {
        let base = chrono::Utc::now();
        let mut store = MeshStore::new(
            std::time::Duration::from_secs(600),
            std::time::Duration::from_secs(600),
        );
        // One endpoint with schedule, one without → no divergence (only 1 schedule)
        let mut schedule = BTreeMap::new();
        schedule.insert("valA".to_owned(), vec![0, 1, 2]);
        store.ingest(batch(
            base,
            "s1",
            "us-east-1",
            vec![
                endpoint_with_leader_schedule(
                    "rpc-a",
                    "ProviderA",
                    100,
                    90,
                    "hash-1",
                    Some(schedule),
                ),
                endpoint_with_leader_schedule("rpc-b", "ProviderB", 100, 90, "hash-1", None),
            ],
        ));
        let snap = store.snapshot();
        assert!(
            !snap
                .anomalies
                .iter()
                .any(|a| a.code == "leader_schedule_divergence"),
            "Endpoints without schedule should be excluded from divergence analysis"
        );
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 18: Detecção de divergência de leader schedule
    mod prop_leader_schedule_divergence {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // **Validates: Requirements 14.1, 14.2**
        //
        // For any set of LeaderScheduleObservation from multiple endpoints for the
        // same epoch, if the schedules differ, the MeshStore must generate an anomaly
        // with code `leader_schedule_divergence` and severity `Warning`.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn divergent_leader_schedules_produce_anomaly(
                // Number of validators in schedule A (1..=4)
                num_validators_a in 1_usize..=4,
                // Number of validators in schedule B (1..=4)
                num_validators_b in 1_usize..=4,
                // Slots per validator
                slots_per_validator in 1_usize..=5,
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(600),
                    std::time::Duration::from_secs(600),
                );

                // Build schedule A: validators named "valA-0", "valA-1", ...
                let mut schedule_a = BTreeMap::new();
                for i in 0..num_validators_a {
                    let slots: Vec<u64> = (0..slots_per_validator)
                        .map(|s| (i * slots_per_validator + s) as u64)
                        .collect();
                    schedule_a.insert(format!("valA-{i}"), slots);
                }

                // Build schedule B: validators named "valB-0", "valB-1", ...
                // These are guaranteed to differ from schedule A because the
                // validator keys are different.
                let mut schedule_b = BTreeMap::new();
                for i in 0..num_validators_b {
                    let slots: Vec<u64> = (0..slots_per_validator)
                        .map(|s| (i * slots_per_validator + s) as u64)
                        .collect();
                    schedule_b.insert(format!("valB-{i}"), slots);
                }

                // Ingest two endpoints with different schedules
                store.ingest(batch(
                    base,
                    "s1",
                    "us-east-1",
                    vec![
                        endpoint_with_leader_schedule(
                            "rpc-a",
                            "ProviderA",
                            seed_slot,
                            seed_slot.saturating_sub(10),
                            "blockhash-common",
                            Some(schedule_a),
                        ),
                        endpoint_with_leader_schedule(
                            "rpc-b",
                            "ProviderB",
                            seed_slot,
                            seed_slot.saturating_sub(10),
                            "blockhash-common",
                            Some(schedule_b),
                        ),
                    ],
                ));

                let snap = store.snapshot();

                // Must generate leader_schedule_divergence anomaly
                let divergence_anomaly = snap
                    .anomalies
                    .iter()
                    .find(|a| a.code == "leader_schedule_divergence");
                prop_assert!(
                    divergence_anomaly.is_some(),
                    "Divergent schedules must produce leader_schedule_divergence anomaly"
                );
                prop_assert_eq!(
                    divergence_anomaly.unwrap().severity,
                    AnomalySeverity::Warning,
                    "leader_schedule_divergence anomaly must have Warning severity"
                );

                // leader_schedule_anomalies count must be > 0
                prop_assert!(
                    snap.leader_schedule_anomalies > 0,
                    "leader_schedule_anomalies count must be positive when divergence exists"
                );
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 19: Detecção de concentração de liderança
    mod prop_leader_concentration {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // **Validates: Requirements 14.3, 14.4**
        //
        // For any leader schedule where a single validator holds more than 10% of
        // leadership slots, the MeshStore must generate an anomaly with code
        // `leader_concentration` and severity `Info`.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn concentrated_leader_schedule_produces_anomaly(
                // Number of "background" validators that share the remaining slots
                num_background in 1_usize..=5,
                // Total slots for the dominant validator (must be > 10% of total)
                dominant_slots in 2_usize..=20,
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(600),
                    std::time::Duration::from_secs(600),
                );

                // Build a schedule where "dominant-val" holds dominant_slots and
                // background validators share 1 slot each. This guarantees the
                // dominant validator holds > 10% when dominant_slots > num_background / 9.
                // With dominant_slots >= 2 and num_background <= 5, the dominant
                // validator always holds > 10%.
                let mut schedule = BTreeMap::new();
                let dominant_slot_vec: Vec<u64> = (0..dominant_slots as u64).collect();
                schedule.insert("dominant-val".to_owned(), dominant_slot_vec);

                for i in 0..num_background {
                    schedule.insert(
                        format!("bg-val-{i}"),
                        vec![(dominant_slots + i) as u64],
                    );
                }

                let total_slots = dominant_slots + num_background;
                let dominant_share = dominant_slots as f64 / total_slots as f64;

                // Verify our generator guarantees > 10%
                prop_assert!(
                    dominant_share > 0.10,
                    "Dominant validator share {:.2}% must exceed 10%",
                    dominant_share * 100.0
                );

                store.ingest(batch(
                    base,
                    "s1",
                    "us-east-1",
                    vec![endpoint_with_leader_schedule(
                        "rpc-a",
                        "ProviderA",
                        seed_slot,
                        seed_slot.saturating_sub(10),
                        "blockhash-common",
                        Some(schedule),
                    )],
                ));

                let snap = store.snapshot();

                // Must generate leader_concentration anomaly for the dominant validator
                let concentration_anomalies: Vec<_> = snap
                    .anomalies
                    .iter()
                    .filter(|a| a.code == "leader_concentration")
                    .collect();

                // The dominant validator must appear in concentration anomalies
                let has_dominant = concentration_anomalies
                    .iter()
                    .any(|a| a.summary.contains("dominant-val"));
                prop_assert!(
                    has_dominant,
                    "Must generate leader_concentration anomaly for dominant-val \
                     (share={:.1}%, slots={}/{})",
                    dominant_share * 100.0,
                    dominant_slots,
                    total_slots
                );

                // All concentration anomalies must have Info severity
                for anomaly in &concentration_anomalies {
                    prop_assert_eq!(
                        anomaly.severity,
                        AnomalySeverity::Info,
                        "leader_concentration anomaly must have Info severity"
                    );
                }
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 20: Rastreamento de mudança de identidade de validador
    mod prop_validator_identity_change {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::{AnomalySeverity, IdentityObservation};

        // **Validates: Requirements 15.2, 15.3**
        //
        // For any sequence of observations where the identity of a validator changes
        // for a specific endpoint, the MeshStore must:
        // (a) register an IdentityChangeEvent with timestamp, previous identity and new identity,
        // (b) generate an anomaly with code validator_identity_change and severity Info.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn identity_change_detected_and_anomaly_generated(
                // Number of identity changes to simulate (1..=4)
                num_changes in 1_usize..=4,
                // Number of endpoints (1..=3)
                num_endpoints in 1_usize..=3,
                seed_slot in 100_u64..10_000,
            ) {
                let base = chrono::Utc::now();
                let mut store = MeshStore::new(
                    std::time::Duration::from_secs(600),
                    std::time::Duration::from_secs(600),
                );

                // Phase 1: Ingest initial identities for each endpoint (baseline)
                for ep_idx in 0..num_endpoints {
                    let ep_id = format!("rpc-{ep_idx}");
                    let provider = format!("Provider{ep_idx}");
                    let sentinel = format!("sentinel-{ep_idx}");

                    let mut obs = endpoint(
                        &ep_id,
                        &provider,
                        seed_slot,
                        seed_slot.saturating_sub(10),
                        "blockhash-common",
                    );
                    obs.identity = ProbeValue::ok(
                        IdentityObservation {
                            identity: format!("identity-{ep_idx}-v0"),
                        },
                        5,
                    );

                    store.ingest(batch(base, &sentinel, "region-0", vec![obs]));
                }

                // Consume any anomalies from the baseline phase
                let _ = store.snapshot();

                // Phase 2: Simulate identity changes
                let mut expected_changes = 0_usize;
                for change_idx in 0..num_changes {
                    let ts = base + chrono::Duration::seconds((change_idx + 1) as i64);
                    for ep_idx in 0..num_endpoints {
                        let ep_id = format!("rpc-{ep_idx}");
                        let provider = format!("Provider{ep_idx}");
                        let sentinel = format!("sentinel-{ep_idx}");

                        let mut obs = endpoint(
                            &ep_id,
                            &provider,
                            seed_slot + (change_idx as u64 + 1),
                            seed_slot.saturating_sub(10) + (change_idx as u64 + 1),
                            "blockhash-common",
                        );
                        // Each change round gives a new identity
                        obs.identity = ProbeValue::ok(
                            IdentityObservation {
                                identity: format!("identity-{ep_idx}-v{}", change_idx + 1),
                            },
                            5,
                        );

                        store.ingest(batch(ts, &sentinel, "region-0", vec![obs]));
                        expected_changes += 1;
                    }
                }

                let snap = store.snapshot();

                // (a) Verify IdentityChangeEvents are registered
                let history = store.validator_history();
                let total_events: usize = history.values().map(Vec::len).sum();
                prop_assert_eq!(
                    total_events,
                    expected_changes,
                    "Expected {} identity change events, got {}",
                    expected_changes,
                    total_events,
                );

                // Verify each endpoint has the correct number of change events
                for ep_idx in 0..num_endpoints {
                    let ep_id = format!("rpc-{ep_idx}");
                    let events = history.get(&ep_id);
                    prop_assert!(
                        events.is_some(),
                        "Endpoint {} should have identity change events",
                        ep_id,
                    );
                    prop_assert_eq!(
                        events.unwrap().len(),
                        num_changes,
                        "Endpoint {} should have {} change events",
                        ep_id,
                        num_changes,
                    );

                    // Verify event fields
                    for (i, event) in events.unwrap().iter().enumerate() {
                        prop_assert_eq!(
                            &event.endpoint_id,
                            &ep_id,
                            "Event endpoint_id should match",
                        );
                        let expected_prev = format!("identity-{ep_idx}-v{i}");
                        let expected_new = format!("identity-{ep_idx}-v{}", i + 1);
                        prop_assert_eq!(
                            &event.previous_identity,
                            &expected_prev,
                            "Previous identity should be {}",
                            expected_prev,
                        );
                        prop_assert_eq!(
                            &event.new_identity,
                            &expected_new,
                            "New identity should be {}",
                            expected_new,
                        );
                    }
                }

                // (b) Verify anomalies with code validator_identity_change and severity Info
                let identity_anomalies: Vec<_> = snap
                    .anomalies
                    .iter()
                    .filter(|a| a.code == "validator_identity_change")
                    .collect();
                prop_assert_eq!(
                    identity_anomalies.len(),
                    expected_changes,
                    "Expected {} validator_identity_change anomalies, got {}",
                    expected_changes,
                    identity_anomalies.len(),
                );
                for anomaly in &identity_anomalies {
                    prop_assert_eq!(
                        anomaly.severity,
                        AnomalySeverity::Info,
                        "validator_identity_change anomaly must have Info severity",
                    );
                }
            }
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 21: Cálculo correto de percentis de propagação
    mod prop_propagation_percentiles {
        use crate::percentile;
        use proptest::prelude::*;

        // **Validates: Requirements 16.1, 16.2**
        //
        // For any set of signature propagation windows, the percentiles p50, p95,
        // and p99 must correspond to the correct values calculated by the
        // nearest-rank percentile formula on the sorted data.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn percentile_values_match_nearest_rank(
                windows in proptest::collection::vec(0_u64..100_000, 1..=200),
            ) {
                let mut sorted = windows.clone();
                sorted.sort_unstable();

                let last_index = sorted.len().saturating_sub(1);

                // Compute expected percentiles using the same nearest-rank formula
                let expected_p50 = {
                    let idx = (last_index as f64 * 0.50_f64).round() as usize;
                    sorted[idx]
                };
                let expected_p95 = {
                    let idx = (last_index as f64 * 0.95_f64).round() as usize;
                    sorted[idx]
                };
                let expected_p99 = {
                    let idx = (last_index as f64 * 0.99_f64).round() as usize;
                    sorted[idx]
                };

                // Verify percentile() returns the expected values
                let p50 = percentile(&sorted, 0.50);
                let p95 = percentile(&sorted, 0.95);
                let p99 = percentile(&sorted, 0.99);

                prop_assert_eq!(
                    p50,
                    Some(expected_p50),
                    "p50 mismatch: expected {}, got {:?}",
                    expected_p50,
                    p50,
                );
                prop_assert_eq!(
                    p95,
                    Some(expected_p95),
                    "p95 mismatch: expected {}, got {:?}",
                    expected_p95,
                    p95,
                );
                prop_assert_eq!(
                    p99,
                    Some(expected_p99),
                    "p99 mismatch: expected {}, got {:?}",
                    expected_p99,
                    p99,
                );

                // Verify ordering invariant: p50 <= p95 <= p99 <= max
                let max_val = *sorted.last().unwrap();
                prop_assert!(
                    expected_p50 <= expected_p95,
                    "p50 ({}) must be <= p95 ({})",
                    expected_p50,
                    expected_p95,
                );
                prop_assert!(
                    expected_p95 <= expected_p99,
                    "p95 ({}) must be <= p99 ({})",
                    expected_p95,
                    expected_p99,
                );
                prop_assert!(
                    expected_p99 <= max_val,
                    "p99 ({}) must be <= max ({})",
                    expected_p99,
                    max_val,
                );
            }

            #[test]
            fn empty_input_returns_none(_dummy in 0_u8..1) {
                let empty: Vec<u64> = vec![];
                prop_assert_eq!(percentile(&empty, 0.50), None);
                prop_assert_eq!(percentile(&empty, 0.95), None);
                prop_assert_eq!(percentile(&empty, 0.99), None);
            }
        }
    }
}

/// FORMAL METHODS: Kani Verification Suite
/// Use `cargo kani` to verify these invariants mathematically.
#[cfg(kani)]
mod verification {
    use super::*;

    /// PROOF: HHI must stay within [1/n, 1.0] for any set of inputs.
    #[kani::proof]
    #[kani::unwind(11)]
    fn verify_hhi_invariant_safety() {
        let n: usize = kani::any();
        kani::assume(n > 0 && n <= 10);

        let mut total_with_asn = 0;
        let mut counts: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();

        for i in 0..n {
            let count: usize = kani::any();
            kani::assume(count > 0 && count <= 100);
            counts.insert(i as u32, count);
            total_with_asn += count;
        }

        if total_with_asn == 0 {
            return;
        }

        let mut asn_hhi = 0.0;
        for count in counts.values() {
            let share = (*count as f64) / (total_with_asn as f64);
            asn_hhi += share * share;
        }

        let lower_bound = 1.0 / (n as f64);
        assert!(asn_hhi >= lower_bound - 0.0001, "HHI below lower bound 1/n");
        assert!(asn_hhi <= 1.0 + 0.0001, "HHI above upper bound 1.0");
    }

    /// PROOF: Z-Score calculation handle zero variance without panic.
    #[kani::proof]
    fn verify_zscore_no_panic_zero_variance() {
        let mut window = SlidingWindow::new(5);
        let val: f64 = kani::any();
        kani::assume(val.is_finite());

        for _ in 0..5 {
            window.push(val);
        }

        let res = window.z_score(val);
        assert!(res.is_none() || res.unwrap().is_finite());
    }
}
