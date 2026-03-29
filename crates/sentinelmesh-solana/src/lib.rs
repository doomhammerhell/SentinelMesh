mod client;

use std::{collections::BTreeMap, time::Duration};

use anyhow::Result;
use client::SolanaRpcClient;
use sentinelmesh_core::{
    AccountObservation, BlockhashObservation, ClusterNodesObservation, EndpointObservation,
    IdentityObservation, LeaderScheduleObservation, ProbeValue, RpcEndpointConfig,
    SignatureObservation, SignatureStatusObservation, TrackedAccountConfig, ValidatorProbeConfig,
    VoteAccountsObservation, stable_hash,
};
use serde::Deserialize;
use serde_json::json;
use tokio::time::Instant;

#[derive(Clone)]
pub struct SolanaProbe {
    client: SolanaRpcClient,
}

impl SolanaProbe {
    pub fn new(request_timeout: Duration) -> Result<Self> {
        Ok(Self {
            client: SolanaRpcClient::new(request_timeout)?,
        })
    }

    pub async fn observe_endpoint(
        &self,
        endpoint: RpcEndpointConfig,
        tracked_accounts: &[TrackedAccountConfig],
        tracked_signatures: &[String],
        validator_probes: &ValidatorProbeConfig,
    ) -> EndpointObservation {
        let started_at = Instant::now();

        let health = self.probe_health(&endpoint);
        let slot = self.probe_slot(&endpoint);
        let block_height = self.probe_block_height(&endpoint);
        let latest_blockhash = self.probe_latest_blockhash(&endpoint);
        let version = self.probe_version(&endpoint);
        let identity = self.probe_identity(&endpoint, validator_probes.include_identity);
        let vote_accounts =
            self.probe_vote_accounts(&endpoint, validator_probes.include_vote_accounts);
        let cluster_nodes =
            self.probe_cluster_nodes(&endpoint, validator_probes.include_cluster_nodes);
        let leader_schedule =
            self.probe_leader_schedule(&endpoint, validator_probes.include_leader_schedule);
        let accounts = self.probe_accounts(&endpoint, tracked_accounts);
        let signatures = self.probe_signatures(&endpoint, tracked_signatures);

        let (
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
        ) = tokio::join!(
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
            signatures
        );

        let mut probe_errors = Vec::new();
        for metric_error in [
            health.error.clone(),
            slot.error.clone(),
            block_height.error.clone(),
            latest_blockhash.error.clone(),
            version.error.clone(),
            identity.error.clone(),
            vote_accounts.error.clone(),
            cluster_nodes.error.clone(),
            leader_schedule.error.clone(),
        ]
        .into_iter()
        .flatten()
        {
            probe_errors.push(metric_error);
        }

        let account_errors = accounts.iter().filter_map(|account| account.error.clone());
        let signature_errors = signatures
            .iter()
            .filter_map(|signature| signature.error.clone());
        probe_errors.extend(account_errors);
        probe_errors.extend(signature_errors);

        EndpointObservation {
            endpoint,
            overall_latency_ms: saturating_elapsed_ms(started_at.elapsed()),
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
            transaction_order: Vec::new(),
        }
    }

    async fn probe_health(&self, endpoint: &RpcEndpointConfig) -> ProbeValue<String> {
        let started_at = Instant::now();
        match self
            .client
            .call::<String>(endpoint, "getHealth", serde_json::Value::Array(vec![]))
            .await
        {
            Ok(result) => ProbeValue::ok(result, saturating_elapsed_ms(started_at.elapsed())),
            Err(error) => ProbeValue::err(
                format!("health probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_slot(&self, endpoint: &RpcEndpointConfig) -> ProbeValue<u64> {
        let started_at = Instant::now();
        match self
            .client
            .call::<u64>(endpoint, "getSlot", json!([{ "commitment": "processed" }]))
            .await
        {
            Ok(result) => ProbeValue::ok(result, saturating_elapsed_ms(started_at.elapsed())),
            Err(error) => ProbeValue::err(
                format!("slot probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_block_height(&self, endpoint: &RpcEndpointConfig) -> ProbeValue<u64> {
        let started_at = Instant::now();
        match self
            .client
            .call::<u64>(
                endpoint,
                "getBlockHeight",
                json!([{ "commitment": "processed" }]),
            )
            .await
        {
            Ok(result) => ProbeValue::ok(result, saturating_elapsed_ms(started_at.elapsed())),
            Err(error) => ProbeValue::err(
                format!("block height probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_latest_blockhash(
        &self,
        endpoint: &RpcEndpointConfig,
    ) -> ProbeValue<BlockhashObservation> {
        let started_at = Instant::now();
        match self
            .client
            .call::<LatestBlockhashResult>(
                endpoint,
                "getLatestBlockhash",
                json!([{ "commitment": "processed" }]),
            )
            .await
        {
            Ok(result) => ProbeValue::ok(
                BlockhashObservation {
                    blockhash: result.value.blockhash,
                    last_valid_block_height: result.value.last_valid_block_height,
                    context_slot: result.context.slot,
                },
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("latest blockhash probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_version(&self, endpoint: &RpcEndpointConfig) -> ProbeValue<String> {
        let started_at = Instant::now();
        match self
            .client
            .call::<VersionResult>(endpoint, "getVersion", serde_json::Value::Array(vec![]))
            .await
        {
            Ok(result) => ProbeValue::ok(
                result.solana_core,
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("version probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_identity(
        &self,
        endpoint: &RpcEndpointConfig,
        enabled: bool,
    ) -> ProbeValue<IdentityObservation> {
        if !enabled {
            return ProbeValue::empty();
        }

        let started_at = Instant::now();
        match self
            .client
            .call::<IdentityResult>(endpoint, "getIdentity", serde_json::Value::Array(vec![]))
            .await
        {
            Ok(result) => ProbeValue::ok(
                IdentityObservation {
                    identity: result.identity,
                },
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("identity probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_vote_accounts(
        &self,
        endpoint: &RpcEndpointConfig,
        enabled: bool,
    ) -> ProbeValue<VoteAccountsObservation> {
        if !enabled {
            return ProbeValue::empty();
        }

        let started_at = Instant::now();
        match self
            .client
            .call::<VoteAccountsResult>(
                endpoint,
                "getVoteAccounts",
                json!([{ "commitment": "processed" }]),
            )
            .await
        {
            Ok(result) => ProbeValue::ok(
                VoteAccountsObservation {
                    current_vote_accounts: result.current.len(),
                    delinquent_vote_accounts: result.delinquent.len(),
                    current_activated_stake: result
                        .current
                        .iter()
                        .map(|vote| vote.activated_stake)
                        .sum(),
                    delinquent_activated_stake: result
                        .delinquent
                        .iter()
                        .map(|vote| vote.activated_stake)
                        .sum(),
                },
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("vote accounts probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_cluster_nodes(
        &self,
        endpoint: &RpcEndpointConfig,
        enabled: bool,
    ) -> ProbeValue<ClusterNodesObservation> {
        if !enabled {
            return ProbeValue::empty();
        }

        let started_at = Instant::now();
        match self
            .client
            .call::<Vec<ClusterNode>>(
                endpoint,
                "getClusterNodes",
                serde_json::Value::Array(vec![]),
            )
            .await
        {
            Ok(result) => ProbeValue::ok(
                ClusterNodesObservation {
                    nodes: result.len(),
                    rpc_nodes: result.iter().filter(|node| node.rpc.is_some()).count(),
                    tpu_nodes: result.iter().filter(|node| node.tpu.is_some()).count(),
                },
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("cluster nodes probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_leader_schedule(
        &self,
        endpoint: &RpcEndpointConfig,
        enabled: bool,
    ) -> ProbeValue<LeaderScheduleObservation> {
        if !enabled {
            return ProbeValue::empty();
        }

        let started_at = Instant::now();
        match self
            .client
            .call::<Option<BTreeMap<String, Vec<usize>>>>(
                endpoint,
                "getLeaderSchedule",
                json!([null, { "commitment": "processed" }]),
            )
            .await
        {
            Ok(Some(result)) => ProbeValue::ok(
                LeaderScheduleObservation {
                    validators: result.len(),
                    total_leader_slots: result.values().map(Vec::len).sum(),
                    schedule: None,
                },
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Ok(None) => ProbeValue::err(
                "leader schedule unavailable for current epoch",
                saturating_elapsed_ms(started_at.elapsed()),
            ),
            Err(error) => ProbeValue::err(
                format!("leader schedule probe failed: {error:#}"),
                saturating_elapsed_ms(started_at.elapsed()),
            ),
        }
    }

    async fn probe_accounts(
        &self,
        endpoint: &RpcEndpointConfig,
        tracked_accounts: &[TrackedAccountConfig],
    ) -> Vec<AccountObservation> {
        if tracked_accounts.is_empty() {
            return Vec::new();
        }

        let mut groups: BTreeMap<&str, Vec<&TrackedAccountConfig>> = BTreeMap::new();
        for account in tracked_accounts {
            groups
                .entry(account.commitment.as_str())
                .or_default()
                .push(account);
        }

        let mut observations = Vec::with_capacity(tracked_accounts.len());
        for (commitment, accounts) in groups {
            let started_at = Instant::now();
            let pubkeys: Vec<&str> = accounts
                .iter()
                .map(|account| account.pubkey.as_str())
                .collect();
            let response = self
                .client
                .call::<RpcContext<Vec<Option<RpcAccount>>>>(
                    endpoint,
                    "getMultipleAccounts",
                    json!([
                        pubkeys,
                        {
                            "commitment": commitment,
                            "encoding": "base64"
                        }
                    ]),
                )
                .await;

            let latency_ms = saturating_elapsed_ms(started_at.elapsed());

            match response {
                Ok(context) => {
                    for (account_config, account) in accounts.iter().zip(context.value.into_iter())
                    {
                        let observation = match account {
                            Some(account) => {
                                let state_hash =
                                    stable_hash(&(context.context.slot, &account)).ok();

                                AccountObservation {
                                    pubkey: account_config.pubkey.clone(),
                                    commitment: commitment.to_owned(),
                                    slot: Some(context.context.slot),
                                    state_hash,
                                    lamports: Some(account.lamports),
                                    owner: Some(account.owner),
                                    executable: Some(account.executable),
                                    rent_epoch: Some(account.rent_epoch),
                                    data_len: Some(account.data.data_len()),
                                    latency_ms,
                                    error: None,
                                }
                            }
                            None => AccountObservation {
                                pubkey: account_config.pubkey.clone(),
                                commitment: commitment.to_owned(),
                                slot: Some(context.context.slot),
                                state_hash: None,
                                lamports: None,
                                owner: None,
                                executable: None,
                                rent_epoch: None,
                                data_len: None,
                                latency_ms,
                                error: Some("account not present on endpoint".to_owned()),
                            },
                        };

                        observations.push(observation);
                    }
                }
                Err(error) => {
                    observations.extend(accounts.into_iter().map(|account_config| {
                        AccountObservation {
                            pubkey: account_config.pubkey.clone(),
                            commitment: commitment.to_owned(),
                            slot: None,
                            state_hash: None,
                            lamports: None,
                            owner: None,
                            executable: None,
                            rent_epoch: None,
                            data_len: None,
                            latency_ms,
                            error: Some(format!("account probe failed: {error:#}")),
                        }
                    }));
                }
            }
        }

        observations
    }

    async fn probe_signatures(
        &self,
        endpoint: &RpcEndpointConfig,
        tracked_signatures: &[String],
    ) -> Vec<SignatureObservation> {
        if tracked_signatures.is_empty() {
            return Vec::new();
        }

        let started_at = Instant::now();
        let response = self
            .client
            .call::<Vec<Option<RpcSignatureStatus>>>(
                endpoint,
                "getSignatureStatuses",
                json!([tracked_signatures, { "searchTransactionHistory": true }]),
            )
            .await;
        let latency_ms = saturating_elapsed_ms(started_at.elapsed());

        match response {
            Ok(statuses) => tracked_signatures
                .iter()
                .zip(statuses.into_iter())
                .map(|(signature, status)| SignatureObservation {
                    signature: signature.clone(),
                    latency_ms,
                    status: status.map(|status| {
                        let finalized = status.confirmation_status.as_deref() == Some("finalized");
                        SignatureStatusObservation {
                            slot: status.slot,
                            confirmation_status: status.confirmation_status,
                            confirmations: status.confirmations,
                            finalized,
                            err: status.err,
                        }
                    }),
                    error: None,
                })
                .collect(),
            Err(error) => tracked_signatures
                .iter()
                .map(|signature| SignatureObservation {
                    signature: signature.clone(),
                    latency_ms,
                    status: None,
                    error: Some(format!("signature probe failed: {error:#}")),
                })
                .collect(),
        }
    }
}

fn saturating_elapsed_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct RpcContext<T> {
    context: SlotContext,
    value: T,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct SlotContext {
    slot: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct LatestBlockhashResult {
    context: SlotContext,
    value: RpcBlockhashValue,
}

#[derive(Clone, Debug, Deserialize)]
struct RpcBlockhashValue {
    blockhash: String,
    #[serde(rename = "lastValidBlockHeight")]
    last_valid_block_height: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct VersionResult {
    #[serde(rename = "solana-core")]
    solana_core: String,
}

#[derive(Clone, Debug, Deserialize)]
struct IdentityResult {
    identity: String,
}

#[derive(Clone, Debug, Deserialize)]
struct VoteAccountsResult {
    current: Vec<VoteAccount>,
    delinquent: Vec<VoteAccount>,
}

#[derive(Clone, Debug, Deserialize)]
struct VoteAccount {
    #[serde(rename = "activatedStake")]
    activated_stake: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct ClusterNode {
    rpc: Option<String>,
    tpu: Option<String>,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct RpcAccount {
    lamports: u64,
    owner: String,
    executable: bool,
    #[serde(rename = "rentEpoch")]
    rent_epoch: u64,
    data: RpcAccountData,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
#[serde(untagged)]
enum RpcAccountData {
    Legacy(String),
    BinaryTuple((String, String)),
    Json(serde_json::Value),
}

impl RpcAccountData {
    fn data_len(&self) -> usize {
        match self {
            Self::Legacy(data) => data.len(),
            Self::BinaryTuple((data, _encoding)) => data.len(),
            Self::Json(value) => value.to_string().len(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RpcSignatureStatus {
    slot: u64,
    confirmations: Option<usize>,
    err: Option<serde_json::Value>,
    #[serde(rename = "confirmationStatus")]
    confirmation_status: Option<String>,
}
