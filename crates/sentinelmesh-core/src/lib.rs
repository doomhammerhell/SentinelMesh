pub mod auth;
pub mod config;
pub mod hlc;
pub mod model;
pub mod telemetry;
pub mod zk;

pub use auth::{BatchVerifier, SigningMaterial, TrustedSigner, sign_batch};
pub use config::{
    AgentConfig, AgentRuntimeConfig, AggregatorConfig, AggregatorServerConfig, AlertsConfig,
    AnalysisConfig, CanaryCliTransferConfig, CanaryConfig, CanaryMode, ClickHouseConfig,
    ConfigLoadError, IngestionAuthConfig, IngestionConfig, KafkaConfig, ObservabilityConfig,
    OtlpConfig, PublishAuthConfig, PublishConfig, RpcEndpointConfig, ServerSecurityConfig,
    SigningKeyConfig, StorageConfig, TlsClientConfig, TrackedAccountConfig, TrustedSignerConfig,
    ValidatorProbeConfig, WebhookConfig, load_from_path,
};
pub use hlc::Hlc;
pub use model::{
    AccountDivergence, AccountObservation, AccountStateVariant, Anomaly, AnomalySeverity,
    AttestationQuote, BatchAuth, BlockhashObservation, ClusterNodesObservation, ControlMessage,
    EndpointObservation, EndpointSample, HealthResponse, IdentityChangeEvent, IdentityObservation,
    InfrastructureConcentration, IngestionResponse, LeaderScheduleObservation, MevAuditSummary,
    NetworkSnapshot, ProbeBatch, ProbeEnvelope, ProbeValue, PropagationSummary, ProviderShare,
    ProviderStatus, SignatureObservation, SignaturePropagation, SignatureStatusObservation,
    TransactionOrderObservation, ValidatorStateDivergence, VoteAccountsObservation, ZScoreReport,
};

use anyhow::Context;
use serde::Serialize;

pub fn stable_hash<T>(value: &T) -> anyhow::Result<String>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).context("failed to serialize value for hashing")?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
