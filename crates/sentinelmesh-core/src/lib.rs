pub mod auth;
pub mod config;
pub mod model;
pub mod telemetry;

pub use auth::{BatchVerifier, SigningMaterial, TrustedSigner, sign_batch};
pub use config::{
    AgentConfig, AgentRuntimeConfig, AggregatorConfig, AggregatorServerConfig, AnalysisConfig,
    CanaryCliTransferConfig, CanaryConfig, CanaryMode, ConfigLoadError, DatabaseConfig,
    IngestionAuthConfig, IngestionConfig, ObservabilityConfig, OtlpConfig, PublishAuthConfig,
    PublishConfig, ReplayLogConfig, RpcEndpointConfig, ServerSecurityConfig, SigningKeyConfig,
    StorageConfig, TlsClientConfig, TrackedAccountConfig, TrustedSignerConfig,
    ValidatorProbeConfig, load_from_path,
};
pub use model::{
    AccountDivergence, AccountObservation, AccountStateVariant, Anomaly, AnomalySeverity,
    BatchAuth, BlockhashObservation, ClusterNodesObservation, EndpointObservation, EndpointSample,
    HealthResponse, IdentityObservation, InfrastructureConcentration, IngestionResponse,
    LeaderScheduleObservation, NetworkSnapshot, ProbeBatch, ProbeEnvelope, ProbeValue,
    PropagationSummary, ProviderShare, ProviderStatus, SignatureObservation, SignaturePropagation,
    SignatureStatusObservation, ValidatorStateDivergence, VoteAccountsObservation,
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
