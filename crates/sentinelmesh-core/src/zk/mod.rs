pub mod merkle;

/// ZK-Telemetry types and constants.
pub const TREE_DEPTH: usize = 20;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ZkMembershipProof {
    pub root_b64: String,
    pub proof_b64: String,
    pub nullifier_b64: String,
}
