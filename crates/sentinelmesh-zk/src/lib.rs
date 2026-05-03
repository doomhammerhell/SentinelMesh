//! SentinelMesh ZK Proof System
//! 
//! This crate provides zero-knowledge proof generation and verification
//! for SentinelMesh probe batches, enabling irrefutable integrity claims.
//! 
//! Uses RISC Zero for proof generation and verification.

use anyhow::{Context, Result};
use risc0_zkvm::{default_prover, ExecutorEnv, ProverOpts, Receipt, VerifierContext};
use sentinelmesh_core::{ProbeBatch, stable_hash};
use serde::{Deserialize, Serialize};

// Include the compiled guest binary
include!(concat!(env!("OUT_DIR"), "/methods.rs"));

/// Configuration for ZK proof generation
#[derive(Clone, Debug)]
pub struct ZkConfig {
    /// Minimum number of endpoints required for a valid batch
    pub min_endpoints: usize,
    /// Maximum age of observations in milliseconds
    pub max_age_ms: i64,
    /// Whether to use dev mode (faster, insecure proofs)
    pub dev_mode: bool,
}

impl Default for ZkConfig {
    fn default() -> Self {
        Self {
            min_endpoints: 2,
            max_age_ms: 30000,
            dev_mode: cfg!(debug_assertions),
        }
    }
}

/// Input to the ZK guest program
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZkBatchInput {
    pub batch_hash: [u8; 32],
    pub sentinel_id: String,
    pub sentinel_location: String,
    pub sampled_at: i64,
    pub endpoint_count: usize,
    pub observation_hashes: Vec<[u8; 32]>,
    pub allowed_sentinels_root: [u8; 32],
    pub sentinel_merkle_proof: Vec<[u8; 32]>,
}

/// Output from the ZK guest program (committed to journal)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZkBatchOutput {
    pub batch_hash: [u8; 32],
    pub sentinel_id: String,
    pub verified: bool,
    pub endpoint_count: usize,
    pub observation_root: [u8; 32],
}

/// A ZK proof receipt for a batch
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BatchProof {
    /// The RISC Zero receipt
    pub receipt: Receipt,
    /// The public output (journal)
    pub output: ZkBatchOutput,
}

/// ZK Prover for SentinelMesh batches
pub struct BatchProver {
    config: ZkConfig,
    allowed_sentinels: Vec<String>,
    sentinels_root: [u8; 32],
}

impl BatchProver {
    /// Create a new prover with a list of allowed sentinel IDs
    pub fn new(config: ZkConfig, allowed_sentinels: Vec<String>) -> Result<Self> {
        let sentinels_root = compute_sentinels_root(&allowed_sentinels);
        
        Ok(Self {
            config,
            allowed_sentinels,
            sentinels_root,
        })
    }
    
    /// Generate a ZK proof for a batch
    /// 
    /// # Arguments
    /// * `batch` - The probe batch to prove
    /// 
    /// # Returns
    /// A `BatchProof` containing the receipt and public output
    pub fn prove(&self, batch: &ProbeBatch) -> Result<BatchProof> {
        // Compute observation hashes
        let observation_hashes: Vec<[u8; 32]> = batch
            .endpoints
            .iter()
            .map(|obs| {
                let json = serde_json::to_vec(obs).unwrap_or_default();
                let mut hasher = blake3::Hasher::new();
                hasher.update(&json);
                *hasher.finalize().as_bytes()
            })
            .collect();
        
        // Generate merkle proof for sentinel
        let sentinel_proof = self.generate_sentinel_proof(&batch.sentinel_id)?;
        
        // Compute batch hash
        let batch_hash = compute_batch_hash(batch)?;
        
        let input = ZkBatchInput {
            batch_hash,
            sentinel_id: batch.sentinel_id.clone(),
            sentinel_location: batch.sentinel_location.clone(),
            sampled_at: batch.sampled_at.timestamp_millis(),
            endpoint_count: batch.endpoints.len(),
            observation_hashes,
            allowed_sentinels_root: self.sentinels_root,
            sentinel_merkle_proof: sentinel_proof,
        };
        
        // Execute the guest program
        let env = ExecutorEnv::builder()
            .write(&input)
            .context("failed to serialize ZK input")?
            .build()
            .context("failed to build executor environment")?;
        
        let prover = default_prover();
        
        let opts = if self.config.dev_mode {
            ProverOpts::fast()
        } else {
            ProverOpts::default()
        };
        
        let receipt = prover
            .prove_with_opts(env, SENTINELMESH_ZK_GUEST_ELF, &opts)
            .context("failed to generate ZK proof")?;
        
        // Extract output from journal
        let output: ZkBatchOutput = receipt
            .journal
            .decode()
            .context("failed to decode ZK output from journal")?;
        
        if !output.verified {
            anyhow::bail!("ZK proof verification failed: batch did not meet criteria");
        }
        
        Ok(BatchProof { receipt, output })
    }
    
    /// Verify a ZK proof
    pub fn verify(&self, proof: &BatchProof) -> Result<bool> {
        let ctx = VerifierContext::default();
        proof
            .receipt
            .verify(SENTINELMESH_ZK_GUEST_ID, &ctx)
            .context("ZK proof verification failed")?;
        
        // Also verify the output matches expectations
        let output: ZkBatchOutput = proof
            .receipt
            .journal
            .decode()
            .context("failed to decode output")?;
        
        Ok(output.verified)
    }
    
    /// Generate a merkle proof that a sentinel is in the allowed list
    fn generate_sentinel_proof(&self, sentinel_id: &str) -> Result<Vec<[u8; 32]>> {
        let sentinel_hash = hash_sentinel_id(sentinel_id);
        
        // Build merkle tree and find path
        let leaves: Vec<[u8; 32]> = self
            .allowed_sentinels
            .iter()
            .map(|id| hash_sentinel_id(id))
            .collect();
        
        let tree = MerkleTree::new(&leaves);
        tree.generate_proof(&sentinel_hash)
            .context("sentinel not found in allowed list")
    }
}

/// Compute the merkle root of allowed sentinels
fn compute_sentinels_root(sentinels: &[String]) -> [u8; 32] {
    let leaves: Vec<[u8; 32]> = sentinels.iter().map(|id| hash_sentinel_id(id)).collect();
    let tree = MerkleTree::new(&leaves);
    tree.root()
}

/// Hash a sentinel ID
fn hash_sentinel_id(sentinel_id: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(sentinel_id.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Compute hash of a batch
fn compute_batch_hash(batch: &ProbeBatch) -> Result<[u8; 32]> {
    let hash_str = stable_hash(batch)?;
    let mut hash = [0u8; 32];
    let bytes = hex::decode(&hash_str).context("failed to decode hash")?;
    hash.copy_from_slice(&bytes[..32.min(bytes.len())]);
    Ok(hash)
}

/// Simple Merkle Tree implementation
pub struct MerkleTree {
    leaves: Vec<[u8; 32]>,
    levels: Vec<Vec<[u8; 32]>>,
}

impl MerkleTree {
    pub fn new(leaves: &[[u8; 32]]) -> Self {
        let leaves = leaves.to_vec();
        let levels = Self::build_tree(&leaves);
        Self { leaves, levels }
    }
    
    fn build_tree(leaves: &[[u8; 32]]) -> Vec<Vec<[u8; 32]>> {
        if leaves.is_empty() {
            return vec![];
        }
        
        let mut levels = vec![leaves.to_vec()];
        let mut current = leaves.to_vec();
        
        while current.len() > 1 {
            let mut next = Vec::new();
            for chunk in current.chunks(2) {
                let left = chunk[0];
                let right = if chunk.len() > 1 { chunk[1] } else { left };
                next.push(hash_pair(left, right));
            }
            levels.push(next.clone());
            current = next;
        }
        
        levels
    }
    
    pub fn root(&self) -> [u8; 32] {
        self.levels
            .last()
            .and_then(|level| level.first())
            .copied()
            .unwrap_or([0u8; 32])
    }
    
    pub fn generate_proof(&self, leaf: &[u8; 32]) -> Option<Vec<[u8; 32]>> {
        let leaf_idx = self.leaves.iter().position(|l| l == leaf)?;
        let mut proof = Vec::new();
        let mut idx = leaf_idx;
        
        for level in &self.levels {
            if level.len() <= 1 {
                break;
            }
            
            let sibling_idx = if idx % 2 == 0 {
                (idx + 1).min(level.len() - 1)
            } else {
                idx - 1
            };
            
            proof.push(level[sibling_idx]);
            idx /= 2;
        }
        
        Some(proof)
    }
}

fn hash_pair(left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    if left.as_ref() < right.as_ref() {
        hasher.update(&left);
        hasher.update(&right);
    } else {
        hasher.update(&right);
        hasher.update(&left);
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_merkle_tree() {
        let leaves: Vec<[u8; 32]> = (0..4)
            .map(|i| {
                let mut hash = [0u8; 32];
                hash[0] = i as u8;
                hash
            })
            .collect();
        
        let tree = MerkleTree::new(&leaves);
        let root = tree.root();
        
        // Verify all leaves have valid proofs
        for leaf in &leaves {
            let proof = tree.generate_proof(leaf).unwrap();
            assert!(verify_proof(*leaf, &proof, root));
        }
    }
    
    fn verify_proof(leaf: [u8; 32], proof: &[[u8; 32]], root: [u8; 32]) -> bool {
        let mut current = leaf;
        for sibling in proof {
            current = hash_pair(current, *sibling);
        }
        current == root
    }
}
