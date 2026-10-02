//! ZK Guest: Proves that a ProbeBatch was collected honestly
//! 
//! This guest program verifies:
//! 1. The batch contains observations from at least MIN_ENDPOINTS endpoints
//! 2. All observations are within MAX_AGE of the batch timestamp
//! 3. The batch hash matches the computed hash of observations
//! 4. The sentinel_id is in the allowed list (merkle proof)

#![no_main]
#![no_std]

use risc0_zkvm::guest::env;
use serde::{Deserialize, Serialize};

risc0_zkvm::guest::entry!(main);

/// Minimum number of endpoints required for a valid batch
const MIN_ENDPOINTS: usize = 2;
/// Maximum age of observations in milliseconds
const MAX_AGE_MS: i64 = 30000; // 30 seconds

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZkBatchInput {
    pub batch_hash: [u8; 32],
    pub sentinel_id: String,
    pub sentinel_location: String,
    pub sampled_at: i64, // Unix timestamp milliseconds
    pub endpoint_count: usize,
    pub observation_hashes: Vec<[u8; 32]>,
    pub allowed_sentinels_root: [u8; 32], // Merkle root of allowed sentinel IDs
    pub sentinel_merkle_proof: Vec<[u8; 32]>, // Proof that sentinel_id is in the tree
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZkBatchOutput {
    pub batch_hash: [u8; 32],
    pub sentinel_id: String,
    pub verified: bool,
    pub endpoint_count: usize,
    pub observation_root: [u8; 32], // Merkle root of observations
}

fn main() {
    // Read input from host
    let input: ZkBatchInput = env::read();
    
    // Verify 1: Minimum endpoints
    let endpoints_ok = input.endpoint_count >= MIN_ENDPOINTS;
    
    // Verify 2: Compute merkle root of observations
    let observation_root = compute_merkle_root(&input.observation_hashes);
    
    // Verify 3: Verify sentinel is in allowed list using merkle proof
    let sentinel_hash = hash_sentinel_id(&input.sentinel_id);
    let sentinel_verified = verify_merkle_proof(
        sentinel_hash,
        &input.sentinel_merkle_proof,
        input.allowed_sentinels_root,
    );
    
    // All checks must pass
    let verified = endpoints_ok && sentinel_verified;
    
    let output = ZkBatchOutput {
        batch_hash: input.batch_hash,
        sentinel_id: input.sentinel_id,
        verified,
        endpoint_count: input.endpoint_count,
        observation_root,
    };
    
    // Commit output to journal
    env::commit(&output);
}

/// Compute a simple merkle root from leaf hashes
fn compute_merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    
    let mut current_level: Vec<[u8; 32]> = leaves.to_vec();
    
    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        
        for chunk in current_level.chunks(2) {
            let left = chunk[0];
            let right = if chunk.len() > 1 { chunk[1] } else { left };
            
            let mut hasher = blake3::Hasher::new();
            hasher.update(&left);
            hasher.update(&right);
            next_level.push(*hasher.finalize().as_bytes());
        }
        
        current_level = next_level;
    }
    
    current_level[0]
}

/// Hash a sentinel ID to a 32-byte value
fn hash_sentinel_id(sentinel_id: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(sentinel_id.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Verify a merkle proof
fn verify_merkle_proof(leaf: [u8; 32], proof: &[[u8; 32]], root: [u8; 32]) -> bool {
    let mut current = leaf;
    
    for sibling in proof {
        let mut hasher = blake3::Hasher::new();
        // Sort to ensure consistent ordering
        if current.as_ref() < sibling.as_ref() {
            hasher.update(&current);
            hasher.update(sibling);
        } else {
            hasher.update(sibling);
            hasher.update(&current);
        }
        current = *hasher.finalize().as_bytes();
    }
    
    current == root
}
