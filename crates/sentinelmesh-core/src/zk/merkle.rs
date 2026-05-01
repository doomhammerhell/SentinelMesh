use blake3;
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};

/// A simple static Merkle Tree for whitelist membership proofs.
/// In "Elite" mode, this would be computed over the Poseidon hash for ZK efficiency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhitelistMerkleTree {
    pub leaves: Vec<[u8; 32]>,
    pub layers: Vec<Vec<[u8; 32]>>,
}

impl WhitelistMerkleTree {
    pub fn new(signer_ids: &[String]) -> Result<Self> {
        let mut leaves: Vec<[u8; 32]> = signer_ids.iter()
            .map(|id| blake3::hash(id.as_bytes()).into())
            .collect();
        
        // Pad to power of 2
        let n = leaves.len().next_power_of_two();
        while leaves.len() < n {
            leaves.push([0u8; 32]);
        }

        let mut layers = vec![leaves.clone()];
        let mut current_layer = leaves;

        while current_layer.len() > 1 {
            let mut next_layer = Vec::with_capacity(current_layer.len() / 2);
            for i in (0..current_layer.len()).step_by(2) {
                let left = current_layer[i];
                let right = current_layer[i+1];
                let mut hasher = blake3::Hasher::new();
                hasher.update(&left);
                hasher.update(&right);
                next_layer.push(hasher.finalize().into());
            }
            layers.push(next_layer.clone());
            current_layer = next_layer;
        }

        Ok(Self { leaves: layers[0].clone(), layers })
    }

    pub fn root(&self) -> [u8; 32] {
        self.layers.last().map(|l| l[0]).unwrap_or([0u8; 32])
    }

    pub fn get_proof(&self, index: usize) -> Result<Vec<[u8; 32]>> {
        let mut proof = Vec::new();
        let mut idx = index;

        for layer in &self.layers[0..self.layers.len() - 1] {
            let sibling_idx = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
            proof.push(layer[sibling_idx]);
            idx /= 2;
        }

        Ok(proof)
    }
}
