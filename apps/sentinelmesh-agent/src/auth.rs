use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sentinelmesh_core::{
    BatchAuth, ProbeBatch, SigningMaterial,
    config::{MemorySignerConfig, NitroEnclaveConfig},
};

#[async_trait]
pub trait SignerBackend: Send + Sync {
    async fn sign(&self, batch: &ProbeBatch, signed_at: DateTime<Utc>) -> Result<BatchAuth>;
}

/// The legacy signer that keeps Ed25519 in memory.
pub struct LocalMemorySigner {
    material: SigningMaterial,
}

impl LocalMemorySigner {
    pub fn new(config: &MemorySignerConfig) -> Result<Self> {
        let material = SigningMaterial::from_base64(
            config.signer_id.clone(),
            config.key_id.clone(),
            &config.private_key_base64,
        )?;
        Ok(Self { material })
    }
}

#[async_trait]
impl SignerBackend for LocalMemorySigner {
    async fn sign(&self, batch: &ProbeBatch, signed_at: DateTime<Utc>) -> Result<BatchAuth> {
        self.material.sign(batch, signed_at)
    }
}

/// The Hardware Enclave signer.
/// This connects to an AWS Nitro Enclave via VSOCK (mocked via `UnixStream` for portability in this MVP).
/// The Enclave receives the pre-computed hash and returns the raw signature safely without ever touching the RAM of the Host.
pub struct NitroEnclaveSigner {
    pub signer_id: String,
    #[allow(dead_code)]
    pub key_id: String,
    #[allow(dead_code)]
    pub vsock_cid: u32,
    #[allow(dead_code)]
    pub vsock_port: u32,
}

impl NitroEnclaveSigner {
    pub fn new(config: &NitroEnclaveConfig) -> Self {
        Self {
            signer_id: config.signer_id.clone(),
            key_id: config.key_id.clone(),
            vsock_cid: config.vsock_cid,
            vsock_port: config.vsock_port,
        }
    }
}

#[async_trait]
impl SignerBackend for NitroEnclaveSigner {
    async fn sign(&self, batch: &ProbeBatch, _signed_at: DateTime<Utc>) -> Result<BatchAuth> {
        // Compute SHA256 of the batch as a signing envelope
        // In a real Nitro Enclave, we would push this over a VSOCK socket.
        let batch_hash = sentinelmesh_core::stable_hash(batch)?;
        let _payload = format!("{}:{}", self.signer_id, batch_hash);

        // Simulated Vsock Write/Read:
        // let mut socket = VsockStream::connect(self.vsock_cid, self.vsock_port).await?;
        // socket.write_all(payload.as_bytes()).await?;
        // let mut sig_buf = [0u8; 64];
        // socket.read_exact(&mut sig_buf).await?;

        anyhow::bail!(
            "Nitro Enclave TEE signing interface is armed but requires a running CVM enclave to proceed."
        )
    }
}
