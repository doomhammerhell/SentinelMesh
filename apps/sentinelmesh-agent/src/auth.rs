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
/// On Linux, this connects to an AWS Nitro Enclave via VSOCK to sign batches.
/// The Enclave receives the pre-computed hash and returns the raw Ed25519 signature
/// without ever exposing the private key to host memory.
pub struct NitroEnclaveSigner {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub signer_id: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub key_id: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub vsock_cid: u32,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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

// ── Linux: real VSOCK implementation ──────────────────────────────────────────
#[cfg(target_os = "linux")]
#[async_trait]
impl SignerBackend for NitroEnclaveSigner {
    async fn sign(&self, batch: &ProbeBatch, signed_at: DateTime<Utc>) -> Result<BatchAuth> {
        use anyhow::Context;
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_vsock::VsockStream;

        let batch_hash = sentinelmesh_core::stable_hash(batch)?;
        let payload = format!("{}:{}:{}", self.signer_id, self.key_id, batch_hash);

        let mut stream = tokio::time::timeout(
            Duration::from_secs(5),
            VsockStream::connect(self.vsock_cid, self.vsock_port),
        )
        .await
        .context("VSOCK connect timeout")?
        .context("VSOCK connect failed")?;

        // Send length-prefixed payload: [4 bytes big-endian length][payload bytes]
        let payload_bytes = payload.as_bytes();
        let len = u32::try_from(payload_bytes.len()).context("payload length exceeds u32")?;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(payload_bytes).await?;

        // Receive 64-byte raw Ed25519 signature
        let mut sig_buf = [0u8; 64];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut sig_buf))
            .await
            .context("VSOCK read timeout")?
            .context("VSOCK read failed")?;

        Ok(BatchAuth {
            signer_id: self.signer_id.clone(),
            key_id: self.key_id.clone(),
            signed_at,
            batch_hash,
            signature_b64: STANDARD.encode(sig_buf),
        })
    }
}

// ── Non-Linux: stub that returns a descriptive error ─────────────────────────
#[cfg(not(target_os = "linux"))]
#[async_trait]
impl SignerBackend for NitroEnclaveSigner {
    async fn sign(&self, _batch: &ProbeBatch, _signed_at: DateTime<Utc>) -> Result<BatchAuth> {
        anyhow::bail!("NitroEnclaveSigner is only supported on Linux with Nitro Enclaves")
    }
}
