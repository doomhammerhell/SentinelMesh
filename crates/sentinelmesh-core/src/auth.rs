use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::Serialize;

use crate::{
    model::{BatchAuth, ProbeBatch},
    stable_hash,
};

#[derive(Clone)]
pub struct SigningMaterial {
    pub signer_id: String,
    pub key_id: String,
    signing_key: SigningKey,
}

impl SigningMaterial {
    pub fn from_base64(
        signer_id: impl Into<String>,
        key_id: impl Into<String>,
        private_key_base64: &str,
    ) -> Result<Self> {
        let raw = STANDARD
            .decode(private_key_base64)
            .context("failed to decode private key as base64")?;
        let bytes: [u8; 32] = raw
            .try_into()
            .map_err(|_| anyhow!("private key must decode to exactly 32 bytes"))?;

        Ok(Self {
            signer_id: signer_id.into(),
            key_id: key_id.into(),
            signing_key: SigningKey::from_bytes(&bytes),
        })
    }

    #[must_use]
    pub fn verifying_key_base64(&self) -> String {
        STANDARD.encode(self.signing_key.verifying_key().to_bytes())
    }

    pub fn sign(&self, batch: &ProbeBatch, signed_at: DateTime<Utc>) -> Result<BatchAuth> {
        sign_batch(batch, self, signed_at)
    }
}

#[derive(Clone)]
pub struct TrustedSigner {
    pub signer_id: Option<String>,
    pub key_id: String,
    verifying_key: VerifyingKey,
}

impl TrustedSigner {
    pub fn from_base64(
        signer_id: Option<String>,
        key_id: impl Into<String>,
        public_key_base64: &str,
    ) -> Result<Self> {
        let raw = STANDARD
            .decode(public_key_base64)
            .context("failed to decode public key as base64")?;
        let bytes: [u8; 32] = raw
            .try_into()
            .map_err(|_| anyhow!("public key must decode to exactly 32 bytes"))?;
        let verifying_key =
            VerifyingKey::from_bytes(&bytes).context("invalid ed25519 public key bytes")?;

        Ok(Self {
            signer_id,
            key_id: key_id.into(),
            verifying_key,
        })
    }
}

#[derive(Clone, Default)]
pub struct BatchVerifier {
    trusted_signers: Vec<TrustedSigner>,
}

impl BatchVerifier {
    #[must_use]
    pub fn new(trusted_signers: Vec<TrustedSigner>) -> Self {
        Self { trusted_signers }
    }

    pub fn verify(&self, batch: &ProbeBatch, auth: &BatchAuth) -> Result<()> {
        let signer = self
            .trusted_signers
            .iter()
            .find(|signer| {
                signer.key_id == auth.key_id
                    && signer
                        .signer_id
                        .as_deref()
                        .is_none_or(|signer_id| signer_id == auth.signer_id)
            })
            .ok_or_else(|| anyhow!("unknown signing key id {}", auth.key_id))?;

        let expected_hash = stable_hash(batch)?;
        if expected_hash != auth.batch_hash {
            bail!("batch hash mismatch for signed envelope");
        }

        let message = signing_message(
            &auth.batch_hash,
            auth.signed_at,
            &auth.signer_id,
            &auth.key_id,
        );
        let raw_signature = STANDARD
            .decode(auth.signature_b64.as_bytes())
            .context("failed to decode batch signature as base64")?;
        let signature = Signature::try_from(raw_signature.as_slice())
            .map_err(|_| anyhow!("invalid ed25519 signature length"))?;
        signer
            .verifying_key
            .verify(&message, &signature)
            .context("ed25519 signature verification failed")?;
        Ok(())
    }
}

pub fn sign_batch(
    batch: &ProbeBatch,
    signing_material: &SigningMaterial,
    signed_at: DateTime<Utc>,
) -> Result<BatchAuth> {
    let batch_hash = stable_hash(batch)?;
    let message = signing_message(
        &batch_hash,
        signed_at,
        &signing_material.signer_id,
        &signing_material.key_id,
    );
    let signature = signing_material.signing_key.sign(&message);
    Ok(BatchAuth {
        signer_id: signing_material.signer_id.clone(),
        key_id: signing_material.key_id.clone(),
        signed_at,
        batch_hash,
        signature_b64: STANDARD.encode(signature.to_bytes()),
    })
}

fn signing_message(
    batch_hash: &str,
    signed_at: DateTime<Utc>,
    signer_id: &str,
    key_id: &str,
) -> Vec<u8> {
    #[derive(Serialize)]
    struct SigningEnvelope<'a> {
        batch_hash: &'a str,
        signed_at: DateTime<Utc>,
        signer_id: &'a str,
        key_id: &'a str,
    }

    serde_json::to_vec(&SigningEnvelope {
        batch_hash,
        signed_at,
        signer_id,
        key_id,
    })
    .expect("signing envelope serialization must succeed")
}
