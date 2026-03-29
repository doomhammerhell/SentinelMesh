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

        let now = chrono::Utc::now();
        let drift = now
            .signed_duration_since(auth.signed_at)
            .num_seconds()
            .abs();
        if drift > 30 {
            bail!(
                "batch rejected: signed_at timestamp is outside the 30-second clock drift window (anti-replay protection)"
            );
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RpcEndpointConfig;
    use crate::model::{
        EndpointObservation, ProbeValue, ProbeBatch,
    };
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    // Feature: sentinelmesh-comprehensive-upgrade, Property 1: Round-trip de assinatura Ed25519
    // **Validates: Requirements 1.2, 1.4, 1.5**

    // --- Strategy helpers (simplified for auth tests) ---

    fn arb_short_string() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_]{1,16}"
    }

    fn arb_rpc_endpoint_config() -> impl Strategy<Value = RpcEndpointConfig> {
        (
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            "https?://[a-z]{3,8}\\.[a-z]{2,4}",
            Just(BTreeMap::new()),
        )
            .prop_map(|(id, label, provider, region, rpc_url, tags)| RpcEndpointConfig {
                id,
                label,
                provider,
                region,
                rpc_url,
                tags,
            })
    }

    fn arb_endpoint_observation() -> impl Strategy<Value = EndpointObservation> {
        arb_rpc_endpoint_config().prop_map(|endpoint| EndpointObservation {
            endpoint,
            overall_latency_ms: 100,
            health: ProbeValue::ok("ok".to_string(), 10),
            slot: ProbeValue::ok(42, 10),
            block_height: ProbeValue::ok(42, 10),
            latest_blockhash: ProbeValue::empty(),
            version: ProbeValue::ok("1.18.0".to_string(), 10),
            identity: ProbeValue::empty(),
            vote_accounts: ProbeValue::empty(),
            cluster_nodes: ProbeValue::empty(),
            leader_schedule: ProbeValue::empty(),
            accounts: vec![],
            signatures: vec![],
            probe_errors: vec![],
            transaction_order: vec![],
        })
    }

    fn arb_probe_batch() -> impl Strategy<Value = ProbeBatch> {
        (
            any::<u16>(),
            arb_short_string(),
            arb_short_string(),
            prop::option::of(any::<u32>()),
            prop::collection::vec(arb_endpoint_observation(), 0..3),
        )
            .prop_map(|(schema_version, sentinel_id, sentinel_location, asn, endpoints)| {
                ProbeBatch {
                    schema_version,
                    batch_id: Uuid::new_v4(),
                    sampled_at: Utc::now(),
                    sentinel_id,
                    sentinel_location,
                    asn,
                    endpoints,
                }
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_ed25519_sign_verify_round_trip(
            batch in arb_probe_batch(),
            key_bytes in prop::array::uniform32(any::<u8>()),
        ) {
            // Feature: sentinelmesh-comprehensive-upgrade, Property 1: Round-trip de assinatura Ed25519
            // For any valid ProbeBatch, signing with LocalMemorySigner and verifying
            // with BatchVerifier using the corresponding public key should succeed.

            // Use proptest-generated bytes to create an Ed25519 keypair
            let private_key_b64 = STANDARD.encode(key_bytes);

            let signer = SigningMaterial::from_base64("test-signer", "test-key", &private_key_b64)
                .expect("SigningMaterial creation should succeed");

            // Sign the batch
            let signed_at = Utc::now();
            let auth = signer.sign(&batch, signed_at)
                .expect("signing should succeed for any valid ProbeBatch");

            // Build a BatchVerifier with the corresponding public key
            let pub_key_b64 = signer.verifying_key_base64();
            let trusted = TrustedSigner::from_base64(
                Some("test-signer".to_string()),
                "test-key",
                &pub_key_b64,
            )
            .expect("TrustedSigner creation should succeed");

            let verifier = BatchVerifier::new(vec![trusted]);

            // Verify the signature — this must succeed for any valid ProbeBatch
            verifier.verify(&batch, &auth)
                .expect("verification should succeed for a correctly signed batch");
        }
    }
}
