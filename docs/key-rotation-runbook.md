# Ed25519 Key Rotation Runbook

This runbook describes the step-by-step procedure for rotating Ed25519 signing keys used by SentinelMesh Agents for Proof of Origin (`BatchAuth`). The process ensures zero-downtime by maintaining an overlap period where both old and new keys are accepted by the Aggregator.

## Overview

SentinelMesh uses Ed25519 signatures to authenticate `ProbeEnvelope` batches. Each Agent is configured with a signing identity composed of:

- **`signer_id`** — the sentinel's logical identity (remains constant across rotations)
- **`key_id`** — the specific key version (changes on each rotation)
- **Private key** — the Ed25519 secret key (stored in-memory or inside a Nitro Enclave)

The Aggregator maintains a list of `trusted_signers` in its configuration. Each entry contains the `key_id`, `public_key_base64`, and an optional `signer_id`. The `BatchVerifier` matches incoming envelopes by `key_id` (and optionally `signer_id`) to select the correct public key for verification.

Key rotation works by temporarily trusting both the old and new public keys on the Aggregator, switching the Agent to sign with the new key, verifying correctness, and then removing the old key.

## Prerequisites

- Access to the Agent and Aggregator configuration files
- `openssl` or another Ed25519 key generation tool
- Ability to restart or hot-reload the Agent and Aggregator processes
- The current `signer_id` and `key_id` for the Agent being rotated

## Procedure

### Step 1: Generate a New Ed25519 Keypair

Generate a new 32-byte Ed25519 private key and derive the public key. Encode both in base64.

```bash
# Generate a random 32-byte private key
openssl genpkey -algorithm ed25519 -outform DER | tail -c 32 | base64 > new_private_key.b64

# Derive the public key from the private key
# (using a helper script or Rust tooling)
# Alternatively, use the sentinelmesh CLI if available:
# sentinelmesh-keygen --private-key-file new_private_key.b64
```

If using Rust tooling directly:

```rust
use ed25519_dalek::SigningKey;
use base64::{Engine as _, engine::general_purpose::STANDARD};

let private_bytes: [u8; 32] = /* read from new_private_key.b64 */;
let signing_key = SigningKey::from_bytes(&private_bytes);
let public_key_b64 = STANDARD.encode(signing_key.verifying_key().to_bytes());
println!("Public key (base64): {}", public_key_b64);
```

Choose a new `key_id` that is distinct from the current one. A recommended convention is a version suffix, e.g., `sentinel-alpha-key-v2`, or a date-based identifier like `key-2025-01-15`.

Record the following values — you will need them in subsequent steps:

| Value | Example |
|---|---|
| `signer_id` | `sentinel-alpha` (unchanged) |
| New `key_id` | `sentinel-alpha-key-v2` |
| New private key (base64) | `<contents of new_private_key.b64>` |
| New public key (base64) | `<derived from private key>` |

### Step 2: Add the New Key to the Aggregator's Trusted Signers

Edit the Aggregator configuration file and add the new public key to the `trusted_signers` list. Keep the old key entry in place — both must coexist during the overlap period.

```yaml
# aggregator-config.yaml
ingestion:
  auth:
    require_signed_batches: true
    api_keys:
      - "your-api-key"
    trusted_signers:
      # Old key — keep during overlap period
      - key_id: "sentinel-alpha-key-v1"
        public_key_base64: "<OLD_PUBLIC_KEY_BASE64>"
        signer_id: "sentinel-alpha"
      # New key — added for rotation
      - key_id: "sentinel-alpha-key-v2"
        public_key_base64: "<NEW_PUBLIC_KEY_BASE64>"
        signer_id: "sentinel-alpha"
```

Restart or reload the Aggregator to pick up the new configuration:

```bash
# Restart the Aggregator
systemctl restart sentinelmesh-aggregator
# or, if running directly:
# kill -HUP <aggregator_pid>
```

Verify the Aggregator started successfully:

```bash
journalctl -u sentinelmesh-aggregator --since "1 minute ago" | grep -i "trusted\|signer\|started"
```

At this point, the Aggregator accepts signatures from both the old and new keys. The Agent is still signing with the old key — this is expected.

### Step 3: Configure the Agent to Use the New Key

Edit the Agent configuration file to use the new key.

For `LocalMemorySigner`:

```yaml
# agent-config.yaml
signing:
  type: memory
  signer_id: "sentinel-alpha"
  key_id: "sentinel-alpha-key-v2"          # Updated
  private_key_base64: "<NEW_PRIVATE_KEY_BASE64>"  # Updated
```

For `NitroEnclaveSigner`:

```yaml
# agent-config.yaml
signing:
  type: nitro_enclave
  signer_id: "sentinel-alpha"
  key_id: "sentinel-alpha-key-v2"    # Updated
  vsock_cid: 16
  vsock_port: 5000
```

> **Note for Nitro Enclaves:** The new private key must be provisioned inside the enclave image before updating the Agent config. Rebuild and deploy the enclave image with the new key, then update the Agent's `key_id` to match.

Restart the Agent:

```bash
systemctl restart sentinelmesh-agent
```

### Step 4: Overlap Period — Verify the New Key Is Working

After restarting the Agent, both keys are trusted by the Aggregator but only the new key is actively used for signing. This is the overlap period.

Verify that the Agent is successfully publishing signed batches:

```bash
# Check Agent logs for successful publishes
journalctl -u sentinelmesh-agent --since "5 minutes ago" | grep -i "publish\|ingest\|success"

# Check Aggregator logs for accepted envelopes with the new key_id
journalctl -u sentinelmesh-aggregator --since "5 minutes ago" | grep "sentinel-alpha-key-v2"
```

Verify via the Aggregator API that fresh data is arriving:

```bash
# Check the latest snapshot — sampled_at should be recent
curl -s http://localhost:8080/v1/snapshot | jq '.samples | length'
```

Check Prometheus metrics for ingestion errors:

```bash
# No increase in verification failures
curl -s http://localhost:9090/api/v1/query?query=sentinelmesh_aggregator_ingest_errors_total
```

**Recommended overlap duration:** Keep both keys trusted for at least 24 hours to ensure the new key is working correctly across multiple probe cycles and to allow time for any issues to surface.

### Step 5: Remove the Old Key from the Aggregator

Once you have confirmed the new key is working correctly, remove the old key from the Aggregator's `trusted_signers` list:

```yaml
# aggregator-config.yaml
ingestion:
  auth:
    require_signed_batches: true
    api_keys:
      - "your-api-key"
    trusted_signers:
      # Only the new key remains
      - key_id: "sentinel-alpha-key-v2"
        public_key_base64: "<NEW_PUBLIC_KEY_BASE64>"
        signer_id: "sentinel-alpha"
```

Restart the Aggregator:

```bash
systemctl restart sentinelmesh-aggregator
```

Verify that ingestion continues normally:

```bash
journalctl -u sentinelmesh-aggregator --since "2 minutes ago" | grep -i "ingest\|error\|reject"
```

### Step 6: Securely Destroy the Old Private Key

After removing the old public key from the Aggregator, securely delete the old private key material:

```bash
# Overwrite and delete the old key file
shred -u old_private_key.b64
```

For Nitro Enclaves, ensure the old enclave image containing the previous key is deregistered and deleted.

## Rollback Procedure

If something goes wrong at any stage, follow the appropriate rollback steps.

### If the Agent fails to publish with the new key

1. Revert the Agent configuration to use the old `key_id` and `private_key_base64`.
2. Restart the Agent.
3. Verify that publishing resumes with the old key (the Aggregator still trusts it during overlap).
4. Investigate the issue before retrying the rotation.

```bash
# Revert agent config to old key, then:
systemctl restart sentinelmesh-agent

# Verify recovery
journalctl -u sentinelmesh-agent --since "2 minutes ago" | grep -i "publish\|success"
```

### If the Aggregator rejects the new key

1. Check that the `key_id` in the Agent config matches exactly the `key_id` in the Aggregator's `trusted_signers`.
2. Check that the `public_key_base64` in the Aggregator corresponds to the private key configured on the Agent.
3. Check that the `signer_id` matches (if specified in the trusted signer entry).
4. Verify there is no clock drift beyond 30 seconds between Agent and Aggregator (the `BatchVerifier` enforces a 30-second anti-replay window on `signed_at`).

### If the old key was already removed and the new key is not working

1. Re-add the old public key to the Aggregator's `trusted_signers`.
2. Restart the Aggregator.
3. Revert the Agent to the old key configuration.
4. Restart the Agent.
5. Investigate before retrying.

## Rotating Keys for Multiple Agents

When rotating keys for multiple Agents, follow this sequence:

1. Add all new public keys to the Aggregator's `trusted_signers` in a single configuration update.
2. Restart the Aggregator once.
3. Rotate each Agent individually (Steps 3–4), verifying each one before proceeding to the next.
4. After all Agents are confirmed on new keys, remove all old keys from the Aggregator in a single update.

This minimizes Aggregator restarts and ensures a clean overlap period for all Agents.

## Checklist

- [ ] New Ed25519 keypair generated
- [ ] New `key_id` chosen (distinct from old)
- [ ] New public key added to Aggregator `trusted_signers`
- [ ] Aggregator restarted and accepting both keys
- [ ] Agent config updated with new `key_id` and private key
- [ ] Agent restarted
- [ ] Verified new key is working (logs, API, metrics)
- [ ] Overlap period observed (recommended: 24 hours)
- [ ] Old key removed from Aggregator `trusted_signers`
- [ ] Aggregator restarted
- [ ] Ingestion verified after old key removal
- [ ] Old private key securely destroyed
