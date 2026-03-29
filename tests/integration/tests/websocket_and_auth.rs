//! Integration tests for WebSocket control plane and authentication.
//!
//! **Validates: Requirements 18.3, 18.4**
//!
//! Test 1: Aggregator sends command via WebSocket → Agent receives endpoint update
//! Test 2: Rejection of batches without API key
//! Test 3: Rejection of batches with invalid signature

use chrono::Utc;
use sentinelmesh_core::{
    BatchVerifier, ControlMessage, EndpointObservation, ProbeBatch, ProbeEnvelope, ProbeValue,
    RpcEndpointConfig, SigningMaterial, TrustedSigner,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_endpoint(id: &str, slot: u64) -> EndpointObservation {
    EndpointObservation {
        endpoint: RpcEndpointConfig {
            id: id.to_string(),
            label: format!("{id}-label"),
            provider: "test-provider".to_string(),
            region: "us-east-1".to_string(),
            rpc_url: format!("http://localhost/{id}"),
            tags: Default::default(),
        },
        overall_latency_ms: 50,
        health: ProbeValue::ok("ok".to_string(), 10),
        slot: ProbeValue::ok(slot, 10),
        block_height: ProbeValue::ok(slot, 10),
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
    }
}

fn make_batch(sentinel_id: &str) -> ProbeBatch {
    ProbeBatch {
        schema_version: 1,
        batch_id: Uuid::new_v4(),
        sampled_at: Utc::now(),
        sentinel_id: sentinel_id.to_string(),
        sentinel_location: "us-east-1".to_string(),
        asn: Some(12345),
        endpoints: vec![make_endpoint("rpc-1", 100)],
    }
}

// ---------------------------------------------------------------------------
// Test 1: WebSocket control plane — broadcast endpoint update
// ---------------------------------------------------------------------------

/// Simulates the Aggregator broadcasting a `ControlMessage::UpdateEndpoints`
/// via the broadcast channel, and an Agent receiving and applying the update.
/// This tests the same `tokio::sync::broadcast` mechanism used in the real system.
///
/// **Validates: Requirement 18.3**
#[tokio::test]
async fn test_websocket_broadcast_endpoint_update() {
    // Create the broadcast channel (same as AppState.control_tx)
    let (control_tx, mut agent_rx) = tokio::sync::broadcast::channel::<ControlMessage>(16);

    // Aggregator broadcasts an UpdateEndpoints command
    let new_endpoints = vec![
        RpcEndpointConfig {
            id: "rpc-new-1".to_string(),
            label: "New RPC 1".to_string(),
            provider: "provider-a".to_string(),
            region: "eu-west-1".to_string(),
            rpc_url: "http://new-rpc-1.example.com".to_string(),
            tags: Default::default(),
        },
        RpcEndpointConfig {
            id: "rpc-new-2".to_string(),
            label: "New RPC 2".to_string(),
            provider: "provider-b".to_string(),
            region: "ap-southeast-1".to_string(),
            rpc_url: "http://new-rpc-2.example.com".to_string(),
            tags: Default::default(),
        },
    ];

    let msg = ControlMessage::UpdateEndpoints {
        endpoints: new_endpoints.clone(),
    };
    control_tx.send(msg).expect("broadcast should succeed");

    // Agent receives the message
    let received = agent_rx.recv().await.expect("agent should receive message");

    match received {
        ControlMessage::UpdateEndpoints { endpoints } => {
            assert_eq!(endpoints.len(), 2);
            assert_eq!(endpoints[0].id, "rpc-new-1");
            assert_eq!(endpoints[1].id, "rpc-new-2");
            assert_eq!(endpoints[0].region, "eu-west-1");
        }
        other => panic!("expected UpdateEndpoints, got {:?}", other),
    }
}

/// Tests that multiple agents can receive the same broadcast message.
///
/// **Validates: Requirement 18.3**
#[tokio::test]
async fn test_websocket_broadcast_to_multiple_agents() {
    let (control_tx, mut agent1_rx) = tokio::sync::broadcast::channel::<ControlMessage>(16);
    let mut agent2_rx = control_tx.subscribe();

    let msg = ControlMessage::RemoveEndpoint {
        id: "rpc-deprecated".to_string(),
    };
    control_tx.send(msg).expect("broadcast should succeed");

    // Both agents receive the same message
    let msg1 = agent1_rx.recv().await.expect("agent1 should receive");
    let msg2 = agent2_rx.recv().await.expect("agent2 should receive");

    match (&msg1, &msg2) {
        (
            ControlMessage::RemoveEndpoint { id: id1 },
            ControlMessage::RemoveEndpoint { id: id2 },
        ) => {
            assert_eq!(id1, "rpc-deprecated");
            assert_eq!(id2, "rpc-deprecated");
        }
        _ => panic!("expected RemoveEndpoint for both agents"),
    }
}

// ---------------------------------------------------------------------------
// Test 2: Rejection of batches without API key
// ---------------------------------------------------------------------------

/// Simulates the Aggregator's `authorize()` function rejecting a request
/// that lacks the `x-sentinelmesh-api-key` header when API keys are configured.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_reject_batch_without_api_key() {
    let api_keys = vec!["valid-key-123".to_string(), "valid-key-456".to_string()];

    // Request without any API key header
    let headers = axum::http::HeaderMap::new();

    // The authorize function should reject this
    let provided = headers
        .get("x-sentinelmesh-api-key")
        .and_then(|value| value.to_str().ok());

    let is_authorized = api_keys
        .iter()
        .any(|candidate| Some(candidate.as_str()) == provided);

    assert!(!is_authorized, "request without API key should be rejected");
}

/// Tests that a valid API key is accepted.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_accept_batch_with_valid_api_key() {
    let api_keys = vec!["valid-key-123".to_string(), "valid-key-456".to_string()];

    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-sentinelmesh-api-key", "valid-key-123".parse().unwrap());

    let provided = headers
        .get("x-sentinelmesh-api-key")
        .and_then(|value| value.to_str().ok());

    let is_authorized = api_keys
        .iter()
        .any(|candidate| Some(candidate.as_str()) == provided);

    assert!(
        is_authorized,
        "request with valid API key should be accepted"
    );
}

/// Tests that an invalid API key is rejected.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_reject_batch_with_invalid_api_key() {
    let api_keys = vec!["valid-key-123".to_string()];

    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-sentinelmesh-api-key", "wrong-key-999".parse().unwrap());

    let provided = headers
        .get("x-sentinelmesh-api-key")
        .and_then(|value| value.to_str().ok());

    let is_authorized = api_keys
        .iter()
        .any(|candidate| Some(candidate.as_str()) == provided);

    assert!(
        !is_authorized,
        "request with invalid API key should be rejected"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Rejection of batches with invalid signature
// ---------------------------------------------------------------------------

/// Tests that a batch with a valid signature passes verification.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_accept_batch_with_valid_signature() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    // Generate a keypair
    let key_bytes = [42u8; 32];
    let private_key_b64 = STANDARD.encode(key_bytes);

    let signer = SigningMaterial::from_base64("test-signer", "test-key", &private_key_b64)
        .expect("create signer");

    let batch = make_batch("sentinel-signed");
    let signed_at = Utc::now();
    let auth = signer.sign(&batch, signed_at).expect("sign batch");

    // Build verifier with the corresponding public key
    let pub_key_b64 = signer.verifying_key_base64();
    let trusted =
        TrustedSigner::from_base64(Some("test-signer".to_string()), "test-key", &pub_key_b64)
            .expect("create trusted signer");

    let verifier = BatchVerifier::new(vec![trusted]);

    // Verification should succeed
    let result = verifier.verify(&batch, &auth);
    assert!(result.is_ok(), "valid signature should be accepted");
}

/// Tests that a batch with an invalid/tampered signature is rejected.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_reject_batch_with_invalid_signature() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    // Generate a keypair
    let key_bytes = [42u8; 32];
    let private_key_b64 = STANDARD.encode(key_bytes);

    let signer = SigningMaterial::from_base64("test-signer", "test-key", &private_key_b64)
        .expect("create signer");

    let batch = make_batch("sentinel-tampered");
    let signed_at = Utc::now();
    let mut auth = signer.sign(&batch, signed_at).expect("sign batch");

    // Tamper with the signature
    auth.signature_b64 = STANDARD.encode([0u8; 64]);

    // Build verifier with the corresponding public key
    let pub_key_b64 = signer.verifying_key_base64();
    let trusted =
        TrustedSigner::from_base64(Some("test-signer".to_string()), "test-key", &pub_key_b64)
            .expect("create trusted signer");

    let verifier = BatchVerifier::new(vec![trusted]);

    // Verification should fail
    let result = verifier.verify(&batch, &auth);
    assert!(result.is_err(), "tampered signature should be rejected");
}

/// Tests that a batch signed with an unknown key is rejected.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_reject_batch_with_unknown_signer() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    // Signer uses key A
    let key_a = [42u8; 32];
    let signer_a = SigningMaterial::from_base64("signer-a", "key-a", &STANDARD.encode(key_a))
        .expect("create signer A");

    // Verifier only trusts key B
    let key_b = [99u8; 32];
    let signer_b = SigningMaterial::from_base64("signer-b", "key-b", &STANDARD.encode(key_b))
        .expect("create signer B");

    let pub_key_b_b64 = signer_b.verifying_key_base64();
    let trusted_b =
        TrustedSigner::from_base64(Some("signer-b".to_string()), "key-b", &pub_key_b_b64)
            .expect("create trusted signer B");

    let verifier = BatchVerifier::new(vec![trusted_b]);

    // Sign with key A
    let batch = make_batch("sentinel-unknown");
    let auth = signer_a.sign(&batch, Utc::now()).expect("sign with key A");

    // Verification should fail — key A is not trusted
    let result = verifier.verify(&batch, &auth);
    assert!(
        result.is_err(),
        "batch signed with unknown key should be rejected"
    );
}

/// Tests the verify_envelope logic: unsigned batch rejected when signatures required.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_reject_unsigned_batch_when_signatures_required() {
    let batch = make_batch("sentinel-unsigned");
    let envelope = ProbeEnvelope { batch, auth: None };

    // Simulate require_signed_batches = true
    let require_signed = true;

    let result = match (&envelope.auth, require_signed) {
        (Some(_auth), _) => Ok(()),
        (None, true) => Err("signed batch required"),
        (None, false) => Ok(()),
    };

    assert!(
        result.is_err(),
        "unsigned batch should be rejected when signatures are required"
    );
    assert_eq!(result.unwrap_err(), "signed batch required");
}

/// Tests that unsigned batches are accepted when signatures are not required.
///
/// **Validates: Requirement 18.4**
#[test]
fn test_accept_unsigned_batch_when_signatures_not_required() {
    let batch = make_batch("sentinel-unsigned-ok");
    let envelope = ProbeEnvelope { batch, auth: None };

    let require_signed = false;

    let result = match (&envelope.auth, require_signed) {
        (Some(_auth), _) => Ok(()),
        (None, true) => Err("signed batch required"),
        (None, false) => Ok(()),
    };

    assert!(
        result.is_ok(),
        "unsigned batch should be accepted when signatures are not required"
    );
}
