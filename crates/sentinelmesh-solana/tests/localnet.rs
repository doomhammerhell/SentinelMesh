use std::{collections::BTreeMap, process::Stdio, time::Duration};

use sentinelmesh_core::{RpcEndpointConfig, ValidatorProbeConfig};
use sentinelmesh_solana::SolanaProbe;

#[tokio::test]
#[ignore = "requires solana-test-validator in PATH"]
async fn observes_local_validator() {
    let mut child = tokio::process::Command::new("solana-test-validator")
        .arg("--reset")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("solana-test-validator should start");

    tokio::time::sleep(Duration::from_secs(8)).await;

    let probe = SolanaProbe::new(Duration::from_secs(5)).expect("probe should build");
    let observation = probe
        .observe_endpoint(
            RpcEndpointConfig {
                id: "localnet".to_owned(),
                label: "localnet".to_owned(),
                provider: "local".to_owned(),
                region: "localhost".to_owned(),
                rpc_url: "http://127.0.0.1:8899".to_owned(),
                tags: BTreeMap::default(),
            },
            &[],
            &[],
            &ValidatorProbeConfig::default(),
        )
        .await;

    let _ = child.start_kill();

    assert_eq!(observation.health.value.as_deref(), Some("ok"));
    assert!(observation.slot.value.is_some());
}
