use std::{collections::BTreeMap, time::Duration};

use sentinelmesh_core::{RpcEndpointConfig, ValidatorProbeConfig};
use sentinelmesh_solana::SolanaProbe;

#[tokio::test]
async fn observes_devnet_validator() {
    let probe = SolanaProbe::new(Duration::from_secs(10)).expect("probe should build");
    let observation = probe
        .observe_endpoint(
            RpcEndpointConfig {
                id: "devnet".to_owned(),
                label: "devnet".to_owned(),
                provider: "solana-labs".to_owned(),
                region: "devnet".to_owned(),
                rpc_url: "https://api.devnet.solana.com".to_owned(),
                tags: BTreeMap::default(),
            },
            &[],
            &[],
            &ValidatorProbeConfig::default(),
        )
        .await;

    assert_eq!(observation.health.value.as_deref(), Some("ok"));
    assert!(observation.slot.value.is_some());
}
