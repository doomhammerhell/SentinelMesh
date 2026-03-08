use std::{collections::BTreeMap, time::Duration};

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use sentinelmesh_analysis::MeshStore;
use sentinelmesh_core::{
    BlockhashObservation, EndpointObservation, ProbeBatch, ProbeValue, RpcEndpointConfig,
};
use uuid::Uuid;

fn benchmark_snapshot(c: &mut Criterion) {
    let mut store = MeshStore::new(Duration::from_secs(600), Duration::from_secs(60));
    for batch_index in 0_u64..100 {
        store.ingest(ProbeBatch {
            schema_version: 2,
            batch_id: Uuid::new_v4(),
            sampled_at: Utc::now(),
            sentinel_id: format!("sentinel-{batch_index}"),
            sentinel_location: "benchmark".to_owned(),
            endpoints: (0_u64..8)
                .map(|endpoint_index| EndpointObservation {
                    endpoint: RpcEndpointConfig {
                        id: format!("endpoint-{endpoint_index}"),
                        label: format!("endpoint-{endpoint_index}"),
                        provider: format!("provider-{}", endpoint_index % 3),
                        region: "global".to_owned(),
                        rpc_url: format!("https://endpoint-{endpoint_index}.example.com"),
                        tags: BTreeMap::default(),
                    },
                    overall_latency_ms: 10,
                    health: ProbeValue::ok("ok".to_owned(), 1),
                    slot: ProbeValue::ok(1_000 + batch_index + endpoint_index, 1),
                    block_height: ProbeValue::ok(900 + batch_index + endpoint_index, 1),
                    latest_blockhash: ProbeValue::ok(
                        BlockhashObservation {
                            blockhash: format!("hash-{batch_index}-{endpoint_index}"),
                            last_valid_block_height: 1_200,
                            context_slot: 1_000 + batch_index,
                        },
                        1,
                    ),
                    version: ProbeValue::ok("2.2.1".to_owned(), 1),
                    identity: ProbeValue::empty(),
                    vote_accounts: ProbeValue::empty(),
                    cluster_nodes: ProbeValue::empty(),
                    leader_schedule: ProbeValue::empty(),
                    accounts: Vec::new(),
                    signatures: Vec::new(),
                    probe_errors: Vec::new(),
                })
                .collect(),
        });
    }

    c.bench_function("meshstore_snapshot", |bencher| {
        bencher.iter(|| store.snapshot());
    });
}

criterion_group!(benches, benchmark_snapshot);
criterion_main!(benches);
