# SentinelMesh Production Security & Economic Incentives

This document describes the three major enhancements implemented for production readiness.

## 1. ZK Proof System (`crates/sentinelmesh-zk`)

### Overview
Zero-knowledge proofs using RISC Zero for irrefutable integrity claims.

### Features
- **Guest Program**: Verifies batch integrity in a zkVM
  - Minimum endpoints check (≥2)
  - Merkle tree of observations
  - Sentinel authorization via Merkle proofs
  
- **Host Integration**: 
  - `BatchProver`: Generates proofs for `ProbeBatch`
  - `MerkleTree`: Efficient commitment scheme
  - Configurable dev/prod modes

### Usage
```rust
use sentinelmesh_zk::{BatchProver, ZkConfig};

let prover = BatchProver::new(config, allowed_sentinels)?;
let proof = prover.prove(&batch)?;
assert!(prover.verify(&proof)?);
```

### CLI Commands
```bash
sentinelmesh zk prove --batch batch.json --output proof.json
sentinelmesh zk verify --proof proof.json
sentinelmesh zk status
```

---

## 2. On-Chain Reputation System (`contracts/sentinelmesh-reputation`)

### Overview
Solana program for economic incentives and Byzantine fault tolerance.

### Features
- **Staking**: Minimum 1 SOL to become a verified sentinel
- **Reputation Scoring**: 0-10000 scale based on honest behavior
- **Rewards**: Earned based on consistency score and reputation
- **Slashing**: Penalties for:
  - Invalid batches (50% slash)
  - Consistent divergence (25% slash)
  - Downtime (10% slash)
- **Cooldown**: 7-day withdrawal period

### Data Structures
```rust
pub struct SentinelAccount {
    pub sentinel_id: String,
    pub stake: u64,
    pub reputation_score: u16,  // 0-10000
    pub batches_submitted: u64,
    pub batches_accepted: u64,
    pub slash_count: u16,
    pub rewards_claimed: u64,
}
```

### CLI Commands
```bash
# Initialize sentinel
sentinelmesh reputation init \
  --sentinel-id sentinel-scl-01 \
  --stake 1.5 \
  --rpc https://api.devnet.solana.com \
  --keypair ~/keypair.json

# Check status
sentinelmesh reputation status \
  --address SENTINEL_PUBKEY \
  --rpc https://api.devnet.solana.com

# Submit batch
sentinelmesh reputation submit \
  --batch-hash 0x... \
  --zk-proof 0x... \
  --rpc https://api.devnet.solana.com \
  --keypair ~/keypair.json

# Claim rewards
sentinelmesh reputation claim \
  --rpc https://api.devnet.solana.com \
  --keypair ~/keypair.json

# View leaderboard
sentinelmesh reputation leaderboard \
  --rpc https://api.devnet.solana.com \
  --limit 10
```

---

## 3. Unified CLI Tool (`crates/sentinelmesh-cli`)

### Overview
Single binary for all SentinelMesh operations.

### Commands

#### Initialization
```bash
sentinelmesh init --deployment-type agent --region sa-east-1
sentinelmesh init --deployment-type aggregator
sentinelmesh init --deployment-type full
```

#### Configuration
```bash
sentinelmesh config validate --file agent.yaml
sentinelmesh config example --config-type agent --output agent.yaml
sentinelmesh config show
```

#### Operations
```bash
# Agent management
sentinelmesh agent run --config agent.yaml --detach
sentinelmesh agent stop
sentinelmesh agent status
sentinelmesh agent logs --follow

# Aggregator management
sentinelmesh aggregator deploy --config aggregator.yaml --env prod
sentinelmesh aggregator status --url http://localhost:9480
sentinelmesh aggregator scale --replicas 5

# Canary testing
sentinelmesh canary --endpoint https://api.mainnet.solana.com \
  --network mainnet \
  --amount 0.000001 \
  --keypair ~/canary-keypair.json

# Dashboard
sentinelmesh dashboard --url http://localhost:9480 --open

# System status
sentinelmesh status
sentinelmesh status --component agent --watch

# Chaos engineering
sentinelmesh chaos --scenario network_partition --duration 5m
```

---

## Integration Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Tool                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────────┐   │
│  │  init   │ │  agent  │ │   zk    │ │  reputation     │   │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────────┬────────┘   │
│       │           │           │                │            │
│       └───────────┴───────────┴────────────────┘            │
│                   │                                          │
│                   ▼                                          │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              sentinelmesh-core (models)                 │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐   ┌──────────┐   ┌──────────────┐
        │   ZK     │   │  Agent   │   │  Reputation  │
        │  System  │   │  Runtime │   │   Program    │
        └──────────┘   └──────────┘   └──────────────┘
              │               │               │
              ▼               ▼               ▼
        ┌──────────────────────────────────────────┐
        │           Solana Network                  │
        │  (Reputation + ZK Verification)          │
        └──────────────────────────────────────────┘
```

---

## Security Properties

### ZK Proofs
- **Completeness**: Honest batches generate valid proofs
- **Soundness**: Invalid batches cannot produce valid proofs
- **Zero-Knowledge**: Proof reveals only batch hash, not contents

### Reputation System
- **Sybil Resistance**: Minimum stake requirement
- **Byzantine Tolerance**: Slashing discourages malicious behavior
- **Economic Finality**: Rewards align incentives with network health

### CLI Security
- Configuration validation before deployment
- Secure key handling (no hardcoded credentials)
- TLS/mTLS support for all connections

---

## Next Steps

1. **Deploy Reputation Program**: Deploy to Solana devnet/mainnet
2. **Integrate ZK in Agent**: Add proof generation to agent publish path
3. **Add BFT Consensus**: Implement quorum-based verification
4. **Production Testing**: Chaos engineering and load testing
5. **Documentation**: Operator runbooks and troubleshooting guides
