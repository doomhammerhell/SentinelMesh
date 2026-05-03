//! Initialize command - Creates new sentinel configuration

use anyhow::{Context, Result};
use colored::Colorize;
use std::fs;
use std::path::PathBuf;

use crate::DeploymentType;

pub async fn execute(
    deployment_type: DeploymentType,
    region: Option<String>,
    output: PathBuf,
) -> Result<()> {
    println!("{}", "🚀 Initializing SentinelMesh deployment...".bold().cyan());
    
    let region = region.unwrap_or_else(|| {
        // Try to detect region from environment
        std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("GCP_REGION"))
            .or_else(|_| std::env::var("REGION"))
            .unwrap_or_else(|_| "unknown-region".to_string())
    });
    
    match deployment_type {
        DeploymentType::Agent => init_agent(&region, &output).await?,
        DeploymentType::Aggregator => init_aggregator(&region, &output).await?,
        DeploymentType::Full => init_full(&region, &output).await?,
    }
    
    println!("{}", "✅ Initialization complete!".bold().green());
    println!("\nNext steps:");
    println!("  1. Review the generated configuration files");
    println!("  2. Configure your RPC endpoints");
    println!("  3. Run: sentinelmesh config validate --file {}/agent.yaml", output.display());
    
    Ok(())
}

async fn init_agent(region: &str, output: &PathBuf) -> Result<()> {
    println!("  {} Creating agent configuration...", "→".dimmed());
    
    fs::create_dir_all(output)?;
    
    let sentinel_id = format!("sentinel-{}", region.replace('_', "-"));
    
    let config = format!(r#"log_filter: info,sentinelmesh_agent=debug

runtime:
  sentinel_id: {}
  location: {}
  sample_interval: 15s
  request_timeout: 5s
  max_concurrency: 16
  data_dir: ./data
  wal_max_entries: 10000
  circuit_breaker:
    failure_threshold: 3
    recovery_interval_secs: 60

publish:
  ingestion_url: http://127.0.0.1:9480/v1/ingest
  timeout: 5s
  auth:
    api_key: CHANGE_ME_IN_PRODUCTION
    # Uncomment to enable ZK proofs:
    # signing:
    #   type: memory
    #   signer_id: {}
    #   key_id: key-2026-04
    #   private_key_base64: REPLACE_WITH_BASE64_KEY

admin:
  bind_address: 127.0.0.1:9490

canary:
  enabled: false
  interval: 45s
  cooldown_seconds: 3600
  mode:
    type: disabled

validator_probes:
  include_identity: true
  include_vote_accounts: true
  include_cluster_nodes: true
  include_leader_schedule: true

endpoints: []

tracked_accounts: []

tracked_signatures: []
"#, sentinel_id, region, sentinel_id);
    
    let config_path = output.join("agent.yaml");
    fs::write(&config_path, config)
        .with_context(|| format!("Failed to write {}", config_path.display()))?;
    
    println!("  {} Created {}", "✓".green(), config_path.display());
    
    // Create .gitignore
    let gitignore = r#"# SentinelMesh Agent
/data/
*.log
*.key
*.json
.DS_Store
"#;
    let gitignore_path = output.join(".gitignore");
    fs::write(&gitignore_path, gitignore)?;
    
    println!("  {} Created {}", "✓".green(), gitignore_path.display());
    
    Ok(())
}

async fn init_aggregator(region: &str, output: &PathBuf) -> Result<()> {
    println!("  {} Creating aggregator configuration...", "→".dimmed());
    
    fs::create_dir_all(output)?;
    
    let config = format!(r#"log_filter: info,sentinelmesh_aggregator=debug

server:
  bind_address: 0.0.0.0:9480

ingestion:
  auth:
    api_keys:
      - CHANGE_ME_IN_PRODUCTION
    trusted_signers: []
    require_signed_batches: false
  max_batch_bytes: 2097152

analysis:
  retention: 10m
  freshness_window: 60s

storage:
  kafka:
    brokers:
      - 127.0.0.1:9092
    topic: sentinelmesh_ingest
    partitions: 3
  clickhouse:
    url: http://127.0.0.1:8123
    user: sentinelmesh
    password: sentinelmesh
    database: sentinelmesh
    refresh_interval: 10s
    max_refresh_interval_secs: 60

security:
  server_cert_path:
  server_key_path:
  trusted_client_ca_path:
  require_client_cert: false

# Reputation program (Solana devnet)
reputation:
  program_id: "Rep111111111111111111111111111111111111111"
  rpc_url: https://api.devnet.solana.com

# Optional: Enable alerts
# alerts:
#   min_severity: warning
#   webhooks:
#     - url: "https://hooks.slack.com/services/..."
"#);
    
    let config_path = output.join("aggregator.yaml");
    fs::write(&config_path, config)?;
    
    println!("  {} Created {}", "✓".green(), config_path.display());
    
    Ok(())
}

async fn init_full(region: &str, output: &PathBuf) -> Result<()> {
    println!("{}", "Setting up full deployment...".dimmed());
    
    // Create agent config
    let agent_dir = output.join("agent");
    init_agent(region, &agent_dir).await?;
    
    // Create aggregator config
    let aggregator_dir = output.join("aggregator");
    init_aggregator(region, &aggregator_dir).await?;
    
    // Create docker-compose for local dev
    let compose = r#"version: '3.8'

services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v23.3.10
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --overprovisioned
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:29092
      - --advertise-kafka-addr PLAINTEXT://redpanda:29092
    ports:
      - "9092:9092"
      - "29092:29092"

  clickhouse:
    image: clickhouse/clickhouse-server:24.3
    ports:
      - "8123:8123"
    environment:
      CLICKHOUSE_USER: sentinelmesh
      CLICKHOUSE_PASSWORD: sentinelmesh
      CLICKHOUSE_DB: sentinelmesh

  aggregator:
    build:
      context: ..
      dockerfile: deploy/docker/Dockerfile.aggregator
    depends_on:
      - redpanda
      - clickhouse
    ports:
      - "9480:9480"
    volumes:
      - ./aggregator/aggregator.yaml:/etc/sentinelmesh/aggregator.yaml:ro

  agent:
    build:
      context: ..
      dockerfile: deploy/docker/Dockerfile.agent
    depends_on:
      - aggregator
    volumes:
      - ./agent/agent.yaml:/etc/sentinelmesh/agent.yaml:ro
"#;
    
    let compose_path = output.join("docker-compose.yml");
    fs::write(&compose_path, compose)?;
    
    println!("  {} Created {}", "✓".green(), compose_path.display());
    
    Ok(())
}
