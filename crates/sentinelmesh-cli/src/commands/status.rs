//! Status command - Check system health

use anyhow::Result;
use colored::Colorize;

pub async fn execute(component: Option<String>, watch: bool) -> Result<()> {
    if watch {
        println!("{}", "👁️  Watch mode enabled (Ctrl+C to exit)".dimmed());
        println!();

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

        loop {
            interval.tick().await;
            print_status(component.as_deref()).await?;
            println!("\n{}", "─".repeat(50));
        }
    } else {
        print_status(component.as_deref()).await
    }
}

async fn print_status(component: Option<&str>) -> Result<()> {
    println!("{}", "📊 SentinelMesh System Status".bold().cyan());
    println!();

    match component {
        Some("agent") | Some("Agent") => print_agent_status().await?,
        Some("aggregator") | Some("Aggregator") => print_aggregator_status().await?,
        Some("storage") | Some("Storage") => print_storage_status().await?,
        _ => {
            print_agent_status().await?;
            println!();
            print_aggregator_status().await?;
            println!();
            print_storage_status().await?;
        }
    }

    Ok(())
}

async fn print_agent_status() -> Result<()> {
    println!("{}", "Agent Status:".bold());

    // TODO: Query actual agent status
    let status = serde_json::json!({
        "status": "running",
        "sentinel_id": "sentinel-scl-01",
        "endpoints": 4,
        "last_batch": "2026-04-30T12:34:56Z",
        "wal_depth": 0,
        "circuit_breakers": {
            "closed": 3,
            "open": 1,
            "half_open": 0
        }
    });

    let status_str = status["status"].as_str().unwrap_or("unknown");
    let status_color = if status_str == "running" {
        "✓".green()
    } else {
        "✗".red()
    };

    println!("  Status: {} {}", status_color, status_str.bold());
    println!(
        "  Sentinel: {}",
        status["sentinel_id"].as_str().unwrap_or("unknown").dimmed()
    );
    println!(
        "  Endpoints: {} ({} closed, {} open, {} half-open)",
        status["endpoints"].as_u64().unwrap_or(0),
        status["circuit_breakers"]["closed"].as_u64().unwrap_or(0),
        status["circuit_breakers"]["open"].as_u64().unwrap_or(0),
        status["circuit_breakers"]["half_open"]
            .as_u64()
            .unwrap_or(0)
    );
    println!("  WAL Depth: {}", status["wal_depth"].as_u64().unwrap_or(0));
    println!(
        "  Last Batch: {}",
        status["last_batch"].as_str().unwrap_or("never").dimmed()
    );

    Ok(())
}

async fn print_aggregator_status() -> Result<()> {
    println!("{}", "Aggregator Status:".bold());

    // TODO: Query actual aggregator status
    let status = serde_json::json!({
        "status": "healthy",
        "active_sentinels": 12,
        "active_endpoints": 48,
        "rpc_consistency_index": 0.97,
        "slot_spread": 3,
        "anomalies_24h": 2
    });

    let status_str = status["status"].as_str().unwrap_or("unknown");
    let status_color = if status_str == "healthy" {
        "✓".green()
    } else {
        "⚠".yellow()
    };

    println!("  Status: {} {}", status_color, status_str.bold());
    println!(
        "  Active Sentinels: {}",
        status["active_sentinels"].as_u64().unwrap_or(0)
    );
    println!(
        "  Active Endpoints: {}",
        status["active_endpoints"].as_u64().unwrap_or(0)
    );
    println!(
        "  RPC Consistency: {:.2}%",
        status["rpc_consistency_index"].as_f64().unwrap_or(0.0) * 100.0
    );
    println!(
        "  Slot Spread: {}",
        status["slot_spread"].as_u64().unwrap_or(0)
    );
    println!(
        "  Anomalies (24h): {}",
        if status["anomalies_24h"].as_u64().unwrap_or(0) > 0 {
            status["anomalies_24h"]
                .as_u64()
                .unwrap_or(0)
                .to_string()
                .yellow()
        } else {
            "0".green()
        }
    );

    Ok(())
}

async fn print_storage_status() -> Result<()> {
    println!("{}", "Storage Status:".bold());

    // TODO: Query actual storage status
    let status = serde_json::json!({
        "kafka": {
            "status": "connected",
            "partitions": 3,
            "lag": 0
        },
        "clickhouse": {
            "status": "connected",
            "tables": ["probe_batches", "sentinelmesh_ingest_kafka"],
            "rows_24h": 152340
        }
    });

    let kafka_status = status["kafka"]["status"].as_str().unwrap_or("unknown");
    let ch_status = status["clickhouse"]["status"].as_str().unwrap_or("unknown");

    println!(
        "  Kafka/Redpanda: {}",
        if kafka_status == "connected" {
            "✓ connected".green()
        } else {
            "✗ disconnected".red()
        }
    );
    println!(
        "    Partitions: {}",
        status["kafka"]["partitions"].as_u64().unwrap_or(0)
    );
    println!(
        "    Consumer Lag: {}",
        status["kafka"]["lag"].as_u64().unwrap_or(0)
    );

    println!(
        "  ClickHouse: {}",
        if ch_status == "connected" {
            "✓ connected".green()
        } else {
            "✗ disconnected".red()
        }
    );
    println!(
        "    Rows (24h): {}",
        status["clickhouse"]["rows_24h"]
            .as_u64()
            .unwrap_or(0)
            .to_string()
            .dimmed()
    );

    Ok(())
}
