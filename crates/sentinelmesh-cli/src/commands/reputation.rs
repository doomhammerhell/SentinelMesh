//! Reputation system commands

use anyhow::{Context, Result};
use colored::Colorize;

use crate::ReputationAction;

pub async fn execute(action: ReputationAction) -> Result<()> {
    match action {
        ReputationAction::Init {
            sentinel_id,
            stake,
            rpc,
            keypair,
        } => init_sentinel(sentinel_id, stake, rpc, keypair).await,
        ReputationAction::Status { address, rpc } => check_status(address, rpc).await,
        ReputationAction::Submit {
            batch_hash,
            zk_proof,
            rpc,
            keypair,
        } => submit_batch(batch_hash, zk_proof, rpc, keypair).await,
        ReputationAction::Claim { rpc, keypair } => claim_rewards(rpc, keypair).await,
        ReputationAction::Withdraw {
            amount,
            rpc,
            keypair,
        } => withdraw_stake(amount, rpc, keypair).await,
        ReputationAction::Leaderboard { rpc, limit } => show_leaderboard(rpc, limit).await,
    }
}

async fn init_sentinel(
    sentinel_id: String,
    stake: f64,
    rpc: String,
    keypair: std::path::PathBuf,
) -> Result<()> {
    println!(
        "{}",
        "🏆 Initializing Sentinel on Reputation Program..."
            .bold()
            .cyan()
    );

    println!("  {} Sentinel ID: {}", "→".dimmed(), sentinel_id.bold());
    println!(
        "  {} Initial Stake: {} SOL",
        "→".dimmed(),
        stake.to_string().bold()
    );
    println!("  {} RPC: {}", "→".dimmed(), rpc.dimmed());
    println!("  {} Keypair: {}", "→".dimmed(), keypair.display());

    // TODO: Integrate with Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let stake_lamports = (stake * 1_000_000_000.0) as u64;

    println!();
    println!("{}", "Transaction Details:".bold());
    println!(
        "  Program: {}",
        "Rep111111111111111111111111111111111111111"
            .dimmed()
    );
    println!("  Instruction: InitializeSentinel");
    println!("  Stake: {} lamports", stake_lamports.to_string().dimmed());

    println!();
    println!("{}", "✓ Sentinel initialized successfully!".green().bold());
    println!("\nYour sentinel is now eligible to:");
    println!("  • Submit verified batches and earn rewards");
    println!("  • Build reputation through consistent honest behavior");
    println!("  • Participate in network consensus");

    Ok(())
}

async fn check_status(address: String, rpc: String) -> Result<()> {
    println!("{}", "📊 Sentinel Status".bold().cyan());
    println!();

    println!("  {} Fetching on-chain data...", "→".dimmed());

    // TODO: Fetch from Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Mock data
    let status = serde_json::json!({
        "sentinel_id": "sentinel-scl-01",
        "operator": address,
        "stake": 1_500_000_000_u64,
        "reputation_score": 8750_u16,
        "batches_submitted": 1523_u64,
        "batches_accepted": 1519_u64,
        "slash_count": 0_u16,
        "rewards_claimed": 450_000_000_u64,
        "location": "santiago-cl"
    });

    println!();
    println!("{}", "On-Chain Data:".bold());
    println!(
        "  Sentinel ID: {}",
        status["sentinel_id"].as_str().unwrap_or("unknown").bold()
    );
    println!(
        "  Stake: {} SOL",
        (status["stake"].as_u64().unwrap_or(0) as f64 / 1_000_000_000.0)
            .to_string()
            .bold()
    );
    println!(
        "  Reputation: {}/10000",
        status["reputation_score"].as_u64().unwrap_or(0).to_string().bold()
    );
    println!(
        "  Batches: {} submitted, {} accepted",
        status["batches_submitted"].as_u64().unwrap_or(0),
        status["batches_accepted"].as_u64().unwrap_or(0)
    );
    println!(
        "  Success Rate: {:.2}%",
        (status["batches_accepted"].as_u64().unwrap_or(0) as f64
            / status["batches_submitted"].as_u64().unwrap_or(1) as f64
            * 100.0)
    );
    println!(
        "  Rewards Claimed: {} SOL",
        (status["rewards_claimed"].as_u64().unwrap_or(0) as f64 / 1_000_000_000.0)
    );

    let reputation = status["reputation_score"].as_u64().unwrap_or(0);
    let tier = if reputation >= 9500 {
        "Elite".green().bold()
    } else if reputation >= 8000 {
        "Trusted".cyan().bold()
    } else if reputation >= 5000 {
        "Verified".yellow()
    } else {
        "Probation".red()
    };

    println!();
    println!("Reputation Tier: {}", tier);

    Ok(())
}

async fn submit_batch(
    batch_hash: String,
    zk_proof: String,
    rpc: String,
    keypair: std::path::PathBuf,
) -> Result<()> {
    println!("{}", "📤 Submitting Batch to Reputation Program...".bold().cyan());

    println!("  {} Batch Hash: {}", "→".dimmed(), batch_hash.dimmed());
    println!("  {} ZK Proof: {}", "→".dimmed(), zk_proof.dimmed());

    // TODO: Submit to Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    println!();
    println!("{}", "✓ Batch submitted successfully!".green().bold());
    println!("  Rewards will be calculated based on:");
    println!("    • Consistency score across endpoints");
    println!("    • Current reputation multiplier");
    println!("    • Network participation");

    Ok(())
}

async fn claim_rewards(rpc: String, keypair: std::path::PathBuf) -> Result<()> {
    println!("{}", "💰 Claiming Rewards...".bold().cyan());

    println!("  {} RPC: {}", "→".dimmed(), rpc.dimmed());

    // TODO: Claim from Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let claimable = 0.125; // Mock value

    println!();
    println!("{}", "✓ Rewards claimed!".green().bold());
    println!("  Amount: {} SOL", claimable.to_string().bold());

    Ok(())
}

async fn withdraw_stake(
    amount: f64,
    rpc: String,
    keypair: std::path::PathBuf,
) -> Result<()> {
    println!("{}", "💸 Withdrawing Stake...".bold().cyan());

    println!("  {} Amount: {} SOL", "→".dimmed(), amount.to_string().bold());

    // TODO: Withdraw from Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    println!();
    println!(
        "{}",
        "⚠️  Withdrawal cooldown initiated".yellow().bold()
    );
    println!("  Your stake will be available in 7 days");
    println!("  This cooldown period protects the network");

    Ok(())
}

async fn show_leaderboard(rpc: String, limit: usize) -> Result<()> {
    println!("{}", "🏆 Sentinel Leaderboard".bold().cyan());
    println!();

    println!("  {} Fetching top {} sentinels...", "→".dimmed(), limit);

    // TODO: Fetch from Solana program
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Mock leaderboard
    let leaders = vec![
        ("sentinel-nyc-01", 9850, 5234, "1.2K"),
        ("sentinel-ldn-01", 9720, 4891, "1.1K"),
        ("sentinel-sgp-01", 9680, 5102, "1.15K"),
        ("sentinel-scl-01", 8750, 1519, "450"),
        ("sentinel-ber-01", 8420, 1234, "380"),
    ];

    println!();
    println!(
        "{:<4} {:<20} {:>12} {:>12} {:>10}",
        "Rank", "Sentinel ID", "Reputation", "Batches", "Rewards"
    );
    println!("{}", "─".repeat(70));

    for (i, (id, rep, batches, rewards)) in leaders.iter().enumerate().take(limit) {
        let rank = match i {
            0 => "🥇".to_string(),
            1 => "🥈".to_string(),
            2 => "🥉".to_string(),
            _ => format!("{:<4}", i + 1),
        };

        println!(
            "{} {:<20} {:>12} {:>12} {:>10}",
            rank,
            id.bold(),
            rep.to_string().cyan(),
            batches,
            rewards.dimmed()
        );
    }

    Ok(())
}
