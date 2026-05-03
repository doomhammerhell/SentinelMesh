use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer, read_keypair_file},
    transaction::Transaction,
};
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{error, info};

use sentinelmesh_core::config::StateCommitterConfig;
use sentinelmesh_storage::StorageEngine;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub async fn start_committer_loop(config: StateCommitterConfig, storage: Arc<StorageEngine>) {
    if !config.enabled {
        info!("State Committer is disabled.");
        return;
    }

    info!("State Committer started. Interval: {:?}", config.interval);

    let keypair = match read_keypair_file(&config.keypair_path) {
        Ok(kp) => kp,
        Err(e) => {
            error!(
                "Failed to load committer keypair from {}: {}",
                config.keypair_path, e
            );
            return;
        }
    };

    let rpc_client = RpcClient::new_with_commitment(config.rpc_url, CommitmentConfig::confirmed());
    let memo_program_id = MEMO_PROGRAM_ID.parse::<Pubkey>().unwrap();

    loop {
        sleep(config.interval).await;
        info!("Running state commitment cycle...");

        if let Err(e) = commit_cycle(&storage, &rpc_client, &keypair, &memo_program_id).await {
            error!("State Commitment cycle failed: {}", e);
        }
    }
}

async fn commit_cycle(
    storage: &StorageEngine,
    rpc_client: &RpcClient,
    keypair: &Keypair,
    memo_program_id: &Pubkey,
) -> Result<()> {
    // For the Grant Submission, we compute the Merkle Root of the active agent whitelist
    // and anchor it to Solana for irrefutability.
    let whitelist_ids: Vec<String> = storage.list_agents().await.unwrap_or_default();
    let root_hex = if !whitelist_ids.is_empty() {
        let tree = sentinelmesh_core::zk::merkle::WhitelistMerkleTree::new(&whitelist_ids)?;
        hex::encode(tree.root())
    } else {
        let mut hasher = Sha256::new();
        hasher.update(b"SentinelMesh-Empty-State");
        hex::encode(hasher.finalize())
    };

    let memo_message = format!("SentinelMesh Root: {}", root_hex);

    let instruction = Instruction::new_with_bincode(*memo_program_id, &memo_message, vec![]);

    let recent_blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .context("failed to get latest blockhash")?;

    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&keypair.pubkey()),
        &[keypair],
        recent_blockhash,
    );

    let signature = rpc_client
        .send_and_confirm_transaction(&transaction)
        .await
        .context("failed to send memo")?;

    info!(
        "State Commitment successful. Root: {}. Signature: {}",
        root_hex, signature
    );

    Ok(())
}
