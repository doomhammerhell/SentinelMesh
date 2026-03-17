use borsh::BorshSerialize;
use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    transaction::Transaction,
};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    rpc_url: String,

    #[arg(long)]
    keypair: String,

    #[arg(long)]
    program_id: String,

    #[arg(long)]
    hash_iterations: u32,
}

#[derive(BorshSerialize, Debug)]
pub struct CanaryInstruction {
    pub hash_iterations: u32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let payer = read_keypair_file(&args.keypair)
        .map_err(|e| anyhow::anyhow!("Failed to read keypair file: {}", e))?;

    let program_id = Pubkey::from_str(&args.program_id)
        .map_err(|e| anyhow::anyhow!("Invalid program ID: {}", e))?;

    let client = RpcClient::new_with_commitment(&args.rpc_url, CommitmentConfig::confirmed());
    let recent_blockhash = client.get_latest_blockhash()?;

    // O Contrato exige target_account mutável onde o owner == program_id.
    // Usaremos uma PDA baseada na public key do payer (Agente) para isolar concorrência.
    let (pda, _bump) = Pubkey::find_program_address(&[&payer.pubkey().to_bytes()], &program_id);

    let instr_data = CanaryInstruction {
        hash_iterations: args.hash_iterations,
    };
    let data = instr_data.try_to_vec()?;

    let instruction = Instruction {
        program_id,
        accounts: vec![AccountMeta::new(pda, false)],
        data,
    };

    let message = Message::new(&[instruction], Some(&payer.pubkey()));
    let mut tx = Transaction::new_unsigned(message);
    tx.try_sign(&[&payer], recent_blockhash)?;

    let signature = client.send_and_confirm_transaction(&tx)?;
    println!("{}", signature);

    Ok(())
}
