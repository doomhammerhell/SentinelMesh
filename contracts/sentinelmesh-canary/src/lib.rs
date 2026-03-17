use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    clock::Clock,
    entrypoint,
    entrypoint::ProgramResult,
    hash::hash,
    program_error::ProgramError,
    pubkey::Pubkey,
    sysvar::Sysvar,
};

entrypoint!(process_instruction);

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct CanaryInstruction {
    /// Number of sequential SHA-256 hashes to perform, simulating DeFi routing loops.
    pub hash_iterations: u32,
}

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    let instruction = CanaryInstruction::try_from_slice(instruction_data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let account_info_iter = &mut accounts.iter();
    let target_account = next_account_info(account_info_iter)?;

    if target_account.owner != program_id {
        return Err(ProgramError::IncorrectProgramId);
    }

    // Heavy computation simulation (Burn Compute Units)
    // Ensures this cannot be trivially skipped or censored by MEV Builders without penalty.
    let mut current_hash = [0u8; 32];
    for i in 0..instruction.hash_iterations {
        let mut data = Vec::with_capacity(36);
        data.extend_from_slice(&current_hash);
        data.extend_from_slice(&i.to_le_bytes());
        current_hash = hash(&data).to_bytes();
    }

    let clock = Clock::get()?;
    let current_timestamp = clock.unix_timestamp;

    // Write to account data to ensure state mutation (Anti-Read-Only caching)
    let mut account_data = target_account.try_borrow_mut_data()?;
    if account_data.len() >= 40 {
        account_data[0..8].copy_from_slice(&current_timestamp.to_le_bytes());
        account_data[8..40].copy_from_slice(&current_hash);
    }

    Ok(())
}
