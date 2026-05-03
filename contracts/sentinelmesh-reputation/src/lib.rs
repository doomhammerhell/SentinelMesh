//! SentinelMesh Reputation Program
//! 
//! On-chain reputation and economic incentives for SentinelMesh agents.
//! 
//! Features:
//! - Stake tokens to become a verified sentinel
//! - Earn reputation based on honest probe submissions
//! - Slashing for Byzantine behavior
//! - Reputation decay for inactive agents

use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    clock::Clock,
    entrypoint,
    entrypoint::ProgramResult,
    msg,
    program::invoke,
    program_error::ProgramError,
    pubkey::Pubkey,
    sysvar::Sysvar,
    system_instruction,
    rent::Rent,
};
use solana_security_txt::security_txt;

entrypoint!(process_instruction);

security_txt! {
    name: "SentinelMesh Reputation",
    project_url: "https://github.com/doomhammerhell/SentinelMesh",
    contacts: "email:mayckonrlyeh@gmail.com,link:https://github.com/doomhammerhell/SentinelMesh/blob/main/SECURITY.md",
    policy: "https://github.com/doomhammerhell/SentinelMesh/blob/main/SECURITY.md",
    preferred_languages: "en,pt",
    source_code: "https://github.com/doomhammerhell/SentinelMesh/tree/main/contracts/sentinelmesh-reputation",
    auditors: "None"
}

/// Program instructions
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReputationInstruction {
    /// Initialize a new sentinel account with initial stake
    /// Accounts:
    /// 0. [signer] The sentinel operator (pays for account creation)
    /// 1. [writable] The sentinel account to create (PDA)
    /// 2. [] System program
    InitializeSentinel {
        sentinel_id: String,
        initial_stake: u64, // In lamports
    },
    
    /// Submit a verified batch and earn reputation
    /// Accounts:
    /// 0. [signer] The sentinel operator
    /// 1. [writable] The sentinel account (PDA)
    /// 2. [writable] The reputation pool account
    /// 3. [] Clock sysvar
    SubmitBatch {
        batch_hash: [u8; 32],
        zk_proof_hash: [u8; 32],
        endpoint_count: u16,
        consistency_score: u16, // 0-10000 (0-100% with 2 decimals)
    },
    
    /// Slash a sentinel for Byzantine behavior
    /// Only callable by authorized slashers (governance)
    /// Accounts:
    /// 0. [signer] The authorized slasher
    /// 1. [writable] The sentinel account to slash (PDA)
    /// 2. [writable] The treasury account
    SlashSentinel {
        reason: SlashReason,
        amount: u64,
    },
    
    /// Withdraw stake (with cooldown period)
    /// Accounts:
    /// 0. [signer] The sentinel operator
    /// 1. [writable] The sentinel account (PDA)
    /// 2. [writable] The destination account
    /// 3. [] Clock sysvar
    WithdrawStake {
        amount: u64,
    },
    
    /// Claim rewards based on reputation
    /// Accounts:
    /// 0. [signer] The sentinel operator
    /// 1. [writable] The sentinel account (PDA)
    /// 2. [writable] The rewards vault
    /// 3. [writable] The destination account
    ClaimRewards,
    
    /// Update sentinel metadata
    /// Accounts:
    /// 0. [signer] The sentinel operator
    /// 1. [writable] The sentinel account (PDA)
    UpdateMetadata {
        location: Option<String>,
        endpoint_url: Option<String>,
    },
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum SlashReason {
    InvalidBatch,
    ConsistentDivergence,
    Downtime,
    Other { code: u16, description: String },
}

/// Sentinel account data
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SentinelAccount {
    /// Version for migrations
    pub version: u8,
    /// The sentinel ID (e.g., "sentinel-scl-01")
    pub sentinel_id: String,
    /// Operator's public key
    pub operator: Pubkey,
    /// Current staked amount in lamports
    pub stake: u64,
    /// Reputation score (0-10000, higher is better)
    pub reputation_score: u16,
    /// Total batches submitted
    pub batches_submitted: u64,
    /// Total batches accepted (verified)
    pub batches_accepted: u64,
    /// Last submission timestamp
    pub last_submission: i64,
    /// Consecutive failed submissions
    pub consecutive_failures: u8,
    /// Total slashes received
    pub slash_count: u16,
    /// Total rewards claimed
    pub rewards_claimed: u64,
    /// Withdrawal cooldown end timestamp
    pub withdrawal_cooldown: i64,
    /// Geographic location
    pub location: String,
    /// PDA bump seed
    pub bump: u8,
}

/// Reputation pool data (global state)
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct ReputationPool {
    pub version: u8,
    /// Total sentinels registered
    pub total_sentinels: u32,
    /// Total batches processed
    pub total_batches: u64,
    /// Total stake locked
    pub total_stake: u64,
    /// Rewards distributed
    pub total_rewards_distributed: u64,
    /// Total slashed amount
    pub total_slashed: u64,
    /// Authorized slashers (governance)
    pub authorized_slashers: Vec<Pubkey>,
}

// Constants
const MIN_STAKE: u64 = 1_000_000_000; // 1 SOL minimum stake
const REPUTATION_DECAY_RATE: u16 = 10; // Reputation decay per day of inactivity
const WITHDRAWAL_COOLDOWN: i64 = 7 * 24 * 60 * 60; // 7 days
const MAX_CONSECUTIVE_FAILURES: u8 = 5;
const SLASH_PERCENTAGE_INVALID: u64 = 50; // 50% slash for invalid batches
const SLASH_PERCENTAGE_DIVERGENCE: u64 = 25; // 25% for consistent divergence

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    let instruction = ReputationInstruction::try_from_slice(instruction_data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    
    match instruction {
        ReputationInstruction::InitializeSentinel { sentinel_id, initial_stake } => {
            process_initialize_sentinel(program_id, accounts, sentinel_id, initial_stake)
        }
        ReputationInstruction::SubmitBatch { batch_hash, zk_proof_hash, endpoint_count, consistency_score } => {
            process_submit_batch(program_id, accounts, batch_hash, zk_proof_hash, endpoint_count, consistency_score)
        }
        ReputationInstruction::SlashSentinel { reason, amount } => {
            process_slash_sentinel(program_id, accounts, reason, amount)
        }
        ReputationInstruction::WithdrawStake { amount } => {
            process_withdraw_stake(program_id, accounts, amount)
        }
        ReputationInstruction::ClaimRewards => {
            process_claim_rewards(program_id, accounts)
        }
        ReputationInstruction::UpdateMetadata { location, endpoint_url } => {
            process_update_metadata(program_id, accounts, location, endpoint_url)
        }
    }
}

fn process_initialize_sentinel(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    sentinel_id: String,
    initial_stake: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let operator = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;
    
    if !operator.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    if initial_stake < MIN_STAKE {
        msg!("Insufficient stake: {} < {}", initial_stake, MIN_STAKE);
        return Err(ProgramError::InsufficientFunds);
    }
    
    // Derive PDA for sentinel account
    let (expected_pda, bump) = Pubkey::find_program_address(
        &[br"sentinel", sentinel_id.as_bytes(), operator.key.as_ref()],
        program_id,
    );
    
    if expected_pda != *sentinel_account.key {
        return Err(ProgramError::InvalidAccountData);
    }
    
    // Create account
    let rent = Rent::get()?;
    let space = std::mem::size_of::<SentinelAccount>();
    let lamports = rent.minimum_balance(space) + initial_stake;
    
    invoke(
        &system_instruction::create_account(
            operator.key,
            sentinel_account.key,
            lamports,
            space as u64,
            program_id,
        ),
        &[operator.clone(), sentinel_account.clone(), system_program.clone()],
    )?;
    
    // Initialize sentinel data
    let sentinel_data = SentinelAccount {
        version: 1,
        sentinel_id,
        operator: *operator.key,
        stake: initial_stake,
        reputation_score: 5000, // Start at 50%
        batches_submitted: 0,
        batches_accepted: 0,
        last_submission: Clock::get()?.unix_timestamp,
        consecutive_failures: 0,
        slash_count: 0,
        rewards_claimed: 0,
        withdrawal_cooldown: 0,
        location: String::new(),
        bump,
    };
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    msg!("Sentinel initialized with stake: {}", initial_stake);
    Ok(())
}

fn process_submit_batch(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    batch_hash: [u8; 32],
    zk_proof_hash: [u8; 32],
    endpoint_count: u16,
    consistency_score: u16,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let operator = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    let _reputation_pool = next_account_info(account_info_iter)?;
    let clock_info = next_account_info(account_info_iter)?;
    
    if !operator.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    let mut sentinel_data = SentinelAccount::try_from_slice(&sentinel_account.data.borrow())?;
    
    // Verify operator owns this sentinel
    if sentinel_data.operator != *operator.key {
        return Err(ProgramError::IllegalOwner);
    }
    
    // Update stats
    sentinel_data.batches_submitted += 1;
    sentinel_data.last_submission = Clock::get()?.unix_timestamp;
    
    // Calculate reputation delta based on consistency score
    let reputation_delta = if consistency_score >= 9500 {
        // Excellent consistency: +50 reputation
        50
    } else if consistency_score >= 8000 {
        // Good consistency: +20 reputation
        20
    } else if consistency_score >= 5000 {
        // Acceptable: +5 reputation
        5
    } else {
        // Poor consistency: -10 reputation
        sentinel_data.consecutive_failures += 1;
        -10
    };
    
    // Apply reputation change with bounds
    let new_reputation = (sentinel_data.reputation_score as i32 + reputation_delta)
        .clamp(0, 10000) as u16;
    sentinel_data.reputation_score = new_reputation;
    
    // Reset consecutive failures on good submission
    if consistency_score >= 8000 {
        sentinel_data.consecutive_failures = 0;
        sentinel_data.batches_accepted += 1;
    }
    
    // Check for excessive failures
    if sentinel_data.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
        msg!("Warning: Sentinel has {} consecutive failures", sentinel_data.consecutive_failures);
    }
    
    // Calculate rewards based on reputation and endpoint count
    let base_reward = 1000u64; // Base reward in lamports
    let reputation_multiplier = sentinel_data.reputation_score as u64;
    let endpoint_bonus = endpoint_count as u64 * 100;
    let reward = (base_reward * reputation_multiplier / 10000) + endpoint_bonus;
    
    // Store reward in pending (actual transfer happens in claim_rewards)
    // For now, just log it
    msg!("Batch submitted: hash={:?}, reward={}", batch_hash, reward);
    msg!("New reputation: {}", sentinel_data.reputation_score);
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    Ok(())
}

fn process_slash_sentinel(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    reason: SlashReason,
    amount: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let slasher = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    let treasury = next_account_info(account_info_iter)?;
    
    if !slasher.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    // TODO: Verify slasher is authorized
    // For now, allow any signer (would check against authorized_slashers in production)
    
    let mut sentinel_data = SentinelAccount::try_from_slice(&sentinel_account.data.borrow())?;
    
    // Calculate slash amount based on reason
    let slash_percentage = match reason {
        SlashReason::InvalidBatch => SLASH_PERCENTAGE_INVALID,
        SlashReason::ConsistentDivergence => SLASH_PERCENTAGE_DIVERGENCE,
        SlashReason::Downtime => 10, // 10% for downtime
        SlashReason::Other { code, .. } => code.min(100) as u64,
    };
    
    let slash_amount = (sentinel_data.stake * slash_percentage / 100).min(amount);
    
    if slash_amount > sentinel_data.stake {
        return Err(ProgramError::InsufficientFunds);
    }
    
    sentinel_data.stake -= slash_amount;
    sentinel_data.slash_count += 1;
    sentinel_data.reputation_score = (sentinel_data.reputation_score as u16)
        .saturating_sub(1000); // -10% reputation on slash
    
    // Transfer slashed amount to treasury
    **sentinel_account.try_borrow_mut_lamports()? -= slash_amount;
    **treasury.try_borrow_mut_lamports()? += slash_amount;
    
    msg!("Sentinel slashed: amount={}, reason={:?}", slash_amount, reason);
    msg!("Remaining stake: {}", sentinel_data.stake);
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    Ok(())
}

fn process_withdraw_stake(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    amount: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let operator = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    let destination = next_account_info(account_info_iter)?;
    let clock_info = next_account_info(account_info_iter)?;
    
    if !operator.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    let mut sentinel_data = SentinelAccount::try_from_slice(&sentinel_account.data.borrow())?;
    
    if sentinel_data.operator != *operator.key {
        return Err(ProgramError::IllegalOwner);
    }
    
    let clock = Clock::from_account_info(clock_info)?;
    
    // Check cooldown
    if sentinel_data.withdrawal_cooldown == 0 {
        // Start cooldown
        sentinel_data.withdrawal_cooldown = clock.unix_timestamp + WITHDRAWAL_COOLDOWN;
        msg!("Withdrawal cooldown started. Available after: {}", sentinel_data.withdrawal_cooldown);
        sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
        return Ok(());
    }
    
    if clock.unix_timestamp < sentinel_data.withdrawal_cooldown {
        msg!(
            "Withdrawal on cooldown. Wait until: {}",
            sentinel_data.withdrawal_cooldown
        );
        return Err(ProgramError::Custom(1)); // Custom error for cooldown
    }
    
    // Ensure minimum stake remains
    let remaining = sentinel_data.stake.saturating_sub(amount);
    if remaining > 0 && remaining < MIN_STAKE {
        msg!("Must maintain minimum stake of {} or withdraw all", MIN_STAKE);
        return Err(ProgramError::InsufficientFunds);
    }
    
    if amount > sentinel_data.stake {
        return Err(ProgramError::InsufficientFunds);
    }
    
    sentinel_data.stake -= amount;
    sentinel_data.withdrawal_cooldown = 0; // Reset cooldown
    
    // Transfer lamports
    **sentinel_account.try_borrow_mut_lamports()? -= amount;
    **destination.try_borrow_mut_lamports()? += amount;
    
    msg!("Stake withdrawn: {}", amount);
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    Ok(())
}

fn process_claim_rewards(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let operator = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    let _rewards_vault = next_account_info(account_info_iter)?;
    let destination = next_account_info(account_info_iter)?;
    
    if !operator.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    let mut sentinel_data = SentinelAccount::try_from_slice(&sentinel_account.data.borrow())?;
    
    if sentinel_data.operator != *operator.key {
        return Err(ProgramError::IllegalOwner);
    }
    
    // Calculate claimable rewards based on reputation and batches
    let base_reward_per_batch = 1000u64;
    let reputation_multiplier = sentinel_data.reputation_score as u128;
    let accepted_batches = sentinel_data.batches_accepted as u128;
    
    let total_rewards = (base_reward_per_batch as u128 * accepted_batches * reputation_multiplier / 10000)
        as u64;
    let claimable = total_rewards.saturating_sub(sentinel_data.rewards_claimed);
    
    if claimable == 0 {
        msg!("No rewards to claim");
        return Ok(());
    }
    
    sentinel_data.rewards_claimed += claimable;
    
    // Transfer rewards (would come from rewards vault in production)
    // For now, just log
    msg!("Rewards claimed: {}", claimable);
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    Ok(())
}

fn process_update_metadata(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    location: Option<String>,
    _endpoint_url: Option<String>,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let operator = next_account_info(account_info_iter)?;
    let sentinel_account = next_account_info(account_info_iter)?;
    
    if !operator.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    
    let mut sentinel_data = SentinelAccount::try_from_slice(&sentinel_account.data.borrow())?;
    
    if sentinel_data.operator != *operator.key {
        return Err(ProgramError::IllegalOwner);
    }
    
    if let Some(loc) = location {
        sentinel_data.location = loc;
    }
    
    msg!("Metadata updated");
    
    sentinel_data.serialize(&mut &mut sentinel_account.data.borrow_mut()[..])?;
    
    Ok(())
}
