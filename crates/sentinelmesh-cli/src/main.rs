//! SentinelMesh CLI
//!
//! Unified command-line interface for operators to manage SentinelMesh deployments.
//!
//! Commands:
//! - `init`: Initialize new sentinel configuration
//! - `config`: Validate and manage configuration
//! - `agent`: Run and manage agent nodes
//! - `aggregator`: Deploy and manage aggregators
//! - `canary`: Test RPC endpoints with canary transactions
//! - `dashboard`: Open the analytics dashboard
//! - `zk`: Generate and verify ZK proofs
//! - `reputation`: Interact with on-chain reputation system
//! - `status`: Check system health and status

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use std::path::PathBuf;

mod commands;
mod config;
mod output;

use commands::*;

#[derive(Parser)]
#[command(name = "sentinelmesh")]
#[command(about = "SentinelMesh - Infrastructure observability for Solana")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Configuration file path
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,
    
    /// Output format
    #[arg(short, long, global = true, value_enum, default_value = "table")]
    format: OutputFormat,
    
    /// Quiet mode (suppress non-essential output)
    #[arg(short, long, global = true)]
    quiet: bool,
    
    /// Verbose mode
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(ValueEnum, Clone, Debug)]
enum OutputFormat {
    Table,
    Json,
    Yaml,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new SentinelMesh deployment
    Init {
        /// Deployment type
        #[arg(value_enum, default_value = "agent")]
        deployment_type: DeploymentType,
        
        /// Region/location identifier
        #[arg(short, long)]
        region: Option<String>,
        
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output: PathBuf,
    },
    
    /// Validate and manage configuration files
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    
    /// Run and manage agent nodes
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },
    
    /// Deploy and manage aggregators
    Aggregator {
        #[command(subcommand)]
        action: AggregatorAction,
    },
    
    /// Test RPC endpoints with canary transactions
    Canary {
        /// RPC endpoint URL to test
        #[arg(short, long)]
        endpoint: String,
        
        /// Network (mainnet, devnet, testnet)
        #[arg(short, long, default_value = "devnet")]
        network: String,
        
        /// Amount in SOL
        #[arg(short, long, default_value = "0.000001")]
        amount: f64,
        
        /// Keypair path for canary transactions
        #[arg(short, long)]
        keypair: Option<PathBuf>,
    },
    
    /// Open the analytics dashboard
    Dashboard {
        /// Aggregator URL
        #[arg(short, long, default_value = "http://localhost:9480")]
        url: String,
        
        /// Open in browser
        #[arg(short, long)]
        open: bool,
    },
    
    /// ZK Proof operations
    Zk {
        #[command(subcommand)]
        action: ZkAction,
    },
    
    /// On-chain reputation system
    Reputation {
        #[command(subcommand)]
        action: ReputationAction,
    },
    
    /// Check system status and health
    Status {
        /// Check specific component
        #[arg(short, long)]
        component: Option<String>,
        
        /// Watch mode (continuous updates)
        #[arg(short, long)]
        watch: bool,
    },
    
    /// Run chaos engineering tests
    Chaos {
        /// Scenario to run
        #[arg(short, long)]
        scenario: Option<String>,
        
        /// Duration of test
        #[arg(short, long, default_value = "5m")]
        duration: String,
    },
}

#[derive(ValueEnum, Clone, Debug)]
enum DeploymentType {
    Agent,
    Aggregator,
    Full,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Validate configuration file
    Validate {
        /// Config file path
        #[arg(short, long)]
        file: PathBuf,
    },
    /// Generate example configuration
    Example {
        /// Output file
        #[arg(short, long)]
        output: Option<PathBuf>,
        
        /// Type of config (agent or aggregator)
        #[arg(short, long, value_enum, default_value = "agent")]
        config_type: ConfigType,
    },
    /// Show current configuration
    Show,
}

#[derive(ValueEnum, Clone, Debug)]
enum ConfigType {
    Agent,
    Aggregator,
}

#[derive(Subcommand)]
enum AgentAction {
    /// Start the agent
    Run {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
        
        /// Detach mode
        #[arg(short, long)]
        detach: bool,
    },
    /// Stop the agent
    Stop,
    /// Check agent status
    Status,
    /// View agent logs
    Logs {
        /// Follow logs
        #[arg(short, long)]
        follow: bool,
        
        /// Number of lines
        #[arg(short, long, default_value = "100")]
        lines: usize,
    },
}

#[derive(Subcommand)]
enum AggregatorAction {
    /// Deploy a new aggregator
    Deploy {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
        
        /// Environment (dev, staging, prod)
        #[arg(short, long, default_value = "dev")]
        env: String,
    },
    /// Check aggregator status
    Status {
        /// Aggregator URL
        #[arg(short, long, default_value = "http://localhost:9480")]
        url: String,
    },
    /// Scale aggregator replicas
    Scale {
        /// Number of replicas
        #[arg(short, long)]
        replicas: u32,
    },
    /// View aggregator logs
    Logs {
        /// Follow logs
        #[arg(short, long)]
        follow: bool,
    },
}

#[derive(Subcommand)]
enum ZkAction {
    /// Generate ZK proof for a batch
    Prove {
        /// Batch file path
        #[arg(short, long)]
        batch: PathBuf,
        
        /// Output file for proof
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Verify a ZK proof
    Verify {
        /// Proof file path
        #[arg(short, long)]
        proof: PathBuf,
    },
    /// Show ZK system status
    Status,
}

#[derive(Subcommand)]
enum ReputationAction {
    /// Initialize a new sentinel on-chain
    Init {
        /// Sentinel ID
        #[arg(short, long)]
        sentinel_id: String,
        
        /// Initial stake in SOL
        #[arg(short, long, default_value = "1.0")]
        stake: f64,
        
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
        
        /// Payer keypair
        #[arg(short, long)]
        keypair: PathBuf,
    },
    /// Check sentinel reputation
    Status {
        /// Sentinel address
        #[arg(short, long)]
        address: String,
        
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
    },
    /// Submit batch to reputation program
    Submit {
        /// Batch hash
        #[arg(short, long)]
        batch_hash: String,
        
        /// ZK proof hash
        #[arg(short, long)]
        zk_proof: String,
        
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
        
        /// Payer keypair
        #[arg(short, long)]
        keypair: PathBuf,
    },
    /// Claim rewards
    Claim {
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
        
        /// Payer keypair
        #[arg(short, long)]
        keypair: PathBuf,
    },
    /// Withdraw stake
    Withdraw {
        /// Amount in SOL
        #[arg(short, long)]
        amount: f64,
        
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
        
        /// Payer keypair
        #[arg(short, long)]
        keypair: PathBuf,
    },
    /// Show leaderboard
    Leaderboard {
        /// RPC URL
        #[arg(short, long)]
        rpc: String,
        
        /// Number of entries
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Set up logging based on verbosity
    if cli.verbose {
        tracing_subscriber::fmt::init();
    }
    
    match cli.command {
        Commands::Init { deployment_type, region, output } => {
            commands::init::execute(deployment_type, region, output).await?;
        }
        Commands::Config { action } => {
            commands::config::execute(action, cli.format).await?;
        }
        Commands::Agent { action } => {
            commands::stubs::agent::execute(action).await?;
        }
        Commands::Aggregator { action } => {
            commands::stubs::aggregator::execute(action).await?;
        }
        Commands::Canary { endpoint, network, amount, keypair } => {
            commands::stubs::canary::execute(endpoint, network, amount, keypair).await?;
        }
        Commands::Dashboard { url, open } => {
            commands::stubs::dashboard::execute(url, open).await?;
        }
        Commands::Zk { action } => {
            commands::zk::execute(action).await?;
        }
        Commands::Reputation { action } => {
            commands::reputation::execute(action).await?;
        }
        Commands::Status { component, watch } => {
            commands::status::execute(component, watch).await?;
        }
        Commands::Chaos { scenario, duration } => {
            commands::stubs::chaos::execute(scenario, duration).await?;
        }
    }
    
    Ok(())
}
