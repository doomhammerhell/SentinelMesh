//! Configuration management for CLI

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    /// Default RPC URL
    pub default_rpc: String,
    /// Default keypair path
    pub default_keypair: Option<PathBuf>,
    /// Output format preference
    pub output_format: String,
    /// Default aggregator URL
    pub aggregator_url: String,
    /// Logging level
    pub log_level: String,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            default_rpc: "https://api.devnet.solana.com".to_string(),
            default_keypair: None,
            output_format: "table".to_string(),
            aggregator_url: "http://localhost:9480".to_string(),
            log_level: "info".to_string(),
        }
    }
}

impl CliConfig {
    /// Load configuration from file
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn save_to_file(&self, path: &PathBuf) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Get configuration directory
    pub fn config_dir() -> Result<PathBuf> {
        let home =
            dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Could not find home directory"))?;
        Ok(home.join(".sentinelmesh"))
    }

    /// Get default config file path
    pub fn default_config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("config.toml"))
    }

    /// Load or create default configuration
    pub fn load_or_create() -> Result<Self> {
        let config_path = Self::default_config_path()?;

        if config_path.exists() {
            Self::load_from_file(&config_path)
        } else {
            let config = Self::default();
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            config.save_to_file(&config_path)?;
            Ok(config)
        }
    }
}
