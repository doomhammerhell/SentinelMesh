//! Output formatting utilities

use anyhow::Result;
use serde::Serialize;
use std::io::Write;

pub trait OutputFormatter {
    fn format<T: Serialize>(&self, data: &T) -> Result<String>;
}

pub struct TableFormatter;

impl OutputFormatter for TableFormatter {
    fn format<T: Serialize>(&self, data: &T) -> Result<String> {
        // For now, just serialize as pretty JSON
        // TODO: Implement proper table formatting
        Ok(serde_json::to_string_pretty(data)?)
    }
}

pub struct JsonFormatter;

impl OutputFormatter for JsonFormatter {
    fn format<T: Serialize>(&self, data: &T) -> Result<String> {
        Ok(serde_json::to_string_pretty(data)?)
    }
}

pub struct YamlFormatter;

impl OutputFormatter for YamlFormatter {
    fn format<T: Serialize>(&self, data: &T) -> Result<String> {
        Ok(serde_yaml::to_string(data)?)
    }
}

pub fn print_output<T: Serialize>(data: &T, format: &str) -> Result<()> {
    let formatter: Box<dyn OutputFormatter> = match format {
        "table" => Box::new(TableFormatter),
        "json" => Box::new(JsonFormatter),
        "yaml" => Box::new(YamlFormatter),
        _ => return Err(anyhow::anyhow!("Unsupported output format: {}", format)),
    };

    let output = formatter.format(data)?;
    println!("{}", output);
    Ok(())
}

pub fn print_success(message: &str) {
    println!("✅ {}", message);
}

pub fn print_error(message: &str) {
    eprintln!("❌ {}", message);
}

pub fn print_warning(message: &str) {
    println!("⚠️  {}", message);
}

pub fn print_info(message: &str) {
    println!("ℹ️  {}", message);
}
