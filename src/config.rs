use serde::Deserialize;

use crate::error::AnemoneBotError;

#[derive(Debug, Deserialize, Clone)]
pub struct BridgeConfig {
    pub discord_channel_id: u64,
    pub qq_group_id: i64,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub bind_addr: String,
    pub bridges: Vec<BridgeConfig>,
}

pub fn load() -> Result<AppConfig, AnemoneBotError> {
    let config_path = std::env::current_dir()?.join("anemone-bot.toml");
    let content = std::fs::read_to_string(&config_path).map_err(|e| {
        AnemoneBotError::Config(format!("failed to read config at {}: {e}", config_path.display()))
    })?;
    toml::from_str(&content)
        .map_err(|e| AnemoneBotError::Config(format!("invalid config TOML: {e}")))
}
