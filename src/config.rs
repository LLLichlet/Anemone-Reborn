use serde::Deserialize;

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

pub fn load() -> AppConfig {
    let config_path = std::env::current_dir().unwrap().join("anemone-bot.toml");
    let content = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|_| panic!("failed to read config at {}", config_path.display()));
    toml::from_str(&content).expect("invalid config TOML")
}
