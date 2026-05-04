/*
    Anemone-bot is a message forwarding bot that connects various chat platforms.
    Copyright (C) 2026  LLLichlet

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

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
        AnemoneBotError::Config(format!(
            "failed to read config at {}: {e}",
            config_path.display()
        ))
    })?;
    toml::from_str(&content)
        .map_err(|e| AnemoneBotError::Config(format!("invalid config TOML: {e}")))
}
