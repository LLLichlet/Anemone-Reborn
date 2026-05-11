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

#[allow(clippy::struct_field_names)]
#[derive(Debug, Deserialize, Clone)]
pub struct BridgeConfig {
    #[serde(default)]
    pub discord_channel_id: Option<u64>,
    #[serde(default)]
    pub qq_group_id: Option<i64>,
    #[serde(default)]
    pub telegram_group_id: Option<i64>,
    #[serde(default)]
    pub matrix_room_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub bind_addr: Option<String>,
    #[serde(default)]
    pub webui_bind_addr: Option<String>,
    #[serde(default)]
    pub discord_token: Option<String>,
    #[serde(default)]
    pub telegram_token: Option<String>,
    #[serde(default)]
    pub matrix_token: Option<String>,
    #[serde(default, alias = "matrix_homeserver")]
    pub matrix_homeserver_url: Option<String>,
    #[serde(default)]
    pub http_proxy: Option<String>,
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
