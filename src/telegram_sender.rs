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

use async_trait::async_trait;
use serde_json::json;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::sender::PlatformSender;

pub struct TelegramSender {
    pub http: reqwest::Client,
    pub token: String,
    pub chat_id: i64,
}

#[async_trait]
impl PlatformSender for TelegramSender {
    fn platform(&self) -> Platform {
        Platform::Telegram
    }

    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError> {
        let prefix = match msg.source() {
            Platform::Discord => "**[Discord]**",
            Platform::QQ => "**[QQ]**",
            Platform::Telegram => unreachable!("bridge filters own platform"),
        };

        let text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());

        let mut body = json!({
            "chat_id": self.chat_id,
            "text": text,
        });

        if let Some(ref reply_id) = reply_to_msg_id {
            if let Ok(id) = reply_id.parse::<i64>() {
                body["reply_parameters"] = json!({"message_id": id});
            }
        }

        let resp: serde_json::Value = self
            .http
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.token
            ))
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        if !resp["ok"].as_bool().unwrap_or(false) {
            return Err(AnemoneBotError::WebSocket(format!(
                "telegram sendMessage failed: {}",
                resp["description"].as_str().unwrap_or("unknown")
            )));
        }

        resp["result"]["message_id"]
            .as_i64()
            .map(|id| id.to_string())
            .ok_or_else(|| AnemoneBotError::WebSocket("missing message_id in response".into()))
    }
}
