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
use reqwest::multipart::{Form, Part};
use serde_json::json;
use tracing::warn;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::proxy::download_bytes;
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

    #[allow(clippy::too_many_lines)]
    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError> {
        let prefix = match msg.source() {
            Platform::Discord => "[Discord]",
            Platform::QQ => "[QQ]",
            Platform::Matrix => "[Matrix]",
            Platform::Telegram => unreachable!("bridge filters own platform"),
        };

        let mut text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());

        // Download images
        let mut image_data: Vec<(Vec<u8>, String)> = Vec::new();
        for att in msg.attachments() {
            let data = match (&att.url, &att.data) {
                (_, Some(bytes)) => Some(bytes.clone()),
                (Some(url), None) => match download_bytes(url, &self.http).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!("telegram download image failed for {url}: {e}");
                        text.push_str("[图片]");
                        None
                    }
                },
                (None, None) => {
                    text.push_str("[图片]");
                    None
                }
            };
            if let Some(bytes) = data {
                let fname = att.filename.clone();
                image_data.push((bytes, fname));
            }
        }

        let api_url = format!("https://api.telegram.org/bot{}", self.token);

        if image_data.is_empty() {
            // Text only — sendMessage
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
                .post(format!("{api_url}/sendMessage"))
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
            return resp["result"]["message_id"]
                .as_i64()
                .map(|id| id.to_string())
                .ok_or_else(|| {
                    AnemoneBotError::WebSocket("missing message_id in response".into())
                });
        }

        if image_data.len() == 1 {
            // Single image — sendPhoto with caption
            let (data, filename) = &image_data[0];
            let mut form = Form::new()
                .text("chat_id", self.chat_id.to_string())
                .text("caption", text.clone())
                .part(
                    "photo",
                    Part::bytes(data.clone()).file_name(filename.clone()),
                );
            if let Some(ref reply_id) = reply_to_msg_id {
                if let Ok(id) = reply_id.parse::<i64>() {
                    form = form.text("reply_parameters", json!({"message_id": id}).to_string());
                }
            }
            let resp: serde_json::Value = self
                .http
                .post(format!("{api_url}/sendPhoto"))
                .multipart(form)
                .send()
                .await?
                .json()
                .await?;
            if !resp["ok"].as_bool().unwrap_or(false) {
                return Err(AnemoneBotError::WebSocket(format!(
                    "telegram sendPhoto failed: {}",
                    resp["description"].as_str().unwrap_or("unknown")
                )));
            }
            return resp["result"]["message_id"]
                .as_i64()
                .map(|id| id.to_string())
                .ok_or_else(|| {
                    AnemoneBotError::WebSocket("missing message_id in response".into())
                });
        }

        // Multiple images — sendMediaGroup with caption on first photo
        let mut media: Vec<serde_json::Value> = Vec::new();
        for (i, (_data, _fname)) in image_data.iter().enumerate() {
            let mut item = json!({
                "type": "photo",
                "media": format!("attach://file{i}"),
            });
            if i == 0 && !text.is_empty() {
                item["caption"] = json!(text);
            }
            media.push(item);
        }

        let mut form = Form::new()
            .text("chat_id", self.chat_id.to_string())
            .text("media", serde_json::to_string(&media)?);

        for (i, (data, filename)) in image_data.iter().enumerate() {
            form = form.part(
                format!("file{i}"),
                Part::bytes(data.clone()).file_name(filename.clone()),
            );
        }

        if let Some(ref reply_id) = reply_to_msg_id {
            if let Ok(id) = reply_id.parse::<i64>() {
                form = form.text("reply_parameters", json!({"message_id": id}).to_string());
            }
        }

        let resp: serde_json::Value = self
            .http
            .post(format!("{api_url}/sendMediaGroup"))
            .multipart(form)
            .send()
            .await?
            .json()
            .await?;

        if !resp["ok"].as_bool().unwrap_or(false) {
            return Err(AnemoneBotError::WebSocket(format!(
                "telegram sendMediaGroup failed: {}",
                resp["description"].as_str().unwrap_or("unknown")
            )));
        }

        // Return the first message_id from the media group
        resp["result"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|m| m["message_id"].as_i64())
            .map(|id| id.to_string())
            .ok_or_else(|| {
                AnemoneBotError::WebSocket("missing message_id in sendMediaGroup response".into())
            })
    }

    async fn delete_message(&self, msg_id: &str) -> Result<(), AnemoneBotError> {
        let message_id = msg_id
            .parse::<i64>()
            .map_err(|_| AnemoneBotError::WebSocket("telegram message id parse failed".into()))?;
        let resp: serde_json::Value = self
            .http
            .post(format!("{}/deleteMessage", self.api_base()))
            .json(&json!({
                "chat_id": self.chat_id,
                "message_id": message_id,
            }))
            .send()
            .await?
            .json()
            .await?;
        if resp["ok"].as_bool().unwrap_or(false) {
            Ok(())
        } else {
            Err(AnemoneBotError::WebSocket(format!(
                "telegram deleteMessage failed: {}",
                resp["description"].as_str().unwrap_or("unknown")
            )))
        }
    }
}

impl TelegramSender {
    fn api_base(&self) -> String {
        format!("https://api.telegram.org/bot{}", self.token)
    }
}
