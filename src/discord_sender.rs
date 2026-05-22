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

use std::sync::Arc;
use std::sync::OnceLock;

use async_trait::async_trait;
use serenity::all::MessageId;
use serenity::builder::{CreateAttachment, CreateMessage};
use tracing::{error, warn};

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::proxy::download_bytes;
use crate::sender::PlatformSender;

pub struct DiscordSender {
    pub http: Arc<OnceLock<Arc<serenity::http::Http>>>,
    pub reqwest: reqwest::Client,
    pub channel_id: u64,
}

fn truncate_to_limit(s: &mut String, limit: usize) {
    if s.len() <= limit {
        return;
    }
    let suffix = "...";
    let end = limit.saturating_sub(suffix.len());
    let end = (0..=end)
        .rev()
        .find(|&i| s.is_char_boundary(i))
        .unwrap_or(0);
    s.truncate(end);
    s.push_str(suffix);
}

#[async_trait]
impl PlatformSender for DiscordSender {
    fn platform(&self) -> Platform {
        Platform::Discord
    }

    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError> {
        let http = self
            .http
            .get()
            .ok_or_else(|| AnemoneBotError::WebSocket("discord http not ready".into()))?;

        let prefix = match msg.source() {
            Platform::QQ => "**[QQ]**",
            Platform::Telegram => "**[Telegram]**",
            Platform::Matrix => "**[Matrix]**",
            Platform::Discord => unreachable!("bridge filters own platform"),
        };

        let mut text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());

        // Download and attach images
        let mut files: Vec<CreateAttachment> = Vec::new();
        for att in msg.attachments() {
            let data = match (&att.url, &att.data) {
                (_, Some(bytes)) => Some(bytes.clone()),
                (Some(url), None) => match download_bytes(url, &self.reqwest).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!("discord download image failed for {url}: {e}");
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
                files.push(CreateAttachment::bytes(bytes, att.filename.clone()));
            }
        }

        truncate_to_limit(&mut text, 2000);

        let channel = serenity::model::id::ChannelId::new(self.channel_id);

        let mut builder = CreateMessage::new().content(&text);

        if let Some(ref reply_id) = reply_to_msg_id {
            if let Ok(id) = reply_id.parse::<u64>() {
                builder = builder.reference_message((channel, MessageId::new(id)));
            }
        }

        for mut file in files {
            if !file.filename.contains('.') {
                file.filename.push_str(".jpg");
            }
            builder = builder.add_file(file);
        }

        match channel.send_message(http, builder).await {
            Ok(discord_msg) => Ok(discord_msg.id.to_string()),
            Err(e) => {
                error!("discord send failed: {e}");
                Err(AnemoneBotError::Serenity(e.to_string()))
            }
        }
    }

    async fn delete_message(&self, msg_id: &str) -> Result<(), AnemoneBotError> {
        let http = self
            .http
            .get()
            .ok_or_else(|| AnemoneBotError::WebSocket("discord http not ready".into()))?;

        let message_id = msg_id
            .parse::<u64>()
            .map_err(|_| AnemoneBotError::WebSocket("discord message id parse failed".into()))?;
        let channel = serenity::model::id::ChannelId::new(self.channel_id);
        channel
            .delete_message(http, MessageId::new(message_id))
            .await
            .map_err(|e| AnemoneBotError::Serenity(e.to_string()))?;
        Ok(())
    }
}
