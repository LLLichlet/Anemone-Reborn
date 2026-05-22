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

use std::fmt::Write;

use async_trait::async_trait;
use base64::prelude::*;
use tracing::warn;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::onebot_api::Api;
use crate::proxy::download_bytes;
use crate::sender::PlatformSender;

pub struct QQSender {
    pub tx: tokio::sync::mpsc::UnboundedSender<String>,
    pub pending: crate::onebot_api::PendingMap,
    pub reqwest: reqwest::Client,
    pub group_id: i64,
}

#[async_trait]
impl PlatformSender for QQSender {
    fn platform(&self) -> Platform {
        Platform::QQ
    }

    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError> {
        let prefix = match msg.source() {
            Platform::Discord => "[Discord]",
            Platform::Telegram => "[Telegram]",
            Platform::Matrix => "[Matrix]",
            Platform::QQ => unreachable!("bridge filters own platform"),
        };

        let mut text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());

        // Download images and append as CQ codes
        for att in msg.attachments() {
            let data = match (&att.url, &att.data) {
                (_, Some(bytes)) => Some(bytes.clone()),
                (Some(url), None) => match download_bytes(url, &self.reqwest).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!("qq download image failed for {url}: {e}");
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
                let b64 = BASE64_STANDARD.encode(&bytes);
                let _ = write!(text, "[CQ:image,file=base64://{b64}]");
            }
        }

        if let Some(ref reply_id) = reply_to_msg_id {
            text = format!("[CQ:reply,id={reply_id}]{text}");
        }

        let api = Api::new(self.tx.clone(), self.pending.clone());
        let msg_id = api.send_group_msg(self.group_id, &text).await?;
        Ok(msg_id.to_string())
    }

    async fn delete_message(&self, msg_id: &str) -> Result<(), AnemoneBotError> {
        let message_id = msg_id
            .parse::<i64>()
            .map_err(|_| AnemoneBotError::WebSocket("qq message id parse failed".into()))?;
        let api = Api::new(self.tx.clone(), self.pending.clone());
        api.delete_msg(message_id).await
    }
}
