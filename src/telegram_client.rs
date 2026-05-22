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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::bridge::{Bridges, TelegramContext};
use crate::error::AnemoneBotError;
use crate::message::{Attachment, TelegramMessage};
use crate::message::Platform;

fn sender_name(from: &serde_json::Value) -> String {
    let first = from["first_name"].as_str().unwrap_or("");
    let last = from["last_name"].as_str().unwrap_or("");
    let name = if last.is_empty() {
        first.to_string()
    } else {
        format!("{first} {last}")
    };
    if name.trim().is_empty() {
        from["username"].as_str().unwrap_or("unknown").to_string()
    } else {
        name
    }
}

/// Resolve a Telegram `file_id` into a `file_path` via `getFile`.
async fn get_file_path(
    http: &reqwest::Client,
    token: &str,
    file_id: &str,
) -> Result<String, AnemoneBotError> {
    let resp: serde_json::Value = http
        .get(format!(
            "https://api.telegram.org/bot{token}/getFile?file_id={file_id}"
        ))
        .send()
        .await?
        .json()
        .await?;
    resp["result"]["file_path"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| AnemoneBotError::WebSocket("getFile: missing file_path".into()))
}

/// Get the bot's own user ID via `getMe`.
async fn get_self_id(http: &reqwest::Client, token: &str) -> Result<i64, AnemoneBotError> {
    let resp: serde_json::Value = http
        .get(format!("https://api.telegram.org/bot{token}/getMe"))
        .send()
        .await?
        .json()
        .await?;
    resp["result"]["id"]
        .as_i64()
        .ok_or_else(|| AnemoneBotError::WebSocket("getMe: missing id".into()))
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    bridges: Arc<Bridges>,
    ctx: &TelegramContext,
    cancel: CancellationToken,
    connected: Arc<AtomicBool>,
) -> Result<(), AnemoneBotError> {
    let self_id = get_self_id(&ctx.http, &ctx.token).await?;
    bridges.set_telegram_self_id(self_id);
    info!("telegram: self_id = {self_id}");
    connected.store(true, Ordering::Relaxed);

    let mut offset: i64 = 0;

    loop {
        if cancel.is_cancelled() {
            info!("telegram: cancellation requested, shutting down");
            break;
        }
        let resp: serde_json::Value = match ctx
            .http
            .get(format!(
                "https://api.telegram.org/bot{}/getUpdates?offset={offset}&timeout=30",
                ctx.token
            ))
            .send()
            .await
        {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    error!("telegram getUpdates json parse: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }
            },
            Err(e) => {
                error!("telegram getUpdates request: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        if !resp["ok"].as_bool().unwrap_or(false) {
            error!(
                "telegram getUpdates error: {}",
                resp["description"].as_str().unwrap_or("unknown")
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }

        let Some(updates) = resp["result"].as_array() else {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        };

        for update in updates {
            let update_id = update["update_id"].as_i64().unwrap_or(0);
            offset = offset.max(update_id + 1);

            if let Some(deleted) = update.get("deleted_business_messages") {
                let chat_id = deleted["chat"]["id"].as_i64().unwrap_or(0);
                if let Some(bridge) = bridges.by_telegram(chat_id) {
                    if let Some(message_ids) = deleted["message_ids"].as_array() {
                        for message_id in message_ids {
                            if let Some(id) = message_id.as_i64() {
                                let id_str = id.to_string();
                                if !bridge.is_source_message(Platform::Telegram, &id_str) {
                                    continue;
                                }
                                if bridge.take_suppressed_recall(Platform::Telegram, &id_str) {
                                    continue;
                                }
                                bridge.recall(Platform::Telegram, &id_str).await;
                            }
                        }
                    }
                }
                continue;
            }

            let Some(msg) = update.get("message") else {
                continue;
            };

            let chat_id = msg["chat"]["id"].as_i64().unwrap_or(0);

            // Filter self messages
            let from_id = msg["from"]["id"].as_i64().unwrap_or(0);
            if bridges.is_self_telegram(from_id) {
                continue;
            }

            // Filter bots
            if msg["from"]["is_bot"].as_bool().unwrap_or(false) {
                continue;
            }

            let Some(bridge) = bridges.by_telegram(chat_id) else {
                continue;
            };

            let text = msg["text"].as_str().unwrap_or("");

            // Parse photo attachments (largest size = last in array)
            let mut images: Vec<Attachment> = Vec::new();
            if let Some(photos) = msg["photo"].as_array() {
                if let Some(largest) = photos.last() {
                    let file_id = largest["file_id"].as_str().unwrap_or("");
                    if !file_id.is_empty() {
                        match get_file_path(&ctx.http, &ctx.token, file_id).await {
                            Ok(file_path) => {
                                let url = format!(
                                    "https://api.telegram.org/file/bot{}/{}",
                                    ctx.token, file_path
                                );
                                images.push(Attachment {
                                    url: Some(url),
                                    filename: format!("{file_id}.jpg"),
                                    content_type: Some("image/jpeg".into()),
                                    data: None,
                                });
                            }
                            Err(e) => warn!("telegram getFile failed: {e}"),
                        }
                    }
                }
            }

            if text.is_empty() && images.is_empty() {
                continue;
            }

            let msg_id = msg["message_id"].as_i64().unwrap_or(0).to_string();
            let name = sender_name(&msg["from"]);
            let reply_to_msg_id = msg["reply_to_message"]["message_id"]
                .as_i64()
                .map(|id| id.to_string());

            info!("telegram -> : [{name}] {text} (+{} images)", images.len());
            let tg_msg = TelegramMessage {
                msg_id,
                sender_name: name,
                content: text.to_string(),
                reply_to_msg_id,
                attachments: images,
            };
            bridge.forward(&tg_msg).await;
        }
    }
    connected.store(false, Ordering::Relaxed);
    Ok(())
}
