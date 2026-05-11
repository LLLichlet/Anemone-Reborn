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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde_json::json;
use tracing::warn;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::proxy::download_bytes;
use crate::sender::PlatformSender;

static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct MatrixSender {
    pub http: reqwest::Client,
    pub token: String,
    pub homeserver_url: String,
    pub room_id: String,
}

fn txn_id() -> String {
    let n = TXN_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("anemone-{n}")
}

/// Strip the homeserver suffix from a Matrix user ID for display.
/// "@alice:matrix.org" → "alice"
fn display_name(user_id: &str) -> &str {
    user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(user_id)
}

#[async_trait]
impl PlatformSender for MatrixSender {
    fn platform(&self) -> Platform {
        Platform::Matrix
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
            Platform::Telegram => "[Telegram]",
            Platform::Matrix => unreachable!("bridge filters own platform"),
        };

        let sender = display_name(msg.sender_name());
        let mut text = format!("{prefix} {sender}: {}", msg.content());

        // Download images
        let mut image_data: Vec<(Vec<u8>, String, String)> = Vec::new();
        for att in msg.attachments() {
            let data = match (&att.url, &att.data) {
                (_, Some(bytes)) => Some(bytes.clone()),
                (Some(url), None) => match download_bytes(url, &self.http).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!("matrix download image failed for {url}: {e}");
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
                // Normalize MIME type — reject empty or malformed values.
                let raw = att.content_type.as_deref().unwrap_or("");
                let mime = if raw.is_empty() || !raw.contains('/') {
                    guess_mime(&bytes, &att.filename)
                } else {
                    raw.to_string()
                };
                // Ensure filename has an extension so clients can infer the type.
                let filename = if att.filename.contains('.') {
                    att.filename.clone()
                } else {
                    let ext = match mime.as_str() {
                        "image/png" => "png",
                        "image/gif" => "gif",
                        "image/webp" => "webp",
                        _ => "jpg",
                    };
                    format!("{}.{ext}", att.filename)
                };
                image_data.push((bytes, filename, mime));
            }
        }

        let base_url = format!(
            "{}/_matrix/client/v3/rooms/{}",
            self.homeserver_url.trim_end_matches('/'),
            self.room_id,
        );

        if image_data.is_empty() {
            send_text(&self.http, &self.token, &base_url, &text, reply_to_msg_id).await
        } else if image_data.len() == 1 {
            let (data, filename, mime) = image_data.into_iter().next().unwrap();
            send_image(
                &self.http,
                &self.token,
                &self.homeserver_url,
                &base_url,
                &text,
                &data,
                &filename,
                &mime,
                reply_to_msg_id,
            )
            .await
        } else {
            let mut first_event_id = None;
            for (i, (data, filename, mime)) in image_data.into_iter().enumerate() {
                // Only include text caption on first image
                let caption = if i == 0 { &text } else { "" };
                let result = send_image(
                    &self.http,
                    &self.token,
                    &self.homeserver_url,
                    &base_url,
                    caption,
                    &data,
                    &filename,
                    &mime,
                    reply_to_msg_id.clone(),
                )
                .await;
                if i == 0 {
                    first_event_id = Some(result?);
                } else if let Err(e) = result {
                    warn!("matrix send subsequent image failed: {e}");
                }
            }
            first_event_id
                .ok_or_else(|| AnemoneBotError::WebSocket("matrix: no images sent".into()))
        }
    }
}

fn guess_mime(data: &[u8], filename: &str) -> String {
    // Extension-based guess
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "png" => return "image/png".into(),
        "gif" => return "image/gif".into(),
        "webp" => return "image/webp".into(),
        "bmp" => return "image/bmp".into(),
        _ => {}
    }
    // Magic bytes
    if data.len() >= 3 && data[..3] == [0xFF, 0xD8, 0xFF] {
        return "image/jpeg".into();
    }
    if data.len() >= 4 && data[..4] == [0x89, b'P', b'N', b'G'] {
        return "image/png".into();
    }
    if data.len() >= 4 && data[..4] == [b'G', b'I', b'F', b'8'] {
        return "image/gif".into();
    }
    if data.len() >= 4 && data[..4] == [b'R', b'I', b'F', b'F'] {
        return "image/webp".into();
    }
    "image/jpeg".into()
}

fn make_body(text: &str, reply_to_msg_id: Option<&String>) -> serde_json::Value {
    let formatted = format!(
        "<strong>{}</strong>",
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    );
    let mut body = json!({
        "msgtype": "m.text",
        "body": text,
        "format": "org.matrix.custom.html",
        "formatted_body": formatted,
    });
    if let Some(ref reply_id) = reply_to_msg_id {
        body["m.relates_to"] = json!({
            "m.in_reply_to": {
                "event_id": reply_id,
            }
        });
    }
    body
}

async fn send_text(
    http: &reqwest::Client,
    token: &str,
    base_url: &str,
    text: &str,
    reply_to_msg_id: Option<String>,
) -> Result<String, AnemoneBotError> {
    let body = make_body(text, reply_to_msg_id.as_ref());
    let url = format!("{base_url}/send/m.room.message/{}", txn_id());
    let resp: serde_json::Value = http
        .put(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?
        .json()
        .await?;
    resp["event_id"].as_str().map(String::from).ok_or_else(|| {
        AnemoneBotError::WebSocket("matrix: missing event_id in send response".into())
    })
}

async fn send_image(
    http: &reqwest::Client,
    token: &str,
    homeserver_url: &str,
    base_url: &str,
    caption: &str,
    data: &[u8],
    filename: &str,
    mime: &str,
    reply_to_msg_id: Option<String>,
) -> Result<String, AnemoneBotError> {
    // 1. Upload media
    let upload_url = format!(
        "{}/_matrix/media/v3/upload?filename={}",
        homeserver_url.trim_end_matches('/'),
        filename,
    );
    let form = Form::new().part(
        "file",
        Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str(mime)
            .map_err(|e| AnemoneBotError::WebSocket(format!("matrix mime error: {e}")))?,
    );
    let upload_resp: serde_json::Value = http
        .post(&upload_url)
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await?
        .json()
        .await?;
    let content_uri = upload_resp["content_uri"].as_str().ok_or_else(|| {
        AnemoneBotError::WebSocket("matrix: missing content_uri in upload response".into())
    })?;

    // 2. Send image message
    let mut body = json!({
        "msgtype": "m.image",
        "body": filename,
        "url": content_uri,
        "info": {
            "mimetype": mime,
        },
    });
    if !caption.is_empty() {
        body["body"] = json!(caption);
        let formatted_caption = format!(
            "<strong>{}</strong>",
            caption
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
        );
        body["format"] = json!("org.matrix.custom.html");
        body["formatted_body"] = json!(formatted_caption);
    }
    if let Some(ref reply_id) = reply_to_msg_id {
        body["m.relates_to"] = json!({
            "m.in_reply_to": {
                "event_id": reply_id,
            }
        });
    }

    let send_url = format!("{base_url}/send/m.room.message/{}", txn_id());
    let resp: serde_json::Value = http
        .put(&send_url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?
        .json()
        .await?;
    resp["event_id"].as_str().map(String::from).ok_or_else(|| {
        AnemoneBotError::WebSocket("matrix: missing event_id in image send response".into())
    })
}
