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

use crate::bridge::{Bridges, MatrixContext};
use crate::error::AnemoneBotError;
use crate::message::{Attachment, MatrixMessage};
use crate::message::Platform;

/// Strip the homeserver suffix from a Matrix user ID for display.
/// "@alice:matrix.org" → "alice"
fn display_name(user_id: &str) -> &str {
    user_id
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or(user_id)
}

/// Get the bot's own user ID via /account/whoami.
async fn get_self_id(
    http: &reqwest::Client,
    homeserver_url: &str,
    token: &str,
) -> Result<String, AnemoneBotError> {
    let url = format!(
        "{}/_matrix/client/v3/account/whoami",
        homeserver_url.trim_end_matches('/')
    );
    let resp: serde_json::Value = http
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .json()
        .await?;
    if let Some(uid) = resp["user_id"].as_str() {
        return Ok(uid.to_string());
    }
    let errcode = resp["errcode"].as_str().unwrap_or("unknown");
    let error = resp["error"].as_str().unwrap_or("no details");
    Err(AnemoneBotError::WebSocket(format!(
        "whoami failed: {errcode} - {error}"
    )))
}

/// Build a download URL for an mxc:// URI.
fn mxc_download_url(homeserver_url: &str, mxc: &str) -> Option<String> {
    // mxc://serverName/mediaId
    let rest = mxc.strip_prefix("mxc://")?;
    let (server, media_id) = rest.split_once('/')?;
    Some(format!(
        "{}/_matrix/media/v3/download/{}/{}",
        homeserver_url.trim_end_matches('/'),
        server,
        media_id,
    ))
}

/// Extract the plain text body and HTML formatted body from event content.
fn extract_text(content: &serde_json::Value) -> String {
    // Prefer formatted_body for richer content, fall back to body
    content["formatted_body"]
        .as_str()
        .or_else(|| content["body"].as_str())
        .unwrap_or("")
        .to_string()
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    bridges: Arc<Bridges>,
    ctx: &MatrixContext,
    cancel: CancellationToken,
    connected: Arc<AtomicBool>,
) -> Result<(), AnemoneBotError> {
    let homeserver = ctx.homeserver_url.trim_end_matches('/');
    let self_id = get_self_id(&ctx.http, homeserver, &ctx.token).await?;
    bridges.set_matrix_self_id(self_id.clone());
    info!("matrix: self_id = {self_id}");
    connected.store(true, Ordering::Relaxed);

    let mut since: Option<String> = None;

    loop {
        if cancel.is_cancelled() {
            info!("matrix: cancellation requested, shutting down");
            break;
        }

        // Build sync URL with optional since and 30s timeout
        let mut url = format!("{homeserver}/_matrix/client/v3/sync?timeout=30000",);
        if let Some(ref s) = since {
            use std::fmt::Write;
            let _ = write!(url, "&since={s}");
        }

        let resp: serde_json::Value = match ctx.http.get(&url).bearer_auth(&ctx.token).send().await
        {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    error!("matrix sync json parse: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }
            },
            Err(e) => {
                error!("matrix sync request: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        // Update cursor for next incremental sync
        if let Some(next_batch) = resp["next_batch"].as_str() {
            since = Some(next_batch.to_string());
        }

        // Process joined rooms
        let Some(rooms) = resp["rooms"]["join"].as_object() else {
            continue;
        };

        for (room_id, room_data) in rooms {
            let Some(bridge) = bridges.by_matrix(room_id) else {
                continue;
            };

            let Some(events) = room_data["timeline"]["events"].as_array() else {
                continue;
            };

            for event in events {
                // Only process m.room.message events
                let event_type = event["type"].as_str().unwrap_or("");
                if event_type != "m.room.message" && event_type != "m.room.redaction" {
                    continue;
                }

                let Some(event_id) = event["event_id"].as_str() else {
                    continue;
                };

                let sender = event["sender"].as_str().unwrap_or("unknown");

                if event_type == "m.room.redaction" {
                    let Some(redacts) = event["redacts"].as_str() else {
                        continue;
                    };
                    if !bridge.is_source_message(Platform::Matrix, redacts) {
                        continue;
                    }
                    if bridge.take_suppressed_recall(Platform::Matrix, redacts) {
                        continue;
                    }
                    bridge.recall(Platform::Matrix, redacts).await;
                    continue;
                }

                // Filter self messages
                if bridges.is_self_matrix(sender) {
                    continue;
                }

                let content = &event["content"];
                let msgtype = content["msgtype"].as_str().unwrap_or("m.text");

                let body = extract_text(content);

                // Parse reply reference
                let reply_to_msg_id = content["m\\.relates_to"]["m\\.in_reply_to"]["event_id"]
                    .as_str()
                    .or_else(|| {
                        content
                            .get("m.relates_to")
                            .and_then(|r| r.get("m.in_reply_to"))
                            .and_then(|r| r.get("event_id"))
                            .and_then(|v| v.as_str())
                    })
                    .map(String::from);

                // Parse image attachments
                let mut images: Vec<Attachment> = Vec::new();
                if msgtype == "m.image" {
                    if let Some(mxc) = content["url"].as_str() {
                        if let Some(dl_url) = mxc_download_url(homeserver, mxc) {
                            let filename = content["body"]
                                .as_str()
                                .map(std::string::ToString::to_string)
                                .unwrap_or_else(|| "image.jpg".into());
                            let content_type = content["info"]["mimetype"]
                                .as_str()
                                .map(std::string::ToString::to_string)
                                .or_else(|| Some("image/jpeg".into()));
                            // Download bytes here so other senders don't need
                            // Matrix auth to fetch the mxc:// media.
                            let data = match ctx
                                .http
                                .get(&dl_url)
                                .bearer_auth(&ctx.token)
                                .send()
                                .await
                            {
                                Ok(resp) => resp.bytes().await.ok().map(|b| b.to_vec()),
                                Err(e) => {
                                    warn!("matrix download mxc failed: {e}");
                                    None
                                }
                            };
                            images.push(Attachment {
                                url: Some(dl_url),
                                filename,
                                content_type,
                                data,
                            });
                        }
                    }
                }

                // Skip empty messages
                if body.is_empty() && images.is_empty() {
                    continue;
                }

                let name = display_name(sender).to_string();

                info!("matrix -> : [{name}] {body} (+{} images)", images.len());
                let mx_msg = MatrixMessage {
                    msg_id: event_id.to_string(),
                    sender_name: name,
                    content: body,
                    reply_to_msg_id,
                    attachments: images,
                };
                bridge.forward(&mx_msg).await;
            }
        }
    }
    connected.store(false, Ordering::Relaxed);
    Ok(())
}
