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

use crate::bridge::Bridges;
use crate::message::{Attachment, QQMessage};
use crate::onebot_types::Event;
use tracing::info;

/// Extract the reply target message ID from a `[CQ:reply,id=<id>]` code.
/// `OneBot` v11 encodes reply info as a CQ code inside the message body,
/// not as a separate JSON field.
fn extract_reply_id(msg: &str) -> Option<String> {
    let start = msg.find("[CQ:reply,")?;
    let slice = &msg[start..];
    let end = slice.find(']')?;
    let inner = &slice[10..end]; // after "[CQ:reply," before "]"
    for part in inner.split(',') {
        let mut kv = part.splitn(2, '=');
        if let (Some("id"), Some(val)) = (kv.next(), kv.next()) {
            return Some(val.trim().to_string());
        }
    }
    None
}

fn strip_cq_codes(msg: &str) -> String {
    let chars: Vec<char> = msg.chars().collect();
    let mut result = String::new();
    let mut i = 0;
    while i < chars.len() {
        if i + 4 <= chars.len()
            && chars[i] == '['
            && chars[i + 1] == 'C'
            && chars[i + 2] == 'Q'
            && chars[i + 3] == ':'
        {
            i += 4;
            let mut depth = 1u32;
            while i < chars.len() && depth > 0 {
                match chars[i] {
                    '[' => depth += 1,
                    ']' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result.trim().to_string()
}

/// Extract `[CQ:image,...]` blocks and return their `url` as `Attachment` entries.
fn extract_images(msg: &str) -> Vec<Attachment> {
    let mut images = Vec::new();
    let chars: Vec<char> = msg.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if i + 10 <= chars.len()
            && chars[i] == '['
            && chars[i + 1] == 'C'
            && chars[i + 2] == 'Q'
            && chars[i + 3] == ':'
            && chars[i + 4] == 'i'
            && chars[i + 5] == 'm'
            && chars[i + 6] == 'a'
            && chars[i + 7] == 'g'
            && chars[i + 8] == 'e'
            && chars[i + 9] == ','
        {
            let start = i;
            i += 10;
            let mut depth = 1u32;
            while i < chars.len() && depth > 0 {
                match chars[i] {
                    '[' => depth += 1,
                    ']' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            let inner: String = chars[start + 10..i - 1].iter().collect();
            let mut url = None;
            for part in inner.split(',') {
                let mut kv = part.splitn(2, '=');
                if let (Some("url"), Some(val)) = (kv.next(), kv.next()) {
                    url = Some(val.trim().to_string());
                }
            }
            if let Some(u) = url {
                images.push(Attachment {
                    url: Some(u),
                    filename: String::from("image.jpg"),
                    content_type: Some("image/jpeg".into()),
                    data: None,
                });
            }
        } else {
            i += 1;
        }
    }
    images
}

pub async fn handle_message(event: Event, bridges: &Bridges) {
    if bridges.is_self_qq(event.user_id) {
        return;
    }
    if event.message_type != "group" {
        return;
    }
    let Some(bridge) = bridges.by_qq(event.group_id) else {
        return;
    };
    let name = event.sender.as_ref().map_or("unknown", |s| {
        if s.card.is_empty() {
            s.nickname.as_str()
        } else {
            s.card.as_str()
        }
    });

    // Decode HTML entities first (e.g. &amp; → &), then parse CQ reply / images
    // from the raw message BEFORE stripping, so the reply ID and image URLs survive.
    let text = htmlescape::decode_html(&event.message).unwrap_or_else(|_| event.message.clone());
    let reply_to_msg_id =
        extract_reply_id(&text).or_else(|| event.reply.map(|r| r.message_id.to_string()));
    let images = extract_images(&text);
    let filtered = strip_cq_codes(&text);
    if filtered.is_empty() && images.is_empty() {
        return;
    }

    info!("qq -> : [{name}] {filtered} (+{} images)", images.len());
    let msg = QQMessage {
        msg_id: event.message_id.to_string(),
        sender_name: name.to_string(),
        content: filtered,
        reply_to_msg_id,
        attachments: images,
    };
    bridge.forward(&msg).await;
}
