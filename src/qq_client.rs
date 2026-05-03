use crate::bridge::Bridge;
use crate::message::QQMessage;
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

pub async fn handle_message(event: Event, bridge: &Bridge) {
    if bridge.is_self_qq(event.user_id) {
        return;
    }
    if event.message_type != "group" || event.group_id != bridge.qq_group_id() {
        return;
    }
    let name = event.sender.as_ref().map_or("unknown", |s| {
        if s.card.is_empty() {
            s.nickname.as_str()
        } else {
            s.card.as_str()
        }
    });

    // Decode HTML entities first (e.g. &amp; → &), then parse CQ reply
    // from the raw message BEFORE stripping, so the reply ID survives.
    let text = htmlescape::decode_html(&event.message).unwrap_or_else(|_| event.message.clone());
    let reply_to_msg_id =
        extract_reply_id(&text).or_else(|| event.reply.map(|r| r.message_id.to_string()));
    let filtered = strip_cq_codes(&text);
    if filtered.is_empty() {
        return;
    }

    info!("qq -> discord: [{name}] {filtered}");
    let msg = QQMessage {
        msg_id: event.message_id.to_string(),
        sender_name: name.to_string(),
        content: filtered,
        reply_to_msg_id,
    };
    bridge.forward(&msg).await;
}
