use crate::bridge::Bridge;
use crate::message::QQMessage;
use crate::onebot_types::Event;
use tracing::info;

/// Strip CQ codes from a message. For now just removes them.
/// TODO: parse CQ codes and translate to platform-agnostic equivalents (images → URLs, at → @mentions, etc.)
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

    let text = htmlescape::decode_html(&event.message).unwrap_or_else(|_| event.message.clone());
    let filtered = strip_cq_codes(&text);
    if filtered.is_empty() {
        return;
    }

    info!("qq -> discord: [{name}] {filtered}");
    let msg = QQMessage {
        sender_name: name.to_string(),
        content: filtered,
    };
    bridge.forward(&msg).await;
}
