use std::sync::Arc;
use std::sync::OnceLock;

use async_trait::async_trait;
use serenity::all::MessageId;
use serenity::builder::CreateMessage;
use tracing::error;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::sender::PlatformSender;

pub struct DiscordSender {
    pub http: Arc<OnceLock<Arc<serenity::http::Http>>>,
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
            Platform::Discord => unreachable!("bridge filters own platform"),
        };

        let mut text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());
        truncate_to_limit(&mut text, 2000);

        let channel = serenity::model::id::ChannelId::new(self.channel_id);

        let mut builder = CreateMessage::new().content(&text);

        if let Some(ref reply_id) = reply_to_msg_id {
            if let Ok(id) = reply_id.parse::<u64>() {
                builder = builder.reference_message((channel, MessageId::new(id)));
            }
        }

        match channel.send_message(http, builder).await {
            Ok(discord_msg) => Ok(discord_msg.id.to_string()),
            Err(e) => {
                error!("discord send failed: {e}");
                Err(AnemoneBotError::Serenity(e.to_string()))
            }
        }
    }
}
