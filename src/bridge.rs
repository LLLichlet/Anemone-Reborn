use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::onebot_api::Api;

#[derive(Clone)]
pub struct Bridge {
    qq_tx: UnboundedSender<String>,
    discord_http: Arc<OnceLock<Arc<serenity::http::Http>>>,
    qq_self_id: Arc<OnceLock<i64>>,
    discord_self_id: Arc<OnceLock<u64>>,
    discord_channel_id: u64,
    qq_group_id: i64,
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

impl Bridge {
    pub fn new(qq_tx: UnboundedSender<String>, discord_channel_id: u64, qq_group_id: i64) -> Self {
        Self {
            qq_tx,
            discord_http: Arc::new(OnceLock::new()),
            qq_self_id: Arc::new(OnceLock::new()),
            discord_self_id: Arc::new(OnceLock::new()),
            discord_channel_id,
            qq_group_id,
        }
    }

    pub fn discord_channel_id(&self) -> u64 {
        self.discord_channel_id
    }

    pub fn qq_group_id(&self) -> i64 {
        self.qq_group_id
    }

    pub fn set_discord_http(&self, http: Arc<serenity::http::Http>) {
        let _ = self.discord_http.set(http);
    }

    pub fn set_qq_self_id(&self, id: i64) {
        let _ = self.qq_self_id.set(id);
    }

    pub fn set_discord_self_id(&self, id: u64) {
        let _ = self.discord_self_id.set(id);
    }

    pub fn is_self_qq(&self, user_id: i64) -> bool {
        self.qq_self_id.get() == Some(&user_id)
    }

    pub fn is_self_discord(&self, user_id: u64) -> bool {
        self.discord_self_id.get() == Some(&user_id)
    }

    /// Forward a platform-agnostic message to the opposite platform(s).
    pub async fn forward(&self, msg: &dyn crate::message::Message) {
        match msg.source() {
            crate::message::Platform::QQ => {
                if let Some(http) = self.discord_http.get() {
                    let mut text = format!("**[QQ] {}**: {}", msg.sender_name(), msg.content());
                    truncate_to_limit(&mut text, 2000);
                    let channel = serenity::model::id::ChannelId::new(self.discord_channel_id);
                    if let Err(e) = channel.say(http, &text).await {
                        tracing::error!("discord send failed: {e}");
                    }
                }
            }
            crate::message::Platform::Discord => {
                let api = Api::new(self.qq_tx.clone());
                let text = format!("[Discord] {}: {}", msg.sender_name(), msg.content());
                if let Err(e) = api.send_group_msg(self.qq_group_id, &text) {
                    tracing::error!("qq send failed: {e}");
                }
            }
        }
    }
}
