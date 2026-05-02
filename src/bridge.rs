use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::config;
use crate::onebot_api::Api;

#[derive(Clone)]
pub struct Bridge {
    qq_tx: UnboundedSender<String>,
    discord_http: Arc<OnceLock<Arc<serenity::http::Http>>>,
    qq_self_id: Arc<OnceLock<i64>>,
    discord_self_id: Arc<OnceLock<u64>>,
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
    pub fn new(qq_tx: UnboundedSender<String>) -> Self {
        Self {
            qq_tx,
            discord_http: Arc::new(OnceLock::new()),
            qq_self_id: Arc::new(OnceLock::new()),
            discord_self_id: Arc::new(OnceLock::new()),
        }
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

    /// Forward a message from QQ to Discord.
    pub async fn forward_qq_to_discord(&self, sender_name: &str, content: &str) {
        if let Some(http) = self.discord_http.get() {
            let mut msg = format!("**[QQ] {}**: {}", sender_name, content);
            truncate_to_limit(&mut msg, 2000);
            let channel = serenity::model::id::ChannelId::new(config::DISCORD_CHANNEL_ID);
            if let Err(e) = channel.say(http, &msg).await {
                tracing::error!("discord send failed: {e}");
            }
        }
    }

    /// Forward a message from Discord to QQ.
    pub fn forward_discord_to_qq(&self, author_name: &str, content: &str) {
        let api = Api::new(self.qq_tx.clone());
        let msg = format!("[Discord] {}: {}", author_name, content);
        if let Err(e) = api.send_group_msg(config::QQ_GROUP_ID, &msg) {
            tracing::error!("qq send failed: {e}");
        }
    }
}
