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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info};

use crate::config::BridgeConfig;
use crate::discord_sender::DiscordSender;
use crate::message::Message;
use crate::onebot_api::PendingMap;
use crate::qq_sender::QQSender;
use crate::sender::PlatformSender;
use crate::store::MessageStore;

#[derive(Clone)]
pub struct Bridge {
    senders: Arc<Vec<Box<dyn PlatformSender>>>,
    store: Arc<MessageStore>,
    group_key: String,
    qq_self_id: Arc<OnceLock<i64>>,
    discord_self_id: Arc<OnceLock<u64>>,
}

fn preview(content: &str, limit: usize) -> String {
    let end = content
        .char_indices()
        .take(limit)
        .last()
        .map_or(0, |(i, c)| i + c.len_utf8());
    let mut s = content[..end].to_string();
    if content.len() > s.len() {
        s.push_str("...");
    }
    s
}

impl Bridge {
    pub fn new(
        senders: Vec<Box<dyn PlatformSender>>,
        store: Arc<MessageStore>,
        group_key: String,
        qq_self_id: Arc<OnceLock<i64>>,
        discord_self_id: Arc<OnceLock<u64>>,
    ) -> Self {
        Self {
            senders: Arc::new(senders),
            store,
            group_key,
            qq_self_id,
            discord_self_id,
        }
    }

    // -- accessors -----------------------------------------------------------

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

    // -- forwarding ----------------------------------------------------------

    /// Forward an incoming message to all other platforms.
    /// Resolves reply mappings via the store, then calls each non-source sender.
    pub async fn forward(&self, msg: &dyn Message) {
        // Resolve reply target if this message is a reply
        let reply_record = msg.reply_to_msg_id().and_then(|rid| {
            self.store
                .query(&self.group_key, msg.source(), rid)
                .unwrap_or_else(|e| {
                    error!("store query failed: {e}");
                    None
                })
        });

        for sender in self.senders.iter() {
            if sender.platform() == msg.source() {
                continue;
            }

            let target_reply_id = reply_record
                .as_ref()
                .and_then(|r| r.id_on(sender.platform()))
                .map(String::from);

            match sender.send(msg, target_reply_id).await {
                Ok(dst_msg_id) => {
                    let _ = self.store.insert(
                        &self.group_key,
                        msg.source(),
                        msg.msg_id(),
                        sender.platform(),
                        &dst_msg_id,
                        msg.sender_name(),
                        &preview(msg.content(), 100),
                    );
                    info!(
                        "forwarded {:?} {} -> {:?} {}",
                        msg.source(),
                        msg.msg_id(),
                        sender.platform(),
                        dst_msg_id,
                    );
                }
                Err(e) => {
                    error!(
                        "forward {:?} -> {:?} failed: {e}",
                        msg.source(),
                        sender.platform(),
                    );
                }
            }
        }
    }
}

/// Container for multiple Bridge instances, keyed by platform channel/group ID.
pub struct Bridges {
    by_discord: HashMap<u64, Bridge>,
    by_qq: HashMap<i64, Bridge>,
}

impl Bridges {
    pub fn new(
        configs: &[BridgeConfig],
        discord_http_lock: &Arc<OnceLock<Arc<serenity::http::Http>>>,
        qq_tx: &UnboundedSender<String>,
        pending: &PendingMap,
        qq_self_id: &Arc<OnceLock<i64>>,
        discord_self_id: &Arc<OnceLock<u64>>,
        store: &Arc<MessageStore>,
    ) -> Self {
        let mut by_discord = HashMap::new();
        let mut by_qq = HashMap::new();

        for cfg in configs {
            let group_key = format!("d_{}:q_{}", cfg.discord_channel_id, cfg.qq_group_id);

            let senders: Vec<Box<dyn PlatformSender>> = vec![
                Box::new(DiscordSender {
                    http: discord_http_lock.clone(),
                    channel_id: cfg.discord_channel_id,
                }),
                Box::new(QQSender {
                    tx: qq_tx.clone(),
                    pending: pending.clone(),
                    group_id: cfg.qq_group_id,
                }),
            ];

            let bridge = Bridge::new(
                senders,
                store.clone(),
                group_key,
                qq_self_id.clone(),
                discord_self_id.clone(),
            );

            by_discord.insert(cfg.discord_channel_id, bridge.clone());
            by_qq.insert(cfg.qq_group_id, bridge);
        }

        Self { by_discord, by_qq }
    }

    pub fn by_discord(&self, channel_id: u64) -> Option<&Bridge> {
        self.by_discord.get(&channel_id)
    }

    pub fn by_qq(&self, group_id: i64) -> Option<&Bridge> {
        self.by_qq.get(&group_id)
    }

    pub fn is_self_discord(&self, user_id: u64) -> bool {
        self.by_discord
            .values()
            .next()
            .is_some_and(|b| b.is_self_discord(user_id))
    }

    pub fn is_self_qq(&self, user_id: i64) -> bool {
        self.by_qq
            .values()
            .next()
            .is_some_and(|b| b.is_self_qq(user_id))
    }

    pub fn set_qq_self_id(&self, id: i64) {
        for bridge in self.by_qq.values() {
            bridge.set_qq_self_id(id);
        }
    }

    pub fn set_discord_self_id(&self, id: u64) {
        for bridge in self.by_discord.values() {
            bridge.set_discord_self_id(id);
        }
    }
}
