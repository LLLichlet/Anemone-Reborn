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

use std::sync::Arc;
use std::sync::OnceLock;

use tracing::{error, info};

use crate::message::Message;
use crate::sender::PlatformSender;
use crate::store::MessageStore;

#[derive(Clone)]
pub struct Bridge {
    senders: Arc<Vec<Box<dyn PlatformSender>>>,
    store: Arc<MessageStore>,
    group_key: String,
    qq_self_id: Arc<OnceLock<i64>>,
    discord_self_id: Arc<OnceLock<u64>>,
    discord_channel_id: u64,
    qq_group_id: i64,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        senders: Vec<Box<dyn PlatformSender>>,
        store: MessageStore,
        group_key: String,
        qq_self_id: Arc<OnceLock<i64>>,
        discord_self_id: Arc<OnceLock<u64>>,
        discord_channel_id: u64,
        qq_group_id: i64,
    ) -> Self {
        Self {
            senders: Arc::new(senders),
            store: Arc::new(store),
            group_key,
            qq_self_id,
            discord_self_id,
            discord_channel_id,
            qq_group_id,
        }
    }

    // -- accessors -----------------------------------------------------------

    pub fn discord_channel_id(&self) -> u64 {
        self.discord_channel_id
    }

    pub fn qq_group_id(&self) -> i64 {
        self.qq_group_id
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

    // -- forwarding ----------------------------------------------------------

    /// Forward an incoming message to all other platforms.
    /// Resolves reply mappings via the store, then calls each non-source sender.
    pub async fn forward(&self, msg: &dyn Message) {
        // Resolve reply target if this message is a reply
        let reply_record = msg.reply_to_msg_id().and_then(|rid| {
            match self.store.query(&self.group_key, msg.source(), rid) {
                Ok(r) => r,
                Err(e) => {
                    error!("store query failed: {e}");
                    None
                }
            }
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
