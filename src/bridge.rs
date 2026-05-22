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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, warn};

use crate::config::BridgeConfig;
use crate::discord_sender::DiscordSender;
use crate::matrix_sender::MatrixSender;
use crate::message::{Message, Platform};
use crate::onebot_api::PendingMap;
use crate::qq_sender::QQSender;
use crate::sender::PlatformSender;
use crate::store::MessageStore;
use crate::telegram_sender::TelegramSender;

// -- per-platform shared context ----------------------------------------------

pub struct DiscordContext {
    pub http_lock: Arc<OnceLock<Arc<serenity::http::Http>>>,
    pub self_id: Arc<OnceLock<u64>>,
    pub token: String,
    pub proxy: Option<String>,
    pub http: reqwest::Client,
}

pub struct QQContext {
    pub tx: UnboundedSender<String>,
    pub pending: PendingMap,
    pub self_id: Arc<OnceLock<i64>>,
    pub http: reqwest::Client,
}

pub struct TelegramContext {
    pub http: reqwest::Client,
    pub self_id: Arc<OnceLock<i64>>,
    pub token: String,
}

pub struct MatrixContext {
    pub http: reqwest::Client,
    pub self_id: Arc<OnceLock<String>>,
    pub token: String,
    pub homeserver_url: String,
}

// -- Bridge -------------------------------------------------------------------

#[derive(Clone)]
pub struct Bridge {
    senders: Arc<Vec<Box<dyn PlatformSender>>>,
    store: Arc<MessageStore>,
    group_key: String,
    qq_self_id: Arc<OnceLock<i64>>,
    discord_self_id: Arc<OnceLock<u64>>,
    telegram_self_id: Arc<OnceLock<i64>>,
    matrix_self_id: Arc<OnceLock<String>>,
    ready: Arc<AtomicBool>,
    suppressed_recalls: Arc<Mutex<HashMap<(Platform, String), Instant>>>,
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
        telegram_self_id: Arc<OnceLock<i64>>,
        matrix_self_id: Arc<OnceLock<String>>,
        ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            senders: Arc::new(senders),
            store,
            group_key,
            qq_self_id,
            discord_self_id,
            telegram_self_id,
            matrix_self_id,
            ready,
            suppressed_recalls: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // -- accessors -----------------------------------------------------------

    pub fn set_qq_self_id(&self, id: i64) {
        let _ = self.qq_self_id.set(id);
    }

    pub fn set_discord_self_id(&self, id: u64) {
        let _ = self.discord_self_id.set(id);
    }

    pub fn set_telegram_self_id(&self, id: i64) {
        let _ = self.telegram_self_id.set(id);
    }

    pub fn set_matrix_self_id(&self, id: String) {
        let _ = self.matrix_self_id.set(id);
    }

    pub fn is_self_qq(&self, user_id: i64) -> bool {
        self.qq_self_id.get() == Some(&user_id)
    }

    pub fn is_self_discord(&self, user_id: u64) -> bool {
        self.discord_self_id.get() == Some(&user_id)
    }

    pub fn is_self_telegram(&self, user_id: i64) -> bool {
        self.telegram_self_id.get() == Some(&user_id)
    }

    pub fn is_self_matrix(&self, user_id: &str) -> bool {
        self.matrix_self_id.get().map(String::as_str) == Some(user_id)
    }

    pub fn is_source_message(&self, platform: Platform, msg_id: &str) -> bool {
        self.store
            .is_source_message(&self.group_key, platform, msg_id)
            .unwrap_or(false)
    }

    pub fn suppress_recall(&self, platform: Platform, msg_id: &str) {
        let mut guard = self.suppressed_recalls.lock().unwrap();
        guard.insert((platform, msg_id.to_string()), Instant::now());
    }

    pub fn take_suppressed_recall(&self, platform: Platform, msg_id: &str) -> bool {
        let mut guard = self.suppressed_recalls.lock().unwrap();
        guard.remove(&(platform, msg_id.to_string())).is_some()
    }

    pub fn prune_suppressed_recalls(&self, max_age: std::time::Duration) {
        let mut guard = self.suppressed_recalls.lock().unwrap();
        guard.retain(|_, created_at| created_at.elapsed() <= max_age);
    }

    // -- forwarding ----------------------------------------------------------

    /// Forward an incoming message to all other platforms.
    /// Resolves reply mappings via the store, then calls each non-source sender.
    pub async fn forward(&self, msg: &dyn Message) {
        if !self.ready.load(Ordering::Acquire) {
            info!(
                "not all platforms ready yet, dropping message from {:?}",
                msg.source()
            );
            return;
        }

        if self
            .store
            .is_recalled(&self.group_key, msg.source(), msg.msg_id())
            .unwrap_or(false)
        {
            info!(
                "source message already recalled, skipping forward for {:?} {}",
                msg.source(),
                msg.msg_id()
            );
            return;
        }

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

            if self
                .store
                .is_recalled(&self.group_key, msg.source(), msg.msg_id())
                .unwrap_or(false)
            {
                info!(
                    "source message recalled during forward, stopping fanout for {:?} {}",
                    msg.source(),
                    msg.msg_id()
                );
                return;
            }

            let target_reply_id = reply_record
                .as_ref()
                .and_then(|r| r.id_on(sender.platform()))
                .map(String::from);

            match sender.send(msg, target_reply_id).await {
                Ok(dst_msg_id) => {
                    if self
                        .store
                        .is_recalled(&self.group_key, msg.source(), msg.msg_id())
                        .unwrap_or(false)
                    {
                        self.suppress_recall(sender.platform(), &dst_msg_id);
                        if let Err(e) = sender.delete_message(&dst_msg_id).await {
                            let _ = self.take_suppressed_recall(sender.platform(), &dst_msg_id);
                            warn!(
                                "late recall cleanup {:?} {} -> {:?} {} failed: {e}",
                                msg.source(),
                                msg.msg_id(),
                                sender.platform(),
                                dst_msg_id,
                            );
                        }
                    }

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

    /// Try to delete every bridged copy of a message.
    pub async fn recall(&self, source_platform: Platform, msg_id: &str) {
        let _ = self
            .store
            .mark_recalled(&self.group_key, source_platform, msg_id);

        let related = match self.store.related_message_ids(&self.group_key, source_platform, msg_id) {
            Ok(ids) => ids,
            Err(e) => {
                error!("store lookup for recall failed: {e}");
                return;
            }
        };

        for (platform, target_msg_id) in related {
            if platform == source_platform && target_msg_id == msg_id {
                continue;
            }

            let Some(sender) = self.senders.iter().find(|sender| sender.platform() == platform) else {
                continue;
            };

            self.suppress_recall(platform, &target_msg_id);
            if let Err(e) = sender.delete_message(&target_msg_id).await {
                let _ = self.take_suppressed_recall(platform, &target_msg_id);
                warn!(
                    "recall {:?} {} -> {:?} {} failed: {e}",
                    source_platform,
                    msg_id,
                    platform,
                    target_msg_id,
                );
            } else {
                info!(
                    "recalled {:?} {} -> {:?} {}",
                    source_platform,
                    msg_id,
                    platform,
                    target_msg_id,
                );
            }
        }
    }
}

// -- Bridges ------------------------------------------------------------------

/// Container for multiple Bridge instances, keyed by platform channel/group ID.
#[allow(clippy::struct_field_names)]
pub struct Bridges {
    by_discord: HashMap<u64, Bridge>,
    by_qq: HashMap<i64, Bridge>,
    by_telegram: HashMap<i64, Bridge>,
    by_matrix: HashMap<String, Bridge>,
    ready: Arc<AtomicBool>,
    disc_self_id: Option<Arc<OnceLock<u64>>>,
    qq_self_id: Option<Arc<OnceLock<i64>>>,
    tg_self_id: Option<Arc<OnceLock<i64>>>,
    matrix_self_id: Option<Arc<OnceLock<String>>>,
}

impl Bridges {
    #[allow(clippy::similar_names)]
    pub fn new(
        configs: &[BridgeConfig],
        discord: Option<&DiscordContext>,
        qq: Option<&QQContext>,
        telegram: Option<&TelegramContext>,
        matrix: Option<&MatrixContext>,
        store: &Arc<MessageStore>,
    ) -> Self {
        let mut by_discord = HashMap::new();
        let mut by_qq = HashMap::new();
        let mut by_telegram = HashMap::new();
        let mut by_matrix = HashMap::new();

        let ready = Arc::new(AtomicBool::new(false));

        let (empty_i64, empty_u64, empty_string): (
            Arc<OnceLock<i64>>,
            Arc<OnceLock<u64>>,
            Arc<OnceLock<String>>,
        ) = (
            Arc::new(OnceLock::new()),
            Arc::new(OnceLock::new()),
            Arc::new(OnceLock::new()),
        );

        let disc_self_id = discord.map(|c| c.self_id.clone());
        let qq_self_id = qq.map(|c| c.self_id.clone());
        let tg_self_id = telegram.map(|c| c.self_id.clone());
        let matrix_self_id = matrix.map(|c| c.self_id.clone());

        let d_id = disc_self_id.as_ref().map_or(&empty_u64, |l| l);
        let q_id = qq_self_id.as_ref().map_or(&empty_i64, |l| l);
        let t_id = tg_self_id.as_ref().map_or(&empty_i64, |l| l);
        let m_id = matrix_self_id.as_ref().map_or(&empty_string, |l| l);

        for (i, cfg) in configs.iter().enumerate() {
            let mut parts: Vec<String> = Vec::new();
            let mut senders: Vec<Box<dyn PlatformSender>> = Vec::new();

            if let (Some(dc), Some(ch)) = (discord, cfg.discord_channel_id) {
                parts.push(format!("d_{ch}"));
                senders.push(Box::new(DiscordSender {
                    http: dc.http_lock.clone(),
                    reqwest: dc.http.clone(),
                    channel_id: ch,
                }));
            }
            if let (Some(qc), Some(grp)) = (qq, cfg.qq_group_id) {
                parts.push(format!("q_{grp}"));
                senders.push(Box::new(QQSender {
                    tx: qc.tx.clone(),
                    pending: qc.pending.clone(),
                    reqwest: qc.http.clone(),
                    group_id: grp,
                }));
            }
            if let (Some(tc), Some(chat)) = (telegram, cfg.telegram_group_id) {
                parts.push(format!("t_{chat}"));
                senders.push(Box::new(TelegramSender {
                    http: tc.http.clone(),
                    token: tc.token.clone(),
                    chat_id: chat,
                }));
            }
            if let (Some(mc), Some(room)) = (matrix, cfg.matrix_room_id.as_ref()) {
                parts.push(format!("m_{room}"));
                senders.push(Box::new(MatrixSender {
                    http: mc.http.clone(),
                    token: mc.token.clone(),
                    homeserver_url: mc.homeserver_url.clone(),
                    room_id: room.clone(),
                }));
            }

            if senders.len() < 2 {
                warn!(
                    "bridge [{i}] has only {} platform(s); at least 2 needed for forwarding",
                    senders.len()
                );
            }

            let group_key = parts.join(":");

            let bridge = Bridge::new(
                senders,
                store.clone(),
                group_key,
                q_id.clone(),
                d_id.clone(),
                t_id.clone(),
                m_id.clone(),
                ready.clone(),
            );

            if let Some(ch) = cfg.discord_channel_id {
                by_discord.insert(ch, bridge.clone());
            }
            if let Some(grp) = cfg.qq_group_id {
                by_qq.insert(grp, bridge.clone());
            }
            if let Some(chat) = cfg.telegram_group_id {
                by_telegram.insert(chat, bridge.clone());
            }
            if let Some(room) = cfg.matrix_room_id.as_ref() {
                by_matrix.insert(room.clone(), bridge);
            }
        }

        Self {
            by_discord,
            by_qq,
            by_telegram,
            by_matrix,
            ready,
            disc_self_id,
            qq_self_id,
            tg_self_id,
            matrix_self_id,
        }
    }

    fn check_ready(&self) {
        let ok = self.disc_self_id.as_ref().is_none_or(|l| l.get().is_some())
            && self.qq_self_id.as_ref().is_none_or(|l| l.get().is_some())
            && self.tg_self_id.as_ref().is_none_or(|l| l.get().is_some())
            && self
                .matrix_self_id
                .as_ref()
                .is_none_or(|l| l.get().is_some());
        if ok {
            self.ready.store(true, Ordering::Release);
        }
    }

    pub fn by_discord(&self, channel_id: u64) -> Option<&Bridge> {
        self.by_discord.get(&channel_id)
    }

    pub fn by_qq(&self, group_id: i64) -> Option<&Bridge> {
        self.by_qq.get(&group_id)
    }

    pub fn by_telegram(&self, chat_id: i64) -> Option<&Bridge> {
        self.by_telegram.get(&chat_id)
    }

    pub fn by_matrix(&self, room_id: &str) -> Option<&Bridge> {
        self.by_matrix.get(room_id)
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

    pub fn is_self_telegram(&self, user_id: i64) -> bool {
        self.by_telegram
            .values()
            .next()
            .is_some_and(|b| b.is_self_telegram(user_id))
    }

    pub fn is_self_matrix(&self, user_id: &str) -> bool {
        self.by_matrix
            .values()
            .next()
            .is_some_and(|b| b.is_self_matrix(user_id))
    }

    pub fn prune_recall_state(&self, max_age: std::time::Duration) {
        for bridge in self.by_discord.values() {
            bridge.prune_suppressed_recalls(max_age);
        }
        for bridge in self.by_qq.values() {
            bridge.prune_suppressed_recalls(max_age);
        }
        for bridge in self.by_telegram.values() {
            bridge.prune_suppressed_recalls(max_age);
        }
        for bridge in self.by_matrix.values() {
            bridge.prune_suppressed_recalls(max_age);
        }
    }

    pub fn set_qq_self_id(&self, id: i64) {
        if let Some(bridge) = self.by_qq.values().next() {
            bridge.set_qq_self_id(id);
        }
        self.check_ready();
    }

    pub fn set_discord_self_id(&self, id: u64) {
        if let Some(bridge) = self.by_discord.values().next() {
            bridge.set_discord_self_id(id);
        }
        self.check_ready();
    }

    pub fn set_telegram_self_id(&self, id: i64) {
        if let Some(bridge) = self.by_telegram.values().next() {
            bridge.set_telegram_self_id(id);
        }
        self.check_ready();
    }

    pub fn set_matrix_self_id(&self, id: String) {
        if let Some(bridge) = self.by_matrix.values().next() {
            bridge.set_matrix_self_id(id);
        }
        self.check_ready();
    }
}
