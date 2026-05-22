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

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::error::AnemoneBotError;
use crate::message::{Platform, ReplyRecord};

/// SQLite-backed bidirectional message-ID mapping.
///
/// `Connection` is wrapped in a `Mutex` because rusqlite's `Connection` is not
/// `Sync` (it uses `RefCell` internally). All public methods lock the mutex
/// for the duration of the call.
pub struct MessageStore {
    conn: Mutex<Connection>,
}

impl MessageStore {
    pub fn new(db_path: &str) -> Result<Self, AnemoneBotError> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS message_routes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                group_key  TEXT NOT NULL,
                src_plat   TEXT NOT NULL,
                src_msg_id TEXT NOT NULL,
                dst_plat   TEXT NOT NULL,
                dst_msg_id TEXT NOT NULL,
                sender     TEXT NOT NULL,
                preview    TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS idx_src_lookup
                ON message_routes(group_key, src_plat, src_msg_id);
            CREATE INDEX IF NOT EXISTS idx_dst_lookup
                ON message_routes(group_key, dst_plat, dst_msg_id);",
        )?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS recalled_messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                group_key  TEXT NOT NULL,
                plat       TEXT NOT NULL,
                msg_id     TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS idx_recalled_lookup
                ON recalled_messages(group_key, plat, msg_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_recalled_unique
                ON recalled_messages(group_key, plat, msg_id);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn plat_str(p: Platform) -> &'static str {
        match p {
            Platform::QQ => "qq",
            Platform::Discord => "discord",
            Platform::Telegram => "telegram",
            Platform::Matrix => "matrix",
        }
    }

    fn plat_from_str(s: &str) -> Platform {
        match s {
            "discord" => Platform::Discord,
            "telegram" => Platform::Telegram,
            "matrix" => Platform::Matrix,
            _ => Platform::QQ,
        }
    }

    /// Look up a message by its source platform + message ID.
    /// Searches both `src_plat` and `dst_plat` columns.
    pub fn query(
        &self,
        group_key: &str,
        platform: Platform,
        msg_id: &str,
    ) -> Result<Option<ReplyRecord>, AnemoneBotError> {
        let conn = self.conn.lock().unwrap();
        let plat = Self::plat_str(platform);
        let mut stmt = conn.prepare(
            "SELECT sender, preview, src_plat, src_msg_id, dst_plat, dst_msg_id
             FROM message_routes
             WHERE group_key = ?1
               AND ((src_plat = ?2 AND src_msg_id = ?3)
                 OR (dst_plat = ?2 AND dst_msg_id = ?3))
             ORDER BY id DESC
             LIMIT 1",
        )?;

        let mut rows = stmt.query_map(params![group_key, plat, msg_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let Some(row) = rows.next() else {
            return Ok(None);
        };

        let (sender, preview, src_plat_str, src_msg, dst_plat_str, dst_msg) = row?;

        let mut discord_id = None;
        let mut qq_id = None;
        let mut telegram_id = None;
        let mut matrix_id = None;

        let mut set = |plat: &str, id: String, is_src: bool| {
            if is_src {
                match plat {
                    "discord" => discord_id = Some(id),
                    "qq" => qq_id = Some(id),
                    "telegram" => telegram_id = Some(id),
                    "matrix" => matrix_id = Some(id),
                    _ => {}
                }
            }
        };
        set(src_plat_str.as_str(), src_msg, true);
        set(dst_plat_str.as_str(), dst_msg, true);

        Ok(Some(ReplyRecord {
            original_sender: sender,
            original_content_preview: preview,
            original_platform: Self::plat_from_str(&src_plat_str),
            discord_msg_id: discord_id,
            qq_msg_id: qq_id,
            telegram_msg_id: telegram_id,
            matrix_msg_id: matrix_id,
        }))
    }

    /// Collect all linked message IDs across platforms for a bridged message.
    ///
    /// This walks the route table until no new endpoint IDs are discovered,
    /// so recalls can fan out to every bridged copy of the same message.
    pub fn related_message_ids(
        &self,
        group_key: &str,
        platform: Platform,
        msg_id: &str,
    ) -> Result<Vec<(Platform, String)>, AnemoneBotError> {
        let conn = self.conn.lock().unwrap();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut queue: VecDeque<(String, String)> = VecDeque::new();
        queue.push_back((Self::plat_str(platform).to_string(), msg_id.to_string()));

        while let Some((plat, id)) = queue.pop_front() {
            if !seen.insert((plat.clone(), id.clone())) {
                continue;
            }

                        let mut stmt = conn.prepare(
                "SELECT src_plat, src_msg_id, dst_plat, dst_msg_id
                 FROM message_routes
                 WHERE group_key = ?1
                   AND ((src_plat = ?2 AND src_msg_id = ?3)
                     OR (dst_plat = ?2 AND dst_msg_id = ?3))",
            )?;

            let rows = stmt.query_map(params![group_key, plat, id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?;

            for row in rows {
                let (src_plat, src_msg_id, dst_plat, dst_msg_id) = row?;
                if !seen.contains(&(src_plat.clone(), src_msg_id.clone())) {
                    queue.push_back((src_plat, src_msg_id));
                }
                if !seen.contains(&(dst_plat.clone(), dst_msg_id.clone())) {
                    queue.push_back((dst_plat, dst_msg_id));
                }
            }
        }

        Ok(seen
            .into_iter()
            .map(|(plat, id)| (Self::plat_from_str(&plat), id))
            .collect())
    }

    /// Remember that a message was already recalled on its source platform.
    /// This lets forwarders short-circuit even if the source delete event
    /// arrives before the original message has been written to `message_routes`.
    pub fn mark_recalled(
        &self,
        group_key: &str,
        platform: Platform,
        msg_id: &str,
    ) -> Result<(), AnemoneBotError> {
        self.conn.lock().unwrap().execute(
            "INSERT OR REPLACE INTO recalled_messages (group_key, plat, msg_id)
             VALUES (?1, ?2, ?3)",
            params![group_key, Self::plat_str(platform), msg_id],
        )?;
        Ok(())
    }

    /// Check whether a message was already recalled on its source platform.
    pub fn is_recalled(
        &self,
        group_key: &str,
        platform: Platform,
        msg_id: &str,
    ) -> Result<bool, AnemoneBotError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT 1 FROM recalled_messages
             WHERE group_key = ?1 AND plat = ?2 AND msg_id = ?3
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![group_key, Self::plat_str(platform), msg_id], |_| {
            Ok(())
        })?;
        Ok(rows.next().is_some())
    }

    /// Check whether a message ID belongs to the source side of a bridge.
    pub fn is_source_message(
        &self,
        group_key: &str,
        platform: Platform,
        msg_id: &str,
    ) -> Result<bool, AnemoneBotError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT 1 FROM message_routes
             WHERE group_key = ?1 AND src_plat = ?2 AND src_msg_id = ?3
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![group_key, Self::plat_str(platform), msg_id], |_| {
            Ok(())
        })?;
        Ok(rows.next().is_some())
    }

    /// Remove recall markers older than `retention_secs`.
    pub fn prune_recalled(&self, retention_secs: i64) -> Result<(), AnemoneBotError> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM recalled_messages WHERE created_at < unixepoch() - ?1",
            params![retention_secs],
        )?;
        Ok(())
    }

    /// Record a forwarded message mapping.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &self,
        group_key: &str,
        src_plat: Platform,
        src_msg_id: &str,
        dst_plat: Platform,
        dst_msg_id: &str,
        sender: &str,
        preview: &str,
    ) -> Result<(), AnemoneBotError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO message_routes
                (group_key, src_plat, src_msg_id, dst_plat, dst_msg_id, sender, preview)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                group_key,
                Self::plat_str(src_plat),
                src_msg_id,
                Self::plat_str(dst_plat),
                dst_msg_id,
                sender,
                preview,
            ],
        )?;
        Ok(())
    }

    /// Remove records older than `retention_secs`.
    pub fn prune(&self, retention_secs: i64) -> Result<(), AnemoneBotError> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM message_routes WHERE created_at < unixepoch() - ?1",
            params![retention_secs],
        )?;
        Ok(())
    }
}
