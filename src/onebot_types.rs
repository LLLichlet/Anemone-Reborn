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

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Sender {
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub card: String,
}

#[derive(Debug, Deserialize)]
pub struct Event {
    #[serde(default)]
    pub post_type: String,
    #[serde(default)]
    pub message_type: String,
    #[serde(default)]
    pub message_id: i64,
    #[serde(default)]
    pub user_id: i64,
    #[serde(default)]
    pub group_id: i64,
    #[serde(default, rename = "raw_message")]
    pub message: String,
    #[serde(default)]
    pub sender: Option<Sender>,
    // reply info
    #[serde(default)]
    pub reply: Option<ReplyInfo>,
    // meta_event fields for capturing bot's own QQ id
    #[serde(default)]
    pub self_id: i64,
    #[serde(default)]
    pub meta_event_type: String,
    #[serde(default)]
    pub sub_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ReplyInfo {
    pub message_id: i64,
}
