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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnemoneBotError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("tls: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serenity: {0}")]
    Serenity(String),
    #[error("channel: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<String>),
    #[error("websocket: {0}")]
    WebSocket(String),
    #[error("config: {0}")]
    Config(String),
    #[error("env: {0}")]
    Env(String),
}
