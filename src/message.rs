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

/// Image or file attachment extracted from an incoming message.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Attachment {
    pub url: Option<String>,
    pub data: Option<Vec<u8>>,
    pub filename: String,
    pub content_type: Option<String>,
}

/// Platform-agnostic message trait.
///
/// Each platform implements this trait on its own struct, so platform-specific
/// fields (embeds, attachments, inline keyboards, etc.) can be added without
/// affecting other platforms.
pub trait Message: Sync {
    fn msg_id(&self) -> &str;
    fn sender_name(&self) -> &str;
    fn content(&self) -> &str;
    fn source(&self) -> Platform;
    fn reply_to_msg_id(&self) -> Option<&str> {
        None
    }
    fn attachments(&self) -> &[Attachment] {
        &[]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    QQ,
    Discord,
    Telegram,
}

pub struct QQMessage {
    pub msg_id: String,
    pub sender_name: String,
    pub content: String,
    pub reply_to_msg_id: Option<String>,
    pub attachments: Vec<Attachment>,
}

impl Message for QQMessage {
    fn msg_id(&self) -> &str {
        &self.msg_id
    }
    fn sender_name(&self) -> &str {
        &self.sender_name
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn source(&self) -> Platform {
        Platform::QQ
    }
    fn reply_to_msg_id(&self) -> Option<&str> {
        self.reply_to_msg_id.as_deref()
    }
    fn attachments(&self) -> &[Attachment] {
        &self.attachments
    }
}

pub struct DiscordMessage {
    pub msg_id: String,
    pub sender_name: String,
    pub content: String,
    pub reply_to_msg_id: Option<String>,
    pub attachments: Vec<Attachment>,
}

impl Message for DiscordMessage {
    fn msg_id(&self) -> &str {
        &self.msg_id
    }
    fn sender_name(&self) -> &str {
        &self.sender_name
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn source(&self) -> Platform {
        Platform::Discord
    }
    fn reply_to_msg_id(&self) -> Option<&str> {
        self.reply_to_msg_id.as_deref()
    }
    fn attachments(&self) -> &[Attachment] {
        &self.attachments
    }
}

pub struct TelegramMessage {
    pub msg_id: String,
    pub sender_name: String,
    pub content: String,
    pub reply_to_msg_id: Option<String>,
    pub attachments: Vec<Attachment>,
}

impl Message for TelegramMessage {
    fn msg_id(&self) -> &str {
        &self.msg_id
    }
    fn sender_name(&self) -> &str {
        &self.sender_name
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn source(&self) -> Platform {
        Platform::Telegram
    }
    fn reply_to_msg_id(&self) -> Option<&str> {
        self.reply_to_msg_id.as_deref()
    }
    fn attachments(&self) -> &[Attachment] {
        &self.attachments
    }
}

/// Resolved result of a reply lookup: maps one original message to IDs on all known platforms.
#[derive(Debug, Clone)]
pub struct ReplyRecord {
    pub original_sender: String,
    pub original_content_preview: String,
    pub original_platform: Platform,
    pub discord_msg_id: Option<String>,
    pub qq_msg_id: Option<String>,
    pub telegram_msg_id: Option<String>,
}

impl ReplyRecord {
    /// Get the message ID on the given platform, if known.
    pub fn id_on(&self, platform: Platform) -> Option<&str> {
        match platform {
            Platform::Discord => self.discord_msg_id.as_deref(),
            Platform::QQ => self.qq_msg_id.as_deref(),
            Platform::Telegram => self.telegram_msg_id.as_deref(),
        }
    }
}
