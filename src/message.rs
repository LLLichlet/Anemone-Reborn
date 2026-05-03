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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    QQ,
    Discord,
}

pub struct QQMessage {
    pub msg_id: String,
    pub sender_name: String,
    pub content: String,
    pub reply_to_msg_id: Option<String>,
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
}

pub struct DiscordMessage {
    pub msg_id: String,
    pub sender_name: String,
    pub content: String,
    pub reply_to_msg_id: Option<String>,
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
}

/// Resolved result of a reply lookup: maps one original message to IDs on all known platforms.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ReplyRecord {
    pub original_sender: String,
    pub original_content_preview: String,
    pub original_platform: Platform,
    pub discord_msg_id: Option<String>,
    pub qq_msg_id: Option<String>,
}

impl ReplyRecord {
    /// Get the message ID on the given platform, if known.
    pub fn id_on(&self, platform: Platform) -> Option<&str> {
        match platform {
            Platform::Discord => self.discord_msg_id.as_deref(),
            Platform::QQ => self.qq_msg_id.as_deref(),
        }
    }
}
