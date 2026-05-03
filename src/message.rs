/// Platform-agnostic message trait.
///
/// Each platform (QQ, Discord, Telegram, etc.) has its own struct implementing this trait.
/// The bridge routes `&dyn Message` to the opposite platform based on `source()`.
///
/// When a new platform adds fields (e.g. Discord embeds, Telegram inline keyboards),
/// only that platform's struct changes — the trait stays stable.
pub trait Message: Sync {
    fn sender_name(&self) -> &str;
    fn content(&self) -> &str;
    fn source(&self) -> Platform;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    QQ,
    Discord,
}

pub struct QQMessage {
    pub sender_name: String,
    pub content: String,
}

impl Message for QQMessage {
    fn sender_name(&self) -> &str {
        &self.sender_name
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn source(&self) -> Platform {
        Platform::QQ
    }
}

pub struct DiscordMessage {
    pub sender_name: String,
    pub content: String,
}

impl Message for DiscordMessage {
    fn sender_name(&self) -> &str {
        &self.sender_name
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn source(&self) -> Platform {
        Platform::Discord
    }
}
