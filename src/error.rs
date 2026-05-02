use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnemoneBotError {
    #[error("channel send error: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<String>),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
