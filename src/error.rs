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
