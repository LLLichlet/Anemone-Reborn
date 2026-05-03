use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, Mutex};

use crate::error::AnemoneBotError;

pub type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>;

pub struct Api {
    tx: UnboundedSender<String>,
    pending: PendingMap,
}

static ECHO_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_echo() -> String {
    format!("anemone_{}", ECHO_COUNTER.fetch_add(1, Ordering::Relaxed))
}

impl Api {
    pub fn new(tx: UnboundedSender<String>, pending: PendingMap) -> Self {
        Self { tx, pending }
    }

    async fn call(
        &self,
        action: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, AnemoneBotError> {
        let echo = next_echo();
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.lock().await.insert(echo.clone(), resp_tx);

        let payload = serde_json::json!({
            "action": action,
            "params": params,
            "echo": echo,
        });
        let text = serde_json::to_string(&payload)?;
        self.tx.send(text)?;

        let resp = tokio::time::timeout(tokio::time::Duration::from_secs(10), resp_rx)
            .await
            .map_err(|_| AnemoneBotError::WebSocket("onebot api timeout".into()))?
            .map_err(|_| AnemoneBotError::WebSocket("onebot response sender dropped".into()))?;

        Ok(resp)
    }

    pub async fn send_group_msg(
        &self,
        group_id: i64,
        message: &str,
    ) -> Result<i64, AnemoneBotError> {
        let resp = self
            .call(
                "send_group_msg",
                &serde_json::json!({
                    "group_id": group_id,
                    "message": message,
                }),
            )
            .await?;

        resp["data"]["message_id"].as_i64().ok_or_else(|| {
            AnemoneBotError::WebSocket("missing message_id in onebot response".into())
        })
    }
}
