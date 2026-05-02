use tokio::sync::mpsc::UnboundedSender;

use crate::error::AnemoneBotError;

pub struct Api {
    tx: UnboundedSender<String>,
}

impl Api {
    pub fn new(tx: UnboundedSender<String>) -> Self {
        Self { tx }
    }

    fn call(&self, action: &str, params: &serde_json::Value) -> Result<(), AnemoneBotError> {
        let payload = serde_json::json!({
            "action": action,
            "params": params,
        });
        let text = serde_json::to_string(&payload)?;
        self.tx.send(text)?;
        Ok(())
    }

    pub fn send_group_msg(&self, group_id: i64, message: &str) -> Result<(), AnemoneBotError> {
        self.call(
            "send_group_msg",
            &serde_json::json!({
                "group_id": group_id,
                "message": message,
            }),
        )
    }
}
