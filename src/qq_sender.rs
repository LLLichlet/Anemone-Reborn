use async_trait::async_trait;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};
use crate::onebot_api::Api;
use crate::sender::PlatformSender;

pub struct QQSender {
    pub tx: tokio::sync::mpsc::UnboundedSender<String>,
    pub pending: crate::onebot_api::PendingMap,
    pub group_id: i64,
}

#[async_trait]
impl PlatformSender for QQSender {
    fn platform(&self) -> Platform {
        Platform::QQ
    }

    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError> {
        let prefix = match msg.source() {
            Platform::Discord => "[Discord]",
            Platform::QQ => unreachable!("bridge filters own platform"),
        };

        let mut text = format!("{prefix} {}: {}", msg.sender_name(), msg.content());

        if let Some(ref reply_id) = reply_to_msg_id {
            text = format!("[CQ:reply,id={reply_id}]{text}");
        }

        let api = Api::new(self.tx.clone(), self.pending.clone());
        let msg_id = api.send_group_msg(self.group_id, &text).await?;
        Ok(msg_id.to_string())
    }
}
