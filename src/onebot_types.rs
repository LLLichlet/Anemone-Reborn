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
    pub user_id: i64,
    #[serde(default)]
    pub group_id: i64,
    #[serde(default, rename = "raw_message")]
    pub message: String,
    #[serde(default)]
    pub sender: Option<Sender>,
    // meta_event fields for capturing bot's own QQ id
    #[serde(default)]
    pub self_id: i64,
    #[serde(default)]
    pub meta_event_type: String,
    #[serde(default)]
    pub sub_type: String,
}
