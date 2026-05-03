use async_trait::async_trait;

use crate::error::AnemoneBotError;
use crate::message::{Message, Platform};

/// Abstracts sending a message to a specific platform.
///
/// The Bridge holds a collection of `PlatformSender` implementations and calls
/// `send()` on each one whose `platform()` does not match the message's source.
/// This eliminates the O(n²) `match source → target` branching: N platforms
/// produce N `PlatformSender` impls, and the Bridge loop stays O(N).
#[async_trait]
pub trait PlatformSender: Send + Sync {
    fn platform(&self) -> Platform;

    /// Send a message to this platform.
    ///
    /// `reply_to_msg_id` is an optional target platform message ID for native
    /// reply threading (resolved from the store by the Bridge).
    ///
    /// Returns the message ID assigned by the target platform on success.
    async fn send(
        &self,
        msg: &dyn Message,
        reply_to_msg_id: Option<String>,
    ) -> Result<String, AnemoneBotError>;
}
