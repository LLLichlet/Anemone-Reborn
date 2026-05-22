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

    /// Try to delete a message on this platform.
    async fn delete_message(&self, msg_id: &str) -> Result<(), AnemoneBotError>;
}
