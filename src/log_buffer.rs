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

use std::io;

use tokio::sync::broadcast;
use tracing_subscriber::fmt::MakeWriter;

/// Ring buffer of formatted log lines, backed by a broadcast channel.
/// Late subscribers receive whatever history is still in the buffer.
/// Use `BroadcastWriter` with `tracing_subscriber::fmt::Layer` to feed it.
pub struct LogRing {
    tx: broadcast::Sender<String>,
}

impl LogRing {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Clone of the broadcast sender, for creating `BroadcastWriter`.
    pub fn sender(&self) -> broadcast::Sender<String> {
        self.tx.clone()
    }

    /// Receive end for SSE streaming.
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

/// An `io::Write` that forwards each write to a broadcast channel.
/// Use with `tracing_subscriber::fmt::Layer::with_writer` so the log format
/// matches the stderr output exactly.
#[derive(Clone)]
pub struct BroadcastWriter {
    tx: broadcast::Sender<String>,
}

impl BroadcastWriter {
    pub fn new(tx: broadcast::Sender<String>) -> Self {
        Self { tx }
    }
}

impl io::Write for BroadcastWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let line = String::from_utf8_lossy(buf).to_string();
        // Non-blocking; drop silently if no room or no receivers
        let _ = self.tx.send(line);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BroadcastWriter {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
