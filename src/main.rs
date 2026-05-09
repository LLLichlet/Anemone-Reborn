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

mod bridge;
mod config;
mod discord_client;
mod discord_sender;
mod error;
mod message;
mod onebot_api;
mod onebot_types;
mod qq_client;
mod qq_sender;
mod sender;
mod store;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::bridge::Bridge;
use crate::config::BridgeConfig;
use crate::discord_sender::DiscordSender;
use crate::error::AnemoneBotError;
use crate::onebot_api::PendingMap;
use crate::onebot_types::Event;
use crate::qq_sender::QQSender;
use crate::store::MessageStore;

struct AppState {
    bridge: Bridge,
    qq_rx: Arc<Mutex<Option<UnboundedReceiver<String>>>>,
    pending: PendingMap,
    _bridges: Vec<BridgeConfig>,
}

#[tokio::main]
async fn main() -> Result<(), AnemoneBotError> {
    tracing_subscriber::fmt::init();

    let config = config::load()?;

    let first_bridge = config
        .bridges
        .first()
        .cloned()
        .ok_or_else(|| AnemoneBotError::Config("at least one [[bridges]] required".into()))?;

    let group_key = format!(
        "d_{}:q_{}",
        first_bridge.discord_channel_id, first_bridge.qq_group_id
    );

    // --- shared state -------------------------------------------------------
    let (qq_tx, qq_rx) = unbounded_channel::<String>();
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let discord_http_lock = Arc::new(OnceLock::new());
    let qq_self_id = Arc::new(OnceLock::new());
    let discord_self_id = Arc::new(OnceLock::new());

    // --- platform senders ---------------------------------------------------
    let senders: Vec<Box<dyn crate::sender::PlatformSender>> = vec![
        Box::new(DiscordSender {
            http: discord_http_lock.clone(),
            channel_id: first_bridge.discord_channel_id,
        }),
        Box::new(QQSender {
            tx: qq_tx.clone(),
            pending: pending.clone(),
            group_id: first_bridge.qq_group_id,
        }),
    ];

    // --- store --------------------------------------------------------------
    let store = MessageStore::new("anemone-bot.db")?;
    // Clean up records older than 7 days
    store.prune(604_800)?;

    // --- bridge -------------------------------------------------------------
    let bridge = Bridge::new(
        senders,
        store,
        group_key,
        qq_self_id.clone(),
        discord_self_id.clone(),
        first_bridge.discord_channel_id,
        first_bridge.qq_group_id,
    );

    // --- spawn Discord client -----------------------------------------------
    let bridge_for_discord = bridge.clone();
    let http_lock_for_discord = discord_http_lock.clone();
    let discord_token = config.discord_token.clone();
    let http_proxy = config.http_proxy.clone();
    tokio::spawn(async move {
        if let Err(e) = discord_client::run(
            bridge_for_discord,
            &discord_token,
            http_lock_for_discord,
            http_proxy.as_deref(),
        )
        .await
        {
            error!("discord client fatal: {e}");
        }
    });

    // --- axum ---------------------------------------------------------------
    let state = Arc::new(AppState {
        bridge,
        qq_rx: Arc::new(Mutex::new(Some(qq_rx))),
        pending,
        _bridges: config.bridges,
    });

    let app = Router::new()
        .route("/onebot/v11/ws", get(ws_handler))
        .with_state(state);

    info!("bot ws server listening on {}", config.bind_addr);
    let listener = TcpListener::bind(&config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    let bridge = state.bridge.clone();
    let qq_rx = state
        .qq_rx
        .lock()
        .await
        .take()
        .expect("qq_rx already consumed; only one WS connection expected");
    let pending = state.pending.clone();

    ws.on_upgrade(move |socket| handle_socket(socket, bridge, qq_rx, pending))
}

async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    bridge: Bridge,
    mut qq_rx: UnboundedReceiver<String>,
    pending: PendingMap,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Write task: forward queued OneBot actions to NapCat over WS
    let write_task = tokio::spawn(async move {
        while let Some(text) = qq_rx.recv().await {
            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop: receive OneBot events and API responses from NapCat
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let text = text.to_string();

                // Try parsing as generic JSON to check for API response (echo)
                let value: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("failed to parse ws message: {e}");
                        continue;
                    }
                };

                // Dispatch API responses (echo + status fields present)
                if let Some(echo) = value.get("echo").and_then(|e| e.as_str()) {
                    if value.get("status").is_some() {
                        if let Some(tx) = pending.lock().await.remove(echo) {
                            let _ = tx.send(value);
                        }
                        continue;
                    }
                }

                // Parse as OneBot event
                match serde_json::from_value::<Event>(value) {
                    Ok(event) if event.post_type == "message" => {
                        info!(
                            "qq message from {} ({}): {}",
                            event.user_id, event.message_type, event.message
                        );
                        qq_client::handle_message(event, &bridge).await;
                    }
                    Ok(event)
                        if event.post_type == "meta_event"
                            && event.meta_event_type == "lifecycle"
                            && event.sub_type == "connect" =>
                    {
                        info!("napcat connected, self_id = {}", event.self_id);
                        bridge.set_qq_self_id(event.self_id);
                    }
                    Ok(_) => {}
                    Err(e) => error!("failed to parse event: {e}"),
                }
            }
            Ok(Message::Close(_)) => {
                info!("napcat disconnected");
                break;
            }
            Err(e) => {
                error!("ws recv error: {e}");
                break;
            }
            _ => {}
        }
    }
    write_task.abort();
}
