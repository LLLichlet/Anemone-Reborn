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
mod proxy;
mod qq_client;
mod qq_sender;
mod sender;
mod store;
mod telegram_client;
mod telegram_sender;

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

use crate::bridge::{Bridges, DiscordContext, QQContext, TelegramContext};
use crate::error::AnemoneBotError;
use crate::onebot_api::PendingMap;
use crate::onebot_types::Event;
use crate::proxy::build_reqwest_client;
use crate::store::MessageStore;

struct AppState {
    bridges: Arc<Bridges>,
    qq_rx: Arc<Mutex<Option<UnboundedReceiver<String>>>>,
    pending: PendingMap,
}

#[tokio::main]
async fn main() -> Result<(), AnemoneBotError> {
    tracing_subscriber::fmt::init();

    let config = config::load()?;

    if config.bridges.is_empty() {
        return Err(AnemoneBotError::Config(
            "at least one [[bridges]] required".into(),
        ));
    }

    // --- store --------------------------------------------------------------
    let store = Arc::new(MessageStore::new("anemone-bot.db")?);
    store.prune(604_800)?;

    // --- per-platform contexts ----------------------------------------------
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

    let (qq_ctx, qq_rx) = if config.bind_addr.is_some() {
        let (tx, rx) = unbounded_channel::<String>();
        let ctx = QQContext {
            tx,
            pending: pending.clone(),
            self_id: Arc::new(OnceLock::new()),
        };
        (Some(ctx), Some(rx))
    } else {
        (None, None)
    };

    let discord_ctx = config.discord_token.as_ref().map(|token| DiscordContext {
        http_lock: Arc::new(OnceLock::new()),
        self_id: Arc::new(OnceLock::new()),
        token: token.clone(),
        proxy: config.http_proxy.clone(),
    });

    let telegram_ctx = config
        .telegram_token
        .as_ref()
        .map(|token| -> Result<_, AnemoneBotError> {
            Ok(TelegramContext {
                http: build_reqwest_client(config.http_proxy.as_deref())?,
                self_id: Arc::new(OnceLock::new()),
                token: token.clone(),
            })
        })
        .transpose()?;

    // --- bridges ------------------------------------------------------------
    let bridges = Arc::new(Bridges::new(
        &config.bridges,
        discord_ctx.as_ref(),
        qq_ctx.as_ref(),
        telegram_ctx.as_ref(),
        &store,
    ));

    // --- spawn Discord client -----------------------------------------------
    if let Some(ref dc) = discord_ctx {
        let bridges_for_discord = bridges.clone();
        let ctx = DiscordContext {
            http_lock: dc.http_lock.clone(),
            self_id: dc.self_id.clone(),
            token: dc.token.clone(),
            proxy: dc.proxy.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = discord_client::run(bridges_for_discord, &ctx).await {
                error!("discord client fatal: {e}");
            }
        });
    }

    // --- spawn Telegram client ----------------------------------------------
    if let Some(ref tc) = telegram_ctx {
        let bridges_for_tg = bridges.clone();
        let ctx = TelegramContext {
            http: tc.http.clone(),
            self_id: tc.self_id.clone(),
            token: tc.token.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = telegram_client::run(bridges_for_tg, &ctx).await {
                error!("telegram client fatal: {e}");
            }
        });
    }

    // --- axum (QQ) ----------------------------------------------------------
    if let (Some(bind_addr), Some(rx)) = (config.bind_addr, qq_rx) {
        let state = Arc::new(AppState {
            bridges,
            qq_rx: Arc::new(Mutex::new(Some(rx))),
            pending,
        });

        let app = Router::new()
            .route("/onebot/v11/ws", get(ws_handler))
            .with_state(state);

        info!("bot ws server listening on {}", bind_addr);
        let listener = TcpListener::bind(&bind_addr).await?;
        axum::serve(listener, app).await?;
    }

    // All platforms omitted — wait forever (signals will still work)
    info!("no servers to bind; idle");
    tokio::signal::ctrl_c().await.ok();
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    let bridges = state.bridges.clone();
    let qq_rx = state
        .qq_rx
        .lock()
        .await
        .take()
        .expect("qq_rx already consumed; only one WS connection expected");
    let pending = state.pending.clone();

    ws.on_upgrade(move |socket| handle_socket(socket, bridges, qq_rx, pending))
}

async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    bridges: Arc<Bridges>,
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
                        qq_client::handle_message(event, &bridges).await;
                    }
                    Ok(event)
                        if event.post_type == "meta_event"
                            && event.meta_event_type == "lifecycle"
                            && event.sub_type == "connect" =>
                    {
                        info!("napcat connected, self_id = {}", event.self_id);
                        bridges.set_qq_self_id(event.self_id);
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
