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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::bridge::{Bridges, DiscordContext, MatrixContext, QQContext, TelegramContext};
use crate::config::{self, AppConfig};
use crate::error::AnemoneBotError;
use crate::onebot_api::PendingMap;
use crate::onebot_types::Event;
use crate::proxy::build_reqwest_client;
use crate::store::MessageStore;

// -- Bot status ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct BotStatus {
    pub running: bool,
    pub discord_configured: bool,
    pub qq_configured: bool,
    pub telegram_configured: bool,
    pub matrix_configured: bool,
    pub discord_connected: bool,
    pub qq_connected: bool,
    pub telegram_connected: bool,
    pub matrix_connected: bool,
}

// -- Bot controller -----------------------------------------------------------

pub struct BotController {
    config: Mutex<AppConfig>,
    inner: Mutex<BotInner>,
}

struct BotInner {
    handle: Option<BotHandle>,
    discord_connected: Arc<AtomicBool>,
    qq_connected: Arc<AtomicBool>,
    telegram_connected: Arc<AtomicBool>,
    matrix_connected: Arc<AtomicBool>,
}

pub struct BotHandle {
    cancel: CancellationToken,
}

/// Runtime state for the QQ OneBot WebSocket endpoint.
/// The caller (CLI or WebUI) uses this to serve the /onebot/v11/ws route.
pub struct QQRuntime {
    pub rx: UnboundedReceiver<String>,
    pub pending: PendingMap,
    pub self_id: Arc<OnceLock<i64>>,
    pub bridges: Arc<Bridges>,
    pub cancel: CancellationToken,
    pub connected: Arc<AtomicBool>,
}

impl BotController {
    pub fn new(config: AppConfig) -> Self {
        Self {
            config: Mutex::new(config),
            inner: Mutex::new(BotInner {
                handle: None,
                discord_connected: Arc::new(AtomicBool::new(false)),
                qq_connected: Arc::new(AtomicBool::new(false)),
                telegram_connected: Arc::new(AtomicBool::new(false)),
                matrix_connected: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    /// Start the bot. Returns QQ runtime if QQ is configured.
    #[allow(clippy::too_many_lines)]
    pub async fn start(&self) -> Result<Option<QQRuntime>, AnemoneBotError> {
        // Acquire config first (consistent with status() lock order), then inner.
        let config = self.config.lock().await.clone();
        let mut inner = self.inner.lock().await;
        if inner.handle.is_some() {
            return Err(AnemoneBotError::Config("bot is already running".into()));
        }
        let cancel = CancellationToken::new();

        inner.discord_connected.store(false, Ordering::SeqCst);
        inner.qq_connected.store(false, Ordering::SeqCst);
        inner.telegram_connected.store(false, Ordering::SeqCst);
        inner.matrix_connected.store(false, Ordering::SeqCst);

        let store = Arc::new(MessageStore::new("anemone-bot.db")?);
        store.prune(604_800)?;

        let http_client = build_reqwest_client(config.http_proxy.as_deref())?;

        // Build QQ channel + context first, save rx to build QQRuntime later
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let qq_self_id = Arc::new(OnceLock::new());

        let (qq_ctx, qq_rx) = if config.bind_addr.is_some() {
            let (tx, rx) = unbounded_channel::<String>();
            let ctx = QQContext {
                tx,
                pending: pending.clone(),
                self_id: qq_self_id.clone(),
                http: http_client.clone(),
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
            http: http_client.clone(),
        });

        let telegram_ctx = config
            .telegram_token
            .as_ref()
            .map(|token| -> Result<_, AnemoneBotError> {
                Ok(TelegramContext {
                    http: http_client.clone(),
                    self_id: Arc::new(OnceLock::new()),
                    token: token.clone(),
                })
            })
            .transpose()?;

        let matrix_ctx = config
            .matrix_token
            .as_ref()
            .zip(config.matrix_homeserver_url.as_ref())
            .map(|(token, homeserver_url)| -> Result<_, AnemoneBotError> {
                Ok(MatrixContext {
                    http: http_client.clone(),
                    self_id: Arc::new(OnceLock::new()),
                    token: token.clone(),
                    homeserver_url: homeserver_url.clone(),
                })
            })
            .transpose()?;

        // -- bridges -----------------------------------------------------------
        let bridges = Arc::new(Bridges::new(
            &config.bridges,
            discord_ctx.as_ref(),
            qq_ctx.as_ref(),
            telegram_ctx.as_ref(),
            matrix_ctx.as_ref(),
            &store,
        ));

        // -- spawn Discord client ----------------------------------------------
        if let Some(ref dc) = discord_ctx {
            let b = bridges.clone();
            let ctx = DiscordContext {
                http_lock: dc.http_lock.clone(),
                self_id: dc.self_id.clone(),
                token: dc.token.clone(),
                proxy: dc.proxy.clone(),
                http: dc.http.clone(),
            };
            let cancel_clone = cancel.clone();
            let conn = inner.discord_connected.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::discord_client::run(b, &ctx, cancel_clone, conn).await {
                    error!("discord client fatal: {e}");
                }
            });
        }

        // -- spawn Telegram client ---------------------------------------------
        if let Some(ref tc) = telegram_ctx {
            let b = bridges.clone();
            let ctx = TelegramContext {
                http: tc.http.clone(),
                self_id: tc.self_id.clone(),
                token: tc.token.clone(),
            };
            let cancel_clone = cancel.clone();
            let conn = inner.telegram_connected.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::telegram_client::run(b, &ctx, cancel_clone, conn).await {
                    error!("telegram client fatal: {e}");
                }
            });
        }

        // -- spawn Matrix client -----------------------------------------------
        if let Some(ref mc) = matrix_ctx {
            let b = bridges.clone();
            let ctx = MatrixContext {
                http: mc.http.clone(),
                self_id: mc.self_id.clone(),
                token: mc.token.clone(),
                homeserver_url: mc.homeserver_url.clone(),
            };
            let cancel_clone = cancel.clone();
            let conn = inner.matrix_connected.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::matrix_client::run(b, &ctx, cancel_clone, conn).await {
                    error!("matrix client fatal: {e}");
                }
            });
        }

        inner.handle = Some(BotHandle {
            cancel: cancel.clone(),
        });

        // Build QQRuntime after bridges is ready
        let qq_connected = inner.qq_connected.clone();
        let qq_runtime = qq_rx.map(|rx| QQRuntime {
            rx,
            pending,
            self_id: qq_self_id,
            bridges,
            cancel,
            connected: qq_connected,
        });

        info!(
            "bot started (discord={}, qq={}, telegram={}, matrix={})",
            discord_ctx.is_some(),
            qq_ctx.is_some(),
            telegram_ctx.is_some(),
            matrix_ctx.is_some(),
        );

        Ok(qq_runtime)
    }

    /// Stop the bot.
    pub async fn stop(&self) {
        let mut inner = self.inner.lock().await;
        if let Some(handle) = inner.handle.take() {
            info!("bot: shutting down");
            handle.cancel.cancel();
            info!("bot: stopped");
        }
    }

    /// Current bot status.
    pub async fn status(&self) -> BotStatus {
        let config = self.config.lock().await;
        let inner = self.inner.lock().await;
        BotStatus {
            running: inner.handle.is_some(),
            discord_configured: config.discord_token.is_some(),
            qq_configured: config.bind_addr.is_some(),
            telegram_configured: config.telegram_token.is_some(),
            matrix_configured: config.matrix_token.is_some(),
            discord_connected: inner.discord_connected.load(Ordering::SeqCst),
            qq_connected: inner.qq_connected.load(Ordering::SeqCst),
            telegram_connected: inner.telegram_connected.load(Ordering::SeqCst),
            matrix_connected: inner.matrix_connected.load(Ordering::SeqCst),
        }
    }

    /// Replace the stored config. Takes effect on next start.
    pub async fn update_config(&self, config: AppConfig) {
        *self.config.lock().await = config;
    }

    pub async fn config(&self) -> AppConfig {
        self.config.lock().await.clone()
    }
}

// -- WebSocket handler (shared by CLI and WebUI binaries) ---------------------

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(qq_state): State<Arc<Mutex<Option<QQRuntime>>>>,
) -> impl axum::response::IntoResponse {
    let runtime = qq_state
        .lock()
        .await
        .take()
        .expect("QQ runtime not available; only one WS connection expected");
    ws.on_upgrade(move |socket| handle_socket(socket, runtime))
}

pub async fn handle_socket(socket: WebSocket, runtime: QQRuntime) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let QQRuntime {
        mut rx,
        pending,
        self_id: qq_self_id,
        bridges,
        cancel,
        connected,
    } = runtime;

    // Write task: forward queued OneBot actions to NapCat over WS
    let write_cancel = cancel.clone();
    let write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = write_cancel.cancelled() => break,
                msg = rx.recv() => {
                    match msg {
                        Some(text) => {
                            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    // Read loop: receive OneBot events and API responses from NapCat
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("qq: cancellation requested during read loop");
                break;
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let text = text.to_string();
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
                                crate::qq_client::handle_message(event, &bridges).await;
                            }
                            Ok(event)
                                if event.post_type == "meta_event"
                                    && event.meta_event_type == "lifecycle"
                                    && event.sub_type == "connect" =>
                            {
                                info!("napcat connected, self_id = {}", event.self_id);
                                bridges.set_qq_self_id(event.self_id);
                                let _ = qq_self_id.set(event.self_id);
                                connected.store(true, Ordering::SeqCst);
                            }
                            Ok(_) => {}
                            Err(e) => error!("failed to parse event: {e}"),
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("napcat disconnected");
                        break;
                    }
                    Some(Err(e)) => {
                        error!("ws recv error: {e}");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    connected.store(false, Ordering::SeqCst);
    write_task.abort();
}

// -- CLI entry point ----------------------------------------------------------

/// Run the bot in CLI mode: load config, start, serve QQ WS endpoint.
pub async fn run_cli() -> Result<(), AnemoneBotError> {
    tracing_subscriber::fmt::init();

    let config = config::load()?;

    if config.bridges.is_empty() {
        return Err(AnemoneBotError::Config(
            "at least one [[bridges]] required".into(),
        ));
    }

    let controller = Arc::new(BotController::new(config.clone()));
    let qq_runtime = controller.start().await?;

    if let (Some(bind_addr), Some(runtime)) = (config.bind_addr, qq_runtime) {
        let qq_state: Arc<Mutex<Option<QQRuntime>>> = Arc::new(Mutex::new(Some(runtime)));
        let app = Router::new()
            .route("/onebot/v11/ws", get(ws_handler))
            .with_state(qq_state);

        info!("bot ws server listening on {}", bind_addr);
        let listener = TcpListener::bind(&bind_addr).await?;
        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    error!("axum server error: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl+c received, shutting down");
            }
        }
    } else {
        info!("no servers to bind; idle");
        tokio::signal::ctrl_c().await.ok();
    }

    controller.stop().await;
    Ok(())
}
