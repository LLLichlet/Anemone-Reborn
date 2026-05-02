mod bot_handler;
mod bridge;
mod config;
mod discord_client;
mod error;
mod onebot_api;
mod onebot_types;

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tracing::{error, info};

use crate::bridge::Bridge;
use crate::config::BridgeConfig;
use crate::onebot_types::Event;

struct AppState {
    bridge: Bridge,
    qq_rx: Arc<tokio::sync::Mutex<Option<UnboundedReceiver<String>>>>,
    _bridges: Vec<BridgeConfig>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = config::load();
    let discord_token = std::env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN env var not set");

    let first_bridge = config
        .bridges
        .first()
        .cloned()
        .expect("at least one [[bridges]] required in config");

    let (qq_tx, qq_rx) = unbounded_channel::<String>();
    let bridge = Bridge::new(
        qq_tx.clone(),
        first_bridge.discord_channel_id,
        first_bridge.qq_group_id,
    );

    // Spawn Discord client
    tokio::spawn(discord_client::run(bridge.clone(), discord_token.leak()));

    let state = Arc::new(AppState {
        bridge,
        qq_rx: Arc::new(tokio::sync::Mutex::new(Some(qq_rx))),
        _bridges: config.bridges,
    });

    let app = Router::new()
        .route("/onebot/v11/ws", get(ws_handler))
        .with_state(state);

    info!("bot ws server listening on {}", config.bind_addr);
    let listener = TcpListener::bind(&config.bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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

    ws.on_upgrade(move |socket| handle_socket(socket, bridge, qq_rx))
}

async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    bridge: Bridge,
    mut qq_rx: UnboundedReceiver<String>,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Forward queued OneBot actions to NapCat over WS
    let write_task = tokio::spawn(async move {
        while let Some(text) = qq_rx.recv().await {
            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop: receive OneBot events from NapCat
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let text = text.to_string();
                match serde_json::from_str::<Event>(&text) {
                    Ok(event) if event.post_type == "message" => {
                        info!(
                            "qq message from {} ({}): {}",
                            event.user_id, event.message_type, event.message
                        );
                        bot_handler::handle_message(event, &bridge).await;
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
