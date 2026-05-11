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

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use anemone_bot::bot_controller::{self, BotController, QQRuntime};
use anemone_bot::config::{self, AppConfig};
use anemone_bot::error::AnemoneBotError;
use anemone_bot::log_buffer::{BroadcastWriter, LogRing};

struct WebUiState {
    controller: Arc<BotController>,
    log_ring: Arc<LogRing>,
    config_path: PathBuf,
    qq_runtime: Arc<Mutex<Option<QQRuntime>>>,
}

#[tokio::main]
async fn main() -> Result<(), AnemoneBotError> {
    let config = config::load()?;

    let webui_addr = config
        .webui_bind_addr
        .clone()
        .or_else(|| env::args().nth(1))
        .unwrap_or_else(|| "127.0.0.1:3000".into());

    let log_ring = Arc::new(LogRing::new(500));

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let ring_layer = tracing_subscriber::fmt::layer()
        .with_writer(BroadcastWriter::new(log_ring.sender()))
        .with_ansi(false)
        .with_filter(filter);
    tracing_subscriber::registry()
        .with(ring_layer)
        .init();

    let config_path = env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("anemone-bot.toml");

    let onebot_addr = config.bind_addr.clone();

    let controller = Arc::new(BotController::new(config));
    let qq_runtime: Arc<Mutex<Option<QQRuntime>>> = Arc::new(Mutex::new(None));

    let state = Arc::new(WebUiState {
        controller,
        log_ring,
        config_path,
        qq_runtime: qq_runtime.clone(),
    });

    let app = Router::new()
        .route("/", get(serve_index))
        .route("/api/config", get(get_config).post(save_config))
        .route("/api/status", get(get_status))
        .route("/api/bot/start", post(start_bot))
        .route("/api/bot/stop", post(stop_bot))
        .route("/api/logs", get(logs_sse))
        .route("/onebot/v11/ws", get(ws_handler))
        .with_state(state);

    // Also serve OneBot WS on the config's bind_addr (separate port) so NapCat
    // can keep its existing connection target.
    if let Some(ref onebot_addr) = onebot_addr {
        let onebot_state = qq_runtime.clone();
        let onebot_app = Router::new()
            .route("/onebot/v11/ws", get(onebot_ws_handler))
            .with_state(onebot_state);
        let addr = onebot_addr.clone();
        tokio::spawn(async move {
            info!("onebot ws server listening on {addr}");
            if let Ok(listener) = TcpListener::bind(&addr).await {
                let _ = axum::serve(listener, onebot_app).await;
            }
        });
    }

    info!("webui listening on http://{webui_addr}");
    let listener = TcpListener::bind(&webui_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// -- HTML page ---------------------------------------------------------------

async fn serve_index() -> impl IntoResponse {
    axum::response::Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Anemone Bot</title>
<style>
  body { font-family: monospace; background: #f0f0f0; color: #222; margin: 0; padding: 1em; }
  h1 { font-size: 1.2em; margin: 0 0 0.5em 0; }
  .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; background: red; }
  .status-dot.on { background: green; }
  .section { background: #fff; border: 1px solid #aaa; padding: 0.8em; margin-bottom: 1em; }
  .section h2 { font-size: 1em; margin: 0 0 0.5em 0; border-bottom: 1px solid #aaa; padding-bottom: 0.3em; }
  textarea { width: 100%; height: 12em; font-family: monospace; font-size: 0.9em; border: 1px solid #888; padding: 4px; box-sizing: border-box; }
  button { font-family: monospace; font-size: 0.9em; padding: 4px 12px; border: 1px solid #666; background: #ddd; cursor: pointer; }
  button:hover { background: #ccc; }
  button:disabled { color: #999; }
  #log-viewer { height: 400px; overflow-y: scroll; background: #1a1a1a; color: #ccc; padding: 4px; font-size: 0.85em; white-space: pre-wrap; word-break: break-all; border: 1px solid #888; }
  #save-feedback { margin-left: 8px; font-size: 0.9em; }
  .error { color: red; }
  .ok { color: green; }
  .platform-indicator { display: inline-block; margin-right: 12px; }
  .platform-indicator b { font-weight: bold; }
  .conn-on { color: green; }
  .conn-off { color: #999; }
</style>
</head>
<body>

<h1>Anemone Bot <span id="status-dot" class="status-dot"></span> <span id="status-text"></span></h1>

<div class="section">
  <h2>Platforms</h2>
  <span class="platform-indicator">Discord: <b id="disc-ind" class="conn-off">-</b></span>
  <span class="platform-indicator">QQ: <b id="qq-ind" class="conn-off">-</b></span>
  <span class="platform-indicator">Telegram: <b id="tg-ind" class="conn-off">-</b></span>
  <br>
  <button id="ctrl-btn" onclick="toggleBot()">Start Bot</button>
</div>

<div class="section">
  <h2>Config (anemone-bot.toml) <button onclick="loadConfig()">Reload</button></h2>
  <textarea id="config-editor" spellcheck="false"></textarea>
  <br>
  <button onclick="saveConfig()">Save</button>
  <span id="save-feedback"></span>
</div>

<div class="section">
  <h2>Logs</h2>
  <pre id="log-viewer"></pre>
</div>

<script>
var running = false;
var logLines = 0;
var maxLogLines = 800;

function $(id) { return document.getElementById(id); }

async function updateStatus() {
  try {
    var r = await fetch('/api/status');
    var s = await r.json();
    running = s.running;
    $('status-dot').className = 'status-dot' + (s.running ? ' on' : '');
    $('status-text').textContent = s.running ? 'Running' : 'Stopped';
    $('ctrl-btn').textContent = s.running ? 'Stop Bot' : 'Start Bot';
    $('ctrl-btn').disabled = false;

    function ci(id, ok, conf) {
      var el = $(id);
      if (!conf) { el.textContent = '-'; el.className = 'conn-off'; }
      else { el.textContent = ok ? 'connected' : 'disconnected'; el.className = ok ? 'conn-on' : 'conn-off'; }
    }
    ci('disc-ind', s.discord_connected, s.discord_configured);
    ci('qq-ind', s.qq_connected, s.qq_configured);
    ci('tg-ind', s.telegram_connected, s.telegram_configured);
  } catch (e) { console.error('status error:', e); }
}

async function toggleBot() {
  $('ctrl-btn').disabled = true;
  try {
    var url = running ? '/api/bot/stop' : '/api/bot/start';
    var r = await fetch(url, { method: 'POST' });
    var j = await r.json();
    if (!r.ok) { alert(j.error || 'failed'); }
  } catch (e) { alert('error: ' + e); }
  updateStatus();
}

async function loadConfig() {
  try {
    var r = await fetch('/api/config');
    $('config-editor').value = await r.text();
  } catch (e) { console.error('load config error:', e); }
}

async function saveConfig() {
  var fb = $('save-feedback');
  fb.textContent = '';
  fb.className = '';
  try {
    var r = await fetch('/api/config', {
      method: 'POST',
      body: $('config-editor').value
    });
    var j = await r.json();
    if (r.ok) {
      fb.textContent = 'Saved. Changes take effect on next Start.';
      fb.className = 'ok';
    } else {
      fb.textContent = j.error || 'save failed';
      fb.className = 'error';
    }
  } catch (e) {
    fb.textContent = 'error: ' + e;
    fb.className = 'error';
  }
}

function initLogs() {
  var es = new EventSource('/api/logs');
  es.onmessage = function(e) {
    var viewer = $('log-viewer');
    viewer.textContent += e.data;
    logLines++;
    // Trim old lines to prevent DOM bloat
    while (logLines > maxLogLines) {
      var i = viewer.textContent.indexOf('\n');
      if (i < 0) break;
      viewer.textContent = viewer.textContent.substring(i + 1);
      logLines--;
    }
    viewer.scrollTop = viewer.scrollHeight;
  };
  es.onerror = function() {
    var viewer = $('log-viewer');
    viewer.textContent += '[log stream disconnected, retrying...]\n';
    logLines++;
  };
}

loadConfig();
updateStatus();
setInterval(updateStatus, 3000);
initLogs();
</script>

</body>
</html>"#;

// -- API handlers ------------------------------------------------------------

async fn get_config(State(state): State<Arc<WebUiState>>) -> impl IntoResponse {
    match tokio::fs::read_to_string(&state.config_path).await {
        Ok(content) => axum::response::Response::builder()
            .header("content-type", "text/plain; charset=utf-8")
            .body(axum::body::Body::from(content))
            .unwrap(),
        Err(e) => axum::response::Response::builder()
            .status(500)
            .header("content-type", "text/plain")
            .body(axum::body::Body::from(format!(
                "failed to read config: {e}"
            )))
            .unwrap(),
    }
}

async fn save_config(State(state): State<Arc<WebUiState>>, body: String) -> impl IntoResponse {
    // Validate TOML
    if let Err(e) = toml::from_str::<AppConfig>(&body) {
        let json = serde_json::json!({"error": format!("invalid TOML: {e}")});
        return (axum::http::StatusCode::BAD_REQUEST, axum::Json(json)).into_response();
    }

    // Write to disk
    if let Err(e) = tokio::fs::write(&state.config_path, &body).await {
        let json = serde_json::json!({"error": format!("write failed: {e}")});
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json),
        )
            .into_response();
    }

    // Reparse and update controller
    match toml::from_str::<AppConfig>(&body) {
        Ok(config) => {
            state.controller.update_config(config).await;
            axum::Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => axum::Json(serde_json::json!({"error": format!("parse: {e}")})).into_response(),
    }
}

async fn get_status(State(state): State<Arc<WebUiState>>) -> impl IntoResponse {
    axum::Json(state.controller.status().await)
}

async fn start_bot(State(state): State<Arc<WebUiState>>) -> impl IntoResponse {
    match state.controller.start().await {
        Ok(Some(runtime)) => {
            *state.qq_runtime.lock().await = Some(runtime);
            axum::Json(serde_json::json!({"ok": true})).into_response()
        }
        Ok(None) => axum::Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn stop_bot(State(state): State<Arc<WebUiState>>) -> impl IntoResponse {
    state.controller.stop().await;
    *state.qq_runtime.lock().await = None;
    axum::Json(serde_json::json!({"ok": true}))
}

async fn logs_sse(
    State(state): State<Arc<WebUiState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let mut rx = state.log_ring.subscribe();
    let (tx, mpsc_rx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(64);

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(line) => {
                    if tx.send(Ok(Event::default().data(line))).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    let _ = tx
                        .send(Ok(
                            Event::default().data(format!("[dropped {n} messages]\n"))
                        ))
                        .await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    Sse::new(ReceiverStream::new(mpsc_rx))
}

// -- OneBot WS handler (extracts QQ runtime from shared state) ----------------

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<WebUiState>>,
) -> impl IntoResponse {
    match state.qq_runtime.lock().await.take() {
        Some(runtime) => {
            ws.on_upgrade(move |socket| bot_controller::handle_socket(socket, runtime))
        }
        None => {
            tracing::warn!("onebot ws connection attempted but bot not started yet");
            axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

/// Handler for the dedicated OneBot WS port (bound to config's `bind_addr`).
async fn onebot_ws_handler(
    ws: WebSocketUpgrade,
    State(qq_runtime): State<Arc<Mutex<Option<QQRuntime>>>>,
) -> impl IntoResponse {
    match qq_runtime.lock().await.take() {
        Some(runtime) => {
            ws.on_upgrade(move |socket| bot_controller::handle_socket(socket, runtime))
        }
        None => {
            tracing::warn!("onebot ws connection attempted but bot not started yet");
            axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
