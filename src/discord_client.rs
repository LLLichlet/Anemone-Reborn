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

use std::sync::Arc;
use std::sync::OnceLock;

use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector as NativeTlsConnector;
use reqwest::Proxy;
use serde_json::json;
use serenity::http::HttpBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{error, info, warn};

use crate::bridge::Bridges;
use crate::error::AnemoneBotError;
use crate::message::DiscordMessage;

/// Connect to `host:port` through an HTTP CONNECT proxy.
async fn proxy_connect(
    proxy_url: &str,
    host: &str,
    port: u16,
) -> Result<TcpStream, AnemoneBotError> {
    let proxy_addr = proxy_url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let mut stream = TcpStream::connect(proxy_addr).await?;

    let req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;

    let mut buf = [0u8; 4096];
    let mut total = 0;
    loop {
        let n = stream.read(&mut buf[total..]).await?;
        if n == 0 {
            break;
        }
        total += n;
        if std::str::from_utf8(&buf[..total])
            .unwrap_or("")
            .contains("\r\n\r\n")
        {
            break;
        }
    }
    let resp = std::str::from_utf8(&buf[..total]).unwrap_or("");
    if !resp.contains("200") {
        return Err(AnemoneBotError::WebSocket(format!(
            "proxy CONNECT rejected: {}",
            resp.lines().next().unwrap_or("")
        )));
    }
    Ok(stream)
}

/// Connect to Discord gateway (via proxy if provided, otherwise direct TLS → WS).
async fn gateway_connect(
    proxy_url: Option<&str>,
    gateway_url: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_native_tls::TlsStream<TcpStream>>,
    AnemoneBotError,
> {
    let host = gateway_url
        .trim_start_matches("wss://")
        .trim_end_matches("/?v=10&encoding=json");

    let tls_stream = if let Some(proxy) = proxy_url {
        let tcp = proxy_connect(proxy, host, 443).await?;
        let tls_conn = NativeTlsConnector::builder().build()?;
        let tls = TlsConnector::from(tls_conn);
        tls.connect(host, tcp)
            .await
            .map_err(|e| AnemoneBotError::WebSocket(format!("tls connect: {e}")))?
    } else {
        let tcp = TcpStream::connect((host, 443)).await?;
        let tls_conn = NativeTlsConnector::builder().build()?;
        let tls = TlsConnector::from(tls_conn);
        tls.connect(host, tcp)
            .await
            .map_err(|e| AnemoneBotError::WebSocket(format!("tls connect: {e}")))?
    };

    let (ws, _) = tokio_tungstenite::client_async(gateway_url, tls_stream)
        .await
        .map_err(|e| AnemoneBotError::WebSocket(format!("ws connect: {e}")))?;
    Ok(ws)
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    bridges: Arc<Bridges>,
    token: &str,
    http_lock: Arc<OnceLock<Arc<serenity::http::Http>>>,
    proxy_url: Option<&str>,
) -> Result<(), AnemoneBotError> {
    if let Some(proxy) = proxy_url {
        info!("discord: using proxy {proxy}");
    }

    // --- HTTP client ---
    let mut client_builder = reqwest::Client::builder();
    if let Some(proxy) = proxy_url {
        client_builder = client_builder.proxy(Proxy::all(proxy)?);
    }
    let reqwest_client = client_builder.build()?;

    let http = Arc::new(
        HttpBuilder::new(token)
            .client(reqwest_client.clone())
            .build(),
    );

    // --- Get gateway URL ---
    let gateway_info: serde_json::Value = reqwest_client
        .get("https://discord.com/api/v10/gateway/bot")
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await?
        .json()
        .await?;

    let gateway_url = gateway_info["url"]
        .as_str()
        .ok_or_else(|| AnemoneBotError::WebSocket("missing gateway url in response".into()))?;

    info!("discord: gateway url = {gateway_url}");
    let gateway_url = format!("{gateway_url}/?v=10&encoding=json");

    loop {
        info!("discord: connecting to gateway...");
        let mut ws = match gateway_connect(proxy_url, &gateway_url).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("gateway connect failed: {e}, retrying in 5s");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("discord: gateway connected, waiting for hello...");

        // --- Gateway loop ---
        let mut last_seq: Option<u64> = None;
        let (hb_tx, mut hb_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let mut hb_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            tokio::select! {
                // Heartbeat: triggered by the heartbeat task via mpsc
                Some(()) = hb_rx.recv() => {
                    let hb = json!({"op": 1, "d": last_seq});
                    if let Err(e) = ws.send(WsMessage::Text(hb.to_string().into())).await {
                        error!("heartbeat send failed: {e}");
                        break;
                    }
                    info!("discord: heartbeat sent, seq={last_seq:?}");
                }
                msg = ws.next() => {
                    let Some(msg) = msg else {
                        info!("discord gateway closed");
                        break;
                    };
                    let msg = match msg {
                        Ok(WsMessage::Text(t)) => t,
                        Ok(WsMessage::Close(_)) => {
                            info!("discord gateway close frame");
                            break;
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            error!("gateway recv error: {e}");
                            break;
                        }
                    };

                    let payload: serde_json::Value = match serde_json::from_str(&msg) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("gateway json parse: {e}");
                            continue;
                        }
                    };

                    let op = payload["op"].as_i64().unwrap_or(-1);
                    let seq = payload["s"].as_u64();
                    if seq.is_some() { last_seq = seq; }

                    match op {
                        1 => {
                            // Server-requested heartbeat — respond immediately
                            let hb = json!({"op": 1, "d": last_seq});
                            if let Err(e) = ws.send(WsMessage::Text(hb.to_string().into())).await {
                                error!("heartbeat send failed: {e}");
                                break;
                            }
                            info!("discord: heartbeat (server-requested) sent");
                        }
                        10 => {
                            let heartbeat_interval = payload["d"]["heartbeat_interval"].as_u64().unwrap_or(45000);
                            info!("discord: hello received, interval={heartbeat_interval}ms");

                            let identify = json!({
                                "op": 2,
                                "d": {
                                    "token": token,
                                    "intents": 33280,
                                    "properties": {
                                        "os": "windows",
                                        "browser": "anemone",
                                        "device": "anemone"
                                    }
                                }
                            });
                            if let Err(e) = ws.send(WsMessage::Text(identify.to_string().into())).await {
                                error!("identify send failed: {e}");
                                break;
                            }

                            // Start keepalive task: sends heartbeat every 30s.
                            // This satisfies Discord's heartbeat_interval requirement
                            // (30s < 41.25s) and keeps the proxy tunnel alive.
                            if let Some(old) = hb_task.take() {
                                old.abort();
                            }
                            let hb_task_tx = hb_tx.clone();
                            hb_task = Some(tokio::spawn(async move {
                                loop {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    hb_task_tx.send(()).ok();
                                }
                            }));
                        }
                        11 => {
                            info!("discord: heartbeat ack");
                        }
                        0 => {
                            let t = payload["t"].as_str().unwrap_or("");
                            match t {
                                "READY" => {
                                    let name = payload["d"]["user"]["username"].as_str().unwrap_or("unknown");
                                    let id = payload["d"]["user"]["id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    info!("discord: ready as {name}#{id}");

                                    http_lock.set(http.clone()).ok();
                                    bridges.set_discord_self_id(id);
                                }
                                "MESSAGE_CREATE" => {
                                    let channel_id = payload["d"]["channel_id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    let Some(bridge) = bridges.by_discord(channel_id) else { continue; };

                                    let author_id = payload["d"]["author"]["id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    if bridges.is_self_discord(author_id) { continue; }
                                    if payload["d"]["author"]["bot"].as_bool().unwrap_or(false) { continue; }

                                    let msg_id = payload["d"]["id"].as_str().unwrap_or("0");
                                    let name = payload["d"]["author"]["global_name"].as_str()
                                        .or_else(|| payload["d"]["author"]["username"].as_str())
                                        .unwrap_or("unknown");
                                    let content = payload["d"]["content"].as_str().unwrap_or("");
                                    let reply_to_msg_id = payload["d"]["referenced_message"]["id"]
                                        .as_str()
                                        .or_else(|| {
                                            payload["d"]["message_reference"]["message_id"].as_str()
                                        })
                                        .map(String::from);

                                    if !content.is_empty() {
                                        info!("discord -> qq: [{name}] {content}");
                                        let msg = DiscordMessage {
                                            msg_id: msg_id.to_string(),
                                            sender_name: name.to_string(),
                                            content: content.to_string(),
                                            reply_to_msg_id,
                                        };
                                        bridge.forward(&msg).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        7 => {
                            warn!("discord: received reconnect");
                            break;
                        }
                        9 => {
                            warn!("discord: invalid session");
                            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        info!("discord: gateway disconnected, reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }
}
