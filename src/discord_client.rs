use std::sync::Arc;

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

use crate::bridge::Bridge;

/// Connect to `host:port` through an HTTP CONNECT proxy.
async fn proxy_connect(
    proxy_url: &str,
    host: &str,
    port: u16,
) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync>> {
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
        return Err(format!(
            "proxy CONNECT rejected: {}",
            resp.lines().next().unwrap_or("")
        )
        .into());
    }
    Ok(stream)
}

/// Connect to Discord gateway via proxy → TLS → WS.
async fn gateway_connect(
    proxy_url: &str,
    gateway_url: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_native_tls::TlsStream<TcpStream>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let host = gateway_url
        .trim_start_matches("wss://")
        .trim_end_matches("/?v=10&encoding=json");
    let tcp = proxy_connect(proxy_url, host, 443).await?;

    let tls_conn = NativeTlsConnector::builder()
        .build()
        .map_err(|e| format!("tls builder: {e}"))?;
    let tls = TlsConnector::from(tls_conn);
    let tls_stream = tls.connect(host, tcp).await?;

    let (ws, _) = tokio_tungstenite::client_async(gateway_url, tls_stream).await?;
    Ok(ws)
}

#[allow(clippy::too_many_lines)]
pub async fn run(bridge: Bridge, token: &str) {
    let proxy_url = std::env::var("HTTP_PROXY")
        .or_else(|_| std::env::var("HTTPS_PROXY"))
        .expect("HTTP_PROXY env var not set");

    info!("discord: using proxy {proxy_url}");

    // --- HTTP client (proxied) ---
    let reqwest_client = reqwest::Client::builder()
        .proxy(Proxy::all(&proxy_url).expect("invalid proxy url"))
        .build()
        .expect("failed to build reqwest client");

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
        .await
        .expect("failed to fetch gateway url")
        .json()
        .await
        .expect("invalid gateway response");

    let gateway_url = gateway_info["url"]
        .as_str()
        .expect("missing gateway url in response");

    info!("discord: gateway url = {gateway_url}");
    let gateway_url = format!("{gateway_url}/?v=10&encoding=json");

    loop {
        info!("discord: connecting to gateway via proxy...");
        let mut ws = match gateway_connect(&proxy_url, &gateway_url).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("gateway connect failed: {e}, retrying in 5s");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("discord: gateway connected, waiting for hello...");

        // --- Gateway loop ---
        let mut heartbeat_interval: u64;
        let mut last_seq: Option<u64> = None;
        let mut hb_timer: Option<tokio::time::Interval> = None;

        loop {
            tokio::select! {
                // Proactive heartbeat: tick at the gateway's requested interval
                _ = async {
                    match hb_timer.as_mut() {
                        Some(timer) => timer.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let hb = json!({"op": 1, "d": last_seq});
                    if let Err(e) = ws.send(WsMessage::Text(hb.to_string().into())).await {
                        error!("heartbeat send failed: {e}");
                        break;
                    }
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
                        }
                        10 => {
                            heartbeat_interval = payload["d"]["heartbeat_interval"].as_u64().unwrap_or(45000);
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
                            ws.send(WsMessage::Text(identify.to_string().into())).await.ok();

                            let timer = tokio::time::interval(
                                tokio::time::Duration::from_millis(heartbeat_interval)
                            );
                            hb_timer = Some(timer);
                        }
                        0 => {
                            let t = payload["t"].as_str().unwrap_or("");
                            match t {
                                "READY" => {
                                    let name = payload["d"]["user"]["username"].as_str().unwrap_or("unknown");
                                    let id = payload["d"]["user"]["id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    info!("discord: ready as {name}#{id}");

                                    bridge.set_discord_http(http.clone());
                                    bridge.set_discord_self_id(id);
                                }
                                "MESSAGE_CREATE" => {
                                    let channel_id = payload["d"]["channel_id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    if channel_id != bridge.discord_channel_id() { continue; }

                                    let author_id = payload["d"]["author"]["id"].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                    if bridge.is_self_discord(author_id) { continue; }
                                    if payload["d"]["author"]["bot"].as_bool().unwrap_or(false) { continue; }

                                    let name = payload["d"]["author"]["global_name"].as_str()
                                        .or_else(|| payload["d"]["author"]["username"].as_str())
                                        .unwrap_or("unknown");
                                    let content = payload["d"]["content"].as_str().unwrap_or("");

                                    if !content.is_empty() {
                                        info!("discord -> qq: [{name}] {content}");
                                        bridge.forward_discord_to_qq(name, content);
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
