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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::response::IntoResponse;
use iced::widget::{button, column, container, row, scrollable, text, text_editor};
use iced::{self, widget, Task, Theme};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use anemone_bot::bot_controller::{self, BotController, BotStatus, QQRuntime};
use anemone_bot::config::{self, AppConfig};
use anemone_bot::log_buffer::{BroadcastWriter, LogRing};

/// Static storage for boot arguments so the boot fn (a plain fn pointer) can
/// access them without capturing.
static BOOT_ARGS: OnceLock<(Arc<BotController>, Arc<LogRing>, PathBuf)> = OnceLock::new();

// -- Entry point -------------------------------------------------------------

fn main() -> iced::Result {
    let log_ring = Arc::new(LogRing::new(500));
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("iced=warn".parse().unwrap())
        .add_directive("iced_wgpu=warn".parse().unwrap())
        .add_directive("iced_winit=warn".parse().unwrap())
        .add_directive("iced_graphics=warn".parse().unwrap())
        .add_directive("wgpu=warn".parse().unwrap())
        .add_directive("winit=warn".parse().unwrap());
    let ring_layer = tracing_subscriber::fmt::layer()
        .with_writer(BroadcastWriter::new(log_ring.sender()))
        .with_ansi(false)
        .with_filter(filter);
    tracing_subscriber::registry().with(ring_layer).init();

    let config_path = env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("anemone-bot.toml");

    let config = config::load().unwrap_or_default();
    let controller = Arc::new(BotController::new(config));

    BOOT_ARGS
        .set((controller, log_ring, config_path))
        .map_err(|_| "BOOT_ARGS already set")
        .unwrap();

    iced::application(gui_boot, GuiApp::update, GuiApp::view)
        .subscription(GuiApp::subscription)
        .theme(|_: &GuiApp| Theme::Light)
        .centered()
        .window_size((860.0, 700.0))
        .title("Anemone-bot")
        .run()
}

fn gui_boot() -> (GuiApp, Task<Message>) {
    let args = match BOOT_ARGS.get() {
        Some(a) => a.clone(),
        None => unreachable!("BOOT_ARGS always set before gui_boot"),
    };
    GuiApp::boot(args.0, args.1, args.2)
}

// -- App state ---------------------------------------------------------------

struct GuiApp {
    controller: Arc<BotController>,
    #[allow(dead_code)]
    log_ring: Arc<LogRing>,
    config_path: PathBuf,
    qq_runtime: Arc<Mutex<Option<QQRuntime>>>,

    status: BotStatus,
    config_content: text_editor::Content,
    save_feedback: Option<Feedback>,
    logs: Vec<String>,
    bot_action_pending: bool,
    log_scroll_id: widget::Id,
}

#[derive(Debug, Clone)]
struct Feedback {
    message: String,
    is_ok: bool,
}

impl GuiApp {
    #[allow(clippy::needless_pass_by_value)]
    fn boot(
        controller: Arc<BotController>,
        log_ring: Arc<LogRing>,
        config_path: PathBuf,
    ) -> (Self, Task<Message>) {
        let status = BotStatus {
            running: false,
            discord_configured: false,
            qq_configured: false,
            telegram_configured: false,
            matrix_configured: false,
            discord_connected: false,
            qq_connected: false,
            telegram_connected: false,
            matrix_connected: false,
        };

        let app = Self {
            controller: controller.clone(),
            log_ring: log_ring.clone(),
            config_path: config_path.clone(),
            qq_runtime: Arc::new(Mutex::new(None)),
            status,
            config_content: text_editor::Content::new(),
            save_feedback: None,
            logs: Vec::new(),
            bot_action_pending: false,
            log_scroll_id: widget::Id::unique(),
        };

        // Spawn OneBot WS listener if configured (background, fire-and-forget)
        if let Some(bind_addr) = config::load().ok().and_then(|c| c.bind_addr) {
            let qq_rt_ref = app.qq_runtime.clone();
            let addr = bind_addr.clone();
            tokio::spawn(async move {
                info!("gui: onebot ws server listening on {addr}");
                if let Ok(listener) = TcpListener::bind(&addr).await {
                    let qq_ref = qq_rt_ref.clone();
                    let _ = axum::serve(
                        listener,
                        axum::Router::new().route(
                            "/onebot/v11/ws",
                            axum::routing::get(move |ws: axum::extract::ws::WebSocketUpgrade| {
                                let state = qq_ref.clone();
                                async move {
                                    #[allow(clippy::single_match_else)]
                                    if let Some(r) = state.lock().await.take() {
                                        ws.on_upgrade(move |socket| {
                                            bot_controller::handle_socket(socket, r)
                                        })
                                    } else {
                                        tracing::warn!(
                                            "gui: onebot ws attempted but bot not started"
                                        );
                                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                                    }
                                }
                            }),
                        ),
                    )
                    .await;
                }
            });
        }

        // Boot tasks: periodic status polling + log streaming + initial config load
        let controller_poll = controller.clone();
        let status_stream = async_stream::stream! {
            // Small initial delay to let the app render first
            tokio::time::sleep(Duration::from_millis(500)).await;
            loop {
                yield controller_poll.status().await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        };

        let log_stream = {
            let mut rx = log_ring.subscribe();
            async_stream::stream! {
                loop {
                    match rx.recv().await {
                        Ok(line) => yield line,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            yield format!("[dropped {n} messages]\n");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        };

        let init_task = {
            let path = config_path.clone();
            let ctrl = controller.clone();
            Task::perform(
                async move {
                    let text = tokio::fs::read_to_string(&path).await.unwrap_or_default();
                    let parsed = toml::from_str::<AppConfig>(&text).unwrap_or_default();
                    ctrl.update_config(parsed).await;
                    let status = ctrl.status().await;
                    (text, status)
                },
                |(text, status)| Message::InitComplete(text, status),
            )
        };

        let tasks = Task::batch([
            Task::run(status_stream, Message::StatusUpdated),
            Task::run(log_stream, Message::LogReceived),
            init_task,
        ]);

        (app, tasks)
    }

    #[allow(clippy::too_many_lines)]
    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::StatusUpdated(status) => {
                self.status = status;
                Task::none()
            }

            Message::ToggleBot => {
                if self.bot_action_pending {
                    return Task::none();
                }
                self.bot_action_pending = true;
                self.save_feedback = None;

                if self.status.running {
                    let controller = self.controller.clone();
                    Task::perform(
                        async move {
                            controller.stop().await;
                        },
                        |()| Message::BotStopped,
                    )
                } else {
                    let controller = self.controller.clone();
                    let qq_runtime = self.qq_runtime.clone();
                    Task::perform(
                        async move {
                            match controller.start().await {
                                Ok(runtime) => {
                                    if let Some(rt) = runtime {
                                        *qq_runtime.lock().await = Some(rt);
                                    }
                                    None
                                }
                                Err(e) => Some(e.to_string()),
                            }
                        },
                        Message::BotStarted,
                    )
                }
            }

            Message::BotStarted(err) => {
                self.bot_action_pending = false;
                if let Some(e) = err {
                    self.save_feedback = Some(Feedback {
                        message: format!("Start failed: {e}"),
                        is_ok: false,
                    });
                }
                Task::none()
            }

            Message::BotStopped => {
                self.bot_action_pending = false;
                *self.qq_runtime.blocking_lock() = None;
                Task::none()
            }

            Message::ConfigEditorAction(action) => {
                self.config_content.perform(action);
                self.save_feedback = None;
                Task::none()
            }

            Message::ReloadConfig => {
                let path = self.config_path.clone();
                Task::perform(
                    async move {
                        tokio::fs::read_to_string(&path)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    Message::ConfigLoaded,
                )
            }

            Message::ConfigLoaded(result) => {
                match result {
                    Ok(content) => {
                        self.config_content = text_editor::Content::with_text(&content);
                        self.save_feedback = Some(Feedback {
                            message: "Config reloaded from disk.".into(),
                            is_ok: true,
                        });
                    }
                    Err(e) => {
                        self.save_feedback = Some(Feedback {
                            message: format!("Failed to read config: {e}"),
                            is_ok: false,
                        });
                    }
                }
                Task::none()
            }

            Message::SaveConfig => {
                if self.status.running {
                    self.save_feedback = Some(Feedback {
                        message: "Stop the bot before saving config.".into(),
                        is_ok: false,
                    });
                    return Task::none();
                }
                let text = self.config_content.text();
                if let Err(e) = toml::from_str::<AppConfig>(&text) {
                    self.save_feedback = Some(Feedback {
                        message: format!("Invalid TOML: {e}"),
                        is_ok: false,
                    });
                    return Task::none();
                }
                let path = self.config_path.clone();
                let controller = self.controller.clone();
                Task::perform(
                    async move {
                        if let Err(e) = tokio::fs::write(&path, &text).await {
                            return Err(format!("write failed: {e}"));
                        }
                        match toml::from_str::<AppConfig>(&text) {
                            Ok(config) => {
                                controller.update_config(config).await;
                                Ok(())
                            }
                            Err(e) => Err(format!("parse: {e}")),
                        }
                    },
                    |result| Message::ConfigSaved(result.err()),
                )
            }

            Message::ConfigSaved(err) => {
                let is_ok = err.is_none();
                let message =
                    err.unwrap_or_else(|| "Saved. Changes take effect on next Start.".into());
                self.save_feedback = Some(Feedback { message, is_ok });
                Task::none()
            }

            Message::LogReceived(line) => {
                const MAX_LOG_LINES: usize = 800;
                self.logs.push(line);
                while self.logs.len() > MAX_LOG_LINES {
                    self.logs.remove(0);
                }
                Task::none()
            }

            Message::InitComplete(config_text, status) => {
                self.config_content = text_editor::Content::with_text(&config_text);
                self.status = status;
                Task::none()
            }
        }
    }

    fn view(&self) -> iced::Element<'_, Message> {
        let status_dot = text("●")
            .style(if self.status.running {
                style::green_text
            } else {
                style::red_text
            })
            .size(20);

        let status_text_label = text(if self.status.running {
            "Running"
        } else {
            "Stopped"
        })
        .size(18);

        let title = row![text("Anemone Bot").size(22), status_dot, status_text_label,]
            .spacing(8)
            .align_y(iced::Alignment::Center);

        // Platforms section
        let disc_ind = GuiApp::platform_indicator(
            "Discord",
            self.status.discord_configured,
            self.status.discord_connected,
        );
        let qq_ind =
            GuiApp::platform_indicator("QQ", self.status.qq_configured, self.status.qq_connected);
        let tg_ind = GuiApp::platform_indicator(
            "Telegram",
            self.status.telegram_configured,
            self.status.telegram_connected,
        );
        let mx_ind = GuiApp::platform_indicator(
            "Matrix",
            self.status.matrix_configured,
            self.status.matrix_connected,
        );

        let ctrl_label = if self.status.running {
            "Stop Bot"
        } else {
            "Start Bot"
        };
        let ctrl_btn =
            button(text(ctrl_label).size(14)).on_press_maybe(if self.bot_action_pending {
                None
            } else {
                Some(Message::ToggleBot)
            });

        let platforms = section(
            "Platforms",
            column![row![disc_ind, qq_ind, tg_ind, mx_ind].spacing(24), ctrl_btn].spacing(10),
        );

        // Config section
        let editor = text_editor(&self.config_content)
            .on_action(Message::ConfigEditorAction)
            .height(160);

        let feedback_text = if let Some(ref fb) = self.save_feedback {
            text(&fb.message).size(13).style(if fb.is_ok {
                style::green_text
            } else {
                style::red_text
            })
        } else {
            text("").size(13)
        };

        let config_section = section(
            "Config (anemone-bot.toml)",
            column![
                editor,
                row![
                    button(text("Reload").size(13)).on_press(Message::ReloadConfig),
                    button(text("Save").size(13)).on_press(Message::SaveConfig),
                    feedback_text,
                ]
                .spacing(8)
                .align_y(iced::Alignment::Center),
            ]
            .spacing(6),
        );

        // Logs section
        let log_text = text(self.logs.concat()).size(12).style(style::log_text);

        let log_viewer = container(
            scrollable(container(log_text).padding(4).width(iced::Length::Fill))
                .id(self.log_scroll_id.clone()),
        )
        .style(style::log_container)
        .height(400);

        let logs_section = section("Logs", log_viewer);

        let content = column![title, platforms, config_section, logs_section]
            .spacing(12)
            .padding(16);

        container(content).into()
    }

    fn subscription(&self) -> iced::Subscription<Message> {
        let _ = self;
        // All ongoing work is handled by Task::run streams in boot.
        iced::Subscription::none()
    }

    fn platform_indicator(
        name: &str,
        configured: bool,
        connected: bool,
    ) -> iced::Element<'_, Message> {
        let (label, label_style) = if !configured {
            ("-", style::dim_text as fn(&Theme) -> text::Style)
        } else if connected {
            ("connected", style::green_text as fn(&Theme) -> text::Style)
        } else {
            ("disconnected", style::dim_text as fn(&Theme) -> text::Style)
        };

        row![
            text(format!("{name}: ")).size(14),
            text(label).size(14).style(label_style),
        ]
        .spacing(4)
        .into()
    }
}

// -- Messages ----------------------------------------------------------------

#[derive(Debug, Clone)]
enum Message {
    StatusUpdated(BotStatus),
    ToggleBot,
    BotStarted(Option<String>),
    BotStopped,
    ConfigEditorAction(text_editor::Action),
    ReloadConfig,
    ConfigLoaded(Result<String, String>),
    SaveConfig,
    ConfigSaved(Option<String>),
    LogReceived(String),
    InitComplete(String, BotStatus),
}

// -- Helpers -----------------------------------------------------------------

fn section<'a>(
    title: &'a str,
    content: impl Into<iced::Element<'a, Message>>,
) -> iced::Element<'a, Message> {
    container(
        column![
            container(text(title).size(15))
                .style(style::section_header)
                .padding([4, 8])
                .width(iced::Length::Fill),
            container(content.into()).padding(8),
        ]
        .spacing(0),
    )
    .style(style::section_box)
    .into()
}

// -- Styling -----------------------------------------------------------------

mod style {
    use iced::widget::{container, text};
    use iced::{Border, Color, Theme};

    pub fn green_text(_t: &Theme) -> text::Style {
        text::Style {
            color: Some(Color::from_rgb(0.0, 0.7, 0.3)),
        }
    }

    pub fn red_text(_t: &Theme) -> text::Style {
        text::Style {
            color: Some(Color::from_rgb(0.9, 0.2, 0.2)),
        }
    }

    pub fn dim_text(_t: &Theme) -> text::Style {
        text::Style {
            color: Some(Color::from_rgb(0.5, 0.5, 0.5)),
        }
    }

    pub fn log_text(_t: &Theme) -> text::Style {
        text::Style {
            color: Some(Color::from_rgb(0.8, 0.8, 0.8)),
        }
    }

    pub fn log_container(_t: &Theme) -> container::Style {
        container::Style {
            background: Some(iced::Background::Color(Color::from_rgb(0.1, 0.1, 0.1))),
            border: Border {
                radius: 2.0.into(),
                ..Border::default()
            },
            ..container::Style::default()
        }
    }

    pub fn section_box(_t: &Theme) -> container::Style {
        container::Style {
            border: Border {
                color: Color::from_rgb(0.3, 0.3, 0.3),
                width: 1.0,
                ..Border::default()
            },
            ..container::Style::default()
        }
    }

    pub fn section_header(_t: &Theme) -> container::Style {
        container::Style {
            border: Border {
                color: Color::from_rgb(0.3, 0.3, 0.3),
                width: 1.0,
                ..Border::default()
            },
            ..container::Style::default()
        }
    }
}
