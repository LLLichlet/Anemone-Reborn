Anemone-bot is a message forwarding bot that connects various chat platforms,
improving reusability through a unified message trait.

Supported platforms: Discord, QQ (via NapCat/OneBot v11), Telegram.

Features:
Bidirectional text message forwarding with reply chain resolution.
Image forwarding across platforms (images are downloaded and re-uploaded
natively; on download failure a "[图片]" placeholder is shown).

Prerequisites:
Discord: create a bot at https://discord.com/developers/applications → Bot → copy token
QQ: install NapCat (https://github.com/NapNeko/NapCatQQ)
Telegram: create a bot via @BotFather → /newbot → copy token

Fill your credentials into anemone-bot.toml.

The anemone-bot.toml is as following:
-------------------------------------------------------------------------------
# QQ / NapCat (optional — omit to skip QQ)
bind_addr = "0.0.0.0:<your-napcat-port>"

# Discord (optional — omit to skip Discord)
discord_token = "your-discord-bot-token"

# Telegram (optional — omit to skip Telegram)
# Disable privacy mode: @BotFather → /setprivacy → Disable, then re-add bot to group.
# NOTE: Telegram bots cannot see messages from other bots in the same group.
telegram_token = "your-telegram-bot-token"

# Optional WebUI management interface bind address (default 127.0.0.1:3000)
# webui_bind_addr = "127.0.0.1:3000"

# Optional HTTP/S proxy for Discord gateway and Telegram API
# http_proxy = "http://127.0.0.1:<your-proxy-port>"

# Each [[bridges]] declares a set of platform IDs to bridge.
# All platform IDs are optional — only configured platforms will be active.
# At least 2 platforms per bridge are needed for forwarding.

[[bridges]]
discord_channel_id = <discord-channel-id>
qq_group_id = <qq-group-id>
telegram_group_id = <telegram-chat-id>   # negative for supergroups

[[bridges]]
qq_group_id = <qq-group-id>
telegram_group_id = <telegram-chat-id>
-------------------------------------------------------------------------------

After configuration, run the program using `cargo run`.

-------------------------------------------------------------------------------
WebUI (optional)

A built-in web management interface is available via the `webui` binary.
Run `cargo run --bin webui` to start it (default http://127.0.0.1:3000).
Pass a custom address: `cargo run --bin webui 0.0.0.0:8080`.
Or set `webui_bind_addr = "127.0.0.1:3000"` in anemone-bot.toml.

The WebUI provides Start/Stop control, a TOML config editor with syntax
validation (changes take effect on next Start), and a live log viewer that
streams the same output as the terminal (without ANSI escape codes).

The management UI listens on webui_bind_addr (or the CLI argument, or
127.0.0.1:3000 by default). The OneBot WebSocket for QQ listens on a
separate port — bind_addr from the config — so NapCat keeps its existing
connection target. The bot starts in "stopped" state; click "Start Bot" to
connect all platforms. Logs respect the RUST_LOG environment variable.

-------------------------------------------------------------------------------
GUI (optional)

A native desktop GUI built with the Iced framework is available via the
`gui` binary. Run `cargo run --bin gui` to launch it.

The GUI provides the same management features as the WebUI: a status bar
with bot running/stopped indicator and per-platform connection status
(Discord, QQ, Telegram), a Start/Stop button, a TOML config editor with
Reload, Save, and inline validation feedback, and a live log viewer with
auto-scroll capped at 800 lines.

The GUI operates directly on the BotController and LogRing without an HTTP
layer. The OneBot WebSocket listener is spawned automatically in the
background if bind_addr is configured. The bot starts in "stopped" state;
click "Start Bot" to connect. Iced, wgpu, and winit internal log messages
are filtered to warn level and above to reduce noise in the log viewer.
