Anemone-bot is a message forwarding bot that connects various chat platforms,
improving reusability through a unified message trait.

Supported platforms: Discord, QQ (via NapCat/OneBot v11), Telegram.

Prerequisites:
- Discord: create a bot at https://discord.com/developers/applications → Bot → copy token
- QQ: install NapCat (https://github.com/NapNeko/NapCatQQ)
- Telegram: create a bot via @BotFather → /newbot → copy token

Fill your credentials into anemone-bot.toml.

The anemone-bot.toml is as following:
--------------------------------------------------
# QQ / NapCat (optional — omit to skip QQ)
bind_addr = "0.0.0.0:<your-napcat-port>"

# Discord (optional — omit to skip Discord)
discord_token = "your-discord-bot-token"

# Telegram (optional — omit to skip Telegram)
# Disable privacy mode: @BotFather → /setprivacy → Disable, then re-add bot to group.
# NOTE: Telegram bots cannot see messages from other bots in the same group.
telegram_token = "your-telegram-bot-token"

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
--------------------------------------------------

After configuration, run the program using `cargo run`.
