Anemone-bot is a message forwarding bot that connects various chat platforms,
improving reusability through a unified message trait.

Required environment variables
DISCORD_TOKEN = <your discord token>
HTTP_PROXY = http://127.0.0.1:<your proxy port>

You need napcat to obtain the QQ client. And fill its port into anemone-bot.toml

The anemone-bot.toml is as following:
--------------------------------------------------
bind_addr = "0.0.0.0:<your napcat port>"

# bridge 1
[[bridges]]
discord_channel_id = <your discord channel id>
qq_group_id = <your qq group id>

# bridge 2
[[bridges]]
discord_channel_id = <your discord channel id>
qq_group_id = <your qq group id>
--------------------------------------------------

After configuration, run the program using `cargo run`.

The functionality is not yet complete; it only supports plain text messages, except for replies.
There will be no maintenance in the near future; future maintenance will depend on my mood.