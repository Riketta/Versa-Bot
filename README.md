# VersaBot

A multi-tenant Discord bot **framework** built as a hexagonal micro-kernel
(ports & adapters): a tiny platform-blind kernel routes events through a
middleware pipeline, and every capability is a plugin. Other chats
(Telegram, Matrix, ...) can be added later as platform adapters - the
kernel and core plugins never learn platform specifics.

## How it works

- **Kernel** - registers plugins, runs the middleware pipeline, owns the
  event bus and the command registry. Knows nothing about Discord.
- **Plugins** - the actual features (source-code extensions under
  `src/plugins/`). They talk to the kernel and to each other through
  kernel-owned ports only, and persist data in guild-scoped storage.
- **Adapters** - platform I/O. The Discord gateway adapter normalizes
  gateway events onto a chat-agnostic taxonomy; Discord-specific
  capabilities (slash-command registration, interaction replies) live here.

Inbound events (`MessageReceived`, `CommandInvoked`, `MemberJoined`, ...)
flow through the middleware chain (auth gates, command dispatcher, activity
tracker, ...).
There is no request/response: plugins produce output through event-scoped
ports - replies land in the event's channel/guild automatically, and slash
replies go through the interaction followup endpoint. Plugins cannot tell
a slash command from any other reply path.

## Current features

- Native Discord slash commands, auto-registered at startup from plugin
  declarations (`/ping` ships as the demo command).
- Per-guild authorization (`auth` plugin): user and role allow-lists;
  unconfigured guilds are open by default; a malformed policy fails closed.
- User activity tracker (`tracker` plugin): logs member joins/leaves to the
  guild's configured audit channel and publishes `UserJoinedGuild` /
  `UserLeftGuild` domain events on the plugin bus for other plugins to
  react to. Per-guild settings live in plugin storage (namespace `tracker`,
  key `config`: `{ "audit_channel_id": "<channel id>" }`); absent config
  means tracking is off for that guild.
- Guild-partitioned document storage: plugins persist JSON documents scoped
  to `(platform, guild)` - reading another guild's data is impossible by
  construction. SQLite (default) and PostgreSQL.
- Observability: `tracing` logging to stdout plus optional Sentry/GlitchTip
  reporting (DSN-driven); every event is traced with its origin.
- Graceful shutdown on Ctrl-C (plugins stop in reverse order).

## Getting started

Prerequisites: Rust 1.88+ (edition 2024).

1. Copy the checked-in example and fill in your Discord token:

   ```sh
   cp versabot.example.toml versabot.toml
   ```

   The real `versabot.toml` is gitignored and looks like this:

   ```toml
   debug = true

   [discord]
   token = "YOUR_TOKEN"

   [storage]
   # SQLite for development; PostgreSQL in production, e.g.
   # url = "postgres://user:password@host/versa_bot"
   url = "sqlite://versabot.db"

   # Optional: Sentry SaaS or self-hosted GlitchTip (Sentry-protocol DSN).
   # [sentry]
   # dsn = "http://your-key@localhost:9000/1"
   # environment = "development"
   ```

2. `cargo run` from the project root (migrations load from `./migrations`).
   Migrations apply automatically, slash commands are published, and the
   bot comes online.

3. Invite the bot with the `bot` + `applications.commands` scopes so slash
   commands appear. Global commands can take up to an hour to propagate on
   first registration.

   The bot also uses the **Server Members** privileged intent (member
   join/leave tracking) - enable "Server Members Intent" for the bot in the
   Discord Developer Portal, otherwise the gateway will disconnect on start.

Environment variables override the file:
`VERSABOT__DISCORD__TOKEN`, `VERSABOT__STORAGE__URL`,
`VERSABOT__SENTRY__DSN`, ...

## Development

- `cargo test` - unit tests cover the kernel pipeline, storage guild
  isolation, the command registry/dispatcher, the auth policy, and the
  activity tracker.
- `cargo clippy --all-targets` - the deny-level lints must stay clean.

### Project layout

```text
src/
├── kernel/            # the micro-kernel (platform-blind)
│   ├── app/
│   │   ├── api_ports/      # driving ports (RequestHandlerPort)
│   │   ├── spi_ports/      # driven ports (storage, chat output, ...)
│   │   ├── plugin_ports/   # plugin contracts (PluginPort, commands, bus)
│   │   └── services/       # KernelService (pipeline runner, lifecycle)
│   └── models/             # event taxonomy, IDs, errors
├── plugins/           # features: auth, command dispatcher, tracker, status (WIP)
├── infrastructure/    # adapters
│   ├── inbound_adapters/   # Discord gateway + scoped output factory
│   ├── outbound_adapters/  # storage (sqlx), Discord command registrar
│   └── plugin_adapters/    # in-memory event bus, command registry
└── common/
```

## Roadmap

- `/auth` management commands (allow users/roles per guild)
- Configuration hot-reload; scheduler (status rotation)
- LLM chat plugin; message history
- Further platform adapters (Telegram, Matrix, ...)
