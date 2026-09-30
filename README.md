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
- Per-guild authorization (`auth` plugin): user and role allow-lists,
  administered from Discord via `/auth` (`action:allow|deny|show` with a
  user or role; Manage Server permission, guild-only, ephemeral answers).
  Unconfigured guilds are open by default; a malformed policy fails closed.
  A denied slash command is answered with an ephemeral error embed, visible
  only to the invoker, naming the rejected permission group (users / roles);
  if the policy itself is unreadable, it gets a "policy unavailable" notice
  instead.
- User activity tracker (`tracker` plugin): logs member joins/leaves to the
  guild's audit channel and publishes `UserJoinedGuild` / `UserLeftGuild`
  domain events on the plugin bus for other plugins to react to. Channel
  assignment is self-service: `/assign_tracker` run in a channel makes it
  the audit channel, `/unassign_tracker` turns tracking off (both require
  the Manage Server permission, enforced by Discord itself).
- Audit trail (`audit_log` plugin): the event bus's first consumer - logs
  membership changes published on the bus as structured `audit` tracing
  events (stdout + Sentry/GlitchTip), with origin fields, no per-guild
  configuration needed.
- Status rotator (`status_rotator` plugin): cycles the bot's activity
  through a configured list on a configured interval - both come from the
  optional `[status]` section of the config file (presence is bot-wide,
  not per-guild); omitted or empty means the rotation is off.
- Guild-partitioned document storage: plugins persist JSON documents scoped
  to `(platform, guild)` - reading another guild's data is impossible by
  construction. SQLite (default) and PostgreSQL.
- Observability: `tracing` logging to stdout plus optional Sentry/GlitchTip
  reporting (DSN-driven); every event is traced with its origin.
- Fault isolation: a panicking plugin cannot crash the bot - pipeline hooks
  and event-bus subscribers are caught and logged (plugin + event), the
  event is dropped, and the rest of the chain or bus keeps working.
- Configuration hot reload: the configuration (file + env overrides) is
  re-read every few seconds; changes to hot-reloadable sections apply
  without a restart - e.g. editing `[status]` re-applies the rotation live
  (identical settings are ignored, removing the section stops it). Startup
  -only settings (token, storage, Sentry) are not affected.
- Graceful shutdown on Ctrl-C (plugins stop in reverse order).

## Getting started

Prerequisites: Rust 1.88+ (edition 2024).

1. Copy the checked-in example and fill in your Discord token:

   ```sh
   cp versabot.example.toml versabot.toml
   ```

   The real `versabot.toml` is gitignored and looks like this:

   ```toml
   # Verbose (debug-level) logging; RUST_LOG overrides it entirely.
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

   # Optional: status rotator - cycles the bot's activity.
   # [status]
   # interval_seconds = 300
   # statuses = ["with the API", "versa-bot"]
   ```

2. `cargo run` from the project root (migrations load from `./migrations`).
   Migrations apply automatically, slash commands are published, and the
   bot comes online.

3. Invite the bot with the `bot` + `applications.commands` scopes so slash
   commands appear. Global commands can take up to an hour to propagate on
   first registration.

### Gateway intents

The bot requests a minimal gateway intent set - every intent is justified
by a concrete plugin or core feature:

| Intent | Kind | Requested for |
|---|---|---|
| `GUILD_MESSAGES` | regular | Core message intake: guild messages flow through the pipeline (the auth gate) |
| `DIRECT_MESSAGES` | regular | Core DM intake: DM events flow with no guild scope |
| `GUILD_MEMBERS` | **privileged** | `tracker` plugin: member join/leave events (`/assign_tracker` audit) |
| `MESSAGE_CONTENT` | privileged | **not requested** - no current feature reads guild message content; the planned LLM chat plugin will add it back |

So exactly one portal toggle is needed today: enable **Server Members
Intent** (Discord Developer Portal → Bot), otherwise the gateway
disconnects on start. When the LLM chat plugin lands, also enable **Message
Content Intent**.

Environment variables override the file:
`VERSABOT__DISCORD__TOKEN`, `VERSABOT__STORAGE__URL`,
`VERSABOT__SENTRY__DSN`, ...

## Docker

CI builds the image on every push to `main` and on `v*` git tags -
GitHub Actions pushes to GHCR (`ghcr.io/<owner>/versa-bot`) and Forgejo to
its instance registry (`git.versalita.net/<owner>/versa-bot`). Tags per
run: `sha-<short>` per commit, `latest` for `main`, and the matching
tag name for every `v*` tag.

The image runs as a non-root user, applies migrations on start, and reads
configuration from `/app/versabot.toml` (optional) with `VERSABOT__*`
environment overrides - in a container, env-only configuration is the
usual choice:

```sh
docker run -d --name versa-bot \
  -e VERSABOT__DISCORD__TOKEN=your-token \
  -e VERSABOT__STORAGE__URL=sqlite:///data/versabot.db \
  -v versa-bot-data:/data \
  ghcr.io/owner/versa-bot:latest
```

For production, point `VERSABOT__STORAGE__URL` at PostgreSQL. To use a
config file instead of env vars, mount it at `/app/versabot.toml:ro`.
Set `RUST_LOG` to tune log verbosity (default `info`).

## Development

- `cargo test` - unit tests cover the kernel pipeline, storage guild
  isolation, the command registry/dispatcher, the auth policy, and the
  activity tracker.
- `cargo clippy --all-targets` - the deny-level lints must stay clean.

### CI

`.github/workflows/ci.yml` (GitHub Actions -> GHCR) and
`.forgejo/workflows/ci.yaml` (Forgejo -> the instance registry) run the
same pipeline on pushes to `main` and on `v*` tags:

1. `cargo fmt --all -- --check`
2. `cargo clippy --all-targets --locked` - deny-level lints gate
3. `cargo test --locked`
4. Docker image build + push (see [Docker](#docker)) - only after 1-3 pass

The compiler is pinned to `rust:1.98` in CI and in the Dockerfile. Every
cargo gate runs `--locked`, so keep `Cargo.lock` in sync with dependency
changes.

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
├── plugins/           # features: auth, command dispatcher, tracker, audit trail, status (WIP)
├── infrastructure/    # adapters
│   ├── inbound_adapters/   # Discord gateway + scoped output factory
│   ├── outbound_adapters/  # storage (sqlx), Discord command registrar
│   └── plugin_adapters/    # in-memory event bus, command registry
└── common/
```

## Roadmap

- LLM chat plugin; message history
- Further platform adapters (Telegram, Matrix, ...)
