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
  administered from Discord via `/auth` - unconfigured guilds are open,
  an empty policy means guild-administrators-only, corruption fails
  closed. Details in [Plugins](#plugins).
- User activity tracker (`tracker` plugin): logs member joins/leaves to the
  guild's audit channel and publishes `UserJoinedGuild` / `UserLeftGuild`
  domain events on the plugin bus for other plugins to react to. Channel
  assignment is self-service: `/assign_tracker` run in a channel makes it
  the audit channel, `/unassign_tracker` turns tracking off (both require
  the Manage Server permission, enforced by Discord itself; replies are
  visible only to the invoker).
- Audit trail (`audit_log` plugin): the event bus's first consumer - logs
  membership changes published on the bus as structured `audit` tracing
  events (stdout + Sentry/GlitchTip), with origin fields, no per-guild
  configuration needed.
- LLM chat bot (`llm` plugin): per-channel chat with conversation history,
  compaction, streaming, token-budget context filling and random
  chime-ins. Operators declare OpenAI-compatible providers in `[llm]`
  (keys via env); guild admins assign and tune each channel via `/llm_*`
  commands. The guild message content the bot reads is why
  `MESSAGE_CONTENT` is requested. Full manual in
  [Plugins](#llm-chat-bot-llm-plugin).
- Status rotator (`status_rotator` plugin): cycles the bot's activity
  through a configured list on a configured interval - both come from the
  optional `[status]` section of the config file (presence is bot-wide,
  not per-guild); omitted or empty means the rotation is off.
- Guild-partitioned document storage: plugins persist JSON documents scoped
  to `(platform, guild)` - reading another guild's data is impossible by
  construction. An append-only record log (`append`/`list_after`/`count_after`)
  sits alongside the documents for high-volume ordered data such as
  conversation history. SQLite (default) and PostgreSQL.
- Observability: `tracing` logging to stdout plus optional Sentry/GlitchTip
  reporting (DSN-driven) with release tagging; every event is traced with
  its origin, an optional sample rate feeds performance transactions, and
  warn/info/error ship as Sentry log items while error-grade failures
  (storage, LLM provider) surface as Issues. Audit-grade records at `info`
  cover command dispatch (who ran what, argument shapes - never contents),
  the command registration trail (per-plugin registrations plus the
  completed Discord sync) and every LLM answer (model, trigger, latency,
  token usage, window sizes); `debug` adds pipeline traversal, provider
  request/response traces, scheduler ticks and chime roll decisions.
- Fault isolation: a panicking plugin cannot crash the bot - pipeline hooks
  and event-bus subscribers are caught and logged (plugin + event), the
  event is dropped, and the rest of the chain or bus keeps working.
- Configuration hot reload: the configuration (file + env overrides) is
  re-read every few seconds; changes to hot-reloadable sections apply
  without a restart - e.g. editing `[status]` re-applies the rotation live
  (identical settings are ignored, removing the section stops it). Startup
  -only settings (token, storage, Sentry, LLM providers) are not affected.
- Graceful shutdown on Ctrl-C (plugins stop in reverse order).

## Getting started

Prerequisites: Rust 1.88+ (edition 2024).

1. Copy the checked-in example and fill in your Discord token:

   ```sh
   cp versabot.example.toml versabot.toml
   ```

   The real `versabot.toml` is gitignored and looks like this:

   ```toml
   # Verbose logging for the bot's internals (reqwest/hyper stay at warn;
   # RUST_LOG overrides this entirely, e.g. RUST_LOG=debug).
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
   # traces_sample_rate = 0.01  # performance sampling; omit for error-only

   # Optional: status rotator - cycles the bot's activity.
   # [status]
   # interval_seconds = 300
   # statuses = ["with the API", "versa-bot"]

   # Optional: LLM chat runtime (providers, model capabilities, defaults -
   # see versabot.example.toml for the full reference).
   # [llm]
   # [llm.providers.local]
   # api_url = "http://127.0.0.1:8001/v1"
   # [llm.models."local/gemma"]
   # reasoning = false
   ```

2. `cargo run` from anywhere (migrations are embedded in the binary).
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
| `MESSAGE_CONTENT` | **privileged** | the LLM chat plugin reads guild message content for conversation history - enable "Message Content Intent" in the Developer Portal |

So exactly two portal toggles are needed today (Developer Portal → Bot),
otherwise the gateway disconnects on start: enable **Server Members Intent**
and **Message Content Intent**.

Environment variables override the file:
`VERSABOT__DISCORD__TOKEN`, `VERSABOT__STORAGE__URL`,
`VERSABOT__SENTRY__DSN`, ...

## Plugins

### Authorization: who can use the bot (`auth` plugin)

The auth plugin is the bot's per-guild access gate. It runs first in the
middleware pipeline on every guild message and slash command; whatever it
rejects never reaches the other plugins.

**Policy model.** Per guild, two allow-lists: allowed **users** and
allowed **roles**. A member passes when they are listed by user or hold
any listed role. There are no deny-lists - `deny` removes an entry from
the allow-lists. The policy is stored in the guild's own storage
namespace, so guilds never see each other's configuration.

Three policy states with deliberately different defaults:

| State | Who can use the bot |
|---|---|
| No policy (fresh guild) | everyone - open by default, otherwise the gate would deny the very commands that configure it |
| Policy exists, both lists empty | only Discord guild administrators - there is always at least one admin |
| Policy has at least one entry | exactly the listed users and holders of listed roles - guild administrators are not special |

The third row is the one to remember: **adding the first entry switches
the guild to list-only mode.** Make sure the first `allow` includes
yourself (or a role you hold), or you lock yourself out until someone
still allowed re-adds you.

Robustness rules:

- A malformed or unreadable policy **fails closed**: the request is
  denied with a "policy is unreadable" notice - corruption never widens
  access.
- Denied slash commands get an ephemeral embed, visible to the invoker
  only, naming the group that rejected them (`users` group / `roles`
  group / "only guild administrators while the policy is empty"). Denied
  plain messages are rejected silently - a public "no" would be a spam
  vector, and ephemeral replies are impossible there.
- Passive events (member joins/leaves, presence) and DMs are not gated:
  auth decides who may *command* the bot, not what happens in the guild.

**Usage.** `/auth` is guild-only and requires the **Manage Server**
permission (Discord hides it from members without it); every answer is
ephemeral, so policy data stays between the bot and the admin.

| Command | Effect |
|---|---|
| `/auth action:show` | show the current policy as mention lists |
| `/auth action:allow user:@member` | allow a user |
| `/auth action:allow role:@role` | allow everyone holding the role |
| `/auth action:deny user:@member` | remove a user; warns when this empties the policy |
| `/auth action:deny role:@role` | remove a role |

Specify either `user` or `role`, never both. Managing the policy is
itself gated by the policy: whoever runs `/auth` must already be allowed
to use the bot - do not deny yourself out.

### LLM chat bot (`llm` plugin)

A per-channel chat assistant driven by OpenAI-compatible endpoints. The
split of responsibilities is deliberate:

- The **bot operator** declares providers (endpoints + keys) and model
  capabilities once in the `[llm]` config section. Startup-only:
  changes require a restart.
- **Guild admins** assign the bot to channels and tune each channel via
  slash commands - picking among the declared models, never configuring
  endpoints. Only declared models are legal: `/llm_assign` and
  `/llm_set model=` reject anything else, and `/llm_models` lists the
  catalog. Keys never appear in config files or guild storage; they
  are resolved from environment variables at boot.

Every channel's conversation lives in its own storage namespace inside
the guild's partition - channels and guilds cannot read each other's
history.

**Operator setup.** Minimal example (full reference in
`versabot.example.toml`):

```toml
[llm]
# Optional defaults: default_system_prompt, default_compaction_prompt,
# compaction_model, compaction_keep_tail, max_message_length,
# stream_interval_ms. `log_raw_traffic = true` dumps every LLM request
# and response body at DEBUG level (stdout only) while debugging a
# provider - it carries conversation content, so it stays off by default.

[llm.providers.zai]
api_url = "https://api.z.ai/api/coding/paas/v4"
api_key_env = "VERSABOT_LLM_ZAI_KEY"
# How the reasoning parameter is rendered: "openai_effort" sends
# reasoning_effort: "<value>" (no off value exists - omitted means the
# provider default, and Z.ai GLM defaults to `max` effort, with `low` as
# the GLM-5.3 minimum); "glm_thinking" sends the boolean
# thinking: {"type": "enabled"|"disabled"} switch, where `off` renders a
# real disable (GLM-4.5 through 5.2; GLM-5.3 thinks forcibly).
reasoning_style = "openai_effort"

[llm.models."zai/glm-5.3-flash"]
reasoning = true          # per-channel reasoning_effort is sent only for these
context_window = 131072   # enables token-budget context filling
```

Undeclared models remain usable but get default capabilities: no
reasoning parameter is ever sent for them, and context filling stays
message-count based.

**How conversations work.**

- **Capture** decides what enters the channel's history. `bot_related`
  (default) tracks only bot-related messages: mentions and replies into
  the captured conversation. `all_messages` tracks everything. The
  bot's own answers are recorded at send time.
- **Trigger** decides when the bot answers: an explicit mention or a
  direct reply to one of the bot's own messages. Replies between users
  are captured but do not trigger (`random_chance` below is the
  exception).
- **Context** is assembled as: system prompt -> compaction summary (or a
  stable placeholder if none) -> live window, oldest first. User turns
  render through the channel's turn template (`{sender}: {message}` by
  default); bot turns are plain assistant messages. The window is
  selected newest-first under the token budget and `depth`, whichever
  bites first - the newest turn is always included.
- **Reasoning** is never exposed: thinking output (`reasoning_content`
  fields, inline `<think>` blocks) is cut at the provider adapter before
  it can be recorded or rendered - complete `<think>...</think>` pairs
  anywhere (thinking models interleave them mid-answer) and leading
  unclosed blocks (thinking-only or cut-off answers) are stripped; the
  final message and the streaming reveal both draw from the clean
  content only. A reasoning-only answer counts as no answer. An
  unclosed `<think>` mentioned mid-sentence stays literal text, so
  answers can discuss the tag.
- **Compaction** runs after a reply once the live window outgrows
  `depth` (100 messages by default): everything except the newest
  `compaction_keep_tail` (10) records folds into a rolling summary via
  the compaction model. The window is never slid between compactions,
  so the prompt prefix stays byte-stable and provider prompt caches
  stay warm. Records are never deleted - compaction only moves the
  cutoff forward.

**Context sizing.** The window fills newest-first up to `depth`
messages. Once the endpoint has reported real token usage (recorded
per channel), filling becomes token-budget based: the channel's
`context_budget` if set, otherwise the model's declared
`context_window` minus the completion reserve and a 10% estimator
margin - with `depth` remaining the secondary cap. Until then (or
without a declared window) only the message limit applies.
`/llm_status` shows which mechanism is active.

**Commands.** All are guild-only and require the **Manage Server**
permission (Discord hides them from members without it). Every reply is
**ephemeral** - visible only to the admin who ran the command: config
confirmations, usage notices and reports never appear in the channel.

| Command | Effect |
|---|---|
| `/llm_assign model:<provider/model>` | assign the bot to this channel - `model` must be one of the operator-declared models (offered as a dropdown, listed by `/llm_models`); re-assigning retunes in place |
| `/llm_models` | lists the declared models - the legal assignment set, with declared capabilities (reasoning, context window) |
| `/llm_unassign` | remove the bot from this channel (history is kept) |
| `/llm_prompt prompt:<text>` | set the channel system prompt; `clear` falls back to the plugin default (inline limit: Discord's ~6000-character option cap) |
| `/llm_prompt_file file:<attachment>` | set the system prompt from an uploaded text/markdown file - for prompts beyond the inline limit; fetched from Discord's CDN only, capped by `[llm] max_prompt_file_bytes` (128 KiB default) |
| `/llm_set key:<key> value:<value>` | tune one channel setting (table below); value `clear`/`none`/`default` resets it |
| `/llm_cutoff` | start a fresh conversation: summary cleared, cutoff moved past all records - stored history is kept |
| `/llm_status` | report: active system prompt (override or plugin default, char count, fingerprint, head preview), model, reasoning setting, window usage, summary preview, link to the context start, last-request token stats (incl. reasoning tokens when reported) |
| `/llm_admin` | make this channel the guild's service channel for error notices (one per guild, last write wins) |
| `/llm_admin_clear` | stop service notices |

**`/llm_set` keys** (invalid values are answered with usage and never
saved):

| Key | Meaning | Default |
|---|---|---|
| `model` | provider/model reference - declared models only (see `/llm_models`) | set by `/llm_assign` |
| `temperature` `top_p` `top_k` `min_p` `frequency_penalty` `presence_penalty` | sampling parameters; cleared = not sent | provider defaults |
| `max_tokens` | completion size cap | provider default |
| `reasoning_effort` | reasoning hint sent only when the model declares `reasoning = true`; any value is sent as-is for effort-style providers (Z.ai GLM: `low`/`high`/`max` on GLM-5.3) and enables thinking for switch-style providers; `off` explicitly disables thinking where the provider supports a switch; `clear`/`none`/`default` sends no reasoning parameter at all (provider default applies - on Z.ai GLM that default is `max`, so prefer `low` over `off` on GLM-5.3) | none |
| `depth` | live-window size in messages; reaching it triggers compaction | 100 |
| `context_budget` | prompt-side token budget; cleared = auto (model window) once calibrated | auto |
| `capture_mode` | `bot_related` or `all_messages` | `bot_related` |
| `compaction` | summarize-and-cutoff on/off | on |
| `compaction_model` | model used for summaries | the channel's chat model |
| `compaction_prompt` | summarization instruction | plugin default |
| `streaming` | edit the answer in place while it renders | off |
| `random_chance` | percent chance to chime in on a captured non-trigger message | 2 |
| `max_length` | per-channel reply-splitting limit | 2000 (`max_message_length`) |
| `turn_template` | user-turn rendering; must contain `{sender}` and `{message}` | `{sender}: {message}` |

**Delivery.** While an answer generates and delivers, the bot holds the
channel's typing indicator - users see it composing, not frozen. Long
answers split on line boundaries - a line that does
not fit moves whole to the next message. With `streaming` on, the
answer is created once and edited in place (throttled by
`stream_interval_ms`, bounded number of edits) until the final full
text. Chime-ins are cooldown-guarded (5 minutes per channel) and only
fire on messages the bot actually captured; the chance draws from a
per-channel deck, so hits balance out over each 100-draw cycle instead
of clumping.

**Failures.** A message that tags the bot or replies to it is guaranteed a
visible response: if the generated answer is impossible (provider
unreachable or rejecting, reasoning-only response, unreadable history),
the channel gets a short generic fallback notice instead - never a raw
error, and never recorded as a bot turn (history stays consistent).
Random chime-ins are unprompted and stay silent on failure. The operator
still sees what happened: a rate-limited embed (at most one per 5 minutes
per service channel) carries the error classification only - endpoint
response bodies can name operator accounts or projects, so they stay in
the logs.

**Logging.** Every generated answer leaves an `info` audit record in the
logs: model, trigger (`triggered` vs `chime`), latency, prompt/completion
token usage (cached and reasoning-token breakdowns when the endpoint
reports them), live-window and currently-used sizes. Sizes and counters
only - message text and prompts never log. At `debug`, chime roll
decisions (cooldown skips and deck draws) and per-request LLM traces
(provider, model, status, duration) explain why the bot stayed quiet or
answered slowly.

**Raw traffic dump.** For provider debugging, `[llm] log_raw_traffic = true`
dumps the unmodified request and response bodies of every completion at
`debug` level - what parameters were actually sent (e.g. whether a
reasoning parameter made it onto the wire) and what the endpoint returned
(reasoning content is visible only there; it is otherwise cut at the
adapter). DEBUG stays on stdout and never ships to Sentry/GlitchTip, but
the bodies carry full conversation content: operator diagnostic, off by
default, flip it off when done.

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
Set `RUST_LOG` to tune log verbosity (default `info`);
`RUST_LOG=versa_bot=debug` adds the bot's own breadcrumbs - pipeline
traversal, LLM request/response traces, chime roll decisions - on top of
the `info`-grade audit records.

A ready-made Compose deployment ships as `docker-compose.yaml`: it passes
`.env` (copy `.env.example`) into the container, keeps the SQLite file in
the `versa-bot-data` volume, and carries a commented PostgreSQL stack -
`cp .env.example .env`, fill in the token, then `docker compose up -d`.

### Running behind a proxy

The bot's outbound traffic splits in two, and only one half can be
proxied from the config:

- **REST** (slash-command registration, replies, presence) honors
  `[discord] proxy` (SOCKS/HTTP).
- **The gateway WebSocket** bypasses it: serenity's gateway uses its own
  connector with no proxy support, and proxy environment variables are
  ignored. On a network where Discord is reachable only through a proxy,
  the bot boots normally and then hangs silently at shard start - the log
  ends after `Telling shard queuer to start shard 0` and the
  `connected as ...` line never appears. A warning is logged at startup
  whenever `discord.proxy` is set.

The fix is to route the whole container through the proxy at the network
level so both paths ride it. For a SOCKS5 proxy, a tun2socks sidecar
works - and the proxy's own host must be excluded from the tunnel, or
the sidecar would loop into itself:

```yaml
services:
  tun2socks:
    image: xjasonlyu/tun2socks:latest
    restart: unless-stopped
    cap_add:
      - NET_ADMIN
    devices:
      - /dev/net/tun
    environment:
      TUN: tun0
      PROXY: socks5://192.168.1.35:8081
      TUN_EXCLUDED_ROUTES: 192.168.1.35/32  # the proxy host, direct via eth0

  versa-bot:
    # ...unchanged, except the network:
    network_mode: "service:tun2socks"
```

With transparent routing in place, `[discord] proxy` becomes redundant.
A VPN endpoint works the same way via a gluetun sidecar
(`network_mode: "service:gluetun"`, no tun2socks needed). To see where a
shard is stuck, set `RUST_LOG: versa_bot=info,serenity=debug` - serenity
logs gateway state only at debug level.

## Development

Verify changes with the same gates CI runs - same commands, same order,
same flags:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked
cargo test --locked
```

- `cargo test` - unit tests cover the kernel pipeline, storage guild
  isolation (documents + record log), the command registry/dispatcher, the
  auth policy, the activity tracker, and the LLM chat plugin (context
  assembly, token budgets, compaction, splitting, provider request
  building, RNG adapters).
- `cargo clippy --all-targets` - the deny-level lints (`indexing_slicing`,
  `string_slice`, declared in the `[lints.clippy]` table in `Cargo.toml`)
  must stay clean. Clippy lints do not run under `cargo test`, so green
  tests never imply a clean clippy gate - and a deny-level failure adds an
  `error:` line without changing the warning count, so check the exit
  status, not the warnings.

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
├── plugins/           # features: auth, command dispatcher, tracker, audit trail, status rotator, llm chat
├── infrastructure/    # adapters
│   ├── inbound_adapters/   # Discord gateway + scoped output factory
│   ├── outbound_adapters/  # storage (sqlx), Discord command registrar, presence
│   └── plugin_adapters/    # in-memory event bus, command registry, scheduler
└── common/
```

## Roadmap

- Further platform adapters (Telegram, Matrix, ...)
