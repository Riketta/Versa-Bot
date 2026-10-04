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

Every capability is a plugin; the full manual of each one - behavior,
commands, permission tiers - lives in [Plugins](#plugins).

- Native Discord slash commands, auto-registered at startup from plugin
  declarations.
- Per-guild authorization: a five-tier access ladder (banned / guest /
  user / moderator / admin) with per-user, per-role and default
  assignments, administered from Discord via `/auth`.
- User activity tracker: logs member joins/leaves to a guild audit
  channel and publishes membership events on the plugin bus.
- Audit trail: records the bus membership events as structured `audit`
  tracing events.
- LLM chat bot: per-channel conversations with history, compaction,
  streaming, token-budget context filling, image recognition, an
  emoji-reaction tool and random chime-ins.
- Status rotator: cycles the bot's activity through a configured list.
- Guild-partitioned storage: JSON documents plus an append-only record
  log, scoped to `(platform, guild)` - reading another guild's data is
  impossible by construction. SQLite (default) and PostgreSQL.
- Observability: `tracing` logging to stdout plus optional
  Sentry/GlitchTip reporting (DSN-driven) with release tagging; every
  event is traced with its origin, and error-grade failures surface as
  Issues. Audit-grade records at `info` cover command dispatch, the
  command registration trail and every LLM answer (model, trigger,
  latency, token usage); `debug` adds pipeline traversal, provider
  traces and scheduler ticks. Privacy rule: shapes and counters, never
  contents.
- Fault isolation: a panicking plugin cannot crash the bot - pipeline
  hooks and event-bus subscribers are caught and logged, the event is
  dropped, and the rest of the chain or bus keeps working.
- Configuration hot reload: hot-reloadable sections apply live
  (`[status]`); startup-only settings (token, storage, Sentry, LLM
  providers) require a restart.
- Graceful shutdown on Ctrl-C (plugins stop in reverse order).

## Getting started

Prerequisites: Rust 1.98 (edition 2024) - the pinned toolchain, the same
compiler CI and the Docker image use.

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

Every capability is a plugin under `src/plugins/`. Each section below is
the plugin's manual: what it does, its commands, and the access tier
every command requires. Tiers are the auth plugin's ladder - `banned` <
`guest` < `user` < `moderator` < `admin`. Every command reply, denial
included, is ephemeral (visible to the invoker alone); a member below a
command's tier gets an ephemeral notice naming the required and actual
tier. Discord guild administrators are always `admin`, and a fresh
guild starts with default tier `user`, so configuration commands are
usable on day one.

### Authorization (`auth` plugin)

The auth plugin is the bot's per-guild access gate. It runs first in the
middleware pipeline on every guild message and slash command; whatever it
rejects never reaches the other plugins.

**Policy model.** Per guild, a tier policy: every member has an effective
tier on a five-step ladder -

| Tier | What it allows |
|---|---|
| `banned` | nothing - the bot ignores their messages and commands entirely, without any reply |
| `guest` | talk to the bot (chat interactions), no commands |
| `user` | basic commands (`/ping`, `/llm_status`, `/llm_models`) |
| `moderator` | every service command (`/assign_tracker`, `/llm_assign`, `/llm_set`, ...) |
| `admin` | everything, including `/auth` tier management |

The policy assigns tiers three ways: per **user**, per **role** (holding
the role grants at least that tier), and a **default tier** for everyone
else. The effective tier is the best of what applies. Two overrides sit
above the stored data:

- An explicit `banned` user assignment beats every role grant.
- **Discord guild administrators are always `admin`**, by construction -
  the clamp cannot be removed, so an admin can never be locked out.

A fresh guild starts open (default tier `user`), so the commands that
configure the bot are usable on day one. The policy is stored in the
guild's own storage namespace, so guilds never see each other's
configuration.

Robustness rules:

- A malformed or unreadable policy **fails closed**: the request is
  denied with a "policy is unreadable" notice - corruption never widens
  access.
- Denied slash commands get an ephemeral embed, visible to the invoker
  only, naming the required and the actual tier. Denied plain messages
  are rejected silently - a public "no" would be a spam vector, and
  ephemeral replies are impossible there. Banned members get no answer
  anywhere.
- Commands carry their required tier in their declaration. The Discord
  adapter does not hide tier-gated commands (only `/auth` keeps the
  native Manage Server gate as defense in depth) - enforcement is
  kernel-side, so a moderator without Discord permissions can still run
  service commands.
- Passive events (member joins/leaves, presence) and DMs are not gated:
  auth decides who may *use* the bot, not what happens in the guild.

**Usage.** `/auth` is guild-only, requires the `admin` tier, and is
additionally hidden behind Discord's **Manage Server** permission
(defense in depth on top of the tier check). Every answer is ephemeral,
so policy data stays between the bot and the admin.

| Command | Tier | Effect |
|---|---|---|
| `/auth action:show` | admin | show the current policy: default tier, user and role assignments |
| `/auth action:set tier:<tier> user:@member` | admin | assign a tier to a user |
| `/auth action:set tier:<tier> role:@role` | admin | grant a tier to everyone holding the role |
| `/auth action:clear user:@member` or `role:@role` | admin | remove an assignment |
| `/auth action:default tier:<tier>` | admin | set the default tier for unlisted members |

Specify either `user` or `role`, never both. Lowering your own tier is
possible and warns in the reply: Discord administrators keep `admin`
regardless, but without that you may need another admin to undo it.

### Command demo (`command` plugin)

Ships `/ping` - the walking-skeleton command proving the full loop
(plugin declaration -> Discord sync -> interaction -> dispatch ->
reply). It answers `Pong` and works in DMs too. Real commands belong to
the feature plugins that own their meaning; this one exists to keep the
loop honest.

| Command | Tier | Effect |
|---|---|---|
| `/ping` | user | check that the bot is alive - it replies with Pong |

### User activity tracker (`tracker` plugin)

Logs guild membership activity. Member joins and leaves arrive as
gateway events; the tracker publishes `UserJoinedGuild` /
`UserLeftGuild` domain events on the plugin bus for other plugins (the
audit log) to react to - unconditionally, even with no audit channel
assigned or its config unreadable - and, when a channel is assigned,
posts a short join/leave notice there. The assignment is per guild,
self-service: run `/assign_tracker` in the channel that should become
the audit channel. Needs the privileged `GUILD_MEMBERS` intent (see
[Gateway intents](#gateway-intents)).

| Command | Tier | Effect |
|---|---|---|
| `/assign_tracker` | moderator | make this channel the guild's audit channel (one per guild, last write wins) |
| `/unassign_tracker` | moderator | stop tracking for this guild |

### Audit log (`audit_log` plugin)

The event bus's first consumer: subscribes to the membership events the
tracker publishes and records each one as a structured `audit` tracing
event with origin fields - stdout, plus Sentry/GlitchTip when reporting
is configured (`info` grade). No per-guild configuration and no
commands; removing the plugin removes only the trail, not the tracking.

### Status rotator (`status_rotator` plugin)

Cycles the bot's Discord activity through a configured list. Presence
is bot-wide, not per-guild. Statuses draw from a shuffled deck - every
status shows once per cycle, in fake-random order - and the first
status lands exactly on connect.

Operator configuration lives in the optional `[status]` config section
(`interval_seconds`, `statuses`); omitted, empty, or a zero interval
means the rotation is off. The section hot-reloads: editing it
re-applies the rotation live, removing it stops the rotation. No
commands.

### LLM chat bot (`llm` plugin)

A per-channel chat assistant driven by OpenAI-compatible endpoints. The
split of responsibilities is deliberate:

- The **bot operator** declares providers (endpoints + keys) and model
  capabilities once in the `[llm]` config section. Startup-only:
  changes require a restart.
- **Guild moderators** assign the bot to channels and tune each channel
  via slash commands - picking among the declared models, never
  configuring endpoints. Only declared models are legal: `/llm_assign`
  and `/llm_set model=` reject anything else, and `/llm_models` lists
  the catalog. Keys never appear in config files or guild storage; they
  are resolved from environment variables at boot.

Every channel's conversation lives in its own storage namespace inside
the guild's partition - channels and guilds cannot read each other's
history.

**Operator setup.** Minimal example (full reference in
`versabot.example.toml`):

```toml
[llm]
# Optional defaults: default_system_prompt, default_compaction_prompt,
# compaction_model, compaction_keep_tail, max_message_length (capped at
# Discord's 2000), stream_interval_ms, max_consecutive_newlines (collapse
# blank-line runs in answers down to N; absent = untouched). `log_raw_traffic = true`
# dumps every LLM request and response body at DEBUG level (stdout only)
# while debugging a provider - it carries conversation content, so it
# stays off by default.
# Image recognition: set image_model to a vision-capable declared model;
# channels then opt in with /llm_set images on. Optional: image_prompt,
# image_max_side (512), image_jpeg_quality (85), image_max_source_bytes
# (8 MiB), max_images_per_message (2), react_max_per_message (3 - the
# emoji-reaction tool's per-answer cap).
# image_model = "local/unsloth/gemma-4-26B-A4B-it-qat-GGUF"

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

Per-model `summary_placement` controls how the compaction summary enters
the context: `system_turn` (default - a separate second system message,
with a stable placeholder keeping the slot present), `system_suffix`
(merged into the end of the system prompt - the one shape every chat
template honors; prefer it for models on templates that silently drop
later system turns, which would erase the summary after every
compaction) or `assistant_turn` (assistant message before the window).

Undeclared models remain usable but get default capabilities: no
reasoning parameter is ever sent for them, and context filling stays
message-count based.

**Provider-specific request fields.** `[llm.providers.<name>.extra_body]`
merges arbitrary JSON fields into every completion body - for endpoint
knobs the adapter does not model. `model` and `messages` are
engine-owned and cannot be overridden; other keys take precedence over
the standard rendering. Typical use: switching thinking off on a local
llama.cpp server, whose templates ignore the `reasoning_effort` scale
entirely (`off` therefore cannot reach it as a wire value):

```toml
[llm.providers.local]
api_url = "http://192.168.1.35:8001/v1"
extra_body = { chat_template_kwargs = { enable_thinking = false } }
# or, on servers supporting the budget field: extra_body = { reasoning_budget = 0 }
```

**Template variables** make the passthrough per-channel reactive. A
string that is exactly `${enable_reasoning}` or `${reasoning_effort}` is
substituted at request time from the channel's reasoning setting:
`enable_reasoning` renders a JSON boolean (`false` only when
`reasoning_effort` is `off`, `true` otherwise - unset means the provider
default applies, which for thinking templates is on);
`reasoning_effort` renders the effort string (`"high"`, ...) or `null`
when unset or `off`. Placeholders embedded in longer strings substitute
textually; unknown variables stay literally in place and warn in the
logs. The variables ignore the model's declared reasoning capability -
the operator decides per provider where the knob applies:

```toml
# `/llm_set reasoning_effort=off` in a channel now reaches llama.cpp
# templates too:
extra_body = { chat_template_kwargs = { enable_thinking = "${enable_reasoning}" } }
```

**How conversations work.**

- **Capture** decides what enters the channel's history. `bot_related`
  (default) tracks only bot-related messages: mentions and replies into
  the captured conversation. `all_messages` tracks everything. The
  bot's own answers are recorded at send time. Chime-ins only roll on
  captured messages - in `bot_related` mode that is essentially never
  (captured non-triggering messages are rare), so random answers need
  `capture_mode = all_messages`.
- **Trigger** decides when the bot answers: an explicit mention or a
  direct reply to one of the bot's own messages. Replies between users
  are captured but do not trigger (`random_chance` below is the
  exception).
- **Context** is assembled as: system prompt -> compaction summary (or a
  stable placeholder if none - default `summary_placement`, which merges
  into the prompt instead when a model declares `system_suffix`) -> live
  window, oldest first. User turns render through the channel's turn
  template (`[{sender}](<@{user_id}>): {message}` by default - the
  `[Name]<@id>` tag shape normalized inbound messages carry, so the model
  can assemble clean mentions; also available: `{guild_name}`, `{time}`
  (unix seconds)); bot turns are plain assistant messages. Template fields
  are baked into the stored record at capture, so a rendered turn never
  changes retroactively. The window is selected newest-first under the
  token budget and `depth`, whichever bites first - the newest turn is
  always included.
- **Mentions** in captured messages are normalized to `[Name]<@id>` - the
  model sees both who was named and the raw tag to imitate in replies.
  Replies using the same shape are converted back to bare mentions on
  send, so a tag the model assembles pings cleanly.
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
- **Image recognition** (opt-in per channel, `/llm_set images on`;
  needs an operator-configured `[llm] image_model`): attached images on
  captured messages are described by a vision-capable model at capture
  time, and the description is stored with the message. The context
  renders it as a markdown image reference - `![description](image.png)`
  (multi-image messages number the placeholders) - so the chat model
  reads what an image showed without ever receiving pixels: any declared
  model works, and costs stay bounded (each image is described once,
  rescaled to `image_max_side`, at most `max_images_per_message` per
  message). The recognition prompt is customizable per channel
  (`image_prompt`) - useful for pinning the description language.
  Undescribed images (feature off, recognition failure, oversize,
  over-cap) still render `![image](image.png)`, so the model at least
  knows an image was posted. Images are fetched from Discord's CDN only;
  recognition usage never mixes into the channel's token stats.
- **Emoji reactions** (the react tool, opt-in per channel,
  `/llm_set react on`): the model may decorate the message it replies to
  by emitting a `[[react: emoji ...]]` marker anywhere in its answer.
  The marker is stripped before the answer is shown or recorded, and the
  tokens fire as reactions on the reply target: Unicode (`🤓`), custom
  (`:dorkiS:` - resolved against the guild's own emojis, foreign ones
  skip silently) and fully qualified (`<:name:id>`) forms all work.
  Degradation is per-token: an invalid token drops, valid siblings fire,
  and a failed reaction never touches the answer. An answer that is only
  a marker is answered with just the reaction - no fallback, no phantom
  bot turn. A hallucinated marker in a react-off channel is stripped
  too, but fires nothing.

**Context sizing.** The engine loads at most the newest `depth` +
compaction-tail records per message (the operational window - a much
longer log serves its newest part, so per-message cost stays flat even
with compaction off). The window then fills newest-first up to `depth`
messages. Once the endpoint has reported real token usage (recorded
per channel), filling becomes token-budget based: the channel's
`context_budget` if set, otherwise the model's declared
`context_window` minus the completion reserve and a 10% estimator
margin - with `depth` remaining the secondary cap. Until then (or
without a declared window) only the message limit applies.
`/llm_status` shows which mechanism is active.

**Commands.** All are guild-only, and every reply is **ephemeral** -
visible only to the member who ran the command: confirmations, usage
notices and reports never appear in the channel; tier denials are
ephemeral too (tiers are enforced by the [auth
plugin](#authorization-auth-plugin)).

| Command | Tier | Effect |
|---|---|---|
| `/llm_assign model:<provider/model>` | moderator | assign the bot to this channel - `model` must be one of the operator-declared models (offered as a dropdown, listed by `/llm_models`); re-assigning retunes in place |
| `/llm_models` | user | list the declared models - the legal assignment set, with declared capabilities (reasoning, context window) |
| `/llm_unassign` | moderator | remove the bot from this channel (history is kept) |
| `/llm_prompt prompt:<text>` | moderator | set the channel system prompt; `clear` falls back to the plugin default (inline limit: Discord's ~6000-character option cap) |
| `/llm_prompt_file file:<attachment>` | moderator | set the system prompt from an uploaded text/markdown file - for prompts beyond the inline limit; fetched from Discord's CDN only, capped by `[llm] max_prompt_file_bytes` (128 KiB default) |
| `/llm_set key:<key> value:<value>` | moderator | tune one channel setting (table below) |
| `/llm_get key:<key>` | moderator | show a setting's current value (defaults render as the effective value, long text truncated); omit `key` to list every setting |
| `/llm_dump` | moderator | dump every setting at once in one copy-pasteable code fence (`key = value`, effective values); prompts are not dumped at all - only set-or-not and size, the text is one `/llm_get key` away |
| `/llm_cutoff` | moderator | start a fresh conversation: summary cleared, cutoff moved past all records - stored history is kept |
| `/llm_status` | user | report: active system prompt (override or plugin default, char count, fingerprint, head preview), model, reasoning setting, window usage, compaction, image recognition (state, model, prompt length), reactions (state, silent-react chance), capture mode, chime-in chance, summary preview, link to the context start, last-request token stats (incl. reasoning tokens when reported), last response time (endpoint-reported or measured) |
| `/llm_admin` | moderator | make this channel the guild's service channel for error notices (one per guild, last write wins) |
| `/llm_admin_clear` | moderator | stop service notices |

**`/llm_set` keys.** Every key takes one value; `clear`, `none` or
`default` as the value resets the key to its default - the two keys
without a default (`model`, `depth`) refuse and point at
`/llm_unassign`. On/off keys accept `on`/`off` (also
`true`/`yes`/`1` and `false`/`no`/`0`). Invalid values are answered
with usage and never saved. `/llm_get` reads the same keys back (the
channel system prompt itself is `/llm_prompt`'s, visible via
`/llm_status`).

| Key | Values | Default | Meaning |
|---|---|---|---|
| `model` | declared model ref | set by `/llm_assign` | switch this channel's chat model; unknown refs are rejected with the declared list |
| `temperature` `top_p` `top_k` `min_p` `frequency_penalty` `presence_penalty` | finite number | not sent | sampling knobs, one per key - tune per-channel tone (an educational channel can run low temperature, an entertainment one high); cleared = not sent |
| `max_tokens` | whole number | not sent | completion size cap |
| `reasoning_effort` | free string, or `off` | none - no parameter sent | reasoning hint, sent only when the model declares `reasoning = true`. Any other value is sent as-is to effort-style providers (Z.ai GLM: `low`/`high`/`max`) and enables thinking on switch-style ones. `off` renders an explicit disable on switch-style providers (GLM-4.5-5.2); effort-style endpoints have no off wire value, so their default applies - on Z.ai GLM that default is `max`, and GLM-5.3 thinks forcibly regardless, so throttle it with `low` |
| `depth` | whole number >= 1 | 100 | live-window size in messages; reaching it triggers compaction |
| `context_budget` | whole number of tokens | auto | prompt-side token budget. Auto = the model's declared `context_window` minus the completion reserve and a margin, active once the endpoint has reported its first usage (before that, count-only filling by `depth`) |
| `capture_mode` | `bot_related` or `all_messages` | `bot_related` | what enters the channel's history: only messages mentioning or replying the bot, or everything (random chime-ins need `all_messages` to have material) |
| `compaction` | on / off | on | summarize-and-cutoff when the window outgrows `depth` |
| `compaction_model` | declared model ref | plugin `[llm] compaction_model`, else the channel's chat model | which model writes the summaries |
| `compaction_prompt` | text | plugin default | summarization instruction |
| `images` | on / off | off | describe attached images on captured messages via the recognition model; needs an operator `[llm] image_model` |
| `image_model` | declared model ref | plugin `[llm] image_model` | recognition model override for this channel |
| `image_prompt` | text | plugin `[llm] image_prompt` | recognition instruction, e.g. pin the description language |
| `react` | on / off | off | emoji-reaction tool: the model may decorate the message it replies to by emitting a `[[react: ...]]` marker, stripped before the answer is shown |
| `streaming` | on / off | off | stream the answer live from the provider (SSE): the message appears with the first tokens and is edited at `stream_interval_ms` |
| `random_chance` | 0-100 (clamped) | 2 | percent chance to chime in on a captured non-trigger message; 0 = off |
| `random_cooldown` | whole seconds | 5 | minimum seconds between chime-ins - the reply and silent-react rolls each keep their own tracker behind it; 0 = none |
| `random_react_chance` | 0-100 (clamped) | 10 | percent chance for a silent react (emoji only, no reply) on a captured non-trigger message; independent of `random_chance`; needs `react` on |
| `max_length` | 1-2000 characters | `max_message_length` (2000) | per-channel reply-splitting limit |
| `turn_template` | template containing `{sender}` and `{message}` | `[{sender}](<@{user_id}>): {message}` | how user turns render into the model context; fields: `{sender}`, `{user_id}`, `{guild_name}`, `{time}` (unix), `{message}` |

**Delivery.** While an answer generates and delivers, the bot holds the
channel's typing indicator - users see it composing, not frozen. The
answer posts as a native Discord reply to the message that triggered it
(the mention/reply target, or the message a chime-in fired on); only the
first message of a split answer carries the reply header, the rest
continue plainly. Long answers split on line boundaries - a line that
does not fit moves whole to the next message. With `streaming` on, the
endpoint is asked for a real SSE stream: the message appears with the
first tokens and is edited in place (throttled by `stream_interval_ms`)
while the model writes; the final edit carries the exact full text, and
answers longer than one message still split. Providers without SSE
degrade gracefully: the answer then arrives as one piece (and a
non-streaming channel always does). Chime-ins are cooldown-guarded
(default 5 seconds between them, per-channel `random_cooldown`) and
only fire on messages the bot actually captured; the chance draws from a
per-channel deck, so hits balance out over each 100-draw cycle instead
of clumping. With `react` on, a second independent roll
(`random_react_chance`, default 10%) can silently react to a captured
message without replying: one single-shot call carrying a dedicated
reaction-only instruction (the model must choose a reaction, not write
a reply), whose prose is discarded, only the marker's emojis apply,
nothing is recorded - the two rolls
each keep their own cooldown and their own deck, and never suppress one
another.

**Failures.** A message that tags the bot or replies to it is guaranteed a
visible response: if the generated answer is impossible (provider
unreachable or rejecting, reasoning-only response, unreadable history),
the channel gets a short generic fallback notice instead - never a raw
error, and never recorded as a bot turn (history stays consistent). A
reply into the conversation cannot even be detected when the history is
unreadable - that failure mode stays silent. If the answer cannot be
delivered at all (platform outage on every send path), nothing is
recorded either - the bot never writes down a turn nobody saw. Random
chime-ins are unprompted and stay silent on failure. The operator
still sees what happened: a rate-limited embed (at most one per 5 minutes
per service channel) carries the error classification only - endpoint
response bodies and storage errors can name operator infrastructure, so
they stay in the logs.

**Logging.** Every generated answer leaves an `info` audit record in the
logs: model, trigger (`triggered` vs `chime`), latency (engine wall clock,
plus the complete provider time with its source - endpoint-reported when
the provider publishes timings like llama.cpp's `timings` block, else
adapter-measured), prompt/completion token usage (cached and
reasoning-token breakdowns when the endpoint reports them), live-window
and currently-used sizes. Sizes and counters only - message text and
prompts never log. At `debug`, chime roll decisions (cooldown skips and
deck draws) and per-request LLM traces (provider, model, status, complete
response time and whether the endpoint or the adapter timed it) explain
why the bot stayed quiet or answered slowly.

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
- The storage adapter ships two SQL dialects (SQLite and PostgreSQL), but
  the default suite runs SQLite only. The `postgres_*` tests in
  `sqlx_storage` opt in via the `VERSABOT_TEST_PG_URL` environment
  variable - point it at a throwaway database (the bot runs its own
  migrations) and they exercise the document roundtrip, guild isolation,
  the reserved namespace guard, and the record log incl. the documented
  concurrent-append contract against the real dialect. Without the
  variable they report a skip and pass.

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
