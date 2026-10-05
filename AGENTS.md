# AGENTS Instructions

This file is the map and the rulebook for working in this repo:
architecture invariants, conventions, and the development gate. It is
deliberately NOT a feature manual - `README.md` documents what the bot
does today (features, config, commands), and rustdoc documents module
contracts. Where this file and the code disagree, the code wins; then fix
the doc.

## Project overview

Multi-tenant chat bot for many unrelated Discord guilds - per-guild data
isolation is a privacy requirement, not a feature. Rust, shipped as a
Docker container. Hexagonal architecture (ports & adapters) around a
micro-kernel core. Discord first; other platforms (Telegram, Matrix,
IRC, ...) arrive later as new adapters, not kernel changes.

Core capabilities:

- Observability via `tracing` + `sentry` (facade, no telemetry port).
- Hot-reloadable config files.
- Guild-partitioned document storage (`sqlx`, SQLite/PostgreSQL) that
  arbitrary plugins extend with their own data.
- Plugin system: source-code plugins (`src/plugins/*`) joined by a
  middleware pipeline + an event bus.
- Commands registered by plugins as native platform commands (Discord
  slash commands) - no prefix parsing in core.

Default plugins: auth (five-tier access ladder, see below), command/ping,
tracker (member lifecycle audit), audit log, status rotator, nickname
(per-guild bot name via `/set_guild_name`), LoL store
tracker (poll-driven: watches the local League client's store, plus
per-user skin/champion watch subscriptions; the one case of a plugin
with NO inbound events - it discovers subscribers via
`StoragePort::list_guilds` instead of an event origin), LoL leaderboard
(command-driven `/lol_leaderboard`: world-data statistics over a
pluggable source port, in-process TTL cache, no storage), LLM chat bot
(per-channel identity, config, and history; plain `reqwest` against
OpenAI-compatible endpoints - no `rig`). A generic message-history plugin
is deferred until a real consumer appears.

**Platform strategy:** universal feature parity across chats is NOT a
goal. The kernel and the event taxonomy are platform-blind; core plugins
work off the taxonomy and degrade gracefully where a platform lacks a
concept. Platform-specific features live in adapters or platform-scoped
plugins. Platform types never enter the kernel or core plugins - because
of economics, not purity: platform branching inside every plugin scales
with (plugins x platforms); new-adapter integration scales with 1.
Platform-FACT exceptions (caps, host names, naming) in core-plugin
constants, comments, and user-facing text are tolerated while each
deployment serves exactly one chat provider - keep them behind a
platform-neutral name where one exists, and scrub them only when a
second adapter actually lands.

## README maintenance

`README.md` is user-facing documentation and must stay in sync with
reality. When a change adds or alters features, configuration, commands,
project layout, or setup steps, update the README in the same change. The
README describes what the bot does today; the Roadmap section is the only
forward-looking part.

## Build & CI

Both forges (`.github/workflows/ci.yml` -> GHCR, `.forgejo/workflows/
ci.yaml` -> the instance registry) run the same pipeline on `main` and
`v*` tags: fmt, clippy, tests, then the Docker image build gated on the
test job. The toolchain is pinned to the same compiler in CI, the
Dockerfile, and local dev.

- The local gate IS the CI gate. Run exactly what CI runs, in this order,
  after the LAST edit of a change:
  `cargo fmt --all -- --check` -> `cargo clippy --all-targets --locked`
  -> `cargo test --locked`.
- Read the clippy run's exit status and `error:` lines, not the warning
  count: a deny-level lint failure adds an error without changing the
  warning tally. Deny lints are declared in `[lints.clippy]` in
  `Cargo.toml` (including `unwrap_used` outside tests). Pedantic/doc
  warnings are tolerated - do not switch to `-D warnings` until a
  zero-warning cleanup pass lands.
- Every cargo gate runs `--locked`: dependency changes ship with an
  updated lockfile, and a stale lockfile must fail fast in tests, not in
  packaging.
- `cargo test` does not execute clippy lints (`[lints]` apply only under
  clippy-driver): green tests never imply a clean clippy gate.

## Hexagonal micro-kernel architecture

- The plugin contracts ARE ports. The plugins ARE adapters. The kernel IS
  the inner hexagon. The kernel assembles the chain but never knows
  what's in it.
- The kernel never imports plugins - it only knows the port traits.
- Plugins never import each other - they communicate only via the
  kernel's event bus. The one allowed coupling: a bus subscriber imports
  the event *type* from the owning plugin's module. That type-level
  import IS the bus contract (no calls, no shared behavior);
  `audit_log` -> `tracker` is the precedent.
- A plugin MAY internally be its own hexagon (the LLM plugin is), but any
  internal structure is valid as long as it honors the plugin contract.
- Kernel domain stays minimal: if logic needs a plugin to exist, it
  doesn't belong in the kernel.

**Port families and cardinality:**

- Kernel service ports (`StoragePort`, `ConfigPort`, `SchedulerPort`,
  `EventBusPort`, ...): infra the kernel consumes and exposes to plugins.
  One active adapter, one instance per kernel, owned by the kernel, wired
  at the composition root, shared with plugins via injection.
- Plugin-facing ports (`PluginPort`, `MiddlewarePluginPort`): contracts
  plugins implement - many adapters, one per participating plugin.
  `PluginPort` is the full implementation surface: identity + lifecycle
  (`init`/`start`/`stop`). `MiddlewarePluginPort` is the opt-in
  per-plugin pipeline STEP, not the pipeline itself: `pre` (forward hook,
  may short-circuit) and `post` (backward cleanup hook). The pipeline is
  a kernel-owned runner (`KernelService` implementing
  `RequestHandlerPort`). A plugin doing both is registered once - the
  same `Arc` is cast into the `plugins` and `middleware` lists.

**Communication:** kernel -> plugin via lifecycle calls and the pipeline;
plugin -> kernel via injected service ports; plugin -> plugin never
directly, always via `EventBusPort` (derived domain events only - the bus
never carries raw inbound events). The kernel routes the bus but never
defines event meanings; the emitting plugin owns the event type.

**Kernel lifecycle:** `KernelService::boot()` runs `init()` on all
plugins, then `start()` on all, so every plugin is initialized before any
starts. It validates wiring (middleware must dual-register in `plugins`;
duplicate names rejected) and rolls back already-started plugins in
reverse order on a failed `start()`. `shutdown()` (explicit call or
`Drop` fallback) stops what reached a successful start, in reverse order,
exactly once.

**Pipeline semantics:** event-driven, not request/response. `handle` is
fire-and-forget - there is no `ResponseContext`; replies go out through
ports. An event may yield zero, one, or many outputs; an unhandled event
simply yields none. `Next` is a pure control signal: `Continue`,
`Stop` (skip remaining `pre`, still run `post` for plugins that ran),
`Abort` (skip everything remaining). Failure policy: hooks return
`Next`, never `Result` - plugins log their own recoverable failures. A
panicking `pre` is logged and treated as `Stop` (fail closed); a
panicking `post` does not skip the other posts. No plugin panic may reach
the driving adapter - plugins must not be able to crash the bot. A
command handler `Err` is answered with a generic ephemeral notice when
the origin is transactional.

**Guild isolation:** `StoragePort` is guild-partitioned document storage
(`sqlx` over an ORM by decision: tiny schema, isolation visible in every
query; SQLite/PostgreSQL via URL, one shared migration set). The kernel
binds a `GuildStorage` handle to the event's guild under the deployment's
platform slug (via `PlatformInfoPort`); the handle exposes no guild
parameter, so cross-guild access is impossible by construction (DMs get
no handle). Namespaces are per-plugin
slug; plugins may sub-partition (the LLM plugin keeps one record log per
channel). The `guild` namespace is reserved for guild settings -
enforced: plugin writes/deletes there are rejected with
`StorageError::Forbidden`. Besides documents, `GuildStorage` exposes an
append-only record log for high-volume ordered data; records are never
deleted - cutoffs move.

**Observability:** `tracing` IS the observability port - a facade, not
infrastructure (same exception category as `serde`); there is
deliberately no `TelemetryPort`. The composition root installs the
subscribers: stdout fmt layer, optional file layer (`tracing-appender`,
`[logging]` config; startup-only, independent filter, fail-fast on dir
errors), plus an optional `sentry-tracing` layer (DSN-driven config;
absent/empty DSN disables reporting; Sentry SaaS or self-hosted
GlitchTip). Level contract: `debug` = stdout-only breadcrumbs
(pipeline traversal, provider traces, job ticks, roll decisions); `info`
= audit grade, also shipped to Sentry as log items (command dispatch,
command registration trail, per-completion LLM record). Privacy rule at
every level: shapes and counters, never contents - no message text, no
prompts, no endpoint bodies in logs or guild-visible embeds. The single
explicit exception is the LLM plugin's `[llm] log_raw_traffic` operator
diagnostic (off by default, stdout only via the dedicated
`llm_raw_traffic` target, never the Sentry layer, captured to files only
when the `[logging]` filter names that target). Real metrics would use
the `metrics` crate facade directly - still no port.

**Inbound events:** `RequestContext` is a chat-agnostic EVENT, not just a
message: kinds (`MessageReceived`, `MemberJoined`, `CommandInvoked`, ...)
plus origin context (guild, channel, optional transactional
`reply_token`, optional `locale`). The deployment serves exactly one chat
platform per process: platform identity is deployment metadata via the
driven `PlatformInfoPort` (adapter-owned stable slug for storage keys,
presentation display name, and platform facts such as the outbound
message limit) - the kernel carries no platform vocabulary,
only the concept. The driving adapter normalizes ALL
platform events onto this taxonomy, including mention-tag rewriting
(`<@id>` -> `[Name]<@id>` inbound, so models see name + id; outbound
sends invert the shape back to the bare tag). Attachments ride
`MessagePayload` as platform-blind DTOs.

**Commands:** plugins declare `CommandDescriptor`s via
`CommandRegistryPort` during `init()` (meaning lives in the owning
plugin); the kernel aggregates them meaning-free; the adapter publishes
descriptors as native commands and normalizes invocations onto
`CommandInvoked` (deferring interactions to meet the platform deadline).
ACL is two-layer: `required_permission` is interpreted platform-side and
reserved for commands whose audience matches a native platform
permission; `required_tier` (`AccessTier`: banned < guest < user <
moderator < admin) is kernel-side data the auth plugin enforces with
ephemeral denials - tier-gated commands deliberately ship without
`required_permission` so the command stays visible. Command replies
default to ephemeral via the shared `common::command_reply` helper.
Descriptions are the users' only in-app documentation: write them as
self-sufficient mini-docs, keep each within Discord's 100-character cap
(guarded by `test_support::assert_descriptions_fit_discord`), and keep
them in sync whenever behavior changes.

**Gateway intents:** minimal, feature-justified set - every requested
intent must map to a concrete plugin or core feature. Privileged intents
are minimized deliberately (Developer Portal toggles, verification
gating).

**Translations (deferred):** catalogs belong to each plugin; the language
is per-guild data in the reserved `guild` namespace; fallback chain:
guild setting -> event locale hint -> `en`. No kernel port; slash-command
localization, if added, is plugin-declared descriptor data mapped by the
adapter onto the platform's native mechanism.

**Pipeline <-> bus bridge:** a middleware plugin that processed an
inbound event MAY publish a derived domain event onto the bus (the
tracker logs `MemberJoined` to its audit channel, then publishes
`UserJoinedGuild`; bus-only plugins react). The bridge is plugin
behavior, not kernel or adapter logic.

**Bus runtime contract:** handlers run inline on the publishing task, in
subscription order - keep them fast and non-blocking; a panicking handler
is caught, logged, and skipped. `subscribe` returns an explicit
unsubscribe handle; bus-only plugins hold subscriptions and release them
in `stop()` (keeps `init()` idempotent).

**Scheduler contract:** non-zero intervals only (a zero interval returns
an already-cancelled handle); jobs tick with `MissedTickBehavior::Delay`
- a slow job skips missed ticks instead of burst-catching-up.

**Config hot reload:** `ConfigPort<C>` is generic over the config type -
the kernel never depends on the concrete infrastructure config; the
composition root wires it. A polling watcher rebuilds file+env config on
a scheduler tick, diffs against the last snapshot, and notifies only on
change; a failed reload keeps the last good snapshot. What reloads is a
per-section decision: global runtime connections (token, proxy, storage,
Sentry, LLM providers) cannot hot-apply. Subscribers treat identical
snapshots as no-ops.

**Event-scoped outbound ports:** outbound ports are bound to the event's
origin - a plugin's `ChatOutputPort::send` lands in the source
channel/guild without any returned response. `ChatOutputFactoryPort`
yields the scoped variants: `channel_output` (a configured channel in the
same guild), `stream_output` (progressive in-place editing of one
message; throttle/split policy is caller-side), `start_typing`, and
`react` (cosmetic by contract - per-token failures, never fatal).
Platform identity and presentation live on the driven `PlatformInfoPort`
(adapter-owned slug + display name) - the kernel contract names no
platform and carries no platform vocabulary.
Origins that cannot stream or react (DMs, transactional tokens,
channel-less events) get undeliverable defaults. `OutboundMessage`
carries an `ephemeral` hint - honored only on transactional replies;
plain channel sends are always public, and denied plain messages stay
silent (a public rejection is a spam vector) - plus an optional
`reply_to` reference that degrades gracefully to a normal send.

**LLM chat plugin** (`src/plugins/llm/`): the first plugin built as its
own hexagon. Invariants only - behavior, configuration, and commands are
documented in the README; mechanics in rustdoc:

- Operator config owns providers, endpoints, keys, and the model
  capability registry. Guild admins choose only among declared models -
  guild config can never introduce an endpoint or reach another guild's
  data (enforced at the command layer).
- Per-channel config and history: conversation records live in
  per-channel record-log namespaces, are immutable once captured
  (template fields baked in), and are never deleted; the cutoff moves
  only through committed compactions.
- Compaction runs after the reply, is chunked, and is never a sliding
  window - prompt prefixes stay byte-stable for provider caches. This
  constraint shapes many rules: constant prompt appendices appended
  last, capture-time baking, summary placement options.
- Reasoning output never reaches a channel: it is cut at the adapter
  boundary before history, reply, or live reveal.
- Tools are a text-marker protocol (`[[name: payload]]`), not the OpenAI
  tools API - frozen parse rules live in the plugin's `tools` module;
  markers are ephemeral side effects, and a tool failure never fails the
  answer.
- A triggered message (mention/reply) is guaranteed a visible response;
  when an answer is impossible, the channel gets a generic fallback
  notice that is never recorded and carries no error detail. An
  unreadable history never produces a model answer. Unprompted
  chime-ins stay silent on failure.
- State-mutating admin commands run under the same per-channel
  processing lock as the engine; `/llm_assign` is the documented
  single-write exception.
- Random chime-ins are two independent rolls (reply, silent react), each
  with its own cooldown tracker and its own purpose-scoped RNG deck
  (deck-style "fake random"; the plain RNG adapter is the swap).
- Per-channel in-memory state (locks, admission permits, cooldowns,
  decks) is process-lifetime and never evicted - bounded by distinct
  channels ever touched, not message volume.

## Codebase map

- `src/kernel/` - the micro-kernel: minimal domain; `app/api_ports/`
  (driving ports the kernel implements, e.g. `RequestHandlerPort`);
  `app/spi_ports/` (driven ports it consumes: storage, config,
  scheduler); `app/plugin_ports/` (plugin contracts); `app/services/`
  (`KernelService` - boot, pipeline runner, wiring; `KernelServices` -
  the per-event service context carrying scoped ports and
  `GuildStorage`).
- `src/plugins/<name>/` - plugins; each owns its commands, storage
  namespaces, string catalogs, and derived events.
- `src/infrastructure/` - concrete adapters: `inbound_adapters/`
  (Discord gateway -> taxonomy), `outbound_adapters/` (Discord sends,
  sqlx storage, command registrar), `plugin_adapters/` (event bus,
  scheduler, command registry, config watcher); plus `config/` and
  `observability.rs`. Adapter FILES are named for the implementation
  library (`serenity_gateway`, `serenity_outbound`, `sqlx_storage`),
  never the chat platform - one platform may gain a second
  implementation (e.g. a second Discord crate); the platform lives in
  the type names and the `PlatformInfoPort` values.
- `src/common/` - shared utilities (e.g. `command_reply`).
- `src/test_support.rs` - shared fakes and assertion helpers for plugin
  and kernel tests.
- `migrations/` - the single sqlx migration set.

Adding a plugin: implement `PluginPort` (plus `MiddlewarePluginPort` if
it intercepts inbound events), declare commands in `init()`, scope
storage under your own slug, publish derived events you own, and wire it
in the composition root (`main.rs`). Reach new platform features through
an adapter - never by letting platform types into the kernel or core
plugins.

**Kernel or plugin:** if users can uninstall it and the framework still
makes sense, it is a plugin. Checklist:

1. Required by every deployment? -> kernel.
2. Removing it breaks the platform itself? -> kernel.
3. Changes often? -> plugin.
4. Organization/community specific? -> plugin.
5. Infrastructural plumbing? -> kernel or adapter.
6. Needs independent release cadence? -> plugin.
7. Third parties would want to replace it? -> plugin or strategy port.

Kernel = operating system (process model -> plugins, filesystem ->
config/state/storage, scheduler -> jobs, permissions -> capability
checks, events -> bus, drivers -> transports). Plugins = applications
(LLM assistant, admin panel, audit logger, status rotator, moderation
suite, monetization).
