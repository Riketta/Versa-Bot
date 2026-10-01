# AGENTS Instructions

## Project overview

This project is complex chat bot framework that should:

- Have advanced observability (powerful logging, tracing and metrics) via `tracing` and `sentry` crates.
- Support config files with hot-reloading.
- Have database (using repository pattern and some ORM (maybe just `sqlx`) with SQLite and PostgreSQL support) that will allow to store guild specific settings and plugin specific settings (probably via JSON documents). Storage and it's API should be designed the way arbitrary plugins can store own arbitrary data and configs (on per guild basis).
- Support plugin system using middleware pattern to handle all kinds of Discord events (messages, user events, etc.), and event bus to let plugins exchange data with each other. Plugins will not be external (like DLLs) but source code extensions: `src/plugins/admin/*`).
- Support chat command system based on native platform commands (e.g. Discord slash commands). Plugins register their own commands with arguments via `CommandRegistryPort`; the platform adapter publishes them to the platform and normalizes invocations onto the taxonomy.
- Some default plugins:
  - Authorization and roles for per guild bot access control.
  - Administration tools so guild administrators can manage bot like they want.
  - Guild and plugin config manager plugin.
  - Chat bot plugin based on LLM. Chat bot can be assigned to various channels with various identities. So system prompt and LLM parameters assigned to channel inside of guild, not guild itself, but guild still store some LLM-bot related settings: e.g. limit of channels that chat-bot can be assigned to, so it can be used for premium features in future, and other things. Implemented on plain `reqwest` against OpenAI-compatible endpoints behind the plugin's own provider port - no `rig` (see the LLM chat plugin paragraph below).
  - Message history accessible by other plugins (e.g. by LLM plugin for context) - deferred: the LLM plugin keeps its own bot-scoped history; a generic history plugin waits for a real consumer.
  - Random user statuses for bot once in a while (if appropriate for current messenger, e.g. Discord).
  - User activity tracker (user joined guild, user left from guild, user created invite link, etc.) that will log such events to assigned channel.

Bot will work as a service: a lot of different not connected with each other Discord guilds use it. So data about each guild should be isolated to others due to privacy concerns.

It will be implemented as hexagonal architecture (ports & adapters) micro-kernel core. It will be initially implemented as walking skeleton.
Bot will be used as a Docker container.

Initially it will be used with Discord, but later it should be possible to use framework with Telegram, Matrix, Jabber, IRC or any other chat.

**Platform strategy (calibrated):** universal feature parity across chats is explicitly NOT a goal. The kernel and the taxonomy are platform-blind; core plugins work off the taxonomy and degrade gracefully where a platform lacks a concept (e.g. no roles -> user allow-lists only). Platform-specific features live in adapters or clearly scoped platform plugins without pretending to be universal. Platform types never enter the kernel or core plugins - because of economics, not purity: platform branching inside every plugin scales with (plugins x platforms), while new-adapter integration scales with 1.

## README maintenance

`README.md` is user-facing documentation and must stay in sync with reality. When a change adds or alters features, configuration, commands, project layout, or setup steps, update the README in the same change. The README describes what the bot does today; the Roadmap section is the only forward-looking part.

## Build & CI

Both forges run the same pipeline (`.github/workflows/ci.yml` -> GHCR, `.forgejo/workflows/ci.yaml` -> the instance registry) on pushes to `main` and `v*` tags: `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked`, `cargo test --locked`, then the Docker image build (`cargo build --release --locked` inside the Dockerfile), gated on the test job. The toolchain is pinned to `rust:1.98-bookworm` in CI and in the Dockerfile - the same compiler everywhere, with the Debian suite pinned alongside it so the binary's glibc ABI matches the `debian:bookworm-slim` runtime stage.

- The clippy gate is deny-level lints only, deliberately NOT `-D warnings`: the codebase carries tolerated pedantic/doc warnings. Do not switch to `-D warnings` until a zero-warning cleanup pass lands.
- Every cargo gate runs `--locked` (tests, clippy, and the image build alike): a stale `Cargo.lock` must fail fast in the test job, not in packaging. Dependency changes ship with an updated lockfile.
- The local gate IS the CI gate: run exactly what CI runs - same commands, same order, same flags (`cargo fmt --all -- --check`, then `cargo clippy --all-targets --locked`, then `cargo test --locked`). The deny lints are declared in the `[lints.clippy]` table in `Cargo.toml`, so cargo applies them to every local clippy run exactly as in CI, and the pinned toolchain is the same - a lint error that appears in CI is always reproducible locally with the exact CI command. If it was not caught locally, the gate was not run after the final edit.
- Run the full trio after the LAST edit of a change, and read the clippy run's exit status and `error:` lines, not the warning count: a deny-level lint failure adds an error without changing the warning tally, so a count-based check is blind to it.
- `cargo test` does not execute clippy lints (`[lints.clippy]` applies only under clippy-driver): green tests never imply a clean clippy gate.
- Docker BuildKit cache mounts persist on the Forgejo host builder (docker-socket packaging job) but do not survive between GitHub hosted runners - image builds there recompile dependencies whenever source changes. Accepted for now.

## Hexagonal Micro-Kernel Architecture

TODO: add `MiddlewarePipelineRunner` trait to kernel (not port!).

- The plugin contracts ARE ports.
- The plugins ARE adapters.
- The kernel IS the inner hexagon.

The kernel assembles the chain, but never knows what's in it.

- Kernel never imports plugins - it only knows `PluginPort`, `MiddlewarePluginPort`, etc.
- Plugins never import each other - they communicate only via kernel's event bus `EventBusPort`.
- Internally, a plugin MAY be structured as its own hexagon (own domain, app layer, adapters) - but this is optional; any internal structure is valid as long as it honors the plugin contract.
- Kernel domain stays minimal - if logic needs a plugin to exist, it doesn't belong in the kernel.

Ports in the kernel fall into two families:

- Kernel service ports - infra the kernel needs and exposes to plugins (e.g. `StoragePort`, `ConfigPort`, `SchedulerPort`, `EventBusPort` access).
- Plugin-facing ports - the contracts plugins implement (`PluginPort`, `MiddlewarePluginPort`). Every plugin implements `PluginPort` (identity + lifecycle). `MiddlewarePluginPort` is opt-in - only plugins that intercept inbound events join the pipeline. A single plugin object that does both is registered once and cast to both traits - the same `Arc` goes into the `plugins` list (as `Arc<dyn PluginPort>`) and the `middleware` list (as `Arc<dyn MiddlewarePluginPort>`).

**Cardinality:**

A kernel service port like `EventBusPort` has a single active adapter and a single instance per kernel - owned by the kernel, wired at its composition root, and the same instance is shared with plugins via injection (the event bus adapter is `Clone`/`Copy`; each plugin receives its own clone at construction).
A plugin-facing port like `MiddlewarePluginPort` is the inverse: it has many adapters (one per participating plugin). The kernel collects those adapters into an ordered list and runs them through a kernel-owned pipeline runner (the `KernelService` `RequestHandlerPort` implementation) - `MiddlewarePluginPort` itself is the per-plugin step contract, not the pipeline.
So `EventBusPort` is injected into the kernel; `MiddlewarePluginPort` implementers are registered into the kernel.

**Communication:**

- Kernel to Plugin: two channels - `PluginPort` lifecycle calls (`init`/`start`/`stop`), and the middleware pipeline (runtime event flow).
- Plugin to Kernel: via kernel-owned service ports injected at registration, so plugins can have access to kernel and infrastructure over kernel.
- Plugin to Plugin: never directly - always via `EventBusPort`.
- Kernel owns the bus - it routes events but never defines their meaning.
- Plugins own their events - `UserJoinedGuild` is defined in `UserActivityTrackerPlugin`, not in the kernel.

**Kernel lifecycle:**

`KernelService::boot()` is the kernel's own entrypoint - it runs the plugin lifecycle in two phases, `init()` on all plugins first, then `start()` on all, so every plugin is initialized before any starts (a plugin's `start` may rely on others being ready). `boot()` validates the wiring: every `MiddlewarePluginPort` must also be registered in `plugins` (dual registration is the only supported shape - a middleware-only plugin would intercept events with no lifecycle), and duplicate names from distinct plugin instances are rejected; a failed `start()` rolls back the already-started plugins (reverse-order `stop`). `KernelService::shutdown()` stops the plugins that reached a successful `start()` under a successful boot, in reverse start order - idempotent (explicit call + `Drop` fallback stop exactly once, tracked via the kernel's started-list, so a failed boot leaves nothing to stop: rolled-back plugins were stopped by the rollback, and a plugin that never started owes no stop).

Inbound events enter via a chat driving adapter and flow through the middleware pipeline (core -> plugins, forward `pre` then backward `post`, chain-breakable, run by `KernelService`).

**Event-scoped storage & guild isolation:** `StoragePort` is guild-partitioned document storage (plain `sqlx`, chosen over an ORM: the schema is tiny, isolation is visible in every query, boring dependency - SQLite and PostgreSQL via URL, one shared migration set). The kernel binds a `GuildStorage` handle to the event's `(platform, guild_id)` origin and carries it in the service context; a handle exposes no guild parameter, so reading or writing another guild's data is impossible by construction (DMs get no handle at all). Namespaces are per-plugin (the plugin's registered slug); plugins may
sub-partition their own namespace (the LLM plugin keeps one record log per
channel, `llm:c:<channel>`, so one channel's context can never read another
channel's conversation); the `guild` namespace is reserved for guild settings - enforced, not conventional: the adapter rejects plugin writes/deletes/appends targeting `guild` with `StorageError::Forbidden` (reads stay permitted; the future config manager reaches it kernel-side). Alongside key-value documents, `GuildStorage` exposes an append-only record log (`append`/`list_after`/`count_after` over one shared `guild_records` table, guild-scoped sequence numbers) for high-volume ordered data that outgrows documents - e.g. LLM conversation history; documents remain the tool for settings and state.

**Observability:** `tracing` itself is the observability port - it is a facade, not infrastructure (same exception category as `serde`), so the kernel and plugins emit tracing events/spans directly and there is deliberately no `TelemetryPort`. The composition root installs the subscribers: stdout fmt layer plus a `sentry-tracing` layer. The Sentry endpoint (Sentry SaaS or self-hosted GlitchTip) is DSN-driven via config (`sentry.dsn`, optional - absent or empty disables reporting); error events carry the release (`sentry::release_name!`), an optional `sentry.traces_sample_rate` (0.0..=1.0, out-of-range warns and stays off) samples performance transactions, and session tracking stays off (the SDK default; GlitchTip does not support it). The `sentry-tracing` `logs` feature ships `warn`/`info`/`error` as Sentry log items (debug/trace stay stdout-only); whether a backend's Logs tab displays them is backend-side (GlitchTip documents OTel for its Logs tab), while `error`-grade events and panics always arrive as Issues - which is why storage/LLM failures log at `error`. The kernel opens a span per event carrying origin fields (`platform`, `kind`, guild/channel/user), so records correlate across backends without plugin effort. The level contract: `debug` (stdout-only breadcrumbs) covers pipeline traversal (event ingest, chain outcome - ran/stopped-by/aborted), provider request/response traces, scheduler job ticks and chime roll decisions; `info` (also ships to Sentry/GlitchTip as log items) is the audit grade - command dispatch (who ran what, with a per-argument summary: short values rendered, long ones as `<N chars>` shapes), the command registration trail (the registry logs each plugin's registration; the Discord registrar logs the completed sync with its count), and the per-completion LLM record (model, trigger, latency, token usage incl. cached/reasoning breakdowns when reported, window sizes). Privacy rule at every level: shapes and counters, never contents - no message text, no prompts, no endpoint response bodies. The single, explicit exception is the LLM plugin's `[llm] log_raw_traffic` operator diagnostic: it dumps raw request/response bodies at `debug` (stdout only, never the Sentry layer) to prove what went on the wire; it is off by default and carries conversation content. If real metrics are ever needed, use the `metrics` crate facade directly - same argument, still no port.
The pipeline is **event-driven, not request/response**: there is no `ResponseContext` - `RequestHandlerPort::handle` is fire-and-forget (replies go out via ports, not return values). Plugins produce output (replies, reactions, presence) by calling injected outbound ports (`ChatOutputPort`, `PresencePort`, ...) carried in the event-scoped service context. An event may yield zero, one, or many outputs (e.g. an LLM plugin streams tokens via repeated `chat_output.send`); an event no plugin handles simply yields no output - there is no "not found" default.

**Inbound event model:**

`RequestContext` is a chat-agnostic inbound EVENT, not just a message - it carries multiple kinds (`MessageReceived`, `MemberJoined`, `CommandInvoked`, ...) plus the origin context (guild, channel, source platform, optional opaque reply token for transactional events like interactions) needed to scope outbound ports.
The driving adapter (`DiscordGatewayAdapter`) normalizes ALL Discord gateway events into this taxonomy (structural ACL & DTOs) and pushes each through the pipeline.
Middleware plugins match on event kind (`CommandPlugin` -> `CommandInvoked`, `AuthPlugin` -> invocation kinds, `UserActivityTrackerPlugin` -> `MemberJoined`); others ignore kinds they don't care about. Cross-platform reach comes from the common taxonomy - each platform adapter maps its native events onto it.

**Commands:** native platform commands (Discord slash) are the primary UX - no prefix parsing in core. Plugins declare `CommandDescriptor`s via `CommandRegistryPort` during `init()` (meaning lives in the owning plugin); the kernel aggregates them meaning-free; the Discord adapter publishes descriptors as global application commands and normalizes `InteractionCreate` onto `CommandInvoked` (deferring the interaction at ingestion to satisfy the ~3s deadline). Command replies reuse the event-scoped `ChatOutputPort`: when the origin carries an interaction reply token, the factory binds it to the interaction followup endpoint - plugins cannot tell the difference. `CommandHandler` receives the event itself, so channel-anchored commands (e.g. `/assign_tracker` assigns the channel it is run in) work without platform types. `required_permission` is interpreted platform-side (the Discord adapter maps it onto `default_member_permissions`, so Discord gates presentation natively); kernel-side per-command ACL checks belong to the auth plugin. Descriptor arguments are typed (`ArgKind` - string, user, role so far) and may carry `choices`; `guild_only` marks DM-unsupported commands. The registrar maps these onto platform mechanics (Discord option types 3/6/8, option `choices`, `dm_permission`); entity-typed arguments arrive in `args` as ID strings. Prefix parsing, if a platform ever needs it, is that platform's adapter concern synthesizing `CommandInvoked`.

**Gateway intents:** the driving adapter requests a minimal, feature-justified intent set - every intent must map to a concrete plugin or core feature. Current set: `GUILD_MESSAGES` + `DIRECT_MESSAGES` (framework-level message/DM intake feeding the pipeline), `GUILD_MEMBERS` (tracker plugin member lifecycle events), and `MESSAGE_CONTENT` (the LLM plugin reads guild message content for conversation history - the feature that justified requesting this privileged intent). Privileged intents are minimized deliberately: they need Developer Portal toggles and gate bot verification at scale.

**Translations (i18n):** split by concern - no kernel port, no adapter catalogs. Catalogs (the strings themselves) belong to each plugin, which owns its meaning like its storage docs and derived events; the lookup engine is at most a shared utility under `src/common/`, added when catalog pressure demands it (no fluent/ICU unless plurals/genders are actually needed). The language is per-guild data in the reserved `guild` storage namespace (key `language`, written by the config plugin), resolved by whoever sends the message; fallback chain: guild setting -> event locale hint -> default (`en`). The platform reports its native locale (Discord guild preferred locale / interaction locale) via an optional `locale` hint on `Origin` - taxonomy, like IDs and `reply_token`; platforms without the concept leave it `None`. Slash-command localization is plugin-declared data on `CommandDescriptor`, mapped by the adapter onto the platform's native mechanism (Discord `name_localizations`/`description_localizations`). Rejected alternatives: a kernel `TranslationPort` (the kernel would own user-facing meaning) and adapter-side catalogs (content would move into the transport layer). Implementation is deferred until the config/LLM plugins create real pressure.

**Pipeline <-> bus bridge:**

`EventBusPort` never carries raw inbound events. But a middleware plugin that processed an inbound event MAY publish a DERIVED domain event onto the bus.
E.g. `UserActivityTrackerPlugin` receives `MemberJoined` via the pipeline, logs to the audit channel via `ChatOutputPort`, and publishes `UserJoinedGuild`; `AuditLogPlugin` (a `PluginPort`-only bus subscriber, not in the pipeline) reacts. So the bridge between inbound happenings and bus-only plugins is plugin behavior, not kernel or adapter logic.

**Bus runtime contract:** `EventBusPort` handlers run inline on the publishing task, in subscription order - keep them fast and non-blocking. A panicking handler is caught, logged, and skipped: a broken subscriber cannot crash the publisher, the pipeline, or other subscribers. `subscribe` returns an `EventBusSubscription` (explicit `unsubscribe`, same style as `JobHandle`; dropping the handle does not unsubscribe) - bus-only plugins hold their subscriptions and release them in `stop()`, which keeps `init()` idempotent. Delivery isolation, cross-task ordering, and backpressure (e.g. an external broker) are deferred until a real consumer needs them.

**Pipeline failure policy:** hooks are fire-and-forget (they return `Next`, never `Result`); plugins log their own recoverable failures internally. Panics are the kernel's concern: a `pre` that panics is logged (plugin + event) and treated as `Stop` - the event does not flow to remaining plugins (fail closed), while `post` still runs for the plugins that ran, the panicking one included (only its call frame unwound, its state is intact); a `post` that panics is logged and remaining posts still run - one broken observer must not skip the others' cleanup. No plugin panic may reach the driving adapter's task - plugins must not be able to crash the bot. A command handler that returns `Err` is answered with a generic ephemeral failure notice when the origin is transactional (a deferred interaction must never hang on "thinking").

**Scheduler contract:** `SchedulerPort::schedule` requires a non-zero interval - a zero interval returns an already-cancelled handle instead of spawning a task that would panic `tokio::time::interval`. Jobs tick with `MissedTickBehavior::Delay`: a slow job skips missed ticks instead of burst-catching-up. Derived domain events (e.g. membership facts) are published regardless of the publishing plugin's own configuration readability - the fact happened; only the plugin's reaction to it may be skipped.

**Configuration hot reload:** `ConfigPort<C>` (spi) is generic over the configuration type - the kernel never depends on the concrete infrastructure config; the composition root wires the concrete instance. The adapter is a polling watcher: it rebuilds file+env configuration on a scheduler tick, diffs it against the last snapshot, and notifies subscribers only on change; a failed reload (half-written file) keeps the last good snapshot. Handlers run inline, panic-isolated, like bus subscribers. What reloads is a per-section decision: global runtime connections (token, proxy, storage, Sentry, LLM providers) cannot hot-apply and are ignored; `[status]` demonstrates the pattern. Subscribers must treat identical snapshots as no-ops so unrelated edits don't reset running state.

**LLM chat plugin:** the first plugin built as its own hexagon. Driven side: an `LlmCompletionPort` with one OpenAI-compatible adapter covering every operator-declared provider (per-provider reqwest client: url, `api_key_env` resolved from the environment, optional proxy, timeout, reasoning style, and a static `extra_body` passthrough merging provider-specific request fields with `model`/`messages` engine-owned) plus a model capability registry gating the per-channel `reasoning_effort` (the registry doubles as the legal assignment set - see Isolation below; configs stored before that validation may still reference undeclared models - they run with default capabilities, warn-once); a scope-aware `RandomPort` (deck-style "fake random" by default - per-scope decks balance chime-ins out per 100-draw cycle, the plain RNG adapter is the swap) whose per-channel state lives inside the adapter. Storage: conversation records per channel in `llm:c:<channel>` namespaces - never deleted, the cutoff moves - plus config/state documents under `llm`; the state doc holds `{summary, cutoff_seq, cutoff_at}` so a compaction commit is one atomic write. Context schema is fixed: system prompt -> always-present slot (compaction summary or placeholder) -> live window, user turns rendered via the channel's `{sender}: {message}` template, bot turns as assistant role (self-recorded at send time - bot messages never re-enter from the gateway). Compaction runs after the reply once the window outgrows `history_depth`: everything but `compaction_keep_tail` newest records folds into the summary via the compaction model - chunked, never a sliding window, so prompt prefixes stay byte-stable for provider caches. Streaming is real endpoint streaming: channels with `streaming` on request `stream: true` over SSE through the completion port's streaming method - content deltas reveal live on one message (begun with the first delta, edits throttled to `stream_interval_ms`), reasoning deltas are cut at the adapter boundary and never surface, and the final edit plus any length splits use the authoritative assembled text - the message is only ever behind, never wrong. Non-streaming channels and compaction stay single-shot; the port's streaming method degrades to single-shot for providers without SSE. A stream that dies mid-answer finalizes what was already revealed (delivered = recorded; the fallback notice never contradicts visible text) and reports through the service channel. Reasoning output is cut at the adapter boundary (`reasoning_content`/`reasoning` response fields are never read; a complete inline `<think>...</think>` pair anywhere is reasoning and is stripped; a leading unclosed block means the answer never got past thinking = no answer; an unclosed mid-text opener is literal text) - it never enters history, the reply, or the progressive reveal, so it cannot reach a channel. Reasoning control is a real three-state, because the wire differs per provider style: unset/`clear` sends no reasoning parameter (the provider default applies - Z.ai GLM defaults to `max` effort, so "nothing sent" means heavy thinking, not no thinking); a value is sent as-is on effort-style providers and enables thinking on switch-style ones; `off` stores `Some("off")` and renders an explicit `thinking: {"type": "disabled"}` for switch-style providers (GLM-4.5 through 5.2) while effort-style providers have no off wire value and fall back to the default - GLM-5.3 series thinks forcibly regardless, `reasoning_effort=low` is its minimum, so operators throttle it with `low`, not `off`. `/llm_status` shows the effective reasoning setting so this distinction is visible. Random chime-ins roll per channel (scoped RNG + cooldown) and only on captured messages. Failure policy: a triggered message (mention or reply to the bot) is guaranteed a visible response - when the generated answer is impossible (provider error, reasoning-only response, unreadable history, failed capture append), the channel gets a generic fallback notice that is never recorded as a bot turn and carries no error detail; unprompted chime-ins stay silent on failure. Logs + rate-limited service-channel embeds carry the error classification only ("endpoint unreachable/rejected the request") - endpoint response bodies can name operator accounts/projects, so they stay in logs, never in guild-visible embeds. History integrity is never guessed around: an unreadable record log or a failed capture append never produces a model answer (it would fabricate context) - only the fallback. The state-mutating admin commands (`/llm_cutoff`, `/llm_set`, `/llm_prompt`, `/llm_prompt_file`) run under the same per-channel processing lock as the engine, so an in-flight run cannot commit an older state over a fresh cutoff and concurrent admin read-modify-writes cannot lose an update. Prompts longer than Discord's inline option limit load from an uploaded attachment (`/llm_prompt_file`): the adapter resolves the attachment option to its Discord CDN URL - the pinned trusted host, the only network peer guild input may ever name - and the plugin downloads it under `max_prompt_file_bytes`. Isolation: provider endpoints, keys and capabilities are operator config; guild admins choose only among declared models, so guild config can never introduce an endpoint or reach another guild's data - enforced at the command layer: `/llm_assign` and `/llm_set model=` validate against the declared registry and reject unknown refs with an ephemeral list of the declared models, the `/llm_assign` `model` argument ships the declared refs as Discord choices (platform cap 25 - beyond it the dropdown truncates with an operator warn, validation is unaffected), and `/llm_models` shows the catalog. Token usage from the endpoint's `usage` block is stored per channel (`channel:{id}:stats`) and blended into a rolling tokens-per-character estimate; context filling is token-budget based whenever the budget is resolvable and usage has been calibrated: channel `context_budget_tokens` overrides, else the declared model `context_window` minus the completion reserve (`max_tokens` or 1024) and a 10% estimator margin - uncalibrated channels fall back to message-count filling with `history_depth` as the cap in all cases. `/llm_status` shows the active system prompt (override or plugin default: char count, an in-process identity fingerprint for version checks, head preview), the effective reasoning setting, the last request's tokens (including cached and, when the endpoint reports the breakdown, reasoning tokens), the calibrated estimate, and the enforced budget.

**Event-scoped outbound ports:**

Outbound ports injected into the pipeline are bound to the current event's origin - a plugin's `ChatOutputPort::send` lands in the source channel/guild. The driving adapter (or kernel) constructs these scoped ports per event. This is how a plugin knows where to reply without a returned response.

Plugins that log to a *configured* channel instead of replying (activity tracker, audit log) obtain a channel-scoped port via `ChatOutputFactoryPort::channel_output(origin, channel_id)` - same platform+guild scope as the event, arbitrary channel inside it. The factory rides in `KernelServices`; the adapter enforces the degenerate cases (DM origins and the `ChannelId(0)` sentinel yield an undeliverable port), while verifying that a channel id actually belongs to the origin guild needs the gateway cache and is deferred - until then, guild scoping of configured channels is backed by storage partitioning, not structural impossibility. Member lifecycle events (join/leave) have no channel at all - `Origin::channel_id` is `0` for them and replying is meaningless; only configured-channel sends make sense.

Progressive rendering of long answers rides a second factory output: `ChatOutputFactoryPort::stream_output(origin)` yields a `ChatStreamPort` (`begin` creates the message and returns its platform message id as the handle; `update(handle, content)` edits in place). Content-only by design. Origins that cannot stream - transactional reply tokens, channel-less events, DMs - get an undeliverable port; a plain `chat_output` stays available there. The streaming cadence (throttle between edits, final full-content update, oversized-content splitting into follow-up sends) is caller-side policy, not port behavior. `ChatOutputFactoryPort::start_typing(origin)` holds the platform typing indicator for the origin channel - adapter-refreshed on the platform's cadence, stopped when the returned guard drops; the LLM engine holds it for the duration of every generated answer.

`OutboundMessage` carries a platform-blind `ephemeral` visibility hint and minimal embed payloads. Adapters honor `ephemeral` only on transactional replies - Discord sets the `EPHEMERAL` flag on interaction followups, so the message is visible to the invoking user alone (e.g. auth denials) - and ignore it for plain channel sends, which are always public. Consequently plugins answer denials only on events with a `reply_token`; denied plain messages stay silent, since a public rejection is a spam vector. Command replies default to ephemeral through the shared `common::command_reply` helper: confirmations, usage notices, corrections and reports stay between the bot and the invoking admin - config and policy details are nobody else's business, and public replies would only add channel noise. Channel-visible outputs (LLM answers, chime-ins, the guaranteed-answer fallback, tracker audit messages, service-channel embeds) are plain sends, deliberately public.

**Development Sequence:**

Prepare the directory structure. The kernel splits its ports by direction: `api_ports` (driving ports the kernel implements - e.g. `RequestHandlerPort`, the inbound entry point driving adapters call), `spi_ports` (driven ports the kernel consumes - e.g. `StoragePort`, `ConfigPort`), and `plugin_ports` (the plugin-facing contracts - `PluginPort`, `MiddlewarePluginPort`, `EventBusPort`). So plugin-specific ports live at `src/kernel/app/plugin_ports/`, separate from the driven ports at `src/kernel/app/spi_ports/`. Services live at `src/kernel/app/services/` (`KernelService`, `KernelServices`). Plugins live at `src/plugins/<name>/`, and their concrete adapters at `src/infrastructure/{inbound_adapters,outbound_adapters,plugin_adapters}/`.

- Define the minimal kernel domain - only what cannot live in a plugin.
- Define `PluginPort` - the base trait every plugin implements: identity (`name`/`meta`) and lifecycle (`init`/`start`/`stop`). It is the primary contract the kernel holds plugins behind - the plugin's actual implementation surface, not a lifecycle-only interface.
- Define `MiddlewarePluginPort` - the per-plugin step contract (NOT the pipeline itself). A plugin that intercepts inbound events implements it as one step in the chain, exposing `pre` (forward hook, may short-circuit) and `post` (backward hook, observability/cleanup) over the event + injected service context. `Next` is a pure control signal carrying no payload: `Continue` (proceed), `Stop` (short-circuit remaining `pre`, still run `post` for plugins that ran - was `Respond`), `Abort` (hard stop, skip remaining `pre` AND all `post`). The pipeline that runs these steps is a kernel-owned runner (`KernelService` implementing `RequestHandlerPort`).
- Define `EventBusPort` - the kernel-owned pub/sub bus for runtime plugin-to-plugin messaging. Plugins publish/subscribe to events they own; the kernel routes but never defines event meanings. It never carries raw inbound events.
- Define which kernel-owned service ports will be injected into plugins at registration.

After that - iterations of the Walking Skeleton for the kernel and the plugins separately.

Kernel:

1. Kernel Driving Side. Test the kernel producing a constant. A driving test calls the Kernel App Service with a fake chat event and asserts a constant reply was sent via a fake `ChatOutputPort`.
2. Kernel Driven Side. Add `EventBusPort` and `MiddlewarePluginPort` with stub implementations. The kernel registers a `FakePlugin`; the plugin receives an event through the pipeline and produces a constant reply via the fake port.
3. Kernel Driving Side. Wire up the real kernel driving adapter - `DiscordGatewayAdapter` (WebSocket) that translates Discord events into the `RequestContext` DTO.
4. Kernel Driven Side. Wire up the real `InMemoryEventBus` instead of the stub implementation. If the system is distributed - replace it with an external broker (NATS, RMQ) through the same `EventBusPort` without changes to the kernel.

Plugins:

1. Plugin Driving Side. Test the plugin in isolation. The test calls the `CommandPlugin` App Service with an event and asserts a reply was sent via a fake `ChatOutputPort` (the plugin returns `Next::Stop`).
2. Plugin Driven Side. Add the plugin's driven ports with a stub implementation. The test calls the `CommandPlugin` App Service, which calls `FakeGuildConfigRepository` and returns a constant.
3. Plugin Driving Side. Register the plugin in the kernel via `PluginPort`. The plugin receives `RequestContext` from the kernel through the middleware pipeline. `AuthPlugin` is wired into the kernel's `MiddlewarePluginPort` pipeline.
4. Plugin Driven Side. Wire up the plugin's real driven actors. `CommandPlugin` wires up the real `GuildConfigRepository` (Sqlx, Postgres/SQLite). `AuthPlugin` wires up the real `PermissionRepository` / `RoleService`.

In standard P&A the skeleton "walks" after step 1. In a Micro-Kernel the skeleton "walks" only when:

- The kernel can register plugins.
- `TestEventBus` can route at least one event.
- The `MiddlewarePluginPort` pipeline passes at least one event to a plugin.
- At least one plugin is registered and producing output.

**Kernel or Plugin:**

> If users can uninstall it and framework still makes sense, it should be a plugin.

Kernel = Operating System:

- Process model -> Plugins.
- Filesystem -> Config/State/Storage.
- Scheduler -> Jobs/Tasks.
- Permissions -> Capability checks.
- Events -> Bus
- Drivers -> Transports.

Plugins = Applications:

- LLM assistant.
- Guild admin panel.
- Audit logger.
- Status rotator.
- Moderation suite.
- Premium monetization.

**Kernel or Plugin checklist:**

1. Is it required for every deployment?

If yes - likely kernel. E.g.: plugin loading, lifecycle management, message dispatch, etc.
If some users don't need it - plugin.

2. Does removing it break the platform itself?

If yes - kernel.
If removing it only removes capability - plugin.

3. Does it change often?

Fast-changing logic should be plugin. E.g.: custom commands, moderation policies, etc.

4. Is it organization/community specific?

If yes - plugin.

5. Is it infrastructural plumbing?

Likely kernel or adapter. E.g.: event bus, scheduler runtime, state store abstraction, plugin sandboxing.

6. Does it need independent release cadence?

If yes - plugin.

7. Would third parties want to replace it?

If yes - plugin or strategy port.
