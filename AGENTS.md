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
  - Chat bot plugin based on LLM. Chat bot can be assigned to various channels with various identities. So system prompt and LLM parameters assigned to channel inside of guild, not guild itself, but guild still store some LLM-bot related settings: e.g. limit of channels that chat-bot can be assigned to, so it can be used for premium features in future, and other things. Via `rig` probably?
  - Message history accessible by other plugins (e.g. by LLM plugin for context).
  - Random user statuses for bot once in a while (if appropriate for current messenger, e.g. Discord).
  - User activity tracker (user joined guild, user left from guild, user created invite link, etc.) that will log such events to assigned channel.

Bot will work as a service: a lot of different not connected with each other Discord guilds use it. So data about each guild should be isolated to others due to privacy concerns.

It will be implemented as hexagonal architecture (ports & adapters) micro-kernel core. It will be initially implemented as walking skeleton.
Bot will be used as a Docker container.

Initially it will be used with Discord, but later it should be possible to use framework with Telegram, Matrix, Jabber, IRC or any other chat.

**Platform strategy (calibrated):** universal feature parity across chats is explicitly NOT a goal. The kernel and the taxonomy are platform-blind; core plugins work off the taxonomy and degrade gracefully where a platform lacks a concept (e.g. no roles -> user allow-lists only). Platform-specific features live in adapters or clearly scoped platform plugins without pretending to be universal. Platform types never enter the kernel or core plugins - because of economics, not purity: platform branching inside every plugin scales with (plugins x platforms), while new-adapter integration scales with 1.

## README maintenance

`README.md` is user-facing documentation and must stay in sync with reality. When a change adds or alters features, configuration, commands, project layout, or setup steps, update the README in the same change. The README describes what the bot does today; the Roadmap section is the only forward-looking part.

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

`KernelService::boot()` is the kernel's own entrypoint - it runs the plugin lifecycle in two phases, `init()` on all plugins first, then `start()` on all, so every plugin is initialized before any starts (a plugin's `start` may rely on others being ready).
`KernelService::shutdown()` calls `stop()` on all plugins.

Inbound events enter via a chat driving adapter and flow through the middleware pipeline (core -> plugins, forward `pre` then backward `post`, chain-breakable, run by `KernelService`).

**Event-scoped storage & guild isolation:** `StoragePort` is guild-partitioned document storage (plain `sqlx`, chosen over an ORM: the schema is tiny, isolation is visible in every query, boring dependency - SQLite and PostgreSQL via URL, one shared migration set). The kernel binds a `GuildStorage` handle to the event's `(platform, guild_id)` origin and carries it in the service context; a handle exposes no guild parameter, so reading or writing another guild's data is impossible by construction (DMs get no handle at all). Namespaces are per-plugin (the plugin's registered slug); the `guild` namespace is reserved for guild settings.

**Observability:** `tracing` itself is the observability port - it is a facade, not infrastructure (same exception category as `serde`), so the kernel and plugins emit tracing events/spans directly and there is deliberately no `TelemetryPort`. The composition root installs the subscribers: stdout fmt layer plus a `sentry-tracing` layer. The Sentry endpoint (Sentry SaaS or self-hosted GlitchTip) is DSN-driven via config (`sentry.dsn`, optional - absent or empty disables reporting). The kernel opens a span per event carrying origin fields, so records correlate across backends without plugin effort. If real metrics are ever needed, use the `metrics` crate facade directly - same argument, still no port.
The pipeline is **event-driven, not request/response**: there is no `ResponseContext` - `RequestHandlerPort::handle` is fire-and-forget (replies go out via ports, not return values). Plugins produce output (replies, reactions, presence) by calling injected outbound ports (`ChatOutputPort`, `PresencePort`, ...) carried in the event-scoped service context. An event may yield zero, one, or many outputs (e.g. an LLM plugin streams tokens via repeated `chat_output.send`); an event no plugin handles simply yields no output - there is no "not found" default.

**Inbound event model:**

`RequestContext` is a chat-agnostic inbound EVENT, not just a message - it carries multiple kinds (`MessageReceived`, `MemberJoined`, `CommandInvoked`, ...) plus the origin context (guild, channel, source platform, optional opaque reply token for transactional events like interactions) needed to scope outbound ports.
The driving adapter (`DiscordGatewayAdapter`) normalizes ALL Discord gateway events into this taxonomy (structural ACL & DTOs) and pushes each through the pipeline.
Middleware plugins match on event kind (`CommandPlugin` -> `CommandInvoked`, `AuthPlugin` -> invocation kinds, `UserActivityTrackerPlugin` -> `MemberJoined`); others ignore kinds they don't care about. Cross-platform reach comes from the common taxonomy - each platform adapter maps its native events onto it.

**Commands:** native platform commands (Discord slash) are the primary UX - no prefix parsing in core. Plugins declare `CommandDescriptor`s via `CommandRegistryPort` during `init()` (meaning lives in the owning plugin); the kernel aggregates them meaning-free; the Discord adapter publishes descriptors as global application commands and normalizes `InteractionCreate` onto `CommandInvoked` (deferring the interaction at ingestion to satisfy the ~3s deadline). Command replies reuse the event-scoped `ChatOutputPort`: when the origin carries an interaction reply token, the factory binds it to the interaction followup endpoint - plugins cannot tell the difference. `CommandHandler` receives the event itself, so channel-anchored commands (e.g. `/assign_tracker` assigns the channel it is run in) work without platform types. `required_permission` is interpreted platform-side (the Discord adapter maps it onto `default_member_permissions`, so Discord gates presentation natively); kernel-side per-command ACL checks belong to the auth plugin. Prefix parsing, if a platform ever needs it, is that platform's adapter concern synthesizing `CommandInvoked`.

**Pipeline <-> bus bridge:**

`EventBusPort` never carries raw inbound events. But a middleware plugin that processed an inbound event MAY publish a DERIVED domain event onto the bus.
E.g. `UserActivityTrackerPlugin` receives `MemberJoined` via the pipeline, logs to the audit channel via `ChatOutputPort`, and publishes `UserJoinedGuild`; `AuditLogPlugin` (a `PluginPort`-only bus subscriber, not in the pipeline) reacts. So the bridge between inbound happenings and bus-only plugins is plugin behavior, not kernel or adapter logic.

**Event-scoped outbound ports:**

Outbound ports injected into the pipeline are bound to the current event's origin - a plugin's `ChatOutputPort::send` lands in the source channel/guild. The driving adapter (or kernel) constructs these scoped ports per event. This is how a plugin knows where to reply without a returned response.

Plugins that log to a *configured* channel instead of replying (activity tracker, audit log) obtain a channel-scoped port via `ChatOutputFactoryPort::channel_output(origin, channel_id)` - same platform+guild scope as the event, arbitrary channel inside it. The factory rides in `KernelServices`, so scope never escapes the event's own guild; there is no way to send across guilds. Member lifecycle events (join/leave) have no channel at all - `Origin::channel_id` is `0` for them and replying is meaningless; only configured-channel sends make sense.

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
