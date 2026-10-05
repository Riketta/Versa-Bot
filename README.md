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
- LoL store tracker: watches the locally running League client's store
  (sales, new skins, Mythic Shop rotations, Your Shop start) and announces
  changes to a per-guild assigned channel.
- LoL leaderboard: on-demand `/lol_leaderboard` dump of aggregated ranked
  leaderboard statistics (role distributions, most picked champions per
  role) parsed from a pluggable data source, with cache and coverage
  reporting.
- Audit trail: records the bus membership events as structured `audit`
  tracing events.
- LLM chat bot: per-channel conversations with history, compaction,
  streaming, token-budget context filling, image recognition, an
  emoji-reaction tool and random chime-ins.
- Status rotator: cycles the bot's activity through a configured list.
- Guild-local bot nickname via `/set_guild_name` (reset by omitting the
  name).
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

   # Optional: persistent file log (independent filter, startup-only -
   # see "File logging" below).
   # [logging]
   # dir = "logs"
   # rotation = "daily"  # or "never": one file, rotate externally
   # level = "info,versa_bot=debug,llm_raw_traffic=debug"  # default: breadcrumbs at debug, raw traffic excluded

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
   first registration. Once they do, wire the bot up in Discord - the
   [demo below](#demo-wiring-up-a-fresh-server) walks a complete setup.

### Demo: wiring up a fresh server

A complete first-run configuration, as a guild administrator would type
it. A fresh guild starts open (default tier `user`, and Discord guild
administrators are always `admin`), so every command below works on day
one without touching `/auth`. Commands marked *in #channel* are
channel-scoped: they bind the channel they are run in, so run them in
the target channel.

```text
/ping                                        # any channel: the loop works
/set_guild_name name:Versa                   # optional: how this server sees the bot
/assign_tracker                              # in #audit: join/leave notices land here

/llm_models                                  # what this deployment offers
/llm_assign model:local/gemma                # in #bot-chat: bind the assistant here
/llm_set reasoning_effort min                # lightest reasoning
/llm_set capture_mode all_messages           # track everything - also what enables random chime-ins
/llm_set images on                           # describe attached images (needs operator [llm] image_model)
/llm_set react on                            # the model may react to the message it answers
/llm_set streaming on                        # the answer live-edits while it generates
/llm_set_prompt kind:system prompt:...       # persona; long prompts ride as an attached file
/llm_status                                  # verify the whole setup at a glance
/llm_admin                                   # in #staff: LLM error notices land here

/lol_store_enable                            # in #lol-sales: store tracking on (admin)
/lol_store_assign                            # in #lol-sales: announcements post here
/lol_store_role role:@Store Pings            # optional: only opted-in members get pinged
/lol_client_status                           # watcher health check
/lol_store_watch target:champion name:Evelynn   # any member: personal sale watch
```

Prerequisites the commands assume: `/llm_assign` and the `/llm_set`
tuning need operator-declared models (the `[llm]` config section);
`/llm_set images on` additionally needs an `[llm] image_model`. The
`/lol_store_*` block needs the `[lol_store]` config section and the
League client running on the bot's host - see the plugin sections for
the operator side. Now @mention the bot in `#bot-chat` and talk to it.

On a locked-down server, close the default instead of leaving it open
(opt-in - see [Authorization](#authorization-auth-plugin) for the full
policy model):

```text
/auth action:default tier:guest
/auth action:set tier:moderator role:@Mods
```

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

### File logging

Without a `[logging]` section the bot logs to stdout (and Sentry, when
configured). With one, it also writes `versa-bot.log.<date>` files into
`dir` - daily rotation by default, `rotation = "never"` for a single
append-only file to pair with an external rotator.

The file layer has its own filter and is startup-only (changes require a
restart). By default it records the bot's breadcrumbs at debug - a flight
recorder that survives stdout running at info - and pins the third-party
HTTP stack to warn; `RUST_LOG` never affects the file. The directory must
be creatable and writable; a failure aborts startup. Full LLM
request/response bodies (`log_raw_traffic`) never reach the file unless
the filter names their target:

```toml
[logging]
dir = "logs"
level = "info,versa_bot=debug,llm_raw_traffic=debug"
```

In containers, mount a volume for the log directory
(`-v ./logs:/app/logs`) or prefer the Docker logging driver
(`--log-opt max-size=10m`); files are never pruned automatically - clean
or archive them externally.

## Plugins

Every capability is a plugin under `src/plugins/`. Each section below is
the plugin's manual: what it does, its commands, and the access tier
every command requires. Tiers are the auth plugin's ladder - `banned` <
`guest` < `user` < `moderator` < `admin`. Every command reply, denial
included, is ephemeral (visible to the invoker alone) unless the
documentation marks it public - the deliberate exceptions post in the
channel; a member below a command's tier gets an ephemeral notice
naming the required and actual tier. Discord guild administrators are always `admin`, and a fresh
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

Two resolution rules that surprise people:

- **Discord's role hierarchy is irrelevant here.** The bot sees role ids,
  not their position in Server Settings. A member holding two mapped
  roles gets the higher *tier* - even when it comes from a role placed
  below the other one in Discord's list. Arrange seniority in Discord as
  you like; the ladder follows tier values only.
- An explicit user assignment is a **floor**, not a ceiling: roles lift
  above it. A member assigned `guest` who holds a role granting
  `moderator` is a `moderator`. Only `banned` (above) overrides role
  grants.

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

### Bot nickname (`nickname` plugin)

Sets the bot's guild-local display name - how members of THIS server
see it (Discord's member nickname; the global bot username is never
touched). Nothing is stored: the platform owns the value, the command
only applies it. The bot needs the **Manage Nicknames** permission in
the guild, otherwise the change is rejected with an ephemeral notice.

| Command | Tier | Effect |
|---|---|---|
| `/set_guild_name name:<text>` | moderator | rename the bot in this server (max 32 characters); omitting `name` resets to the bot's real name |

### LoL store tracker (`lol_store` plugin)

Announces League of Legends store events - new sales, newly listed
skins, Mythic Shop rotation changes, Your Shop starts - from the
**locally running League client** to a per-guild assigned channel. The
client's REST API is read-only here: your logged-in account is only the
API key, and no account data (wallet, inventory, purchases) is ever
read or announced. Because there is no purchase-history endpoint, every
event is detected by diffing store snapshots - two changes inside one
poll window merge into a single line. Listings print cheapest-first
(priceless items last); sales keep their end-date grouping (soonest
date first, price order within a date).

Setup: point `[lol_store]` at the client's `lockfile` (the bot reads the
per-start port and token from it, so client restarts self-heal), enable
per guild with `/lol_store_enable`, and assign the announcement channel
With `/lol_store_assign`. Announcements render as one embed per poll
cycle - sales grouped under their end date with the percent off, skins,
each Mythic rotation, Your Shop - and split across several embeds when
one would overflow, so every deal prints; nothing is cut. The champion
itself going on sale is not store news and is skipped. The first
successful poll after a restart is a catch-up (announcing what changed
while the bot was down) or, with no stored state, a silent baseline.
Delivery is at-most-once: a guild whose send fails gets a log line, not
a replay - the poll state still advances so nothing is announced twice.
`/lol_store_dump` and
`/lol_client_status` show the bot-global watcher state (there is one
League client per bot), which may differ from what an individual guild
received. A dump before the first announced update renders the current
store (sales, Mythic rotations, Your Shop) instead - "new skins" needs
history, so that section only appears once updates have been announced.

| Command | Tier | Effect |
|---|---|---|
| `/lol_client_status` | moderator | watcher health: client link, last poll, tracked counts, announce flags (ephemeral) |
| `/lol_store_enable` | admin | enable store tracking for this guild |
| `/lol_store_disable` | admin | disable store tracking for this guild (the channel binding is kept) |
| `/lol_store_assign` | moderator | post store events in this channel (run it in the target channel) |
| `/lol_store_unassign` | moderator | stop posting store events in this guild |
| `/lol_store_role` | moderator | tag this role on store announcements - the subscription role (run without the argument to clear) |
| `/lol_store_dump` | moderator | force-post the latest store update summary in the current channel; on a fresh launch (nothing announced yet), the current store instead (public, no role tag) |
| `/lol_store_watch` | user | watch a skin or a champion for sales, Mythic Shop rotations, new releases (ephemeral) |
| `/lol_store_unwatch` | user | remove one own watch by its list id, or every own watch with `all` (ephemeral) |
| `/lol_store_watchlist` | user | list your own watches (private, ephemeral) |

The `/lol_store_role` subscription pattern: create a mentionable role
(e.g. `Store Pings`), let members join it, and bind it once - every
announcement then tags the role, so only opted-in members are pinged.
The tag rides the message content (embeds never notify on Discord); the
role must be marked "allow anyone to mention", or the bot needs the
mention-everyone permission.

**Per-user watches.** Beyond the guild-wide role, any member can
subscribe personally with `/lol_store_watch`: either a specific skin
line (`target: skin`, e.g. *Blood Moon Evelynn*) or a champion's whole skin
line (`target: champion`, e.g. *Evelynn* - future skins are caught by
construction). The `kinds` argument selects what fires: `sale`,
`mythic`, `release`, or `all` (default - all three categories). Matching runs on the same
store deltas the announcements use, so catch-up after a restart covers
watches too, and the per-guild announce flags (`announce_sales`, ...)
never suppress personal watches. When something fires, the assigned
announcement channel gets one embed with the matched items, and every
watcher is tagged in the message content (never the announcement role).

Watch notes:

- Name resolution searches the last known store catalog, so subscribing
  requires the League client to have been reachable; matching afterwards
  is pure id/name comparison. An ambiguous name replies with a candidate
  list - re-run it with the exact name. The live champion table lists
  some champions twice (live + Classic variants share one display name);
  a watch binds to the id the store catalog can actually reach, and when
  several ids qualify the candidates carry their ids - re-run with the
  id (pasting the candidate line works) to pick one. Same-named variant
  skins are listed with their item ids the same way.
- `/lol_store_watch` reports current activity in its confirmation
  ("currently on sale", "currently in the mythic rotation"). Firing is
  edge-based: a skin already on sale when you subscribe will not notify
  until its next sale starts.
- The `release` kind only ever fires for champion watches - a skin the
  catalog can resolve is by definition already released.
- Watchlists are private (`/lol_store_watchlist` shows only your own),
  watches are guild-scoped, and a leaver's stale watch is inert (tags
  just do not resolve). Caps: `watch_user_cap` per member,
  `watch_guild_cap` per guild.

Operator configuration lives in the optional `[lol_store]` section
(**startup-only** - an absent section, an empty `lockfile_path`, or a
zero `poll_secs` keep the watcher off):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lockfile_path` | string | *(empty)* | path to the client's `lockfile`; empty = watcher disabled |
| `address` | string | `"127.0.0.1"` | host the client API listens on (the port always comes from the lockfile) |
| `poll_secs` | integer | `300` | poll cadence in seconds; `0` disables |
| `announce_sales` | bool | `true` | announce new sales (name, % off, RP price, end date) |
| `announce_new_skins` | bool | `true` | announce newly listed skins (champion, skin, RP price) |
| `announce_mythic_rotation` | bool | `true` | announce Mythic Shop rotation changes (skin, Mythic Essence price, rotation end) |
| `announce_yourshop` | bool | `true` | announce Your Shop starts (start and end times) |
| `watch_user_cap` | integer | `20` | maximum `/lol_store_watch` subscriptions per member per guild |
| `watch_guild_cap` | integer | `300` | maximum `/lol_store_watch` subscriptions per guild |

Announced trackers that are toggled off still update the watcher's
state, so re-enabling never replays old events. The watcher publishes
`lol_store.announced` events on the plugin bus for other plugins.

Maintainer notes - how to explore the League client's local API (the
`/help` schema crawl, the vector-parameter encoding trap, a verified
endpoint cheat sheet) - live in
[`src/plugins/lol_store/README.md`](src/plugins/lol_store/README.md).

Deployment note: the League client only listens on the machine's own
loopback, so the watcher requires the bot to run **on the same Windows
instance as the client** (natively, or in a container with host-level
networking plus a read-only mount of the lockfile's *directory* - the
file itself is rewritten on every client start).

### LoL leaderboard (`lol_leaderboard` plugin)

Dumps aggregated ranked-leaderboard statistics on demand: role
distribution per region and pooled across regions (TOP 300 / TOP 1000
rows), and the most picked champions per role - the kind of summary
analytics sites publish, delivered where the question is asked. The
data comes from a pluggable source behind the plugin's port (the built
in adapter reads DeepLoL's public API - no auth, no League client
needed); the data source is startup-only config, and there is **no
per-guild setup**: the command answers wherever it is invoked.

The bot parses at most `parse_depth` players per configured region,
sequentially and rate-limited; the champion tables pool each region's
highest-ranked `champ_pool_depth` players. Results are cached
process-lifetime for `cache_ttl` - a fresh cache answers instantly, a
stale one triggers a re-parse first (the typing indicator shows during
the parse; a cold multi-region parse takes tens of seconds). Mind the
ceiling: a cold parse walks `parse_depth` / 100 source pages per region
at the configured request pace - keep the worst case comfortably inside
the platform's interaction window (about 15 minutes on Discord), or the
invocation's "thinking" state expires before the answer lands. The output
leads with a coverage line (`Parsed 2941/3000 players from 3 regions ·
data age 2h`), renders a bucket row only when the parses actually cover
it, degrades holes (a player without role/champion data leaves that
table but never breaks the answer), and splits across messages on line
boundaries. A failing region is named in the dump and served from cache
when possible; only a total failure replies with an error. Concurrent
invocations share the refresh (singleflight) but each gets its own full
dump.

| Command | Tier | Effect |
|---|---|---|
| `/lol_leaderboard` | user | post the leaderboard statistics in the current channel (public) |

Operator configuration lives in the optional `[lol_leaderboard]`
section (**startup-only**; an absent section - or no regions served by
the source - keeps the command in "not configured" mode):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `regions` | string array | *(empty)* | regions to aggregate: `kr`, `euw`, `eun`, `na`, `jp`, `br`, `tr`, `tw`, `vn`, `sea`; empty = plugin off |
| `request_interval_secs` | integer | `1` | minimum delay between source requests (sequential, rate-limit politeness); `0` disables the plugin |
| `parse_depth` | integer | `1000` | top players parsed per region (clamped to what the source's board has); `0` disables; capped at 10 000 |
| `display_buckets` | integer array | `[300, 1000]` | player-count rows for the role tables; values are clamped to `parse_depth`, sorted, deduplicated; empty = a single full-depth row |
| `champ_pool_depth` | integer | `1000` | per-region player pool (highest ranked first) behind the champion tables; clamped to `parse_depth` |
| `champs_per_role` | integer | `5` | champions listed per role |
| `cache_ttl_secs` | integer | `64800` (18h) | how long cached data stays fresh; `0` disables |
| `proxy` | string | *(none)* | optional proxy for source requests (`socks5://` or `http://`); the Discord proxy does not apply |

Example output shape (abridged):

````markdown
# LoL Leaderboard Statistics

Parsed 3000/3000 players from 3 regions · data age 2h 5m old

## Role distribution in average

**TOP 300:** Top: 18.67% | Jungle: 26.33% | Middle: 19.67% | Bot: 24.67% | Supporter: 10.67%

## Role distribution in KR

**TOP 300:** Top: 18.67% | Jungle: 26.33% | Middle: 19.67% | Bot: 24.67% | Supporter: 10.67%

## Most picked champions per role

**Top** - 540 players

1. Ambessa - 14.8%
2. Rumble - 14.4%
````

Maintainer notes - how to explore and re-verify the data source (the
platform-id gotcha, the lane enum, pagination, the champion info
endpoint) - live in
[`src/plugins/lol_leaderboard/README.md`](src/plugins/lol_leaderboard/README.md).

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
# Optional defaults: default_system_prompt, default_compaction_prompt
# (both template-rendered per request - see "Prompt templates" below),
# bot_name / bot_id (template identity overrides; discovered from the
# platform at boot), compaction_model, compaction_keep_tail,
# max_message_length (clamped to the platform's message limit at boot), stream_interval_ms,
# time_offset_minutes (0 = UTC), max_consecutive_newlines (collapse
# blank-line runs in answers down to N; absent = untouched). `log_raw_traffic = true`
# dumps every LLM request and response body at DEBUG level (stdout only)
# while debugging a provider - it carries conversation content, so it
# stays off by default.
# Image recognition: set image_model to a vision-capable declared model;
# channels then opt in with /llm_set images on. Optional: image_prompt,
# image_max_side (512), image_jpeg_quality (85), image_max_source_bytes
# (16 MiB), max_images_per_message (2), react_max_per_message (3 - the
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
- **Prompt templates**: the system and compaction prompts (channel
  overrides and `[llm]` defaults alike) render `{{token}}` per request -
  `{{bot}}` (the bot's identity as `name (id)`, degrading to whichever
  part is known), `{{bot_name}}`, `{{bot_id}}`, `{{date}}` (`YYYY-MM-DD`),
  `{{weekday}}`, `{{hour}}` (`HH:00-HH:59` - minute-free on purpose, so
  provider prompt caches rebuild at most once an hour; shifted by
  `[llm] time_offset_minutes`, UTC by default), `{{platform}}`,
  `{{guild_name}}` (empty when unknown) and `{{model}}` (the model
  executing the prompt). Identity comes from the platform at boot;
  `[llm] bot_name`/`bot_id` override it per part. Unknown tokens stay
  literal - `/llm_set_prompt` rejects them outright with the valid list.
  The built-in default prompts carry `You are {{bot}}` so the bot can
  identify itself; custom prompts opt in by using the tokens.
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
  (multi-image messages number the placeholders; the placeholder carries
  the attachment's real extension, resolved from the platform's content
  type with the file name as fallback and `png` last, so an animated
  `image.gif` hints at motion even though recognition only saw the first
  frame) - so the chat model
  reads what an image showed without ever receiving pixels: any declared
  model works, and costs stay bounded (each image is described once,
  rescaled to `image_max_side`, at most `max_images_per_message` per
  message). The recognition prompt is customizable per channel
  (`image_prompt`) - useful for pinning the description language.
  Undescribed images (feature off, recognition failure, oversize,
  over-cap) still render `![image without description](image.png)`, so
  the model at least knows an image was posted. Images are fetched from
  Discord's CDN only;
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
| `/llm_set_prompt kind:<kind> prompt:<text>` | moderator | set a channel prompt - `kind` is `system` (persona), `compaction` (summary instruction) or `image` (recognition instruction); `clear` falls back to the plugin default; attach `file` instead of `prompt` for long prompts (Discord CDN only, capped by `[llm] max_prompt_file_bytes`, 128 KiB default); omitting both arguments shows the current value |
| `/llm_set key:<key> value:<value>` | moderator | tune one channel setting (table below) |
| `/llm_get key:<key>` | moderator | show a setting's current value (defaults render as the effective value, long text truncated); omit `key` to list every setting |
| `/llm_dump` | moderator | dump every setting at once in one copy-pasteable code fence (`key = value`, effective values); lines that differ from a fresh `/llm_assign` carry a `*` marker; prompts are not dumped at all - only set-or-not and size, the text is one argument-free `/llm_set_prompt kind` away |
| `/llm_cutoff` | moderator | start a fresh conversation: summary cleared, cutoff moved past all records - stored history is kept |
| `/llm_status` | user | report: active system prompt (override or plugin default, char count, fingerprint, head preview), model, reasoning setting, window usage, compaction, image recognition (state, model, prompt length), reactions (state, silent-react chance), capture mode, chime-in chance, summary preview, link to the context start, last-request token stats (incl. reasoning tokens when reported), last response time (endpoint-reported or measured) |
| `/llm_admin` | moderator | make this channel the guild's service channel for error notices (one per guild, last write wins) |
| `/llm_admin_clear` | moderator | stop service notices |

**`/llm_set` keys.** Every key takes one value; `clear`, `none` or
`default` as the value resets the key to its default - the two keys
without a default (`model`, `depth`) refuse and point at
`/llm_unassign`. On/off keys accept `on`/`off` (also
`true`/`yes`/`1` and `false`/`no`/`0`). Invalid values are answered
with usage and never saved. `/llm_get` reads the same keys back. The
three channel prompts (system persona, compaction and image
instructions) are deliberately not keys here - they have their own
command, `/llm_set_prompt`, which also reads them back and accepts
uploaded files for long texts.

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
| `images` | on / off | off | describe attached images on captured messages via the recognition model; needs an operator `[llm] image_model` |
| `image_model` | declared model ref | plugin `[llm] image_model` | recognition model override for this channel |
| `react` | on / off | off | emoji-reaction tool: the model may decorate the message it replies to by emitting a `[[react: ...]]` marker, stripped before the answer is shown |
| `streaming` | on / off | off | stream the answer live from the provider (SSE): the message appears with the first tokens and is edited at `stream_interval_ms` |
| `random_chance` | 0-100 (clamped) | 2 | percent chance to chime in on a captured non-trigger message; 0 = off |
| `random_cooldown` | whole seconds | 5 | minimum seconds between chime-ins - the reply and silent-react rolls each keep their own tracker behind it; 0 = none |
| `random_react_chance` | 0-100 (clamped) | 10 | percent chance for a silent react (emoji only, no reply) on a captured non-trigger message; independent of `random_chance`; needs `react` on |
| `max_length` | 1 up to the platform's message limit | `max_message_length` | per-channel reply-splitting limit |
| `turn_template` | template containing `{sender}` and `{message}` | `[{sender}](<@{user_id}>): {message}` | how user turns render into the model context; fields: `{sender}`, `{user_id}`, `{guild_name}`, `{time}` (unix), `{message}` |

**Delivery.** The bot holds the channel's typing indicator from the
moment a triggered run is accepted - through any wait for a previous
answer on the same channel to finish - across generation and delivery;
users see it composing, not frozen. The answer posts as a native Discord
reply to the message that triggered it
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
per-channel deck, so hits balance out per cycle instead of clumping -
chances up to ~28% normalize to a single hit (2% is one guaranteed hit
per 50 draws, not two per 100 that might pair up; sub-percent chances
stay exact), higher chances keep the exact percent as hits per 100
draws. With `react` on, a second independent roll
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
├── plugins/           # features: auth, command dispatcher, tracker, audit trail, status rotator, nickname, lol store tracker, lol leaderboard, llm chat
├── infrastructure/    # adapters
│   ├── inbound_adapters/   # Discord gateway + scoped output factory
│   ├── outbound_adapters/  # storage (sqlx), Discord command registrar, presence
│   └── plugin_adapters/    # in-memory event bus, command registry, scheduler
└── common/
```

## Roadmap

- Further platform adapters (Telegram, Matrix, ...)
