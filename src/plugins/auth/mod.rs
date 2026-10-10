use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::kernel::{
    models::{Embed, EventKind, EventPayload, OutboundMessage, PluginError, RequestContext},
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort,
        MiddlewarePluginPort, Next, Permission, PluginPort,
    },
    services::KernelServices,
    spi_ports::GuildStorage,
};

mod command;

pub use command::AuthCommandHandler;

/// Guild storage namespace owned by this plugin (the plugin's slug).
pub(crate) const NAMESPACE: &str = "auth";
/// Per-guild storage key holding the serialized [`AuthConfig`].
pub(crate) const CONFIG_KEY: &str = "config";

/// Discord's `ADMINISTRATOR` permission bit. The taxonomy carries author
/// permissions as opaque platform data; this bit value is Discord's, passed
/// through by the driving adapter, and nothing interprets the other bits.
const GUILD_ADMINISTRATOR_BIT: u64 = 0x8;

/// Per-guild tier policy, stored as a JSON document in the plugin's guild
/// storage namespace. The `/auth` management command (same plugin,
/// interactive half) writes this document. Schema v2 - tier assignments
/// instead of flat allow-lists; there is no legacy migration: a document
/// that does not carry the new fields deserializes to the open default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Tier for members with no explicit assignment and no qualifying role.
    /// `User` keeps a fresh guild open by default - otherwise the gate would
    /// deny the very commands that configure it.
    pub default_tier: AccessTier,
    /// Explicit per-user tier assignments (platform user IDs as strings).
    /// A `Banned` assignment wins over every role grant - the one exception
    /// is Discord's administrator clamp, which resolves such a member to
    /// `Admin` regardless.
    pub users: BTreeMap<String, AccessTier>,
    /// Per-role tier assignments (platform role IDs as strings): holding the
    /// role grants at least that tier.
    pub roles: BTreeMap<String, AccessTier>,
}

impl AuthConfig {
    /// Clamps every stored tier to [`AccessTier::Admin`]. Guild-side data
    /// must never mint an [`AccessTier::Owner`]: only the config-injected
    /// owner list may. A hand-edited policy document containing `"owner"`
    /// deserializes fine but resolves at most to `Admin`.
    #[must_use]
    pub fn sanitized(mut self) -> Self {
        self.default_tier = self.default_tier.min(AccessTier::Admin);
        for tier in self.users.values_mut() {
            *tier = (*tier).min(AccessTier::Admin);
        }
        for tier in self.roles.values_mut() {
            *tier = (*tier).min(AccessTier::Admin);
        }
        self
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { default_tier: AccessTier::User, users: BTreeMap::new(), roles: BTreeMap::new() }
    }
}

/// Deployment-global bot-owner identities (platform user IDs as strings),
/// injected from the composition root's config. Never guild data: no
/// guild-side code path can read or change the list. Shared between the
/// auth gate (tier resolution) and the `/auth` command (write guard,
/// owner self-status). Hot-reloadable: the config watcher swaps the
/// contents via [`AuthPlugin::update_owners`].
#[derive(Clone, Default)]
pub struct OwnerList {
    ids: Arc<RwLock<Vec<String>>>,
}

impl OwnerList {
    /// Builds from raw config strings: trimmed, empties dropped, sorted and
    /// deduplicated - the order carries no meaning.
    #[must_use]
    pub fn from_ids(raw: &[String]) -> Self {
        Self { ids: Arc::new(RwLock::new(normalize_owners(raw))) }
    }

    /// Replaces the list in place. Returns whether anything changed
    /// (identical content is a no-op, per the config-watcher contract).
    #[must_use]
    pub fn replace(&self, raw: &[String]) -> bool {
        let normalized = normalize_owners(raw);
        let mut ids = self.ids.write();
        if *ids == normalized {
            return false;
        }
        *ids = normalized;
        true
    }

    #[must_use]
    pub fn contains(&self, user_id: &str) -> bool {
        self.ids.read().iter().any(|id| id == user_id)
    }

    /// Number of configured owners - boot and reload telemetry only.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.read().is_empty()
    }
}

fn normalize_owners(raw: &[String]) -> Vec<String> {
    let mut ids: Vec<String> =
        raw.iter().map(|id| id.trim().to_owned()).filter(|id| !id.is_empty()).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Per-guild policy cache for the gate's hot path: every inbound message
/// and command resolves a tier policy, and a storage round-trip per event
/// is the most frequent query in the system. Entries serve for at most
/// `ttl` and die wholesale on every `/auth` write (an epoch bump -
/// precise per-guild invalidation would save only a handful of reads per
/// admin-frequency write, and a global epoch is immune to keying
/// mistakes). Only successful reads are cached: storage failures and
/// malformed documents always observe live storage, so the fail-closed
/// paths gain no staleness.
#[derive(Clone)]
pub(crate) struct PolicyCache {
    inner: Arc<PolicyCacheInner>,
    /// Test seam: the production value is [`Self::DEFAULT_TTL`].
    pub(crate) ttl: Duration,
}

struct PolicyCacheInner {
    epoch: AtomicU64,
    entries: Mutex<HashMap<(&'static str, u64), CachedPolicy>>,
}

struct CachedPolicy {
    policy: Arc<AuthConfig>,
    fetched_at: Instant,
    epoch: u64,
}

impl PolicyCache {
    /// How long a cached policy serves without a storage re-read. `/auth`
    /// writes invalidate immediately; the TTL only bounds how long an
    /// out-of-band storage edit (a direct DB change) can lag.
    const DEFAULT_TTL: Duration = Duration::from_secs(5);

    /// The current invalidation epoch. Callers capture this BEFORE the
    /// storage read and hand it to [`Self::put`]: an entry is stored with
    /// the epoch the read started under, so a write that lands mid-read
    /// (read old data, epoch already bumped) leaves the entry tagged with
    /// the pre-write epoch - the next `get` sees the mismatch and misses.
    fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::Acquire)
    }

    fn get(&self, platform: &'static str, guild: u64) -> Option<Arc<AuthConfig>> {
        let epoch = self.inner.epoch.load(Ordering::Acquire);
        let entries = self.inner.entries.lock();
        let entry = entries.get(&(platform, guild))?;
        (entry.epoch == epoch && entry.fetched_at.elapsed() < self.ttl)
            .then(|| Arc::clone(&entry.policy))
    }

    /// Stores `policy` under `observed_epoch` - the epoch the caller
    /// captured before the storage read that produced the policy. Loading
    /// the epoch HERE instead would be a put-after-invalidate race: the
    /// storage read awaits, a `/auth` write + bump can land inside that
    /// window, and a pre-write policy tagged with the post-bump epoch
    /// would serve as fresh for the whole TTL.
    fn put(
        &self,
        platform: &'static str,
        guild: u64,
        policy: AuthConfig,
        observed_epoch: u64,
    ) -> Arc<AuthConfig> {
        let policy = Arc::new(policy);
        self.inner.entries.lock().insert(
            (platform, guild),
            CachedPolicy {
                policy: Arc::clone(&policy),
                fetched_at: Instant::now(),
                epoch: observed_epoch,
            },
        );
        policy
    }

    /// Any policy write drops every guild's entry at once.
    fn invalidate_all(&self) {
        self.inner.epoch.fetch_add(1, Ordering::AcqRel);
    }
}

impl Default for PolicyCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(PolicyCacheInner {
                epoch: AtomicU64::new(0),
                entries: Mutex::new(HashMap::new()),
            }),
            ttl: Self::DEFAULT_TTL,
        }
    }
}

/// Per-guild bot access control - the first short-circuiting middleware.
///
/// Policy:
/// - Gates bot *invocation* (`MessageReceived` and `CommandInvoked`).
///   Passive events (member join/leave, presence) flow to their plugins
///   regardless - auth is about who may use the bot, not about what happens
///   in the guild. Direct messages pass too: no guild scope to protect.
/// - Every member has an effective [`AccessTier`] (see [`Self::effective_tier`]).
/// Banned members are dropped silently - messages and commands alike,
/// without any denial output: bans never announce themselves. An open
/// transactional slot (a deferred interaction) is dismissed without
/// content, so the invoker sees the pending state clear, never a reply.
/// - Commands are tier-gated by their descriptor's `required_tier` (looked
///   up in the command registry); a member below the requirement gets an
///   ephemeral denial on transactional origins. Unknown commands pass -
///   the dispatcher ignores them anyway.
/// - Plain messages only need to not be banned: chatting with the bot is
///   the `Guest` floor.
/// - A malformed config document fails closed - corruption never widens
///   access. A storage failure fails closed the same way.
/// - Above the guild policy sits the deployment-global [`OwnerList`]: bot
///   owners resolve to [`AccessTier::Owner`] regardless of any guild-side
///   rank, ban included. The list comes from the operator's config and is
///   hot-reloadable ([`Self::update_owners`]); the bot itself cannot
///   change it.
pub struct AuthPlugin {
    registry: Arc<dyn CommandRegistryPort>,
    owners: OwnerList,
    policy_cache: PolicyCache,
}

impl AuthPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>, owners: OwnerList) -> Self {
        Self { registry, owners, policy_cache: PolicyCache::default() }
    }

    /// Hot reload: swaps the owner list. Identical content is a no-op.
    pub fn update_owners(&self, raw: &[String]) {
        if self.owners.replace(raw) {
            tracing::info!(count = self.owners.len(), "config owners hot-reloaded");
        }
    }
}

impl PluginPort for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
    }

    fn init(&self) -> Result<(), PluginError> {
        // The interactive half of this plugin. Discord additionally hides
        // the command behind Manage Server (`default_member_permissions`) -
        // defense in depth on top of the `Admin` tier check below.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "auth".to_owned(),
                description: "Manage member tiers for bot access in this server".to_owned(),
                arguments: vec![
                    ArgDescriptor {
                        name: "action".to_owned(),
                        description: "show, set, clear, or default the tier policy".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(vec![
                            "show".to_owned(),
                            "set".to_owned(),
                            "clear".to_owned(),
                            "default".to_owned(),
                        ]),
                    },
                    ArgDescriptor {
                        name: "tier".to_owned(),
                        description: "Tier for `set`/`default`: banned, guest, user, moderator, \
                             admin"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::String,
                        // `owner` deliberately absent: config-injected rank,
                        // never offered as a choice or accepted by the parser.
                        choices: Some(vec![
                            "banned".to_owned(),
                            "guest".to_owned(),
                            "user".to_owned(),
                            "moderator".to_owned(),
                            "admin".to_owned(),
                        ]),
                    },
                    ArgDescriptor {
                        name: "user".to_owned(),
                        description: "User to assign a tier to (`set`/`clear`; one target per \
                             call; bot owners are config-managed)"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::User,
                        choices: None,
                    },
                    ArgDescriptor {
                        name: "role".to_owned(),
                        description: "Role to grant a tier (`set`/`clear`; one target per call)"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::Role,
                        choices: None,
                    },
                ],
                required_permission: Some(Permission { name: "manage_guild".to_owned() }),
                required_tier: Some(AccessTier::Admin),
                guild_only: true,
            },
            Arc::new(
                AuthCommandHandler::new()
                    .with_owners(self.owners.clone())
                    .with_policy_cache(self.policy_cache.clone()),
            ),
        );
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for AuthPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        if !matches!(event.kind, EventKind::MessageReceived | EventKind::CommandInvoked) {
            return Next::Continue;
        }

        // Direct messages: no guild scope to protect.
        let Some(storage) = &services.guild_storage else {
            return Next::Continue;
        };

        // Hot path: the sanitized policy comes through the per-guild cache;
        // a storage round-trip per event would be the system's most
        // frequent query. Failures never enter the cache.
        let config = match self.cached_policy(services, event, storage).await {
            Ok(config) => config,
            Err(embed) => {
                self.answer_denial(services, event, embed).await;
                return Next::Stop;
            }
        };

        let tier = Self::effective_tier(&config, &self.owners, event);

        if tier == AccessTier::Banned {
            tracing::info!(user = %event.origin.user_id, guild_id = ?event.origin.guild_id, "event denied by auth: banned (ignored silently)");
            // A transactional origin (a deferred command interaction) owes
            // the platform a resolution: dismiss the open slot without
            // content so the invoker is not left on "thinking" until the
            // token expires. No denial output rides it - bans never
            // announce themselves. Plain origins have no slot (no-op).
            if event.origin.reply_token.is_some() {
                services.chat_output.dismiss().await;
            }
            return Next::Stop;
        }

        if let EventPayload::Command(command) = &event.payload {
            match self.registry.descriptor(&command.name) {
                None => {} // Unknown command: the dispatcher ignores it anyway.
                Some(descriptor) => {
                    let required = descriptor.required_tier.unwrap_or(AccessTier::Guest);
                    if tier < required {
                        tracing::info!(
                            user = %event.origin.user_id,
                            guild_id = ?event.origin.guild_id,
                            command = %command.name,
                            required = %required,
                            effective = %tier,
                            "command denied by auth tier"
                        );
                        let denial = Self::denial_embed(&command.name, required, tier);
                        self.answer_denial(services, event, denial).await;
                        return Next::Stop;
                    }
                }
            }
        }

        Next::Continue
    }
}

impl AuthPlugin {
    /// Resolves the sanitized guild policy through the per-guild cache.
    /// Hits skip the storage round-trip; misses read, sanitize, and cache
    /// (an unconfigured guild caches the open default - the `/auth` write
    /// path invalidates it the moment a policy appears). Failures return
    /// the denial embed and never enter the cache: the fail-closed paths
    /// always observe live storage.
    async fn cached_policy(
        &self,
        services: &KernelServices,
        event: &RequestContext,
        storage: &Arc<dyn GuildStorage>,
    ) -> Result<Arc<AuthConfig>, Embed> {
        let platform = services.platform_info.slug();
        let key = event.origin.guild_id.map(|guild| (platform, guild.get()));
        if let Some((platform, guild)) = key
            && let Some(policy) = self.policy_cache.get(platform, guild)
        {
            return Ok(policy);
        }
        // Captured BEFORE the read: the entry this read produces is stored
        // under this epoch, so any `/auth` write landing mid-read bumps the
        // live epoch and the stale entry never serves (see `PolicyCache::put`).
        let observed_epoch = self.policy_cache.epoch();
        let loaded = match storage.get(NAMESPACE, CONFIG_KEY).await {
            // Unconfigured = the open default policy (default tier `User`).
            Ok(None) => None,
            Ok(Some(raw)) => Some(serde_json::from_value::<AuthConfig>(raw)),
            // Unreadable = fail closed, same as a malformed policy: a storage
            // failure must never widen access, for any guild.
            Err(err) => {
                tracing::error!(
                    namespace = NAMESPACE,
                    %err,
                    "auth config unreadable - failing closed"
                );
                return Err(Self::policy_unavailable_embed(
                    event,
                    "temporarily unavailable (storage error)",
                    "Try again in a moment.",
                ));
            }
        };
        let config = match loaded {
            Some(Ok(config)) => config.sanitized(),
            Some(Err(_)) => {
                tracing::warn!(namespace = NAMESPACE, "auth config is malformed - failing closed");
                return Err(Self::policy_unavailable_embed(
                    event,
                    "unreadable (malformed)",
                    "Ask a guild admin to fix the bot configuration.",
                ));
            }
            None => AuthConfig::default(),
        };
        Ok(match key {
            Some((platform, guild)) => {
                self.policy_cache.put(platform, guild, config, observed_epoch)
            }
            // Guild-less events never reach the gate's storage arm; the
            // branch keeps the helper total regardless.
            None => Arc::new(config),
        })
    }

    /// Effective tier of the event's author:
    /// - A configured bot owner is [`AccessTier::Owner`], above every
    ///   guild-side rank: bans, explicit assignments, and the Discord
    ///   administrator clamp never reach an owner.
    /// - Discord guild administrators are `Admin` by construction - the
    ///   clamp IS the "cannot be removed" guarantee; there is no stored
    ///   entry to lose, and no code path anywhere needs a special case.
    /// - An explicit `Banned` user assignment wins over every role grant.
    /// - Otherwise: the user's assignment (or the guild default) lifted by
    ///   the best qualifying role - an explicit assignment below the
    ///   default (e.g. `Guest`) stays below it until a role lifts it.
    ///
    /// The config arrives pre-sanitized, so stored tiers top out at
    /// `Admin`: only the owner list can yield `Owner`.
    fn effective_tier(
        config: &AuthConfig,
        owners: &OwnerList,
        event: &RequestContext,
    ) -> AccessTier {
        let user_id = event.origin.user_id.get().to_string();
        if owners.contains(&user_id) {
            return AccessTier::Owner;
        }

        let (author_roles, author_permissions) = match &event.payload {
            EventPayload::Message(message) => (&message.author_roles, message.author_permissions),
            EventPayload::Command(command) => (&command.author_roles, command.author_permissions),
            _ => return config.default_tier,
        };

        if author_permissions & GUILD_ADMINISTRATOR_BIT != 0 {
            return AccessTier::Admin;
        }

        if config.users.get(&user_id) == Some(&AccessTier::Banned) {
            return AccessTier::Banned;
        }

        let base = config.users.get(&user_id).copied().unwrap_or(config.default_tier);
        let role_tier =
            author_roles.iter().filter_map(|role| config.roles.get(role)).copied().max();
        base.max(role_tier.unwrap_or(base))
    }

    /// Delivers a denial to transactional events only (slash commands owe
    /// the invoker an answer; make it ephemeral so it never spams the
    /// channel or exposes the policy). Plain messages are not interactions -
    /// ephemeral is impossible there and a public reply would be a spam
    /// vector - so they stay silent (bans are silent everywhere).
    async fn answer_denial(&self, services: &KernelServices, event: &RequestContext, embed: Embed) {
        if event.origin.reply_token.is_none() {
            return;
        }
        let denial = OutboundMessage::embed(embed).ephemeral();
        if let Err(err) = services.chat_output.send(denial).await {
            tracing::warn!(%err, "failed to deliver auth denial");
        }
    }

    /// Denial when the policy itself could not be read: no tiers to quote,
    /// point at the cause instead. A storage outage and a malformed policy
    /// both deny - the wording must not confuse one with the other.
    fn policy_unavailable_embed(event: &RequestContext, cause: &str, advice: &str) -> Embed {
        let what = match &event.payload {
            EventPayload::Command(command) => {
                format!("Command `/{}` could not be authorized.", command.name)
            }
            _ => "The action could not be authorized.".to_owned(),
        };
        Embed {
            title: "⛔ Not authorized".to_owned(),
            description: format!(
                "{what}\nThis guild's access policy is {cause}, so the request was denied.\n{advice}"
            ),
        }
    }

    fn denial_embed(command_name: &str, required: AccessTier, effective: AccessTier) -> Embed {
        Embed {
            title: "⛔ Not authorized".to_owned(),
            description: format!(
                "Command `/{command_name}` requires the {required} tier (you have {effective}).\nAsk a guild admin if you think this is a mistake."
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{
            ChannelId, CommandPayload, GuildId, MemberPayload, MessageId, MessagePayload, Origin,
            UserId,
        },
        plugin_ports::{CommandArgs, CommandHandler},
        spi_ports::{GUILD_SETTINGS, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Registry fixture carrying the descriptors the gate looks up. The
    /// `/auth` handler tests live in `command.rs`; handlers here are no-ops.
    fn test_plugin(commands: &[(&str, AccessTier)]) -> AuthPlugin {
        AuthPlugin::new(test_registry(commands), OwnerList::default())
    }

    /// Same gate, but with configured bot owners (deployment-global
    /// identities above every guild-side rank).
    fn test_plugin_with_owners(commands: &[(&str, AccessTier)], owner_ids: &[u64]) -> AuthPlugin {
        let owners: Vec<String> = owner_ids.iter().map(ToString::to_string).collect();
        AuthPlugin::new(test_registry(commands), OwnerList::from_ids(&owners))
    }

    fn test_registry(commands: &[(&str, AccessTier)]) -> Arc<InMemoryCommandRegistry> {
        struct NoopHandler;

        #[async_trait]
        impl CommandHandler for NoopHandler {
            async fn invoke(
                &self,
                _event: &RequestContext,
                _args: &CommandArgs,
                _services: &KernelServices,
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        let registry = Arc::new(InMemoryCommandRegistry::new());
        for (name, tier) in commands {
            registry.register(
                CommandDescriptor {
                    plugin_id: "test".to_owned(),
                    name: (*name).to_owned(),
                    description: "test command".to_owned(),
                    arguments: Vec::new(),
                    required_permission: None,
                    required_tier: Some(*tier),
                    guild_only: true,
                },
                Arc::new(NoopHandler),
            );
        }
        registry
    }

    fn origin(user_id: u64) -> Origin {
        Origin {
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(user_id),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn command_event(user_id: u64, name: &str, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(user_id),
            payload: EventPayload::Command(CommandPayload {
                name: name.to_owned(),
                args: Vec::new(),
                author_roles: roles.iter().map(|role| (*role).to_owned()).collect(),
                author_permissions: 0,
            }),
        }
    }

    fn message_event(user_id: u64, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::MessageReceived,
            origin: origin(user_id),
            payload: EventPayload::Message(MessagePayload {
                content: "!ping".to_owned(),
                attachments: Vec::new(),
                author_name: None,
                guild_name: None,
                author_roles: roles.iter().map(|role| (*role).to_owned()).collect(),
                author_permissions: 0,
                reply_to: None,
                mentions_bot: false,
            }),
        }
    }

    /// Command event carrying explicit platform permission bits (opaque
    /// pass-through data; `0x8` marks a Discord guild administrator).
    fn command_event_with_permissions(user_id: u64, author_permissions: u64) -> RequestContext {
        let mut event = command_event(user_id, "auth", &[]);
        if let EventPayload::Command(payload) = &mut event.payload {
            payload.author_permissions = author_permissions;
        }
        event
    }

    fn join_event(user_id: u64) -> RequestContext {
        RequestContext {
            kind: EventKind::MemberJoined,
            origin: origin(user_id),
            payload: EventPayload::Member(MemberPayload { username: Some("someone".to_owned()) }),
        }
    }

    fn test_services(storage: &InMemoryStorage) -> (KernelServices, Arc<RecordingChatOutput>) {
        services_with(Some(storage.guild_scoped("test", GuildId(1))))
    }

    fn services_with(
        guild: Option<Arc<dyn crate::kernel::spi_ports::GuildStorage>>,
    ) -> (KernelServices, Arc<RecordingChatOutput>) {
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: guild,
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        (services, output)
    }

    fn configured_storage(
        default_tier: AccessTier,
        users: &[(&str, AccessTier)],
        roles: &[(&str, AccessTier)],
    ) -> Arc<InMemoryStorage> {
        let storage = InMemoryStorage::new();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            json!({
                "default_tier": default_tier.as_str(),
                "users": users.iter().map(|(id, tier)| (id.to_string(), tier.as_str()))
                    .collect::<BTreeMap<_, _>>(),
                "roles": roles.iter().map(|(id, tier)| (id.to_string(), tier.as_str()))
                    .collect::<BTreeMap<_, _>>(),
            }),
        );
        Arc::new(storage)
    }

    /// Storage wrapper counting document reads - proves the policy cache
    /// serves repeated events without touching storage.
    struct CountingStorage {
        inner: Arc<InMemoryStorage>,
        reads: Arc<AtomicUsize>,
    }

    struct CountingView {
        guild: Arc<dyn crate::kernel::spi_ports::GuildStorage>,
        reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::kernel::spi_ports::GuildStorage for CountingView {
        async fn get(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<Option<Value>, crate::kernel::models::StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.guild.get(namespace, key).await
        }

        async fn set(
            &self,
            namespace: &str,
            key: &str,
            value: Value,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.set(namespace, key, value).await
        }

        async fn delete(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.delete(namespace, key).await
        }

        async fn list_keys(
            &self,
            namespace: &str,
        ) -> Result<Vec<String>, crate::kernel::models::StorageError> {
            self.guild.list_keys(namespace).await
        }

        async fn append(
            &self,
            namespace: &str,
            payload: Value,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.append(namespace, payload).await
        }

        async fn list_after(
            &self,
            namespace: &str,
            after_seq: u64,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_after(namespace, after_seq, limit).await
        }

        async fn list_last(
            &self,
            namespace: &str,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_last(namespace, limit).await
        }

        async fn count_after(
            &self,
            namespace: &str,
            after_seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.count_after(namespace, after_seq).await
        }

        async fn delete_record(
            &self,
            namespace: &str,
            seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.delete_record(namespace, seq).await
        }
    }

    #[async_trait]
    impl crate::kernel::spi_ports::StoragePort for CountingStorage {
        fn guild_scoped(
            &self,
            platform: &str,
            guild_id: GuildId,
        ) -> Arc<dyn crate::kernel::spi_ports::GuildStorage> {
            Arc::new(CountingView {
                guild: self.inner.guild_scoped(platform, guild_id),
                reads: Arc::clone(&self.reads),
            })
        }

        async fn list_guilds(
            &self,
        ) -> Result<Vec<(String, GuildId)>, crate::kernel::models::StorageError> {
            self.inner.list_guilds().await
        }
    }

    /// Storage whose first document read fails, then delegates: proves the
    /// fail-closed path never caches a failure.
    struct FlakyStorage {
        inner: Arc<InMemoryStorage>,
        fail: Arc<AtomicBool>,
    }

    struct FlakyView {
        guild: Arc<dyn crate::kernel::spi_ports::GuildStorage>,
        fail: Arc<AtomicBool>,
    }

    #[async_trait]
    impl crate::kernel::spi_ports::GuildStorage for FlakyView {
        async fn get(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<Option<Value>, crate::kernel::models::StorageError> {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(crate::kernel::models::StorageError::Database(
                    "simulated transient failure".to_owned(),
                ));
            }
            self.guild.get(namespace, key).await
        }

        async fn set(
            &self,
            namespace: &str,
            key: &str,
            value: Value,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.set(namespace, key, value).await
        }

        async fn delete(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.delete(namespace, key).await
        }

        async fn list_keys(
            &self,
            namespace: &str,
        ) -> Result<Vec<String>, crate::kernel::models::StorageError> {
            self.guild.list_keys(namespace).await
        }

        async fn append(
            &self,
            namespace: &str,
            payload: Value,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.append(namespace, payload).await
        }

        async fn list_after(
            &self,
            namespace: &str,
            after_seq: u64,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_after(namespace, after_seq, limit).await
        }

        async fn list_last(
            &self,
            namespace: &str,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_last(namespace, limit).await
        }

        async fn count_after(
            &self,
            namespace: &str,
            after_seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.count_after(namespace, after_seq).await
        }

        async fn delete_record(
            &self,
            namespace: &str,
            seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.delete_record(namespace, seq).await
        }
    }

    #[async_trait]
    impl crate::kernel::spi_ports::StoragePort for FlakyStorage {
        fn guild_scoped(
            &self,
            platform: &str,
            guild_id: GuildId,
        ) -> Arc<dyn crate::kernel::spi_ports::GuildStorage> {
            Arc::new(FlakyView {
                guild: self.inner.guild_scoped(platform, guild_id),
                fail: Arc::clone(&self.fail),
            })
        }

        async fn list_guilds(
            &self,
        ) -> Result<Vec<(String, GuildId)>, crate::kernel::models::StorageError> {
            self.inner.list_guilds().await
        }
    }

    /// Repeated events within the TTL resolve from the cache: one storage
    /// read serves the whole burst, tiers included - the gate is the
    /// system's hottest path.
    #[tokio::test]
    async fn policy_cache_serves_repeated_events_without_storage_reads() {
        let reads = Arc::new(AtomicUsize::new(0));
        let storage =
            CountingStorage { inner: Arc::new(InMemoryStorage::new()), reads: Arc::clone(&reads) };
        let (services, output) = services_with(Some(storage.guild_scoped("test", GuildId(1))));
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);

        for _ in 0..3 {
            let mut event = command_event(3, "assign_tracker", &[]);
            assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        }

        // One read served all three events (default tier User denies the
        // moderator command every time - the cached policy is applied, not
        // just counted).
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(output.messages().len(), 0);
    }

    /// A zero TTL expires instantly: every event re-reads storage.
    #[tokio::test]
    async fn policy_cache_expires_after_the_ttl() {
        let mut plugin = test_plugin(&[]);
        plugin.policy_cache.ttl = Duration::ZERO;
        let reads = Arc::new(AtomicUsize::new(0));
        let storage =
            CountingStorage { inner: Arc::new(InMemoryStorage::new()), reads: Arc::clone(&reads) };
        let (services, _output) = services_with(Some(storage.guild_scoped("test", GuildId(1))));

        for _ in 0..2 {
            let mut event = message_event(3, &[]);
            assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        }

        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    /// A successful `/auth` write invalidates immediately: the very next
    /// event resolves the fresh policy, with no TTL wait.
    #[tokio::test]
    async fn auth_write_invalidates_the_policy_cache_immediately() {
        let reads = Arc::new(AtomicUsize::new(0));
        let storage =
            CountingStorage { inner: Arc::new(InMemoryStorage::new()), reads: Arc::clone(&reads) };
        let (services, output) = services_with(Some(storage.guild_scoped("test", GuildId(1))));
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let handler = AuthCommandHandler::new().with_policy_cache(plugin.policy_cache.clone());

        let mut denied = command_event(3, "assign_tracker", &[]);
        denied.origin.reply_token = Some("token".to_owned());
        assert!(matches!(plugin.pre(&mut denied, &services).await, Next::Stop));
        assert!(output.messages().len() == 1, "default tier User denies the moderator command");

        let mut admin = command_event(9, "auth", &[]);
        admin.origin.reply_token = Some("token".to_owned());
        let args = CommandArgs(vec![
            ("action".to_owned(), "set".to_owned()),
            ("tier".to_owned(), "moderator".to_owned()),
            ("user".to_owned(), "3".to_owned()),
        ]);
        handler.invoke(&admin, &args, &services).await.expect("set expected to succeed");

        let mut promoted = command_event(3, "assign_tracker", &[]);
        assert!(
            matches!(plugin.pre(&mut promoted, &services).await, Next::Continue),
            "the promotion applies on the very next event"
        );
        // Gate's initial read + the handler's own read_policy + the gate's
        // post-invalidation read.
        assert_eq!(reads.load(Ordering::SeqCst), 3);
    }

    /// The put-after-invalidate race, pinned at the cache seam: an entry
    /// produced by a read that started BEFORE a write is stored under the
    /// pre-write epoch and must never serve afterwards. (Loading the epoch
    /// at insert time instead would tag this stale entry fresh for the
    /// whole TTL.)
    #[test]
    fn a_read_overlapping_a_write_never_resurrects_the_old_policy() {
        let cache = PolicyCache::default();
        let observed = cache.epoch();
        // The `/auth` write lands while the gate's storage read is in flight.
        cache.invalidate_all();
        cache.put("discord", 1, AuthConfig::default(), observed);

        assert!(cache.get("discord", 1).is_none(), "the pre-write entry must not serve");
    }

    /// Storage view whose FIRST config read captures its value, then parks
    /// until released: models a gate read that was in flight (holding the
    /// pre-write snapshot) while a `/auth` write committed and invalidated.
    struct OverlappingReadView {
        guild: Arc<dyn crate::kernel::spi_ports::GuildStorage>,
        read_started: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        consumed: AtomicBool,
    }

    #[async_trait]
    impl crate::kernel::spi_ports::GuildStorage for OverlappingReadView {
        async fn get(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<Option<Value>, crate::kernel::models::StorageError> {
            if !self.consumed.swap(true, Ordering::SeqCst) {
                self.read_started.store(true, Ordering::SeqCst);
                let captured = self.guild.get(namespace, key).await?;
                while !self.release.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                return Ok(captured);
            }
            self.guild.get(namespace, key).await
        }

        async fn set(
            &self,
            namespace: &str,
            key: &str,
            value: Value,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.set(namespace, key, value).await
        }

        async fn delete(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<(), crate::kernel::models::StorageError> {
            self.guild.delete(namespace, key).await
        }

        async fn list_keys(
            &self,
            namespace: &str,
        ) -> Result<Vec<String>, crate::kernel::models::StorageError> {
            self.guild.list_keys(namespace).await
        }

        async fn append(
            &self,
            namespace: &str,
            payload: Value,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.append(namespace, payload).await
        }

        async fn list_after(
            &self,
            namespace: &str,
            after_seq: u64,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_after(namespace, after_seq, limit).await
        }

        async fn list_last(
            &self,
            namespace: &str,
            limit: u32,
        ) -> Result<Vec<crate::kernel::spi_ports::StoredRecord>, crate::kernel::models::StorageError>
        {
            self.guild.list_last(namespace, limit).await
        }

        async fn count_after(
            &self,
            namespace: &str,
            after_seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.count_after(namespace, after_seq).await
        }

        async fn delete_record(
            &self,
            namespace: &str,
            seq: u64,
        ) -> Result<u64, crate::kernel::models::StorageError> {
            self.guild.delete_record(namespace, seq).await
        }
    }

    /// The full interleaving: a gate event's storage read parks holding the
    /// pre-write policy; a `/auth` write commits and invalidates inside that
    /// window. The entry the parked read produces must never serve - the
    /// next event re-reads storage and sees the promotion.
    #[tokio::test]
    async fn write_during_an_inflight_read_does_not_resurrect_the_old_policy() {
        let storage = Arc::new(InMemoryStorage::new());
        let view = Arc::new(OverlappingReadView {
            guild: storage.guild_scoped("test", GuildId(1)),
            read_started: Arc::new(AtomicBool::new(false)),
            release: Arc::new(AtomicBool::new(false)),
            consumed: AtomicBool::new(false),
        });
        let (gate_services, _gate_output) = services_with(Some(Arc::clone(&view) as Arc<_>));
        let (admin_services, _admin_output) = services_with(Some(Arc::clone(&view) as Arc<_>));
        let plugin = Arc::new(test_plugin(&[("assign_tracker", AccessTier::Moderator)]));
        let handler = AuthCommandHandler::new().with_policy_cache(plugin.policy_cache.clone());

        // The gate event parks mid-read, holding the pre-write default.
        let gate_plugin = Arc::clone(&plugin);
        let gate = tokio::spawn(async move {
            let mut event = command_event(3, "assign_tracker", &[]);
            gate_plugin.pre(&mut event, &gate_services).await
        });
        while !view.read_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        // The write commits and invalidates while that read is in flight.
        let mut admin = command_event(9, "auth", &[]);
        admin.origin.reply_token = Some("token".to_owned());
        let args = CommandArgs(vec![
            ("action".to_owned(), "set".to_owned()),
            ("tier".to_owned(), "moderator".to_owned()),
            ("user".to_owned(), "3".to_owned()),
        ]);
        handler.invoke(&admin, &args, &admin_services).await.expect("set expected to succeed");

        view.release.store(true, Ordering::SeqCst);
        assert!(
            matches!(gate.await.expect("gate joins"), Next::Stop),
            "the parked read's pre-write policy denies, as it must"
        );

        // The stale entry the parked read produced never serves: the next
        // event resolves the promotion from live storage.
        let (check_services, _check_output) = services_with(Some(Arc::clone(&view) as Arc<_>));
        let mut promoted = command_event(3, "assign_tracker", &[]);
        assert!(
            matches!(plugin.pre(&mut promoted, &check_services).await, Next::Continue),
            "the write landing mid-read must invalidate the entry it produces"
        );
    }

    /// Setting the default tier to its current value is a no-op reply, not
    /// a storage write + cache invalidation (same contract as `set`).
    #[tokio::test]
    async fn set_default_same_value_skips_the_write() {
        let (services, output) = services_with(Some(
            Arc::new(InMemoryStorage::new()).guild_scoped("test", GuildId(1)) as Arc<_>,
        ));
        let handler = AuthCommandHandler::new();
        let args = CommandArgs(vec![
            ("action".to_owned(), "default".to_owned()),
            ("tier".to_owned(), "moderator".to_owned()),
        ]);

        let mut admin = command_event(9, "auth", &[]);
        admin.origin.reply_token = Some("token".to_owned());
        handler.invoke(&admin, &args, &services).await.expect("first invoke");
        handler.invoke(&admin, &args, &services).await.expect("second invoke");

        let messages = output.messages();
        assert_eq!(messages.len(), 2, "both invocations replied");
        let first = messages.first().expect("first reply");
        let second = messages.get(1).expect("second reply");
        assert!(first.contains("is now"), "first is a real change: {first}");
        assert!(second.contains("already"), "same-value reply: {second}");
        assert!(!second.contains("is now"), "no false success line: {second}");
    }

    /// A storage failure is never cached: the denial repeats until storage
    /// genuinely answers - the fail-closed path always observes live
    /// storage.
    #[tokio::test]
    async fn policy_storage_failures_are_never_cached() {
        let fail = Arc::new(AtomicBool::new(true));
        let storage =
            FlakyStorage { inner: Arc::new(InMemoryStorage::new()), fail: Arc::clone(&fail) };
        let (services, output) = services_with(Some(storage.guild_scoped("test", GuildId(1))));
        let plugin = test_plugin(&[("ping", AccessTier::User)]);

        let mut first = command_event(3, "ping", &[]);
        first.origin.reply_token = Some("token".to_owned());
        assert!(matches!(plugin.pre(&mut first, &services).await, Next::Stop));
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .expect("denial expected")
                .contains("temporarily unavailable")
        );

        let mut second = command_event(3, "ping", &[]);
        assert!(
            matches!(plugin.pre(&mut second, &services).await, Next::Continue),
            "the transient failure was not cached - the retry reads storage"
        );
    }

    #[tokio::test]
    async fn unconfigured_guild_allows_default_tier() {
        let storage = InMemoryStorage::new();
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Continue));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn user_tier_cannot_run_moderator_commands() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent.first().expect("denial expected").ephemeral);
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/assign_tracker` requires the Moderator tier"));
        assert!(text.contains("you have User"));
    }

    #[tokio::test]
    async fn moderator_assignment_runs_moderator_commands() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Moderator)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn role_mapping_lifts_tier() {
        let storage = configured_storage(AccessTier::User, &[], &[("42", AccessTier::Moderator)]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// Guests may chat (plain messages pass) but commands above Guest tier
    /// are denied with the tier named.
    #[tokio::test]
    async fn guest_default_allows_chat_but_denies_commands() {
        let storage = configured_storage(AccessTier::Guest, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);
        command.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Continue));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("requires the User tier"));
        assert!(text.contains("you have Guest"));
    }

    /// An explicit assignment below the default survives: `max()` must not
    /// let the guild default lift a deliberate demotion.
    #[tokio::test]
    async fn explicit_guest_assignment_beats_open_default() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Guest)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("you have Guest"));
    }

    /// Banned members are dropped without any output - bans never announce
    /// themselves, on messages and on commands alike. A transactional
    /// command still resolves its deferred slot: dismissed once, silently,
    /// with no content.
    #[tokio::test]
    async fn banned_members_are_dropped_silently() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);
        command.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Stop));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
        // Only the transactional command dismissed its slot; the plain
        // message has none and must not touch the output at all.
        assert_eq!(output.dismissals(), 1);
    }

    /// An explicit ban wins over role grants: a banned user holding a
    /// moderator role stays banned.
    #[tokio::test]
    async fn ban_beats_role_grants() {
        let storage = configured_storage(
            AccessTier::User,
            &[("3", AccessTier::Banned)],
            &[("42", AccessTier::Moderator)],
        );
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// Discord guild administrators are Admin by construction: even a
    /// policy that never mentions them lets them run `/auth`.
    #[tokio::test]
    async fn guild_admin_is_admin_by_construction() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event_with_permissions(3, 0x8);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// The clamp beats an explicit ban, too - removing a guild admin's
    /// access is impossible by construction, in every code path.
    #[tokio::test]
    async fn guild_admin_clamp_beats_explicit_ban() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event_with_permissions(3, 0x8);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// Non-admins cannot run admin-tier commands; the denial names both
    /// sides of the comparison.
    #[tokio::test]
    async fn non_admin_cannot_run_auth() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event(3, "auth", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("Command `/auth` requires the Admin tier"));
        assert!(text.contains("you have User"));
    }

    /// Commands without a registered descriptor pass: the dispatcher has no
    /// handler for them, so gating them would change nothing.
    #[tokio::test]
    async fn unknown_command_passes() {
        let storage = configured_storage(AccessTier::Guest, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[]);
        let mut event = command_event(3, "nonexistent", &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// A legacy pre-tier policy document has no migration: it deserializes
    /// to the open default (unknown fields ignored, defaults filled in).
    #[tokio::test]
    async fn legacy_document_behaves_as_open_default() {
        let storage = InMemoryStorage::new();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            json!({ "allowed_users": ["999"], "allowed_roles": [] }),
        );
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn malformed_config_fails_closed() {
        let storage = InMemoryStorage::new();
        storage.seed("test", GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// A broken policy cannot quote tiers, but a slash command still owes
    /// the invoker an ephemeral answer pointing at the configuration.
    #[tokio::test]
    async fn malformed_config_denies_commands_with_policy_unavailable_embed() {
        let storage = InMemoryStorage::new();
        storage.seed("test", GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent.first().expect("denial expected").ephemeral);
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/ping` could not be authorized."));
        assert!(text.contains("unreadable (malformed)"));
        assert!(!text.contains("tier"), "no policy, no reasons");
    }

    /// Passive events are not gated - even a banned member's join flows to
    /// the plugins that need it.
    #[tokio::test]
    async fn non_message_events_pass_through() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[]);
        let mut event = join_event(3);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn direct_messages_pass_through() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: None,
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let plugin = test_plugin(&[]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
        drop(storage);
    }

    /// Guards the reserved namespace constant the guild settings plugin will
    /// rely on; auth must never collide with it.
    #[test]
    fn auth_namespace_is_not_the_reserved_guild_namespace() {
        assert_ne!(NAMESPACE, GUILD_SETTINGS);
    }

    /// A storage failure must fail closed (deny, like a malformed policy),
    /// never silently fail open - and transactional events owe the invoker
    /// the ephemeral "policy unavailable" notice.
    #[tokio::test]
    async fn unreadable_policy_fails_closed() {
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped("test", GuildId(1)),
            ),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let plugin = test_plugin(&[]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let messages = output.messages();
        assert_eq!(messages.len(), 1, "denial embed expected");
        assert!(messages.first().is_some_and(|m| m.contains("temporarily unavailable")));
        assert!(output.sent().first().is_some_and(|m| m.ephemeral));
    }

    /// A configured owner is above every guild-side rank: an explicit
    /// `Banned` assignment (and the ban's silent drop) never reaches them,
    /// and no denial output appears.
    #[tokio::test]
    async fn configured_owner_overrides_ban_and_assignments() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin_with_owners(&[("auth", AccessTier::Admin)], &[3]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "auth", &[]);

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Continue));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// The owner tier comes only from the config list. Guild-side data is
    /// sanitized to `Admin` at most: even a hand-edited `"owner"` assignment
    /// in the policy document stays below an Owner-gated command, while the
    /// configured owner passes it - and overrides a ban on top.
    #[tokio::test]
    async fn owner_tier_comes_only_from_config() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Owner)], &[]);
        let (services, _output) = test_services(&storage);
        let plugin = test_plugin(&[("owner_tool", AccessTier::Owner)]);
        let mut denied = command_event(3, "owner_tool", &[]);
        assert!(matches!(plugin.pre(&mut denied, &services).await, Next::Stop));

        let plugin = test_plugin_with_owners(&[("owner_tool", AccessTier::Owner)], &[3]);
        let mut allowed = command_event(3, "owner_tool", &[]);
        assert!(matches!(plugin.pre(&mut allowed, &services).await, Next::Continue));

        let banned = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (banned_services, _out) = test_services(&banned);
        let plugin = test_plugin_with_owners(&[("owner_tool", AccessTier::Owner)], &[3]);
        let mut command = command_event(3, "owner_tool", &[]);
        assert!(matches!(plugin.pre(&mut command, &banned_services).await, Next::Continue));
    }

    /// A hand-edited `"owner"` assignment is clamped to `Admin` - enough for
    /// admin commands, never for owner-only surfaces.
    #[tokio::test]
    async fn hand_edited_owner_assignment_clamps_to_admin() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Owner)], &[]);
        let (services, _output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut command = command_event(3, "auth", &[]);

        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Continue));
    }

    #[test]
    fn sanitized_clamps_stored_tiers_to_admin() {
        let config = AuthConfig {
            default_tier: AccessTier::Owner,
            users: BTreeMap::from([
                ("3".to_owned(), AccessTier::Owner),
                ("4".to_owned(), AccessTier::User),
            ]),
            roles: BTreeMap::from([("9".to_owned(), AccessTier::Owner)]),
        }
        .sanitized();

        assert_eq!(config.default_tier, AccessTier::Admin);
        assert_eq!(config.users.get("3"), Some(&AccessTier::Admin));
        assert_eq!(config.users.get("4"), Some(&AccessTier::User));
        assert_eq!(config.roles.get("9"), Some(&AccessTier::Admin));
    }

    /// Config strings are trimmed, empties dropped, duplicates collapsed;
    /// matching is exact (a padded ID never matches).
    #[test]
    fn owner_list_normalizes_and_matches_exactly() {
        let owners =
            OwnerList::from_ids(&[" 7 ".to_owned(), "7".to_owned(), String::new(), "3".to_owned()]);

        assert_eq!(owners.len(), 2);
        assert!(owners.contains("7"));
        assert!(owners.contains("3"));
        assert!(!owners.contains(" 7 "));
        assert!(!owners.is_empty());
    }

    /// Hot reload: identical content is a no-op, new content swaps in place
    /// (the same handle the gate and the command share see the update).
    #[test]
    fn owner_list_replace_reports_changes() {
        let owners = OwnerList::from_ids(&["3".to_owned()]);

        assert!(!owners.replace(&[" 3 ".to_owned()]));
        assert!(owners.replace(&["4".to_owned()]));
        assert!(owners.contains("4"));
        assert!(!owners.contains("3"));
        assert!(owners.replace(&[]));
        assert!(owners.is_empty());
    }
}
