use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::command_reply;
use crate::kernel::{
    models::{
        ChannelId, EventKind, EventPayload, InvitePayload, MemberPayload, MessagePayload, Origin,
        OutboundMessage, PluginError, RequestContext, StorageError,
    },
    plugin_ports::{
        AccessTier, CommandArgs, CommandDescriptor, CommandHandler, CommandRegistryPort,
        EventBusPort, MiddlewarePluginPort, Next, PluginPort,
    },
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::events::{MessageDeleted, MessageEdited, MessagePurged, UserJoinedGuild, UserLeftGuild};
use super::invites::{INVITES_KEY, InviteDoc, attribute_join};
use super::messages::{
    MessageCache, MessageKey, delete_notice, delete_unknown_notice, edit_notice, purge_notice,
};

/// Guild storage namespace owned by this plugin (the plugin's slug).
const NAMESPACE: &str = "tracker";
/// Guild storage key holding the serialized [`TrackerConfig`].
const CONFIG_KEY: &str = "config";

/// Per-guild tracker settings, stored as a JSON document in the plugin's
/// guild storage namespace. Platform identifiers are stored as strings -
/// snowflakes exceed JSON number precision.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackerConfig {
    /// Channel receiving join/leave audit messages. Absent = tracking is
    /// off for the guild.
    #[serde(default)]
    pub audit_channel_id: Option<String>,
}

/// User activity tracker: observes member lifecycle, invite, and message
/// lifecycle events in the pipeline and turns each into two things:
///
/// - plugin-owned domain events on the bus ([`UserJoinedGuild`] /
///   [`UserLeftGuild`] / [`MessageEdited`] / [`MessageDeleted`] /
///   [`MessagePurged`]), published for EVERY guild event regardless of
///   policy-config readability, so bus-only plugins can react without
///   joining the pipeline themselves, and
/// - an audit message in the guild's configured channel (best effort - an
///   unconfigured or unreadable config or a broken destination skips only
///   the log line; it never blocks the pipeline or the publication).
///
/// Invite tracking rides the same audit channel: creates and deletes are
/// logged as the platform reports them, and joins are attributed to their
/// invite via a uses-count diff against the stored directory observations
/// ([`attribute_join`]). Attribution needs the platform's invite-directory
/// read permission (Discord: Manage Server) - without it the diff degrades
/// to plain join notices, never to an error.
///
/// Message edit/delete/purge tracking rides the same audit channel too. The
/// original text of an edit or deletion comes from an in-memory capture of
/// recent human messages ([`MessageCache`], FIFO-capped) - messages sent
/// before the bot started (or beyond the cap) are noticed with an explicit
/// "not seen" placeholder instead of a fabricated original.
///
/// Passive middleware: always `Continue` - tracking observes, never gates.
/// Policy contrasts with auth: a malformed config here fails *open* (skip
/// the audit line, keep publishing) because observability must not break
/// the event flow.
///
/// The audit channel is assigned per guild via `/assign_tracker` (run in the
/// channel to assign) and cleared via `/unassign_tracker`; both are declared
/// by this plugin in `init()` - meaning lives here, not in the dispatcher.
pub struct UserActivityTrackerPlugin<B: EventBusPort> {
    bus: B,
    registry: Arc<dyn CommandRegistryPort>,
    /// Per-side text cap for quoted message text in edit/deletion notices
    /// (the `[tracker]` `message_notice_limit` config; startup-only).
    notice_limit: usize,
    /// Recent human messages, the source for the "was" side of edit and
    /// deletion notices. Process-lifetime, FIFO-capped by the cache itself.
    messages: MessageCache,
    /// Serializes the per-guild invite-map read-modify-write (create upserts
    /// and join diffs mutate one stored document). Process-lifetime; only
    /// this plugin writes the document, so an instance lock suffices.
    invites_lock: tokio::sync::Mutex<()>,
}

impl<B: EventBusPort> UserActivityTrackerPlugin<B> {
    #[must_use]
    pub fn new(bus: B, registry: Arc<dyn CommandRegistryPort>, notice_limit: usize) -> Self {
        Self {
            bus,
            registry,
            notice_limit,
            messages: MessageCache::new(),
            invites_lock: tokio::sync::Mutex::new(()),
        }
    }
}

impl<B: EventBusPort> PluginPort for UserActivityTrackerPlugin<B> {
    fn name(&self) -> &'static str {
        "tracker"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "assign_tracker".to_owned(),
                description: "Send tracker notices: joins, leaves, invites, message edits and \
                     deletions"
                    .to_owned(),
                arguments: Vec::new(),
                // No platform gate: moderators may lack Discord's Manage
                // Server permission; the auth plugin enforces the tier.
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(AssignTrackerHandler),
        );
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "unassign_tracker".to_owned(),
                description: "Stop member join/leave notices for this guild".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(UnassignTrackerHandler),
        );
        Ok(())
    }
}

#[async_trait]
impl<B: EventBusPort> MiddlewarePluginPort for UserActivityTrackerPlugin<B> {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        match (&event.kind, &event.payload) {
            (EventKind::InviteCreated, EventPayload::Invite(invite)) => {
                self.invite_created(&event.origin, services, invite).await;
            }
            (EventKind::InviteDeleted, EventPayload::Invite(invite)) => {
                self.invite_deleted(&event.origin, services, invite).await;
            }
            (kind, EventPayload::Member(member))
                if matches!(kind, EventKind::MemberJoined | EventKind::MemberLeft) =>
            {
                self.member_lifecycle(
                    &event.origin,
                    services,
                    member,
                    kind == &EventKind::MemberJoined,
                )
                .await;
            }
            // Fresh conversation turns feed the edit/deletion capture; DMs
            // are never tracked (no guild to scope the audit to).
            (EventKind::MessageReceived, EventPayload::Message(message)) => {
                if let (Some(guild_id), Some(message_id)) =
                    (event.origin.guild_id, event.origin.message_id)
                {
                    self.messages.remember(
                        MessageKey::new(guild_id, event.origin.channel_id, message_id),
                        message.author_name.clone(),
                        message.content.clone(),
                    );
                }
            }
            (EventKind::MessageEdited, EventPayload::Message(message)) => {
                self.message_edited(&event.origin, services, message).await;
            }
            (EventKind::MessageDeleted, EventPayload::Empty) => {
                self.message_deleted(&event.origin, services).await;
            }
            (EventKind::MessagePurged, EventPayload::Purge(purge)) => {
                self.message_purged(&event.origin, services, purge.count).await;
            }
            _ => {}
        }
        Next::Continue
    }
}

impl<B: EventBusPort> UserActivityTrackerPlugin<B> {
    /// The member lifecycle flow: the domain event is published for EVERY
    /// guild member event, regardless of policy-config readability; the
    /// audit message is best effort and secondary. Joins carry invite
    /// attribution when the platform's directory yields a signal.
    async fn member_lifecycle(
        &self,
        origin: &Origin,
        services: &KernelServices,
        member: &MemberPayload,
        joined: bool,
    ) {
        // Direct messages: nothing to track.
        let Some(guild_id) = origin.guild_id else {
            return;
        };
        if joined {
            self.bus.publish(UserJoinedGuild {
                guild_id,
                user_id: origin.user_id,
                username: member.username.clone(),
            });
        } else {
            self.bus.publish(UserLeftGuild {
                guild_id,
                user_id: origin.user_id,
                username: member.username.clone(),
            });
        }

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let display =
            member.username.clone().unwrap_or_else(|| format!("user {}", origin.user_id.get()));
        let line = if joined {
            match self.attribute(origin, services).await {
                Some((code, creator)) => {
                    format!("📥 **{display}** joined via `{code}` (created by {creator})")
                }
                None => format!("📥 **{display}** joined the guild"),
            }
        } else {
            format!("📤 **{display}** left the guild")
        };
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// The message-edit flow: identical to the member lifecycle shape - the
    /// domain event is published for EVERY guild edit (with the captured
    /// original when it exists), and the audit notice is best effort. A
    /// no-op edit (content unchanged - embed and pin state also ride the
    /// platform's update event) publishes nothing at all. The capture is
    /// revised either way, so successive edits chain their "was" sides.
    async fn message_edited(
        &self,
        origin: &Origin,
        services: &KernelServices,
        message: &MessagePayload,
    ) {
        // Direct messages: nothing to track.
        let Some(guild_id) = origin.guild_id else {
            return;
        };
        let Some(message_id) = origin.message_id else {
            return;
        };
        let key = MessageKey::new(guild_id, origin.channel_id, message_id);
        let previous =
            self.messages.revise(key, message.author_name.clone(), message.content.clone());
        if previous.as_deref() == Some(message.content.as_str()) {
            return;
        }

        let author_name =
            message.author_name.clone().unwrap_or_else(|| format!("user {}", origin.user_id.get()));
        self.bus.publish(MessageEdited {
            guild_id,
            channel_id: origin.channel_id,
            message_id,
            user_id: origin.user_id,
            author_name: message.author_name.clone(),
            old_content: previous.clone(),
            new_content: message.content.clone(),
        });

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let line = edit_notice(
            self.notice_limit,
            &author_name,
            key,
            previous.as_deref(),
            &message.content,
        );
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// The message-deletion flow: the platform names only WHICH message
    /// vanished, so the quoted text comes from the capture cache when the
    /// message was seen - and the notice degrades to a plain fact when it
    /// was not.
    async fn message_deleted(&self, origin: &Origin, services: &KernelServices) {
        let Some(guild_id) = origin.guild_id else {
            return;
        };
        let Some(message_id) = origin.message_id else {
            return;
        };
        let key = MessageKey::new(guild_id, origin.channel_id, message_id);
        let captured = self.messages.forget(key);
        self.bus.publish(MessageDeleted {
            guild_id,
            channel_id: origin.channel_id,
            message_id,
            user_id: origin.user_id,
            author_name: captured.as_ref().and_then(|message| message.author_name.clone()),
            content: captured.as_ref().map(|message| message.content.clone()),
        });

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let line = match captured {
            Some(message) => {
                // Known message: name from the capture, else the reported
                // author id (`0` = the platform did not identify one).
                let author = message.author_name.clone().unwrap_or_else(|| {
                    if origin.user_id.get() == 0 {
                        "someone".to_owned()
                    } else {
                        format!("user {}", origin.user_id.get())
                    }
                });
                delete_notice(self.notice_limit, &author, key, &message.content)
            }
            None => delete_unknown_notice(key),
        };
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// The purge flow: a mass deletion reports only a count, so neither a
    /// per-message quote nor a cache cleanup is possible - capped FIFO
    /// eviction absorbs the stale entries.
    async fn message_purged(&self, origin: &Origin, services: &KernelServices, count: usize) {
        // Direct messages: nothing to track.
        let Some(guild_id) = origin.guild_id else {
            return;
        };
        self.bus.publish(MessagePurged { guild_id, channel_id: origin.channel_id, count });

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let line = purge_notice(count, origin.channel_id);
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// `INVITE_CREATE`: record the invite (creator, limits, fresh uses
    /// baseline) and log the creation. Best effort like every audit line.
    /// Only a new (or previously deleted) code starts from zero - a
    /// duplicate or replayed event must not regress a live baseline.
    async fn invite_created(
        &self,
        origin: &Origin,
        services: &KernelServices,
        invite: &InvitePayload,
    ) {
        let Some(storage) = services.guild_storage.as_ref() else {
            return;
        };
        let guard = self.invites_lock.lock().await;
        match load_invites(storage).await {
            Ok(mut invites) => {
                match invites.get_mut(&invite.code) {
                    // Live record: duplicate or replayed event - refresh the
                    // metadata, keep the observed uses baseline.
                    Some(doc) if !doc.deleted => {
                        doc.creator_id = invite.creator_id.map(|user| user.get().to_string());
                        doc.creator_name.clone_from(&invite.creator_name);
                        doc.max_uses = invite.max_uses;
                    }
                    // Re-created after an observed deletion: fresh lifecycle.
                    Some(doc) => *doc = InviteDoc::from_create(invite),
                    None => {
                        invites.insert(invite.code.clone(), InviteDoc::from_create(invite));
                    }
                }
                if let Err(err) = save_invites(storage, &invites).await {
                    tracing::warn!(namespace = NAMESPACE, %err, "failed to persist invite record");
                }
            }
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "stored invite map unavailable - creation not recorded");
            }
        }
        drop(guard);

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let creator = invite
            .creator_name
            .clone()
            .or_else(|| invite.creator_id.map(|user| format!("user {}", user.get())))
            .unwrap_or_else(|| "unknown creator".to_owned());
        let limits = match invite.max_uses {
            Some(1) => " (single-use)".to_owned(),
            Some(n) if n > 1 => format!(" (up to {n} uses)"),
            _ => String::new(),
        };
        let line = format!("🎟️ Invite `{}` created by {creator}{limits}", invite.code);
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// `INVITE_DELETE`: mark the stored record deleted (kept for its audit
    /// trail and creator attribution) and log the deletion.
    async fn invite_deleted(
        &self,
        origin: &Origin,
        services: &KernelServices,
        invite: &InvitePayload,
    ) {
        let Some(storage) = services.guild_storage.as_ref() else {
            return;
        };
        let guard = self.invites_lock.lock().await;
        match load_invites(storage).await {
            Ok(mut invites) => {
                match invites.get_mut(&invite.code) {
                    Some(doc) => doc.deleted = true,
                    None => {
                        invites.insert(
                            invite.code.clone(),
                            InviteDoc {
                                code: invite.code.clone(),
                                deleted: true,
                                ..InviteDoc::default()
                            },
                        );
                    }
                }
                if let Err(err) = save_invites(storage, &invites).await {
                    tracing::warn!(namespace = NAMESPACE, %err, "failed to persist invite deletion");
                }
            }
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "stored invite map unavailable - deletion not recorded");
            }
        }
        drop(guard);

        let Some(channel_id) = self.audit_channel(services).await else {
            return;
        };
        let line = format!("🗑️ Invite `{}` deleted", invite.code);
        self.deliver(origin, services, channel_id, &line).await;
    }

    /// Join attribution: fetch the directory (outside the write lock - the
    /// slow REST read needs no stored state), then diff uses against the
    /// stored baseline under the lock and persist the new baseline (and
    /// freshly learned creators). `None` = no signal - directory unreadable
    /// (missing permission), empty, ambiguous, no growth, or the stored
    /// map temporarily unavailable; the audit line degrades to the plain
    /// join notice.
    async fn attribute(
        &self,
        origin: &Origin,
        services: &KernelServices,
    ) -> Option<(String, String)> {
        let storage = services.guild_storage.as_ref()?;
        let fetched = services.invite_directory.invites(origin).await;
        if fetched.is_empty() {
            return None;
        }
        let _guard = self.invites_lock.lock().await;
        let mut invites = match load_invites(storage).await {
            Ok(invites) => invites,
            // A backend hiccup is not an empty map: diffing from nothing
            // would mis-pin the join and overwrite the stored state.
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "stored invite map unavailable - join not attributed");
                return None;
            }
        };
        // A code re-created while unobserved re-appears in the listing
        // still flagged deleted from its previous life: the stale counter
        // would mis-diff, so revive the record from the listing instead.
        for invite in &fetched {
            if let Some(doc) = invites.get_mut(&invite.code)
                && doc.deleted
            {
                let mut fresh = InviteDoc::from_directory(invite);
                fresh.last_seen_uses = invite.uses;
                *doc = fresh;
            }
        }
        let attributed = attribute_join(&invites, &fetched);
        // Vanished codes matching the consumption pattern are accounted as
        // consumed whether or not they won the attribution - otherwise the
        // same doc would re-candidate on every later join.
        for doc in invites.values_mut() {
            let present = fetched.iter().any(|invite| invite.code == doc.code);
            if !present && doc.one_use_from_limit() {
                doc.last_seen_uses = doc.max_uses;
            }
        }
        for invite in &fetched {
            let doc = invites
                .entry(invite.code.clone())
                .or_insert_with(|| InviteDoc::from_directory(invite));
            if let Some(uses) = invite.uses {
                doc.last_seen_uses = Some(uses);
            }
            // A first-time listing fills in creators the gateway event
            // never showed (invites created before this feature shipped).
            if doc.creator_id.is_none() {
                doc.creator_id = invite.creator_id.map(|user| user.get().to_string());
                doc.creator_name.clone_from(&invite.creator_name);
            }
        }
        if let Err(err) = save_invites(storage, &invites).await {
            tracing::warn!(namespace = NAMESPACE, %err, "failed to persist invite baseline");
        }
        attributed.map(|code| {
            let creator = invites
                .get(&code)
                .map(|doc| doc.creator_display())
                .unwrap_or_else(|| "unknown creator".to_owned());
            (code, creator)
        })
    }

    /// The guild's configured audit channel, if any is readable and valid.
    async fn audit_channel(&self, services: &KernelServices) -> Option<ChannelId> {
        let storage = services.guild_storage.as_ref()?;
        let raw = match storage.get(NAMESPACE, CONFIG_KEY).await {
            Ok(Some(raw)) => raw,
            // Unconfigured = audit off for this guild (the event is out already).
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "tracker config unreadable - audit skipped");
                return None;
            }
        };
        let Ok(config) = serde_json::from_value::<TrackerConfig>(raw) else {
            tracing::warn!(namespace = NAMESPACE, "tracker config is malformed - skipping");
            return None;
        };
        let channel_id = match config.audit_channel_id.as_deref().map(str::parse::<u64>) {
            Some(Ok(channel_id)) => channel_id,
            Some(Err(_)) | None => {
                tracing::warn!(namespace = NAMESPACE, "tracker audit channel missing or invalid");
                return None;
            }
        };
        Some(ChannelId(channel_id))
    }

    /// Best effort: deliver to the configured audit channel; a broken
    /// destination is logged, never propagated - the pipeline keeps going.
    async fn deliver(
        &self,
        origin: &Origin,
        services: &KernelServices,
        channel_id: ChannelId,
        line: &str,
    ) {
        let output = services.chat_output_factory.channel_output(origin, channel_id);
        if let Err(err) = output.send(OutboundMessage::text(line)).await {
            tracing::warn!(%err, "failed to deliver member audit message");
        }
    }
}

/// The guild's stored invite map. A malformed document starts fresh - the
/// map is attribution state, and corruption must not wedge future joins.
/// A backend error surfaces instead: the caller skips the round rather
/// than guessing from (and persisting over) an empty map.
async fn load_invites(
    storage: &Arc<dyn GuildStorage>,
) -> Result<HashMap<String, InviteDoc>, StorageError> {
    match storage.get(NAMESPACE, INVITES_KEY).await {
        Ok(Some(raw)) => Ok(serde_json::from_value(raw).unwrap_or_else(|err| {
            tracing::warn!(namespace = NAMESPACE, %err, "stored invite map unreadable - starting fresh");
            HashMap::new()
        })),
        Ok(None) => Ok(HashMap::new()),
        Err(err) => {
            tracing::warn!(namespace = NAMESPACE, %err, "stored invite map unavailable - skipping this round");
            Err(err)
        }
    }
}

async fn save_invites(
    storage: &Arc<dyn GuildStorage>,
    invites: &HashMap<String, InviteDoc>,
) -> Result<(), StorageError> {
    storage
        .set(
            NAMESPACE,
            INVITES_KEY,
            serde_json::to_value(invites).expect("invite map expected to serialize"),
        )
        .await
}

/// `/assign_tracker`: the channel the command is run in becomes the guild's
/// audit channel (last write wins - one channel per guild). Requires a guild
/// origin; the reply lands back in that channel via the event-scoped port.
struct AssignTrackerHandler;

#[async_trait]
impl CommandHandler for AssignTrackerHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({
                    "audit_channel_id": event.origin.channel_id.get().to_string()
                }),
            )
            .await?;

        services
            .chat_output
            .send(command_reply(
                "Tracker assigned: member joins and leaves will be logged in this channel.",
            ))
            .await?;
        Ok(())
    }
}

/// `/unassign_tracker`: clears the guild's tracker config (idempotent).
struct UnassignTrackerHandler;

#[async_trait]
impl CommandHandler for UnassignTrackerHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage.delete(NAMESPACE, CONFIG_KEY).await?;

        services
            .chat_output
            .send(command_reply(
                "Tracker unassigned: member joins and leaves are no longer logged.",
            ))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{
        models::{
            BusEvent, ChannelId as ChannelIdModel, CommandPayload, GuildId, InvitePayload,
            MemberPayload, MessageId, Origin, UserId,
        },
        plugin_ports::EventHandler,
        spi_ports::{
            ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard,
            DirectoryInvite, GUILD_SETTINGS, InviteDirectoryPort, StoragePort,
        },
    };
    use crate::test_support::{FakeInviteDirectory, InMemoryStorage, RecordingChatOutput};
    use crate::{
        infrastructure::plugin_adapters::{InMemoryCommandRegistry, InMemoryEventBus},
        kernel::models::OutboundError,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;

    // --- recording factory that remembers the destination channel ---

    #[derive(Default)]
    struct Sent {
        messages: Mutex<Vec<(u64, String)>>,
    }

    struct ChannelRecorder {
        sent: Arc<Sent>,
        channel: u64,
    }

    #[async_trait]
    impl ChatOutputPort for ChannelRecorder {
        async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
            self.sent.messages.lock().push((self.channel, message.content));
            Ok(())
        }
    }

    struct ChannelRecordingFactory {
        sent: Arc<Sent>,
    }

    impl ChatOutputFactoryPort for ChannelRecordingFactory {
        fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
            self.channel_output(origin, origin.channel_id)
        }

        fn channel_output(
            &self,
            _origin: &Origin,
            channel_id: ChannelIdModel,
        ) -> Arc<dyn ChatOutputPort> {
            Arc::new(ChannelRecorder { sent: Arc::clone(&self.sent), channel: channel_id.get() })
        }

        fn stream_output(&self, _origin: &Origin) -> Arc<dyn ChatStreamPort> {
            Arc::new(NoStream) as Arc<dyn ChatStreamPort>
        }

        fn message_link(
            &self,
            _origin: &Origin,
            _channel_id: ChannelIdModel,
            _message_id: MessageId,
        ) -> Option<String> {
            None
        }

        fn start_typing(&self, _origin: &Origin) -> ChatTypingGuard {
            ChatTypingGuard::dead()
        }
    }

    struct NoStream;

    #[async_trait]
    impl ChatStreamPort for NoStream {
        async fn begin(&self, _message: OutboundMessage) -> Result<MessageId, OutboundError> {
            Err(OutboundError::Send("streaming not supported in this test".to_owned()))
        }

        async fn update(&self, _message: MessageId, _content: String) -> Result<(), OutboundError> {
            Err(OutboundError::Send("streaming not supported in this test".to_owned()))
        }
    }

    // --- bus recorder ---

    struct Recorder<E: BusEvent + Clone + Send + Sync> {
        events: Mutex<Vec<E>>,
    }

    impl<E: BusEvent + Clone + Send + Sync> EventHandler<E> for Recorder<E> {
        fn handle(&self, event: &E) {
            self.events.lock().push(event.clone());
        }
    }

    // --- fixtures ---

    fn origin(guild: Option<u64>) -> Origin {
        Origin {
            guild_id: guild.map(GuildId),
            channel_id: ChannelIdModel(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn member_event(kind: EventKind, guild: Option<u64>, username: &str) -> RequestContext {
        RequestContext {
            kind,
            origin: origin(guild),
            payload: EventPayload::Member(MemberPayload { username: Some(username.to_owned()) }),
        }
    }

    fn configured_storage(channel: &str) -> Arc<InMemoryStorage> {
        let storage = InMemoryStorage::new();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "audit_channel_id": channel }),
        );
        Arc::new(storage)
    }

    struct Fixture {
        sent: Arc<Sent>,
        output: Arc<RecordingChatOutput>,
        joined: Arc<Recorder<UserJoinedGuild>>,
        left: Arc<Recorder<UserLeftGuild>>,
        edited: Arc<Recorder<MessageEdited>>,
        deleted: Arc<Recorder<MessageDeleted>>,
        purged: Arc<Recorder<MessagePurged>>,
        directory: Arc<FakeInviteDirectory>,
    }

    fn fixture(
        storage: Option<Arc<InMemoryStorage>>,
    ) -> (UserActivityTrackerPlugin<InMemoryEventBus>, KernelServices, Fixture) {
        let bus = InMemoryEventBus::new();
        let joined = Arc::new(Recorder::<UserJoinedGuild> { events: Mutex::new(Vec::new()) });
        let left = Arc::new(Recorder::<UserLeftGuild> { events: Mutex::new(Vec::new()) });
        let edited = Arc::new(Recorder::<MessageEdited> { events: Mutex::new(Vec::new()) });
        let deleted = Arc::new(Recorder::<MessageDeleted> { events: Mutex::new(Vec::new()) });
        let purged = Arc::new(Recorder::<MessagePurged> { events: Mutex::new(Vec::new()) });
        bus.subscribe(Arc::clone(&joined) as Arc<dyn EventHandler<UserJoinedGuild>>);
        bus.subscribe(Arc::clone(&left) as Arc<dyn EventHandler<UserLeftGuild>>);
        bus.subscribe(Arc::clone(&edited) as Arc<dyn EventHandler<MessageEdited>>);
        bus.subscribe(Arc::clone(&deleted) as Arc<dyn EventHandler<MessageDeleted>>);
        bus.subscribe(Arc::clone(&purged) as Arc<dyn EventHandler<MessagePurged>>);

        let sent = Arc::new(Sent::default());
        let output = RecordingChatOutput::new();
        let directory = FakeInviteDirectory::new(Vec::new());
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(ChannelRecordingFactory { sent: Arc::clone(&sent) }),
            invite_directory: Arc::clone(&directory) as Arc<dyn InviteDirectoryPort>,
            guild_storage: storage.map(|storage| storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };

        let plugin =
            UserActivityTrackerPlugin::new(bus, Arc::new(InMemoryCommandRegistry::new()), 500);
        (
            plugin,
            services,
            Fixture { sent, output, joined, left, edited, deleted, purged, directory },
        )
    }

    // --- tests ---

    #[tokio::test]
    async fn join_is_logged_to_configured_channel_and_published() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📥 **someone** joined the guild".to_owned())]
        );
        let published = fixture.joined.events.lock().clone();
        let first = published.first().expect("join event expected to be published");
        assert_eq!(first.guild_id, GuildId(1));
        assert_eq!(first.user_id, UserId(3));
        assert_eq!(first.username.as_deref(), Some("someone"));
        assert_eq!(published.len(), 1);
        assert!(fixture.left.events.lock().is_empty());
    }

    #[tokio::test]
    async fn leave_is_logged_and_published() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = member_event(EventKind::MemberLeft, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📤 **someone** left the guild".to_owned())]
        );
        assert_eq!(fixture.left.events.lock().len(), 1);
        assert!(fixture.joined.events.lock().is_empty());
    }

    /// Publish-always: even an unconfigured guild gets the derived event;
    /// only the audit message is skipped.
    #[tokio::test]
    async fn unconfigured_guild_publishes_without_audit() {
        let (plugin, services, fixture) = fixture(Some(Arc::new(InMemoryStorage::new())));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert!(fixture.sent.messages.lock().is_empty());
        assert_eq!(fixture.joined.events.lock().len(), 1, "publish is unconditional");
    }

    /// Unlike auth (fail closed), a malformed config only skips the audit
    /// line - the domain event is still published exactly once.
    #[tokio::test]
    async fn malformed_config_skips_audit_but_publishes() {
        let storage = InMemoryStorage::new();
        storage.seed("test", GuildId(1), NAMESPACE, CONFIG_KEY, serde_json::json!("not an object"));
        let (plugin, services, fixture) = fixture(Some(Arc::new(storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert_eq!(fixture.joined.events.lock().len(), 1);
    }

    /// Storage failure (unreadable config): audit is skipped, the domain
    /// event is still published exactly once (publish-always contract).
    #[tokio::test]
    async fn unreadable_config_skips_audit_but_publishes() {
        let bus = InMemoryEventBus::new();
        let joined = Arc::new(Recorder::<UserJoinedGuild> { events: Mutex::new(Vec::new()) });
        bus.subscribe(Arc::clone(&joined) as Arc<dyn EventHandler<UserJoinedGuild>>);
        let sent = Arc::new(Sent::default());
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(ChannelRecordingFactory { sent: Arc::clone(&sent) }),
            invite_directory: FakeInviteDirectory::new(Vec::new()) as Arc<dyn InviteDirectoryPort>,
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped("test", GuildId(1)),
            ),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let plugin =
            UserActivityTrackerPlugin::new(bus, Arc::new(InMemoryCommandRegistry::new()), 500);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert!(sent.messages.lock().is_empty(), "audit must be skipped");
        assert_eq!(joined.events.lock().len(), 1, "event must still be published");
    }

    /// Config present but no usable audit channel: skip the log line, keep
    /// the publication.
    #[tokio::test]
    async fn missing_audit_channel_skips_audit_but_publishes() {
        let storage = InMemoryStorage::new();
        storage.seed("test", GuildId(1), NAMESPACE, CONFIG_KEY, serde_json::json!({}));
        let (plugin, services, fixture) = fixture(Some(Arc::new(storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert!(fixture.sent.messages.lock().is_empty());
        assert_eq!(fixture.joined.events.lock().len(), 1);
    }

    #[tokio::test]
    async fn direct_messages_are_ignored() {
        let (plugin, services, fixture) = fixture(None);
        let mut event = member_event(EventKind::MemberJoined, None, "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    // --- invite tracking ---

    fn invite_event(kind: EventKind, code: &str) -> RequestContext {
        RequestContext {
            kind,
            origin: origin(Some(1)),
            payload: EventPayload::Invite(InvitePayload {
                code: code.to_owned(),
                channel_id: Some(ChannelIdModel(55)),
                creator_id: Some(UserId(7)),
                creator_name: Some("rikk".to_owned()),
                max_uses: Some(1),
                temporary: Some(false),
            }),
        }
    }

    fn directory(code: &str, uses: u64) -> DirectoryInvite {
        DirectoryInvite {
            code: code.to_owned(),
            channel_id: None,
            creator_id: Some(UserId(7)),
            creator_name: Some("rikk".to_owned()),
            uses: Some(uses),
            max_uses: None,
        }
    }

    async fn stored_invites(storage: &InMemoryStorage) -> serde_json::Value {
        storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, INVITES_KEY)
            .await
            .expect("invite map readable")
            .expect("invite map present")
    }

    /// One stored doc field, with a loud failure naming what is missing.
    fn field<'a>(map: &'a serde_json::Value, code: &str, key: &str) -> &'a serde_json::Value {
        map.get(code)
            .unwrap_or_else(|| panic!("invite doc `{code}` expected: {map}"))
            .get(key)
            .unwrap_or_else(|| panic!("field `{key}` expected on `{code}`: {map}"))
    }

    /// `INVITE_CREATE` logs the creator and limits, and records the invite
    /// with a fresh uses baseline for the attribution diff.
    #[tokio::test]
    async fn invite_create_is_logged_and_recorded() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut event = invite_event(EventKind::InviteCreated, "abc123");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "🎟️ Invite `abc123` created by rikk (single-use)".to_owned())]
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "creator_name"), "rikk");
        assert_eq!(field(&map, "abc123", "last_seen_uses"), 0);
        assert_eq!(field(&map, "abc123", "deleted"), false);
    }

    /// `INVITE_DELETE` logs the code and marks the stored record deleted -
    /// the record stays for its audit trail and creator attribution.
    #[tokio::test]
    async fn invite_delete_is_logged_and_marked() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        let mut deleted = invite_event(EventKind::InviteDeleted, "abc123");

        assert!(matches!(plugin.pre(&mut deleted, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![
                (777, "🎟️ Invite `abc123` created by rikk (single-use)".to_owned()),
                (777, "🗑️ Invite `abc123` deleted".to_owned()),
            ]
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "deleted"), true);
    }

    /// A delete for an invite the bot never saw is still marked (and kept).
    #[tokio::test]
    async fn unknown_invite_delete_still_records_the_code() {
        let storage = configured_storage("777");
        let (plugin, services, _fixture) = fixture(Some(Arc::clone(&storage)));
        let mut deleted = invite_event(EventKind::InviteDeleted, "ghost");

        assert!(matches!(plugin.pre(&mut deleted, &services).await, Next::Continue));

        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "ghost", "deleted"), true);
        assert_eq!(field(&map, "ghost", "creator_id"), &serde_json::Value::Null);
    }

    /// Attribution: one code grew by exactly one since the create baseline
    /// - the join line names it and its creator, and the baseline moves.
    #[tokio::test]
    async fn join_attributes_through_the_uses_diff() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        fixture.directory.set(vec![directory("abc123", 1)]);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone** joined via `abc123` (created by rikk)".to_owned())
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "last_seen_uses"), 1);
    }

    /// Two codes grew in the same window (offline catch-up, concurrent
    /// joins): the diff refuses to guess - plain line, but the baseline
    /// resyncs so later joins attribute again.
    #[tokio::test]
    async fn ambiguous_growth_keeps_the_plain_join_and_resyncs() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        for code in ["a", "b"] {
            let mut created = invite_event(EventKind::InviteCreated, code);
            plugin.pre(&mut created, &services).await;
        }
        fixture.directory.set(vec![directory("a", 1), directory("b", 1)]);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone** joined the guild".to_owned())
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "a", "last_seen_uses"), 1);
        assert_eq!(field(&map, "b", "last_seen_uses"), 1);
    }

    /// No directory signal (no permission, empty listing): the plain join
    /// notice - attribution degrades, never fails.
    #[tokio::test]
    async fn empty_directory_degrades_to_the_plain_join() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📥 **someone** joined the guild".to_owned())]
        );
    }

    /// A directory listing also fills in creators for invites the bot
    /// never saw created (shipped before the feature, created offline).
    #[tokio::test]
    async fn directory_listing_enriches_unknown_creators() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        fixture.directory.set(vec![directory("pre-historic", 5)]);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        // Unknown code with uses: attributed - the code exists and was used.
        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone** joined via `pre-historic` (created by rikk)".to_owned())
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "pre-historic", "last_seen_uses"), 5);
    }

    /// A consumed single-use invite vanishes from the directory (the
    /// platform deletes it on the last use) - the stored doc still pins
    /// the join, and the doc is accounted so it never re-candidates.
    #[tokio::test]
    async fn consumed_single_use_invite_attributes_from_the_stored_doc() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        // The join consumed the code: gone from the listing now.
        fixture.directory.set(vec![directory("other", 0)]);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone** joined via `abc123` (created by rikk)".to_owned())
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "last_seen_uses"), 1, "accounted as consumed");

        // The accounted doc never re-candidates on later joins.
        let mut again = member_event(EventKind::MemberJoined, Some(1), "someone-else");
        assert!(matches!(plugin.pre(&mut again, &services).await, Next::Continue));
        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone-else** joined the guild".to_owned())
        );
    }

    /// A code re-created while the bot was offline re-appears in the
    /// listing still flagged deleted from its previous life: it must be
    /// revived from the listing instead of being pinned by its stale
    /// counter (which sits at exactly stale + 1 here).
    #[tokio::test]
    async fn recycled_code_with_stale_baseline_never_false_pins() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        // Old life of the code: observed up to 4 uses, then deleted.
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        fixture.directory.set(vec![directory("abc123", 4)]);
        let mut warmup = member_event(EventKind::MemberJoined, Some(1), "warmup");
        plugin.pre(&mut warmup, &services).await;
        let mut deleted = invite_event(EventKind::InviteDeleted, "abc123");
        plugin.pre(&mut deleted, &services).await;

        // Offline re-create; the fresh counter already sits at 5 - the
        // value the stale arithmetic would call "growth by one". The join
        // actually came through something directory-invisible (vanity).
        fixture.directory.set(vec![directory("abc123", 5)]);
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().last().expect("join line expected").clone(),
            (777, "📥 **someone** joined the guild".to_owned()),
            "a recycled code must not be pinned by its stale counter"
        );
        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "deleted"), false, "listing presence revives the doc");
        assert_eq!(field(&map, "abc123", "last_seen_uses"), 5);
    }

    /// Re-creating a code after an observed deletion starts a fresh
    /// lifecycle: the baseline resets instead of keeping the old counter.
    #[tokio::test]
    async fn recreated_invite_after_delete_gets_a_fresh_baseline() {
        let storage = configured_storage("777");
        let (plugin, services, _fixture) = fixture(Some(Arc::clone(&storage)));
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        let mut deleted = invite_event(EventKind::InviteDeleted, "abc123");
        plugin.pre(&mut deleted, &services).await;
        let mut recreated = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut recreated, &services).await;

        let map = stored_invites(&storage).await;
        assert_eq!(field(&map, "abc123", "deleted"), false);
        assert_eq!(field(&map, "abc123", "last_seen_uses"), 0);
    }

    /// A duplicate `INVITE_CREATE` must not regress a live record's baseline.
    #[tokio::test]
    async fn duplicate_invite_create_keeps_the_uses_baseline() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut created = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut created, &services).await;
        // A join lifts the baseline to 1 before the duplicate arrives.
        fixture.directory.set(vec![directory("abc123", 1)]);
        let mut join = member_event(EventKind::MemberJoined, Some(1), "someone");
        plugin.pre(&mut join, &services).await;
        let mut duplicate = invite_event(EventKind::InviteCreated, "abc123");
        plugin.pre(&mut duplicate, &services).await;

        let map = stored_invites(&storage).await;
        assert_eq!(
            field(&map, "abc123", "last_seen_uses"),
            1,
            "duplicate create must not regress the baseline"
        );
        assert_eq!(field(&map, "abc123", "deleted"), false);
    }

    /// A backend failure must not be mistaken for an empty map: the old
    /// behavior fabricated an attribution from nothing and overwrote the
    /// stored map with the result. Attribution skips instead.
    #[tokio::test]
    async fn storage_failure_skips_attribution() {
        let sent = Arc::new(Sent::default());
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(ChannelRecordingFactory { sent: Arc::clone(&sent) }),
            invite_directory: FakeInviteDirectory::new(vec![directory("only-one", 4)])
                as Arc<dyn InviteDirectoryPort>,
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped("test", GuildId(1)),
            ),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let plugin = UserActivityTrackerPlugin::new(
            InMemoryEventBus::new(),
            Arc::new(InMemoryCommandRegistry::new()),
            500,
        );

        let attributed = plugin.attribute(&origin(Some(1)), &services).await;

        assert!(attributed.is_none(), "a backend failure must not fabricate an attribution");
    }

    /// A malformed stored map starts fresh (corruption must not wedge
    /// future joins) - but a backend error surfaces as `Err` instead of
    /// pretending the map is empty.
    #[tokio::test]
    async fn load_invites_splits_corruption_from_backend_failure() {
        let corrupt = InMemoryStorage::new();
        corrupt.seed("test", GuildId(1), NAMESPACE, INVITES_KEY, serde_json::json!("garbage"));
        let corrupt_view = corrupt.guild_scoped("test", GuildId(1));
        let fresh =
            load_invites(&corrupt_view).await.expect("corruption falls back to a fresh map");
        assert!(fresh.is_empty());

        let failing = crate::test_support::FailingStorage.guild_scoped("test", GuildId(1));
        assert!(
            load_invites(&failing).await.is_err(),
            "a backend error must surface, not reset the attribution state"
        );
    }

    #[tokio::test]
    async fn non_member_events_are_ignored() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(Some(1)),
            payload: EventPayload::Command(CommandPayload {
                name: "ping".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        };

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn missing_username_falls_back_to_user_id() {
        let storage = InMemoryStorage::new();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "audit_channel_id": "777" }),
        );
        let (plugin, services, fixture) = fixture(Some(Arc::new(storage)));
        let mut event = RequestContext {
            kind: EventKind::MemberJoined,
            origin: origin(Some(1)),
            payload: EventPayload::Member(MemberPayload { username: None }),
        };

        let _ = plugin.pre(&mut event, &services).await;

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📥 **user 3** joined the guild".to_owned())]
        );
    }

    // --- message edit/deletion tracking ---

    /// A human conversation turn: fixed ids (guild 1, channel 2, author 3)
    /// with a per-test message id, mirroring the adapter's output shape.
    fn message_event(
        kind: EventKind,
        guild: Option<u64>,
        message_id: u64,
        content: &str,
    ) -> RequestContext {
        RequestContext {
            kind,
            origin: Origin {
                guild_id: guild.map(GuildId),
                channel_id: ChannelIdModel(2),
                user_id: UserId(3),
                message_id: Some(MessageId(message_id)),
                reply_token: None,
            },
            payload: EventPayload::Message(MessagePayload {
                content: content.to_owned(),
                attachments: Vec::new(),
                author_name: Some("Riketta".to_owned()),
                guild_name: None,
                author_roles: Vec::new(),
                author_permissions: 0,
                reply_to: None,
                mentions_bot: false,
            }),
        }
    }

    #[tokio::test]
    async fn edit_quotes_the_captured_original_and_publishes() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));

        let mut received = message_event(EventKind::MessageReceived, Some(1), 4, "one");
        let _ = plugin.pre(&mut received, &services).await;
        let mut edited = message_event(EventKind::MessageEdited, Some(1), 4, "two");
        let _ = plugin.pre(&mut edited, &services).await;

        let (channel, line) =
            fixture.sent.messages.lock().first().expect("edit notice expected").clone();
        assert_eq!(channel, 777);
        assert!(line.contains("✏️ **Riketta** edited a message in <#2>"), "line: {line}");
        assert!(line.contains("https://discord.com/channels/1/2/4"), "line: {line}");
        assert!(line.contains("was:"), "line: {line}");
        assert!(line.contains("one"), "line: {line}");
        assert!(line.contains("now:"), "line: {line}");
        assert!(line.contains("two"), "line: {line}");

        let published = fixture.edited.events.lock().clone();
        let first = published.first().expect("edit event expected to be published");
        assert_eq!(first.old_content.as_deref(), Some("one"));
        assert_eq!(first.new_content, "two");
        assert_eq!(first.author_name.as_deref(), Some("Riketta"));
        assert_eq!(published.len(), 1);
    }

    #[tokio::test]
    async fn edit_without_capture_says_so_instead_of_inventing_an_original() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut edited = message_event(EventKind::MessageEdited, Some(1), 4, "two");

        let _ = plugin.pre(&mut edited, &services).await;

        let (_, line) = fixture.sent.messages.lock().first().expect("edit notice expected").clone();
        assert!(line.contains("(original not seen - sent before capture or bot restart)"));
        let first = fixture.edited.events.lock().first().expect("publish expected").clone();
        assert_eq!(first.old_content, None);
    }

    #[tokio::test]
    async fn unchanged_edit_is_neither_published_nor_posted() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));

        let mut received = message_event(EventKind::MessageReceived, Some(1), 4, "same");
        let _ = plugin.pre(&mut received, &services).await;
        let mut edited = message_event(EventKind::MessageEdited, Some(1), 4, "same");
        let _ = plugin.pre(&mut edited, &services).await;

        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.edited.events.lock().is_empty());
    }

    #[tokio::test]
    async fn successive_edits_chain_their_was_sides() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));

        let mut received = message_event(EventKind::MessageReceived, Some(1), 4, "one");
        let _ = plugin.pre(&mut received, &services).await;
        let mut first = message_event(EventKind::MessageEdited, Some(1), 4, "two");
        let _ = plugin.pre(&mut first, &services).await;
        let mut second = message_event(EventKind::MessageEdited, Some(1), 4, "three");
        let _ = plugin.pre(&mut second, &services).await;

        let lines = fixture.sent.messages.lock().clone();
        assert_eq!(lines.len(), 2, "both edits noticed: {lines:?}");
        let first = lines.first().expect("first edit notice expected");
        let second = lines.get(1).expect("second edit notice expected");
        assert!(first.1.contains("one"), "first edit quotes the capture: {}", first.1);
        assert!(second.1.contains("two"), "second edit quotes the last state: {}", second.1);
        assert!(second.1.contains("three"), "second edit shows the new state: {}", second.1);
        assert_eq!(fixture.edited.events.lock().len(), 2);
    }

    #[tokio::test]
    async fn deletion_quotes_the_capture_then_degrades_to_the_plain_fact() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));

        let mut received = message_event(EventKind::MessageReceived, Some(1), 4, "gone soon");
        let _ = plugin.pre(&mut received, &services).await;
        // The adapter maps MESSAGE_DELETE onto an Empty payload - only the
        // origin names the message.
        let mut deleted = RequestContext {
            kind: EventKind::MessageDeleted,
            origin: origin(Some(1)),
            payload: EventPayload::Empty,
        };
        let _ = plugin.pre(&mut deleted, &services).await;

        let lines = fixture.sent.messages.lock().clone();
        assert_eq!(lines.len(), 1, "one deletion noticed: {lines:?}");
        let notice = lines.first().expect("deletion notice expected");
        assert!(notice.1.contains("🗑 **Riketta** deleted a message in <#2>"), "{}", notice.1);
        assert!(notice.1.contains("gone soon"), "{}", notice.1);
        let published = fixture.deleted.events.lock().clone();
        let first = published.first().expect("delete event expected");
        assert_eq!(first.content.as_deref(), Some("gone soon"));

        // The cache entry was consumed: a second deletion of the same id is
        // the generic fact (e.g. a re-posted-and-deleted id is unlikely, but
        // the contract is degradation, not fabrication).
        let mut again = RequestContext {
            kind: EventKind::MessageDeleted,
            origin: origin(Some(1)),
            payload: EventPayload::Empty,
        };
        let _ = plugin.pre(&mut again, &services).await;
        let lines = fixture.sent.messages.lock().clone();
        assert_eq!(lines.len(), 2);
        let second = lines.get(1).expect("second deletion notice expected");
        assert!(second.1.contains("a message was deleted in <#2>"), "{}", second.1);
    }

    #[tokio::test]
    async fn purge_reports_one_count_per_event() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = RequestContext {
            kind: EventKind::MessagePurged,
            origin: origin(Some(1)),
            payload: EventPayload::Purge(crate::kernel::models::MessagePurgePayload { count: 12 }),
        };

        let _ = plugin.pre(&mut event, &services).await;

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "🗑️ 12 messages were purged in <#2>".to_owned())]
        );
        assert_eq!(fixture.purged.events.lock().first().map(|e| e.count), Some(12));
    }

    #[tokio::test]
    async fn direct_message_lifecycle_is_never_tracked() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));

        let mut received = message_event(EventKind::MessageReceived, None, 4, "dm");
        let _ = plugin.pre(&mut received, &services).await;
        let mut edited = message_event(EventKind::MessageEdited, None, 4, "dm edited");
        let _ = plugin.pre(&mut edited, &services).await;
        let mut deleted = RequestContext {
            kind: EventKind::MessageDeleted,
            origin: origin(None),
            payload: EventPayload::Empty,
        };
        let _ = plugin.pre(&mut deleted, &services).await;

        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.edited.events.lock().is_empty());
        assert!(fixture.deleted.events.lock().is_empty());
    }

    /// Publish-always, audit-best-effort - an unconfigured guild still gets
    /// the domain events, just no Discord-side notice.
    #[tokio::test]
    async fn unconfigured_guild_message_edits_publish_without_audit() {
        let (plugin, services, fixture) = fixture(Some(Arc::new(InMemoryStorage::new())));
        let mut edited = message_event(EventKind::MessageEdited, Some(1), 4, "two");

        let _ = plugin.pre(&mut edited, &services).await;

        assert!(fixture.sent.messages.lock().is_empty());
        assert_eq!(fixture.edited.events.lock().len(), 1);
    }

    /// The configured clip bounds the quoted text per side (plugin-global
    /// `[tracker] message_notice_limit`).
    #[tokio::test]
    async fn notice_limit_clips_quoted_text() {
        let bus = InMemoryEventBus::new();
        let edited = Arc::new(Recorder::<MessageEdited> { events: Mutex::new(Vec::new()) });
        bus.subscribe(Arc::clone(&edited) as Arc<dyn EventHandler<MessageEdited>>);
        let sent = Arc::new(Sent::default());
        let services = KernelServices {
            chat_output: RecordingChatOutput::new() as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(ChannelRecordingFactory { sent: Arc::clone(&sent) }),
            invite_directory: FakeInviteDirectory::new(Vec::new()) as Arc<dyn InviteDirectoryPort>,
            guild_storage: Some(configured_storage("777").guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let plugin =
            UserActivityTrackerPlugin::new(bus, Arc::new(InMemoryCommandRegistry::new()), 5);

        let mut received = message_event(EventKind::MessageReceived, Some(1), 4, "short");
        let _ = plugin.pre(&mut received, &services).await;
        let long = "x".repeat(50);
        let mut edited_event = message_event(EventKind::MessageEdited, Some(1), 4, &long);
        let _ = plugin.pre(&mut edited_event, &services).await;

        let (_, line) = sent.messages.lock().first().expect("edit notice expected").clone();
        assert!(line.contains("short"), "was side clipped at 5: {line}");
        assert!(line.contains(&format!("{}…", "x".repeat(5))), "now side clipped at 5: {line}");
        assert!(!line.contains(&long), "full text must not appear: {line}");
    }

    /// `/assign_tracker` run in a channel stores that channel and confirms.
    #[tokio::test]
    async fn assign_handler_stores_current_channel_and_replies() {
        let storage = Arc::new(InMemoryStorage::new());
        let (_plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");
        event.origin.channel_id = ChannelIdModel(777);

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        let stored = storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, Some(serde_json::json!({ "audit_channel_id": "777" })));
        assert_eq!(fixture.output.messages().len(), 1);
    }

    /// Last write wins: assigning another channel replaces the previous one.
    #[tokio::test]
    async fn assign_handler_replaces_previous_channel() {
        let storage = configured_storage("777");
        let (_plugin, services, _fixture) = fixture(Some(Arc::clone(&storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");
        event.origin.channel_id = ChannelIdModel(888);

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        let stored = storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, Some(serde_json::json!({ "audit_channel_id": "888" })));
    }

    /// `/unassign_tracker` clears the config; tracking goes silent afterwards.
    #[tokio::test]
    async fn unassign_handler_clears_config() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let event = member_event(EventKind::MemberJoined, Some(1), "someone");

        UnassignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("unassign expected to succeed");

        let stored = storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, None);
        assert_eq!(fixture.output.messages().len(), 1);

        // Tracking is off now: a join skips the audit message but still
        // publishes the domain event (publish-always contract).
        let mut join = member_event(EventKind::MemberJoined, Some(1), "someone");
        let _ = plugin.pre(&mut join, &services).await;
        assert!(fixture.sent.messages.lock().is_empty());
        assert_eq!(fixture.joined.events.lock().len(), 1);
    }

    #[tokio::test]
    async fn commands_in_direct_messages_reply_guild_only() {
        let (_plugin, services, fixture) = fixture(None);
        let event = member_event(EventKind::MemberJoined, None, "someone");

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        assert_eq!(fixture.output.messages(), ["This command only works inside a server."]);
    }

    /// The plugin declares its own commands during `init()`; the dispatcher
    /// and the Discord registrar consume the descriptors meaning-free.
    #[test]
    fn init_registers_tracker_commands() {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let plugin = UserActivityTrackerPlugin::new(InMemoryEventBus::new(), registry.clone(), 500);

        plugin.init().expect("init expected to succeed");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 2);
        assert!(descriptors.iter().all(|d| d.plugin_id == "tracker"));
        crate::test_support::assert_descriptions_fit_discord(&descriptors);
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"assign_tracker"));
        assert!(names.contains(&"unassign_tracker"));
        assert!(
            descriptors.iter().all(
                |d| d.required_tier == Some(crate::kernel::plugin_ports::AccessTier::Moderator)
            )
        );
        assert!(descriptors.iter().all(|d| d.required_permission.is_none()));
    }

    /// Guards the reserved namespace constant; the tracker must never
    /// collide with guild settings.
    #[test]
    fn tracker_namespace_is_not_the_reserved_guild_namespace() {
        assert_ne!(NAMESPACE, GUILD_SETTINGS);
    }
}
