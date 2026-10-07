use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serenity::all::{
    ChannelId as SerenityChannelId, CreateAttachment, CreateMessage, EditMessage, Emoji, EmojiId,
    GuildId as SerenityGuildId, Http, MessageId as SerenityMessageId, MessageReference,
    MessageReferenceKind, ReactionType,
};
use serenity::async_trait;
use tokio_util::sync::CancellationToken;

use crate::kernel::{
    models::{ChannelId, Embed, MessageId, Origin, OutboundError, OutboundMessage},
    spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard, GuildEmojiPort,
        ReactableEmoji, ReactionPort, UndeliverableGuildEmojiPort, UndeliverableReactionPort,
    },
};

/// Which Discord entity a mention tag points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MentionKind {
    User,
    Role,
    Channel,
}

/// Parses a Discord mention tag at the start of `text`: user `<@id>` /
/// `<@!id>`, role `<@&id>`, channel `<#id>`. Returns the kind, the id and
/// the tag's length in bytes. Custom emoji (`<:name:id>`) and any other
/// shape are not mention tags.
pub(crate) fn parse_mention_tag(text: &str) -> Option<(MentionKind, u64, usize)> {
    let bytes = text.as_bytes();
    let mut index = 1; // past '<'
    let kind = match bytes.get(index) {
        Some(b'@') => {
            index += 1;
            if bytes.get(index) == Some(&b'&') {
                index += 1;
                MentionKind::Role
            } else {
                if bytes.get(index) == Some(&b'!') {
                    index += 1;
                }
                MentionKind::User
            }
        }
        Some(b'#') => {
            index += 1;
            MentionKind::Channel
        }
        _ => return None,
    };

    let mut id: u64 = 0;
    let mut digits = 0;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                id = id.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
                digits += 1;
                index += 1;
            }
            b'>' if digits > 0 => return Some((kind, id, index + 1)),
            _ => return None,
        }
    }
    None
}

/// The outbound inverse: `[Name]<@id>` -> `<@id>`, so tags the model
/// assembles from normalized history arrive as clean Discord mentions
/// (rendered output shows the resolved name anyway). The name segment must
/// be bracket-adjacent to the tag and free of nested brackets; literal user
/// text matching that same shape (e.g. `[at this]<@123>`) is inherently
/// ambiguous and is rewritten too - only non-adjacent or nested brackets
/// pass through byte-identical.
fn denormalize_mention_tags(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some((before, tail)) = rest.split_once(']') {
        if let Some((_, _, length)) = parse_mention_tag(tail) {
            if let Some((prefix, name)) = before.rsplit_once('[')
                && !name.is_empty()
                && !name.contains('[')
            {
                out.push_str(prefix);
                out.push_str(tail.get(..length).unwrap_or(""));
                rest = tail.get(length..).unwrap_or("");
                continue;
            }
            out.push_str(before);
            out.push(']');
            rest = tail;
        } else {
            out.push_str(before);
            out.push(']');
            rest = tail;
        }
    }
    out.push_str(rest);
    out
}

/// `ChatOutputFactoryPort` bound to Discord: produces `ChatOutputPort`s
/// scoped to a specific event origin. When the event carries an interaction
/// reply token, sends go through the interaction followup endpoint instead
/// of a plain channel message - plugins cannot tell the difference.
pub struct SerenityChatOutputFactory<T = Http> {
    http: Arc<T>,
    emoji_cache: Arc<GuildEmojiCache>,
}

// `new` is deliberately unbounded - the private `ChatApi` bound lives on the
// impls that need it, so the public constructor stays lint-clean.
impl<T> SerenityChatOutputFactory<T> {
    /// Takes the shared REST client: serenity rate limiting is per `Http`,
    /// so every driven Discord caller must share one instance.
    pub fn new(http: Arc<T>) -> Self {
        Self { http, emoji_cache: Arc::new(GuildEmojiCache::new()) }
    }
}

impl<T: ChatApi> ChatOutputFactoryPort for SerenityChatOutputFactory<T> {
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
        if let Some(token) = &origin.reply_token {
            return Arc::new(InteractionFollowupOutput::new(Arc::clone(&self.http), token.clone()));
        }

        // Member lifecycle origins carry no channel (`ChannelId(0)`); a
        // Discord call for channel 0 is a guaranteed 404, so fail fast.
        if origin.channel_id.get() == 0 {
            return Arc::new(UndeliverableChatOutput);
        }

        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
        })
    }

    /// Configured-channel logging. Enforcement is degenerate-case-only (the
    /// origin must be guild-bound and the channel non-zero): verifying that
    /// a channel id actually belongs to the origin guild requires the
    /// gateway cache and is deferred.
    fn channel_output(&self, origin: &Origin, channel_id: ChannelId) -> Arc<dyn ChatOutputPort> {
        // Configured-channel logging is never an interaction reply, so a
        // reply token on the origin is deliberately ignored here.
        if origin.guild_id.is_none() || channel_id.get() == 0 {
            return Arc::new(UndeliverableChatOutput);
        }
        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(channel_id.get()),
        })
    }

    /// Streaming is plain-channel progressive rendering: never an interaction
    /// reply (followup edits are a different endpoint and no plugin needs
    /// them yet) and never a channel-less origin.
    fn stream_output(&self, origin: &Origin) -> Arc<dyn ChatStreamPort> {
        if origin.reply_token.is_some() || origin.guild_id.is_none() || origin.channel_id.get() == 0
        {
            return Arc::new(UndeliverableChatStream);
        }
        Arc::new(SerenityChatStream {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
        })
    }

    /// Discord message links are constructible from ids alone - no API call.
    fn message_link(
        &self,
        origin: &Origin,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> Option<String> {
        let guild_id = origin.guild_id?;
        Some(format!(
            "https://discord.com/channels/{}/{}/{}",
            guild_id.get(),
            channel_id.get(),
            message_id.get()
        ))
    }

    /// Typing refresh rides serenity's own `Typing` handle (re-broadcasts on
    /// its internal cadence, stops when dropped); the kernel guard's drop
    /// cancels the bridge task holding it. Channel-less origins get a dead
    /// guard - a typing call for channel 0 is a guaranteed 404.
    fn start_typing(&self, origin: &Origin) -> ChatTypingGuard {
        // Member lifecycle origins carry no channel - nothing to type into.
        if origin.channel_id.get() == 0 {
            return ChatTypingGuard::dead();
        }
        let channel = SerenityChannelId::new(origin.channel_id.get());
        let token = CancellationToken::new();
        self.http.run_typing(channel, token.clone());
        ChatTypingGuard::new(token)
    }

    /// Reactions are guild-scoped (custom emoji resolution needs the
    /// guild's emoji list), so DM and channel-less origins yield the
    /// undeliverable port.
    fn react(&self, origin: &Origin) -> Arc<dyn ReactionPort> {
        let Some(guild_id) = origin.guild_id else {
            return Arc::new(UndeliverableReactionPort);
        };
        if origin.channel_id.get() == 0 {
            return Arc::new(UndeliverableReactionPort);
        }
        Arc::new(SerenityReaction {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
            guild_id: SerenityGuildId::new(guild_id.get()),
            emoji_cache: Arc::clone(&self.emoji_cache),
        })
    }

    /// Guild-bound emoji listing: one cached `get_emojis` per guild per TTL
    /// window serves both prompt injection and bare-name resolution.
    fn reactable_emojis(&self, origin: &Origin) -> Arc<dyn GuildEmojiPort> {
        let Some(guild_id) = origin.guild_id else {
            return Arc::new(UndeliverableGuildEmojiPort);
        };
        Arc::new(SerenityGuildEmojis {
            http: Arc::clone(&self.http),
            emoji_cache: Arc::clone(&self.emoji_cache),
            guild_id: SerenityGuildId::new(guild_id.get()),
        })
    }
}

struct SerenityChatOutput<T: ChatApi = Http> {
    http: Arc<T>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl<T: ChatApi> ChatOutputPort for SerenityChatOutput<T> {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        // Discord rejects messages with neither content nor embeds (400).
        if message.is_empty() {
            tracing::warn!(channel = %self.channel_id, "dropping empty outbound message");
            return Ok(());
        }
        // Plain channel sends are always public - the ephemeral hint has no
        // meaning here and is ignored. Normalized mention tags
        // (`[Name]<@id>`) invert back to bare mentions on the way out.
        let mut create = CreateMessage::new();
        if !message.content.is_empty() {
            create = create.content(denormalize_mention_tags(&message.content));
        }
        if !message.embeds.is_empty() {
            create = create.embeds(message.embeds.iter().map(discord_embed).collect());
        }
        if let Some(reply_to) = message.reply_to {
            create = create.reference_message(reply_reference(self.channel_id, reply_to));
        }

        self.http
            .send_message(self.channel_id, Vec::new(), &create)
            .await
            .map_err(OutboundError::Send)?;
        Ok(())
    }
}

/// Terminal output for origins with no deliverable Discord destination
/// (channel-less member lifecycle events, configured channels outside any
/// guild): every send logs a warning and reports failure instead of
/// building a Discord call that is guaranteed to 404.
struct UndeliverableChatOutput;

#[async_trait]
impl ChatOutputPort for UndeliverableChatOutput {
    async fn send(&self, _message: OutboundMessage) -> Result<(), OutboundError> {
        tracing::warn!("dropping outbound message: origin has no deliverable Discord channel");
        Err(OutboundError::Send("no deliverable Discord channel for this origin".to_owned()))
    }
}

/// Reaction port bound to one origin: applies emoji reactions to messages
/// in the origin channel. Custom `:name:` tokens resolve against the
/// guild's cached emoji list (shared with prompt injection - see
/// [`GuildEmojiCache`]).
struct SerenityReaction<T: ChatApi = Http> {
    http: Arc<T>,
    channel_id: SerenityChannelId,
    guild_id: SerenityGuildId,
    emoji_cache: Arc<GuildEmojiCache>,
}

/// Shared, TTL-cached guild emoji catalog: prompt injection and the
/// bare-name reaction resolver consume one `get_emojis` per guild per TTL
/// window instead of hammering the REST endpoint. A failed listing is
/// cached empty under a short TTL - self-heals without hammering a down
/// endpoint.
struct GuildEmojiCache {
    entries: Mutex<HashMap<u64, CachedEmojis>>,
}

/// Successful listings stay fresh for the full TTL.
const EMOJI_TTL: Duration = Duration::from_secs(600);
/// Failed listings are retried after the short one.
const EMOJI_FAILURE_TTL: Duration = Duration::from_secs(30);

struct CachedEmojis {
    fetched_at: Instant,
    emojis: Arc<Vec<Emoji>>,
    ok: bool,
}

impl GuildEmojiCache {
    fn new() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
    }

    async fn emojis<T: ChatApi>(&self, http: &T, guild_id: SerenityGuildId) -> Arc<Vec<Emoji>> {
        {
            let entries = self.entries.lock();
            if let Some(cached) = entries.get(&guild_id.get()) {
                let ttl = if cached.ok { EMOJI_TTL } else { EMOJI_FAILURE_TTL };
                if cached.fetched_at.elapsed() < ttl {
                    return Arc::clone(&cached.emojis);
                }
            }
        }
        let (emojis, ok) = match http.get_emojis(guild_id).await {
            Ok(emojis) => (Arc::new(emojis), true),
            Err(err) => {
                tracing::warn!(guild = guild_id.get(), error = %err, "guild emoji listing failed");
                (Arc::new(Vec::new()), false)
            }
        };
        self.entries.lock().insert(
            guild_id.get(),
            CachedEmojis { fetched_at: Instant::now(), emojis: Arc::clone(&emojis), ok },
        );
        emojis
    }
}

/// Guild-bound emoji listing for one origin's guild.
struct SerenityGuildEmojis<T: ChatApi = Http> {
    http: Arc<T>,
    emoji_cache: Arc<GuildEmojiCache>,
    guild_id: SerenityGuildId,
}

#[async_trait]
impl<T: ChatApi> GuildEmojiPort for SerenityGuildEmojis<T> {
    async fn list(&self) -> Vec<ReactableEmoji> {
        self.emoji_cache
            .emojis(self.http.as_ref(), self.guild_id)
            .await
            .iter()
            .filter_map(|emoji| {
                let name = emoji.name.as_str();
                if name.is_empty() {
                    return None;
                }
                let brackets = if emoji.animated { ("<a:", ">") } else { ("<:", ">") };
                Some(ReactableEmoji {
                    name: name.to_owned(),
                    token: format!("{}{}:{}{}", brackets.0, name, emoji.id, brackets.1),
                })
            })
            .collect()
    }
}

#[async_trait]
impl<T: ChatApi> ReactionPort for SerenityReaction<T> {
    async fn add_reaction(&self, message_id: MessageId, emoji: &str) -> Result<(), OutboundError> {
        let reaction = self.resolve(emoji).await?;
        self.http
            .create_reaction(self.channel_id, SerenityMessageId::new(message_id.get()), &reaction)
            .await
            .map_err(OutboundError::Reaction)
    }
}

impl<T: ChatApi> SerenityReaction<T> {
    /// Maps a raw protocol token onto a Discord reaction type. Fully
    /// qualified forms (`<:name:id>`, `<a:name:id>`, `:name:id`, and the
    /// trailing-colon blend `:name:id:`) build the custom reaction
    /// directly; bare `:name:` needs the guild emoji list; anything else is
    /// a Unicode emoji (tokens the plugin already validated - an
    /// unparseable token here fails per-token, by design).
    async fn resolve(&self, emoji: &str) -> Result<ReactionType, OutboundError> {
        let parsed = parse_reaction_token(emoji);
        match parsed {
            ParsedReaction::Custom { animated, name, id } => {
                Ok(custom_reaction(animated, &name, &id))
            }
            ParsedReaction::Name(name) => {
                let emojis = self.emoji_cache.emojis(self.http.as_ref(), self.guild_id).await;
                emojis
                    .iter()
                    .find(|known| known.name.as_str() == name.as_str())
                    .map(|known| ReactionType::Custom {
                        animated: known.animated,
                        id: known.id,
                        name: Some(known.name.clone()),
                    })
                    .ok_or_else(|| {
                        OutboundError::Reaction(format!(
                            "custom emoji `:{name}:` not found in this server"
                        ))
                    })
            }
            ParsedReaction::Unicode => Ok(ReactionType::Unicode(emoji.to_owned())),
            ParsedReaction::Invalid => {
                Err(OutboundError::Reaction(format!("unrecognized emoji token `{emoji}`")))
            }
        }
    }
}

/// The adapter-side parse of one reaction token.
#[derive(Debug, PartialEq, Eq)]
enum ParsedReaction {
    /// Fully qualified custom form - usable without resolution.
    Custom { animated: bool, name: String, id: String },
    /// Bare `:name:` - resolve against the guild's emojis.
    Name(String),
    /// Unicode emoji (any token with a non-ASCII character).
    Unicode,
    /// Not an emoji token at all.
    Invalid,
}

fn parse_reaction_token(token: &str) -> ParsedReaction {
    fn valid_name(name: &str) -> bool {
        !name.is_empty() && name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }
    fn numeric(id: &str) -> bool {
        !id.is_empty() && id.chars().all(|ch| ch.is_ascii_digit())
    }

    if let Some(body) = token.strip_prefix('<').and_then(|rest| rest.strip_suffix('>')) {
        let (animated, rest) = match body.strip_prefix("a:") {
            Some(rest) => (true, rest),
            None => match body.strip_prefix(':') {
                Some(rest) => (false, rest),
                None => return ParsedReaction::Invalid,
            },
        };
        return match rest.split_once(':') {
            Some((name, id)) if valid_name(name) && numeric(id) => {
                ParsedReaction::Custom { animated, name: name.to_owned(), id: id.to_owned() }
            }
            _ => ParsedReaction::Invalid,
        };
    }
    if let Some(body) = token.strip_prefix(':') {
        // `:name:id:` - the trailing-colon blend of the bare and qualified
        // forms - degrades to the qualified custom reaction; bare `:name:`
        // keeps its plain path.
        let body = match body.strip_suffix(':') {
            Some(stripped) if stripped.contains(':') => stripped,
            _ => body,
        };
        if let Some(name) = body.strip_suffix(':') {
            return if valid_name(name) {
                ParsedReaction::Name(name.to_owned())
            } else {
                ParsedReaction::Invalid
            };
        }
        // `:name:id` - the id makes it fully qualified.
        return match body.rsplit_once(':') {
            Some((name, id)) if valid_name(name) && numeric(id) => {
                ParsedReaction::Custom { animated: false, name: name.to_owned(), id: id.to_owned() }
            }
            _ => ParsedReaction::Invalid,
        };
    }
    if token.chars().any(|ch| !ch.is_ascii()) {
        ParsedReaction::Unicode
    } else {
        ParsedReaction::Invalid
    }
}

fn custom_reaction(animated: bool, name: &str, id: &str) -> ReactionType {
    // Both parsers validate digits before handing the id here - a parse
    // failure would be an invariant breach, not a runtime condition.
    let id = EmojiId::new(id.parse::<u64>().expect("reaction token id is digit-validated"));
    ReactionType::Custom { animated, id, name: Some(name.to_owned()) }
}

/// Progressive-rendering output: creates the message on `begin`, then edits
/// it in place as content arrives. Content-only - embeds are ignored in a
/// message that exists to be overwritten.
struct SerenityChatStream<T: ChatApi = Http> {
    http: Arc<T>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl<T: ChatApi> ChatStreamPort for SerenityChatStream<T> {
    async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError> {
        if message.content.is_empty() {
            tracing::warn!(channel = %self.channel_id, "dropping empty streaming placeholder");
            return Err(OutboundError::Send("empty streaming placeholder".to_owned()));
        }
        let mut create = CreateMessage::new().content(denormalize_mention_tags(&message.content));
        if let Some(reply_to) = message.reply_to {
            create = create.reference_message(reply_reference(self.channel_id, reply_to));
        }
        let created = self
            .http
            .send_message(self.channel_id, Vec::new(), &create)
            .await
            .map_err(OutboundError::Send)?;
        Ok(MessageId(created.get()))
    }

    async fn update(&self, message: MessageId, content: String) -> Result<(), OutboundError> {
        self.http
            .edit_message(
                self.channel_id,
                SerenityMessageId::new(message.get()),
                &EditMessage::new().content(denormalize_mention_tags(&content)),
                Vec::new(),
            )
            .await
            .map_err(OutboundError::Send)?;
        Ok(())
    }
}

/// Terminal streaming output for origins with no streamable Discord channel
/// (transactional reply tokens, channel-less events, direct messages).
struct UndeliverableChatStream;

#[async_trait]
impl ChatStreamPort for UndeliverableChatStream {
    async fn begin(&self, _message: OutboundMessage) -> Result<MessageId, OutboundError> {
        tracing::warn!("dropping streaming begin: origin has no streamable Discord channel");
        Err(OutboundError::Send("no streamable Discord channel for this origin".to_owned()))
    }

    async fn update(&self, _message: MessageId, _content: String) -> Result<(), OutboundError> {
        tracing::warn!("dropping streaming update: origin has no streamable Discord channel");
        Err(OutboundError::Send("no streamable Discord channel for this origin".to_owned()))
    }
}

/// The two followup operations the interaction reply path needs, split
/// from serenity's `Http` so the slot dance is testable against a scripted
/// transport (same pattern as the LCU adapter's `Transport`).
#[async_trait]
trait InteractionApi: Send + Sync {
    /// Posts one followup message; returns its id.
    async fn create_followup(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<SerenityMessageId, String>;

    /// Deletes one followup message.
    async fn delete_followup(&self, token: &str, id: SerenityMessageId) -> Result<(), String>;
}

#[async_trait]
impl InteractionApi for Http {
    async fn create_followup(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<SerenityMessageId, String> {
        self.create_followup_message(token, body, Vec::new())
            .await
            .map(|message| message.id)
            .map_err(|err| err.to_string())
    }

    async fn delete_followup(&self, token: &str, id: SerenityMessageId) -> Result<(), String> {
        self.delete_followup_message(token, id).await.map_err(|err| err.to_string())
    }
}

/// The transport seam of the plain delivery half: the REST calls the channel
/// send, streaming, and reaction paths make, plus the typing bridge. Split
/// from serenity's `Http` so those paths are testable against a scripted
/// transport (same pattern as [`InteractionApi`], whose two followup calls it
/// subsumes as a supertrait: the factory's reply-token branch hands `T` to
/// [`InteractionFollowupOutput`], so one bound covers both seams). Parameter
/// shapes mirror the `Http` calls they replace; return values are narrowed to
/// what the callers consume (the message id, not the whole message).
#[async_trait]
trait ChatApi: InteractionApi + 'static {
    /// Mirrors `Http::send_message`; yields the created message's id.
    async fn send_message(
        &self,
        channel_id: SerenityChannelId,
        files: Vec<CreateAttachment>,
        builder: &CreateMessage,
    ) -> Result<SerenityMessageId, String>;

    /// Mirrors `Http::edit_message`; yields the edited message's id.
    async fn edit_message(
        &self,
        channel_id: SerenityChannelId,
        message_id: SerenityMessageId,
        builder: &EditMessage,
        new_attachments: Vec<CreateAttachment>,
    ) -> Result<SerenityMessageId, String>;

    /// Mirrors `Http::create_reaction`.
    async fn create_reaction(
        &self,
        channel_id: SerenityChannelId,
        message_id: SerenityMessageId,
        reaction_type: &ReactionType,
    ) -> Result<(), String>;

    /// Mirrors `Http::get_emojis`.
    async fn get_emojis(&self, guild_id: SerenityGuildId) -> Result<Vec<Emoji>, String>;

    /// Keeps serenity's typing indicator refreshing on the channel until
    /// `cancel` fires: spawns the bridge task that holds serenity's `Typing`
    /// handle and drops it on cancellation (which stops the refresh loop).
    /// Sync by contract - the factory returns the guard immediately, and
    /// refresh failures never surface to the guard's holder. The receiver is
    /// `&Arc<Self>` because serenity's typing handle wants the owned client.
    fn run_typing(self: &Arc<Self>, channel_id: SerenityChannelId, cancel: CancellationToken);
}

#[async_trait]
impl ChatApi for Http {
    async fn send_message(
        &self,
        channel_id: SerenityChannelId,
        files: Vec<CreateAttachment>,
        builder: &CreateMessage,
    ) -> Result<SerenityMessageId, String> {
        Http::send_message(self, channel_id, files, builder)
            .await
            .map(|message| message.id)
            .map_err(|err| err.to_string())
    }

    async fn edit_message(
        &self,
        channel_id: SerenityChannelId,
        message_id: SerenityMessageId,
        builder: &EditMessage,
        new_attachments: Vec<CreateAttachment>,
    ) -> Result<SerenityMessageId, String> {
        Http::edit_message(self, channel_id, message_id, builder, new_attachments)
            .await
            .map(|message| message.id)
            .map_err(|err| err.to_string())
    }

    async fn create_reaction(
        &self,
        channel_id: SerenityChannelId,
        message_id: SerenityMessageId,
        reaction_type: &ReactionType,
    ) -> Result<(), String> {
        Http::create_reaction(self, channel_id, message_id, reaction_type)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_emojis(&self, guild_id: SerenityGuildId) -> Result<Vec<Emoji>, String> {
        Http::get_emojis(self, guild_id).await.map_err(|err| err.to_string())
    }

    fn run_typing(self: &Arc<Self>, channel_id: SerenityChannelId, cancel: CancellationToken) {
        let http = Arc::clone(self);
        tokio::spawn(async move {
            let typing = http.start_typing(channel_id);
            cancel.cancelled().await;
            drop(typing);
        });
    }
}

struct InteractionFollowupOutput<T: InteractionApi> {
    api: Arc<T>,
    token: String,
    /// Whether the first-followup slot has been consumed. The first followup
    /// of an interaction edits the deferred ephemeral original response and
    /// inherits its ephemeral state regardless of the flags it carries, so a
    /// PUBLIC first reply must be routed around that slot (see [`send`]).
    /// Fresh per interaction: one output instance is built per event origin.
    first_followup_used: AtomicBool,
}

#[async_trait]
impl<T: InteractionApi> ChatOutputPort for InteractionFollowupOutput<T> {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        if message.is_empty() {
            tracing::warn!("dropping empty interaction followup (no content, no embeds)");
            return Ok(());
        }
        // Ephemeral replies are invoker-only wherever they land: the first
        // followup edits the deferred ephemeral original (inheriting its
        // state), later ones carry the flag explicitly. Single post.
        if message.ephemeral {
            let body = followup_body(&message);
            let posted = self.post(&body).await;
            if posted.is_ok() {
                // The ephemeral post consumed the platform-side slot (it
                // edited the original); a later public reply must skip the
                // placeholder dance, not repeat it.
                self.first_followup_used.store(true, Ordering::Relaxed);
            }
            return posted;
        }
        // Public reply. The first followup would edit the deferred ephemeral
        // original and inherit its ephemeral state - public content cannot
        // ride it. Consume the slot on a small invoker-only placeholder,
        // then post the real content as a second followup (a new message
        // whose flags we control), and clean the placeholder up. Handlers
        // send sequentially, so "first" is well defined. If the placeholder
        // itself fails, the content post degrades to the inheritance
        // behavior (invoker-only) - the same shape the pre-dance adapter had.
        if !self.first_followup_used.swap(true, Ordering::Relaxed) {
            match self.api.create_followup(&self.token, &placeholder_body()).await {
                Ok(placeholder) => {
                    if let Err(err) = self.api.delete_followup(&self.token, placeholder).await {
                        tracing::warn!(
                            %err,
                            "failed to delete the public-reply placeholder (invoker-only, harmless)"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "public-reply placeholder failed; content may land invoker-only"
                    );
                }
            }
        }
        // Normalized mention tags invert back to bare mentions on the way
        // out, like every other Discord send - see `followup_body`.
        let body = followup_body(&message);
        self.post(&body).await
    }

    /// Consumes the deferred slot silently: a placeholder goes in (editing
    /// the deferred original) and is deleted immediately - the invoker's
    /// "thinking" state clears without any content arriving. Nothing to
    /// do once a real reply already consumed the slot.
    async fn dismiss(&self) {
        if !self.first_followup_used.swap(true, Ordering::Relaxed)
            && let Ok(placeholder) =
                self.api.create_followup(&self.token, &placeholder_body()).await
        {
            if let Err(err) = self.api.delete_followup(&self.token, placeholder).await {
                tracing::warn!(%err, "failed to clean up the dismissed interaction slot");
            }
        }
    }
}

impl<T: InteractionApi> InteractionFollowupOutput<T> {
    fn new(api: Arc<T>, token: String) -> Self {
        Self { api, token, first_followup_used: AtomicBool::new(false) }
    }

    /// Posts one followup body for this interaction.
    async fn post(&self, body: &serde_json::Value) -> Result<(), OutboundError> {
        self.api
            .create_followup(&self.token, body)
            .await
            .map(|_: SerenityMessageId| ())
            .map_err(OutboundError::Send)
    }
}

/// Body for the sacrificial first followup that precedes a public send (see
/// [`InteractionFollowupOutput::send`]): invoker-only, generic wording -
/// the adapter is command-agnostic. Deleted again once the real content is
/// out; if deletion fails it lingers as a harmless status line.
fn placeholder_body() -> serde_json::Value {
    serde_json::json!({ "content": "Processing…", "flags": 64 })
}

/// Builds the interaction-followup JSON body. Pure so the flag half of the
/// reply-visibility contract is verifiable side by side with the defer (see
/// `deferred_ephemeral_response`): a followup carries EPHEMERAL when the
/// message is ephemeral; public bodies omit flags - they are only ever
/// posted as second-or-later followups (or past a consumed first slot), so
/// they land as fresh public messages. `reply_to` is ignored: a
/// transactional reply is already anchored to its interaction. Normalized
/// mention tags invert back to bare mentions, like every other Discord send.
fn followup_body(message: &OutboundMessage) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    if !message.content.is_empty() {
        body.insert(
            "content".to_owned(),
            serde_json::json!(denormalize_mention_tags(&message.content)),
        );
    }
    if !message.embeds.is_empty() {
        let embeds: Vec<serde_json::Value> = message
            .embeds
            .iter()
            .map(|embed| {
                serde_json::json!({
                    "title": denormalize_mention_tags(&embed.title),
                    "description": denormalize_mention_tags(&embed.description),
                })
            })
            .collect();
        body.insert("embeds".to_owned(), serde_json::json!(embeds));
    }
    if message.ephemeral {
        // EPHEMERAL flag (1 << 6): visible to the invoking user only.
        body.insert("flags".to_owned(), serde_json::json!(64));
    }
    serde_json::Value::Object(body)
}

fn discord_embed(embed: &Embed) -> serenity::all::CreateEmbed {
    serenity::all::CreateEmbed::new()
        .title(denormalize_mention_tags(&embed.title))
        .description(denormalize_mention_tags(&embed.description))
}

/// Reply reference for a message in the destination channel. `fail_if_not_exists`
/// stays off: a deleted target must degrade to a normal send, never fail the
/// delivery (the LLM guaranteed-answer contract rides these sends).
fn reply_reference(channel_id: SerenityChannelId, reply_to: MessageId) -> MessageReference {
    MessageReference::new(MessageReferenceKind::Default, channel_id)
        .message_id(serenity::all::MessageId::new(reply_to.get()))
        .fail_if_not_exists(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{GuildId, UserId};

    #[test]
    fn reaction_tokens_parse_into_their_reaction_kinds() {
        // Fully qualified custom forms build directly, animated flag kept.
        assert_eq!(
            parse_reaction_token("<:face:9>"),
            ParsedReaction::Custom { animated: false, name: "face".to_owned(), id: "9".to_owned() }
        );
        assert_eq!(
            parse_reaction_token("<a:spin:456>"),
            ParsedReaction::Custom {
                animated: true,
                name: "spin".to_owned(),
                id: "456".to_owned()
            }
        );
        assert_eq!(
            parse_reaction_token(":name_id:789"),
            ParsedReaction::Custom {
                animated: false,
                name: "name_id".to_owned(),
                id: "789".to_owned()
            }
        );
        // Bare name needs guild resolution.
        assert_eq!(parse_reaction_token(":dorkiS:"), ParsedReaction::Name("dorkiS".to_owned()));
        assert_eq!(parse_reaction_token(":x:"), ParsedReaction::Name("x".to_owned()));
        // The trailing-colon blend models slip into between the bare and
        // qualified forms degrades to the qualified custom reaction.
        assert_eq!(
            parse_reaction_token(":dorkiS:872106192514711582:"),
            ParsedReaction::Custom {
                animated: false,
                name: "dorkiS".to_owned(),
                id: "872106192514711582".to_owned()
            }
        );
        // Anything with a non-ASCII character is a Unicode emoji.
        assert_eq!(parse_reaction_token("🤓"), ParsedReaction::Unicode);
        assert_eq!(parse_reaction_token("👍🏽"), ParsedReaction::Unicode);
        // Malformed tokens fail per-token (the plugin's per-item rule); a
        // name-shaped token like `:x:` stays resolvable - the API has the
        // final say on whether the emoji exists.
        for bad in [
            "word",
            "::",
            ":bad name:",
            "<nope>",
            "<:bad>",
            "42",
            ":id:abc",
            ":id:abc:",
            ":id:1:2:",
        ] {
            assert_eq!(parse_reaction_token(bad), ParsedReaction::Invalid, "token `{bad}`");
        }
    }

    #[test]
    fn custom_reaction_builds_the_serenity_type() {
        let reaction = custom_reaction(true, "spin", "456");
        match reaction {
            ReactionType::Custom { animated, id, name } => {
                assert!(animated);
                assert_eq!(id, EmojiId::new(456));
                assert_eq!(name.as_deref(), Some("spin"));
            }
            other => panic!("expected a custom reaction, got {other:?}"),
        }
    }

    #[test]
    fn mention_tag_parsing_recognizes_the_wire_shapes() {
        assert_eq!(parse_mention_tag("<@123> x"), Some((MentionKind::User, 123, 6)));
        assert_eq!(parse_mention_tag("<@!123>"), Some((MentionKind::User, 123, 7)));
        assert_eq!(parse_mention_tag("<@&7>"), Some((MentionKind::Role, 7, 5)));
        assert_eq!(parse_mention_tag("<#5>"), Some((MentionKind::Channel, 5, 4)));
        assert_eq!(parse_mention_tag("<:face:9>"), None);
        assert_eq!(parse_mention_tag("<@>"), None);
        assert_eq!(parse_mention_tag("<@1x>"), None);
    }

    /// The outbound inverse strips the name prefix only when it is
    /// bracket-adjacent to a mention tag and free of nested brackets;
    /// everything else passes through byte-identical.
    #[test]
    fn denormalize_inverts_the_normalized_shape() {
        assert_eq!(denormalize_mention_tags("hey [Alice]<@123>!"), "hey <@123>!");
        assert_eq!(denormalize_mention_tags("[Mods]<@&7> [general]<#5>"), "<@&7> <#5>");
        // Literal brackets and non-tag shapes pass through.
        assert_eq!(
            denormalize_mention_tags("[b] <@123> [unclosed<@123>"),
            "[b] <@123> [unclosed<@123>"
        );
        assert_eq!(denormalize_mention_tags("[a[b]]<@123>"), "[a[b]]<@123>");
        assert_eq!(denormalize_mention_tags("plain text"), "plain text");
        // Markdown adjacent to a tag is never mangled: the link's bracket
        // pair stays untouched, and names containing parentheses still
        // strip (only nested `[` rejects a candidate).
        assert_eq!(denormalize_mention_tags("[text](url)<@123>"), "[text](url)<@123>");
        assert_eq!(denormalize_mention_tags("[Name (x)]<@123>"), "<@123>");
        assert_eq!(denormalize_mention_tags("[l](u) [Name]<@1>"), "[l](u) <@1>");
    }

    /// Owner => everything; anyone else => @everyone + member roles union.
    /// This is what makes the auth plugin's Discord-admin clamp hold on the
    /// message path, where payloads usually carry no permission bits.
    #[test]
    fn followup_body_carries_the_ephemeral_flag() {
        let message = OutboundMessage::text("not authorized".to_owned()).ephemeral();
        let body = followup_body(&message);
        assert_eq!(body.get("flags").and_then(serde_json::Value::as_u64), Some(64));

        let public = OutboundMessage::text("hello".to_owned());
        assert!(followup_body(&public).get("flags").is_none());
    }

    /// The public-reply placeholder consumes the first-followup slot: it
    /// must be non-empty (Discord rejects empty posts) and explicitly
    /// ephemeral, and it stays generic - the adapter is command-agnostic.
    #[test]
    fn placeholder_is_ephemeral_and_non_empty() {
        let body = placeholder_body();
        let content = body.get("content").and_then(serde_json::Value::as_str);
        assert!(content.is_some_and(|text| !text.is_empty()), "placeholder needs content");
        assert_eq!(body.get("flags").and_then(serde_json::Value::as_u64), Some(64));
    }

    /// Followup content and embeds denormalize mention tags back to bare
    /// tags, and a `reply_to` reference is ignored - the interaction itself
    /// is the reply anchor.
    #[test]
    fn followup_body_denormalizes_and_ignores_reply_reference() {
        let message = OutboundMessage::embed(Embed {
            title: "Hey [Alice]<@123>".to_owned(),
            description: "ping [Bob]<@77>".to_owned(),
        })
        .replying_to(MessageId(42));

        let body = followup_body(&message);
        let embeds =
            body.get("embeds").and_then(serde_json::Value::as_array).expect("embeds expected");
        let first = embeds.first().expect("one embed expected");
        assert_eq!(first.get("title").and_then(serde_json::Value::as_str), Some("Hey <@123>"));
        assert_eq!(
            first.get("description").and_then(serde_json::Value::as_str),
            Some("ping <@77>")
        );
        assert!(body.get("message_reference").is_none());
    }

    /// Scripted [`InteractionApi`] recording the call sequence. The
    /// placeholder is distinguished from real content by its ephemeral
    /// flags (only it and ephemeral replies carry them).
    struct ScriptedApi {
        calls: std::sync::Mutex<Vec<String>>,
        placeholder_fails: bool,
        delete_fails: bool,
    }

    impl ScriptedApi {
        fn new(placeholder_fails: bool, delete_fails: bool) -> Self {
            Self { calls: std::sync::Mutex::new(Vec::new()), placeholder_fails, delete_fails }
        }

        fn log(&self) -> Vec<String> {
            self.calls.lock().expect("call log expected").clone()
        }
    }

    #[async_trait]
    impl InteractionApi for ScriptedApi {
        async fn create_followup(
            &self,
            _token: &str,
            body: &serde_json::Value,
        ) -> Result<SerenityMessageId, String> {
            let label = if body.get("flags").is_some() { "create:flags" } else { "create:public" };
            self.calls.lock().expect("call log expected").push(label.to_owned());
            if self.placeholder_fails && body.get("flags").is_some() {
                return Err("placeholder rejected".to_owned());
            }
            Ok(SerenityMessageId::new(1))
        }

        async fn delete_followup(
            &self,
            _token: &str,
            _id: SerenityMessageId,
        ) -> Result<(), String> {
            self.calls.lock().expect("call log expected").push("delete:placeholder".to_owned());
            if self.delete_fails {
                return Err("delete rejected".to_owned());
            }
            Ok(())
        }
    }

    fn scripted_output(api: Arc<ScriptedApi>) -> InteractionFollowupOutput<ScriptedApi> {
        InteractionFollowupOutput::new(api, "token".to_owned())
    }

    /// The first public reply must route around the deferred ephemeral
    /// original: placeholder consumes the slot, real content posts as a
    /// second followup, the placeholder is cleaned up.
    #[tokio::test]
    async fn first_public_reply_consumes_the_slot_with_a_placeholder() {
        let api = Arc::new(ScriptedApi::new(false, false));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hello")).await.expect("first reply delivers");
        output.send(OutboundMessage::text("again")).await.expect("second reply delivers");

        assert_eq!(
            api.log(),
            ["create:flags", "delete:placeholder", "create:public", "create:public"]
        );
    }

    /// Ephemeral replies inherit the deferred ephemeral state - they post
    /// directly, no slot dance.
    #[tokio::test]
    async fn ephemeral_replies_skip_the_slot() {
        let api = Arc::new(ScriptedApi::new(false, false));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hi").ephemeral()).await.expect("ephemeral delivers");

        assert_eq!(api.log(), ["create:flags"]);
    }

    /// An ephemeral first reply consumes the slot at the platform (it edits
    /// the original): a later PUBLIC reply must post directly - repeating
    /// the placeholder dance would burn two calls and flash the invoker.
    #[tokio::test]
    async fn ephemeral_first_reply_consumes_the_slot_for_later_public_ones() {
        let api = Arc::new(ScriptedApi::new(false, false));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hi").ephemeral()).await.expect("ephemeral delivers");
        output.send(OutboundMessage::text("now public")).await.expect("public delivers");

        assert_eq!(api.log(), ["create:flags", "create:public"]);
    }

    /// Dismissing resolves the pending slot silently (placeholder in,
    /// delete out) and consumes it: a later public reply posts directly.
    #[tokio::test]
    async fn dismiss_resolves_the_slot_silently_and_consumes_it() {
        let api = Arc::new(ScriptedApi::new(false, false));
        let output = scripted_output(Arc::clone(&api));

        output.dismiss().await;
        output.send(OutboundMessage::text("late reply")).await.expect("reply delivers");

        assert_eq!(api.log(), ["create:flags", "delete:placeholder", "create:public"]);
    }

    /// Dismissing after a real reply is a no-op: the slot is gone, and the
    /// call must not mint a stray placeholder followup.
    #[tokio::test]
    async fn dismiss_after_a_real_reply_is_a_no_op() {
        let api = Arc::new(ScriptedApi::new(false, false));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hi").ephemeral()).await.expect("ephemeral delivers");
        output.dismiss().await;

        assert_eq!(api.log(), ["create:flags"]);
    }

    /// If the placeholder itself fails, the content still posts (degraded
    /// to invoker-only) and the slot stays consumed - the second public
    /// reply must not retry the dance.
    #[tokio::test]
    async fn failed_placeholder_still_posts_content_and_keeps_the_slot() {
        let api = Arc::new(ScriptedApi::new(true, false));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hello")).await.expect("degraded delivery expected");
        output.send(OutboundMessage::text("again")).await.expect("second reply delivers");

        assert_eq!(api.log(), ["create:flags", "create:public", "create:public"]);
    }

    /// A failing placeholder cleanup never blocks the content post.
    #[tokio::test]
    async fn failed_placeholder_delete_does_not_block_the_content() {
        let api = Arc::new(ScriptedApi::new(false, true));
        let output = scripted_output(Arc::clone(&api));

        output.send(OutboundMessage::text("hello")).await.expect("delivery expected");

        assert_eq!(api.log(), ["create:flags", "delete:placeholder", "create:public"]);
    }

    // --- Delivery half: plain sends, streaming, reactions, factory routing ---
    //
    // `RecordingApi` extends the `ScriptedApi` seam pattern to the rest of
    // the transport (`ChatApi`), recording each call richly enough to assert
    // content, flags, and reference fields.

    /// One recorded transport call, modeled on the real call shape so tests
    /// can assert ids and serialized bodies.
    #[derive(Debug, Clone, PartialEq)]
    enum RecordedCall {
        /// Plain channel send; `body` is the builder as the wire sees it.
        SendMessage { channel: SerenityChannelId, body: serde_json::Value },
        /// In-place edit; `body` is the builder as the wire sees it.
        EditMessage {
            channel: SerenityChannelId,
            message: SerenityMessageId,
            body: serde_json::Value,
        },
        /// Reaction applied to a message.
        CreateReaction {
            channel: SerenityChannelId,
            message: SerenityMessageId,
            reaction: ReactionType,
        },
        /// Guild emoji list fetch (bare `:name:` resolution).
        GetEmojis { guild: SerenityGuildId },
        /// Interaction followup create.
        CreateFollowup { token: String, body: serde_json::Value },
        /// Interaction followup delete.
        DeleteFollowup { token: String, message: SerenityMessageId },
        /// Typing bridge started for a channel.
        RunTyping { channel: SerenityChannelId },
    }

    /// Scripted [`ChatApi`] recording every transport call. `emojis` scripts
    /// the guild emoji list that bare `:name:` tokens resolve against; the
    /// `failures` flags script transport errors per method, so the port's
    /// negative paths (the `map_err` conversions) can be exercised.
    struct RecordingApi {
        calls: std::sync::Mutex<Vec<RecordedCall>>,
        emojis: Vec<Emoji>,
        failures: ApiFailures,
    }

    /// Which transports fail; everything unset succeeds.
    #[derive(Default)]
    struct ApiFailures {
        send: bool,
        edit: bool,
        reaction: bool,
        emojis: bool,
    }

    impl RecordingApi {
        fn new(emojis: Vec<Emoji>) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                emojis,
                failures: ApiFailures::default(),
            }
        }

        fn record(&self, call: RecordedCall) {
            self.calls.lock().expect("call log expected").push(call);
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().expect("call log expected").clone()
        }

        /// Builds one guild emoji fixture from its wire shape (`Emoji` is
        /// non-exhaustive, so it cannot be constructed field-by-field here).
        fn emoji(name: &str, id: u64) -> Emoji {
            serde_json::from_value(serde_json::json!({
                "id": id.to_string(),
                "name": name,
                "user": null,
            }))
            .expect("emoji fixture deserializes")
        }
    }

    #[async_trait]
    impl InteractionApi for RecordingApi {
        async fn create_followup(
            &self,
            token: &str,
            body: &serde_json::Value,
        ) -> Result<SerenityMessageId, String> {
            self.record(RecordedCall::CreateFollowup {
                token: token.to_owned(),
                body: body.clone(),
            });
            Ok(SerenityMessageId::new(500))
        }

        async fn delete_followup(
            &self,
            token: &str,
            message: SerenityMessageId,
        ) -> Result<(), String> {
            self.record(RecordedCall::DeleteFollowup { token: token.to_owned(), message });
            Ok(())
        }
    }

    #[async_trait]
    impl ChatApi for RecordingApi {
        async fn send_message(
            &self,
            channel: SerenityChannelId,
            files: Vec<CreateAttachment>,
            builder: &CreateMessage,
        ) -> Result<SerenityMessageId, String> {
            assert!(files.is_empty(), "the delivery half never attaches files");
            self.record(RecordedCall::SendMessage {
                channel,
                body: serde_json::to_value(builder).expect("builder serializes"),
            });
            if self.failures.send {
                return Err("send transport failed".to_owned());
            }
            Ok(SerenityMessageId::new(600))
        }

        async fn edit_message(
            &self,
            channel: SerenityChannelId,
            message: SerenityMessageId,
            builder: &EditMessage,
            new_attachments: Vec<CreateAttachment>,
        ) -> Result<SerenityMessageId, String> {
            assert!(new_attachments.is_empty(), "the delivery half never attaches files");
            self.record(RecordedCall::EditMessage {
                channel,
                message,
                body: serde_json::to_value(builder).expect("builder serializes"),
            });
            if self.failures.edit {
                return Err("edit transport failed".to_owned());
            }
            Ok(message)
        }

        async fn create_reaction(
            &self,
            channel: SerenityChannelId,
            message: SerenityMessageId,
            reaction: &ReactionType,
        ) -> Result<(), String> {
            self.record(RecordedCall::CreateReaction {
                channel,
                message,
                reaction: reaction.clone(),
            });
            if self.failures.reaction {
                return Err("reaction transport failed".to_owned());
            }
            Ok(())
        }

        async fn get_emojis(&self, guild: SerenityGuildId) -> Result<Vec<Emoji>, String> {
            self.record(RecordedCall::GetEmojis { guild });
            if self.failures.emojis {
                return Err("emoji transport failed".to_owned());
            }
            Ok(self.emojis.clone())
        }

        fn run_typing(self: &Arc<Self>, channel: SerenityChannelId, _cancel: CancellationToken) {
            self.record(RecordedCall::RunTyping { channel });
        }
    }

    fn delivery_api(emojis: Vec<Emoji>) -> Arc<RecordingApi> {
        Arc::new(RecordingApi::new(emojis))
    }

    /// An API whose scripted transport failures exercise the port's
    /// negative paths (the `map_err` conversions the ChatApi refactor
    /// rewrote); only the named methods fail.
    fn failing_api(failures: ApiFailures) -> Arc<RecordingApi> {
        Arc::new(RecordingApi {
            calls: std::sync::Mutex::new(Vec::new()),
            emojis: Vec::new(),
            failures,
        })
    }

    fn delivery_output(api: &Arc<RecordingApi>) -> SerenityChatOutput<RecordingApi> {
        SerenityChatOutput { http: Arc::clone(api), channel_id: SerenityChannelId::new(5) }
    }

    fn delivery_stream(api: &Arc<RecordingApi>) -> SerenityChatStream<RecordingApi> {
        SerenityChatStream { http: Arc::clone(api), channel_id: SerenityChannelId::new(5) }
    }

    fn delivery_reaction(api: &Arc<RecordingApi>) -> SerenityReaction<RecordingApi> {
        SerenityReaction {
            http: Arc::clone(api),
            channel_id: SerenityChannelId::new(5),
            guild_id: SerenityGuildId::new(7),
            emoji_cache: Arc::new(GuildEmojiCache::new()),
        }
    }

    fn delivery_factory(api: &Arc<RecordingApi>) -> SerenityChatOutputFactory<RecordingApi> {
        SerenityChatOutputFactory::new(Arc::clone(api))
    }

    fn origin(guild_id: Option<u64>, channel_id: u64, reply_token: Option<&str>) -> Origin {
        Origin {
            guild_id: guild_id.map(GuildId),
            channel_id: ChannelId(channel_id),
            user_id: UserId(1),
            message_id: None,
            reply_token: reply_token.map(str::to_owned),
        }
    }

    /// An empty plain send (no content, no embeds) is dropped with `Ok` -
    /// Discord would reject it with a 400, and the transport is never touched.
    #[tokio::test]
    async fn plain_send_of_an_empty_message_drops_without_a_call() {
        let api = delivery_api(vec![]);
        let output = delivery_output(&api);

        output.send(OutboundMessage::text("")).await.expect("dropped, not failed");

        assert!(api.calls().is_empty(), "an empty message must not reach the transport");
    }

    /// A plain send denormalizes `[Name]<@id>` back to bare tags in content
    /// and embed fields, and the ephemeral hint is ignored: plain channel
    /// sends are always public, so no flags ride the wire.
    #[tokio::test]
    async fn plain_send_denormalizes_and_ignores_the_ephemeral_hint() {
        let api = delivery_api(vec![]);
        let output = delivery_output(&api);
        let message = OutboundMessage {
            content: "hey [Alice]<@123>!".to_owned(),
            embeds: vec![Embed {
                title: "Hi [Bob]<@7>".to_owned(),
                description: "see [Cara]<@8>".to_owned(),
            }],
            ephemeral: true,
            reply_to: None,
        };

        output.send(message).await.expect("plain send delivers");

        let calls = api.calls();
        assert_eq!(calls.len(), 1, "exactly one plain send");
        let RecordedCall::SendMessage { channel, body } = calls.first().expect("one call") else {
            panic!("expected a plain channel send, got {:?}", calls.first());
        };
        assert_eq!(*channel, SerenityChannelId::new(5));
        assert_eq!(body.get("content").and_then(serde_json::Value::as_str), Some("hey <@123>!"));
        let embeds =
            body.get("embeds").and_then(serde_json::Value::as_array).expect("embeds expected");
        let embed = embeds.first().expect("one embed expected");
        assert_eq!(embed.get("title").and_then(serde_json::Value::as_str), Some("Hi <@7>"));
        assert_eq!(embed.get("description").and_then(serde_json::Value::as_str), Some("see <@8>"));
        assert_eq!(body.get("flags"), None, "the ephemeral hint is ignored on plain sends");
        assert_eq!(body.get("message_reference"), None);
    }

    /// A `reply_to` rides the send as a Default-kind message reference pinned
    /// to the destination channel, with `fail_if_not_exists` off: a deleted
    /// target degrades to a normal send instead of failing the delivery (the
    /// LLM guaranteed-answer contract rides these sends).
    #[tokio::test]
    async fn plain_send_carries_the_reply_reference_and_degrades_gracefully() {
        let api = delivery_api(vec![]);
        let output = delivery_output(&api);

        output
            .send(OutboundMessage::text("answering").replying_to(MessageId(42)))
            .await
            .expect("reply send delivers");

        let calls = api.calls();
        let RecordedCall::SendMessage { body, .. } = calls.first().expect("one call") else {
            panic!("expected a plain channel send, got {:?}", calls.first());
        };
        let reference = body.get("message_reference").expect("reply reference expected");
        assert_eq!(reference.get("type").and_then(serde_json::Value::as_u64), Some(0));
        assert_eq!(reference.get("message_id").and_then(serde_json::Value::as_str), Some("42"));
        assert_eq!(reference.get("channel_id").and_then(serde_json::Value::as_str), Some("5"));
        assert_eq!(
            reference.get("fail_if_not_exists").and_then(serde_json::Value::as_bool),
            Some(false),
            "a deleted target must degrade to a normal send, never fail"
        );
    }

    /// An empty streaming placeholder is rejected before the transport: there
    /// is nothing to create, and the stream contract wants an `Err`, not a
    /// silent message.
    #[tokio::test]
    async fn stream_begin_rejects_an_empty_placeholder_without_a_call() {
        let api = delivery_api(vec![]);
        let stream = delivery_stream(&api);

        let result = stream.begin(OutboundMessage::text("")).await;

        assert!(matches!(result, Err(OutboundError::Send(_))));
        assert!(api.calls().is_empty());
    }

    /// `begin` creates on the origin channel with denormalized content (and
    /// the reply reference, when present) and returns the created handle;
    /// `update` edits that message in place with denormalized content.
    #[tokio::test]
    async fn stream_begin_and_update_target_the_channel_and_denormalize() {
        let api = delivery_api(vec![]);
        let stream = delivery_stream(&api);

        let created = stream
            .begin(OutboundMessage::text("hey [Alice]<@123>!").replying_to(MessageId(42)))
            .await
            .expect("begin delivers");
        stream.update(created, "done [Bob]<@7>".to_owned()).await.expect("update delivers");

        assert_eq!(created, MessageId(600), "begin yields the created message's id");
        let calls = api.calls();
        assert_eq!(calls.len(), 2);
        let RecordedCall::SendMessage { channel, body } = calls.first().expect("create expected")
        else {
            panic!("expected a channel send, got {:?}", calls.first());
        };
        assert_eq!(*channel, SerenityChannelId::new(5));
        assert_eq!(body.get("content").and_then(serde_json::Value::as_str), Some("hey <@123>!"));
        assert!(body.get("message_reference").is_some(), "the reply anchor survives begin");

        let RecordedCall::EditMessage { channel, message, body } =
            calls.get(1).expect("edit expected")
        else {
            panic!("expected an edit, got {:?}", calls.get(1));
        };
        assert_eq!(*channel, SerenityChannelId::new(5));
        assert_eq!(*message, SerenityMessageId::new(600), "edits ride the created handle");
        assert_eq!(body.get("content").and_then(serde_json::Value::as_str), Some("done <@7>"));
    }

    /// A bare `:name:` token resolves through the cached guild emoji list:
    /// the hit reacts as that custom emoji, one lookup ahead of it.
    #[tokio::test]
    async fn reaction_bare_name_resolves_through_the_guild_emoji_list() {
        let api =
            delivery_api(vec![RecordingApi::emoji("dorkiS", 9), RecordingApi::emoji("other", 3)]);
        let reaction = delivery_reaction(&api);

        reaction.add_reaction(MessageId(42), ":dorkiS:").await.expect("known emoji reacts");

        assert_eq!(
            api.calls(),
            vec![
                RecordedCall::GetEmojis { guild: SerenityGuildId::new(7) },
                RecordedCall::CreateReaction {
                    channel: SerenityChannelId::new(5),
                    message: SerenityMessageId::new(42),
                    reaction: ReactionType::Custom {
                        animated: false,
                        id: EmojiId::new(9),
                        name: Some("dorkiS".to_owned()),
                    },
                },
            ],
            "resolution fetches the guild list, then reacts with the matched emoji"
        );
    }

    /// A bare `:name:` the guild does not have fails per-token with no
    /// reaction call - cosmetic by contract, callers log and continue.
    #[tokio::test]
    async fn reaction_unknown_bare_name_fails_per_token_without_a_reaction_call() {
        let api = delivery_api(vec![RecordingApi::emoji("other", 3)]);
        let reaction = delivery_reaction(&api);

        let result = reaction.add_reaction(MessageId(42), ":dorkiS:").await;

        assert!(matches!(result, Err(OutboundError::Reaction(_))));
        assert_eq!(
            api.calls(),
            vec![RecordedCall::GetEmojis { guild: SerenityGuildId::new(7) }],
            "only the lookup happens - no reaction is attempted"
        );
    }

    /// An unparseable token fails closed before any transport call: no emoji
    /// fetch, no reaction.
    #[tokio::test]
    async fn reaction_invalid_token_fails_closed_without_a_call() {
        let api = delivery_api(vec![RecordingApi::emoji("dorkiS", 9)]);
        let reaction = delivery_reaction(&api);

        let result = reaction.add_reaction(MessageId(42), "not an emoji").await;

        assert!(matches!(result, Err(OutboundError::Reaction(_))));
        assert!(api.calls().is_empty());
    }

    // ---- Transport-failure mapping (the negative paths the ChatApi seam
    // rewrote to point-free `map_err`) ----

    /// A failed send transport surfaces as the port's `Send` error carrying
    /// the transport message - no panic, no silent drop.
    #[tokio::test]
    async fn plain_send_transport_failure_maps_onto_the_port_error() {
        let api = failing_api(ApiFailures { send: true, ..ApiFailures::default() });
        let output = delivery_output(&api);

        let result = output.send(OutboundMessage::text("hello")).await;

        match result {
            Err(OutboundError::Send(message)) => assert_eq!(message, "send transport failed"),
            other => panic!("expected a send error, got {other:?}"),
        }
    }

    /// A failed edit transport mid-stream surfaces as the port's `Send`
    /// error on the `update` half, exactly like on `begin`.
    #[tokio::test]
    async fn stream_update_transport_failure_maps_onto_the_port_error() {
        let api = failing_api(ApiFailures { edit: true, ..ApiFailures::default() });
        let stream = delivery_stream(&api);
        let created = stream
            .begin(OutboundMessage::text("hi"))
            .await
            .expect("begin succeeds while only edits fail");

        let result = stream.update(created, "more".to_owned()).await;

        match result {
            Err(OutboundError::Send(message)) => assert_eq!(message, "edit transport failed"),
            other => panic!("expected a send error, got {other:?}"),
        }
    }

    /// A failed reaction transport surfaces as the port's `Reaction` error
    /// carrying the transport message - per-token, never fatal by contract.
    #[tokio::test]
    async fn reaction_transport_failure_maps_onto_the_port_error() {
        let api = failing_api(ApiFailures { reaction: true, ..ApiFailures::default() });
        let reaction = delivery_reaction(&api);

        let result = reaction.add_reaction(MessageId(42), "🍌").await;

        match result {
            Err(OutboundError::Reaction(message)) => {
                assert_eq!(message, "reaction transport failed");
            }
            other => panic!("expected a reaction error, got {other:?}"),
        }
    }

    /// A failed emoji-list fetch degrades to an empty list: bare `:name:`
    /// resolution fails closed per token ("not found"), the transport error
    /// is warn-logged by the cache, and the failure is cached briefly so a
    /// down endpoint is not hammered.
    #[tokio::test]
    async fn emoji_lookup_failure_fails_closed_per_token() {
        let api = failing_api(ApiFailures { emojis: true, ..ApiFailures::default() });
        let reaction = delivery_reaction(&api);

        let result = reaction.add_reaction(MessageId(42), ":dorkiS:").await;

        assert!(matches!(result, Err(OutboundError::Reaction(_))));
    }

    /// Bare-name resolution shares one cached guild listing: repeated
    /// reactions within the TTL window fetch the list exactly once.
    #[tokio::test]
    async fn reaction_bare_name_resolution_caches_the_guild_list() {
        let api = delivery_api(vec![RecordingApi::emoji("dorkiS", 9)]);
        let reaction = delivery_reaction(&api);

        reaction.add_reaction(MessageId(42), ":dorkiS:").await.expect("first reacts");
        reaction.add_reaction(MessageId(43), ":dorkiS:").await.expect("second reacts");

        let lookups = api
            .calls()
            .iter()
            .filter(|call| matches!(call, RecordedCall::GetEmojis { .. }))
            .count();
        assert_eq!(lookups, 1, "the guild list is fetched once per TTL window");
    }

    /// The emoji-listing port renders exact wire forms (animated flag kept)
    /// and is empty for origins outside any guild.
    #[tokio::test]
    async fn reactable_emojis_lists_wire_forms_and_defaults_outside_guilds() {
        let api = delivery_api(vec![RecordingApi::emoji("dorkiS", 9)]);
        let factory = delivery_factory(&api);

        let listed = factory.reactable_emojis(&origin(Some(7), 5, None)).list().await;
        assert_eq!(
            listed,
            vec![ReactableEmoji { name: "dorkiS".to_owned(), token: "<:dorkiS:9>".to_owned() }]
        );
        let fetches = api.calls().len();

        let outside = factory.reactable_emojis(&origin(None, 5, None)).list().await;
        assert!(outside.is_empty(), "no guild - nothing to list");
        assert_eq!(api.calls().len(), fetches, "the undeliverable port never fetches");
    }

    /// A reply-token origin routes every send onto the interaction followup
    /// path - the plain channel path is never touched. The first PUBLIC
    /// content rides the placeholder dance: the deferred ephemeral original
    /// cannot carry public content (editing it inherits its state), so the
    /// slot is consumed with an invoker-only placeholder, that placeholder
    /// is deleted, and the real content posts as a fresh followup.
    #[tokio::test]
    async fn factory_reply_token_origins_route_onto_the_followup_path() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        let output = factory.chat_output(&origin(Some(7), 5, Some("tok-1")));
        output.send(OutboundMessage::text("hello")).await.expect("followup delivers");

        let calls = api.calls();
        let [placeholder, delete, content] = calls.as_slice() else {
            panic!("expected placeholder, delete, content; got {calls:?}");
        };
        // The placeholder stays generic - only its shape is contractual
        // (see `placeholder_is_ephemeral_and_non_empty`): invoker-only,
        // some content, bound to the interaction token.
        let RecordedCall::CreateFollowup { token, body: placeholder_body } = placeholder else {
            panic!("expected a followup create first, got {placeholder:?}");
        };
        assert_eq!(token, "tok-1");
        assert_eq!(placeholder_body.get("flags"), Some(&serde_json::json!(64)));
        assert!(
            placeholder_body
                .get("content")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| !text.is_empty()),
            "the placeholder must carry some content"
        );
        assert_eq!(
            delete,
            &RecordedCall::DeleteFollowup {
                token: "tok-1".to_owned(),
                message: SerenityMessageId::new(500),
            }
        );
        assert_eq!(
            content,
            &RecordedCall::CreateFollowup {
                token: "tok-1".to_owned(),
                body: serde_json::json!({ "content": "hello" }),
            }
        );
    }

    /// A channel-less origin (guild-bound but `ChannelId(0)`) fails fast on
    /// plain sends: a Discord call for channel 0 is a guaranteed 404, so
    /// none is attempted.
    #[tokio::test]
    async fn factory_channel_less_origins_fail_fast_without_a_call() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        let output = factory.chat_output(&origin(Some(7), 0, None));
        let result = output.send(OutboundMessage::text("hi")).await;

        assert!(matches!(result, Err(OutboundError::Send(_))));
        assert!(api.calls().is_empty());
    }

    /// Configured-channel logging is guild-scoped by contract: a DM origin or
    /// a zero channel yields the undeliverable port (fail, zero calls), while
    /// a guild-bound origin lands on the configured channel itself.
    #[tokio::test]
    async fn factory_configured_channels_are_guild_scoped() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        let dm = factory.channel_output(&origin(None, 5, None), ChannelId(9));
        let channel_less = factory.channel_output(&origin(Some(7), 5, None), ChannelId(0));
        assert!(matches!(dm.send(OutboundMessage::text("log")).await, Err(OutboundError::Send(_))));
        assert!(matches!(
            channel_less.send(OutboundMessage::text("log")).await,
            Err(OutboundError::Send(_))
        ));

        let guild_bound = factory.channel_output(&origin(Some(7), 5, None), ChannelId(9));
        guild_bound.send(OutboundMessage::text("log")).await.expect("guild-bound log delivers");

        let calls = api.calls();
        assert_eq!(calls.len(), 1, "only the guild-bound send reaches the transport");
        let RecordedCall::SendMessage { channel, body } = calls.first().expect("one call") else {
            panic!("expected a plain channel send, got {:?}", calls.first());
        };
        assert_eq!(*channel, SerenityChannelId::new(9), "the configured channel is honored");
        assert_eq!(body.get("content").and_then(serde_json::Value::as_str), Some("log"));
    }

    /// Reply-token, DM, and channel-less origins get an undeliverable stream:
    /// both stream calls fail and nothing reaches the transport - guild data
    /// must not land on a guild-visible surface by accident.
    #[tokio::test]
    async fn factory_non_streamable_origins_get_an_undeliverable_stream() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        let reply_token = factory.stream_output(&origin(Some(7), 5, Some("tok-1")));
        let dm = factory.stream_output(&origin(None, 5, None));
        let channel_less = factory.stream_output(&origin(Some(7), 0, None));
        for stream in [&reply_token, &dm, &channel_less] {
            assert!(matches!(
                stream.begin(OutboundMessage::text("x")).await,
                Err(OutboundError::Send(_))
            ));
            assert!(matches!(
                stream.update(MessageId(1), "x".to_owned()).await,
                Err(OutboundError::Send(_))
            ));
        }

        assert!(api.calls().is_empty(), "streaming never reaches the transport here");
    }

    /// A DM origin cannot react: the factory hands out the undeliverable
    /// reaction port, which fails without touching the transport.
    #[tokio::test]
    async fn factory_dm_origins_cannot_react() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        let reaction = factory.react(&origin(None, 5, None));
        let result = reaction.add_reaction(MessageId(42), "👍").await;

        assert!(matches!(result, Err(OutboundError::Reaction(_))));
        assert!(api.calls().is_empty());
    }

    /// Typing bridges real channels through the transport seam and stays
    /// dead for channel-less origins: no bridge, no transport call.
    #[test]
    fn factory_start_typing_bridges_real_channels_only() {
        let api = delivery_api(vec![]);
        let factory = delivery_factory(&api);

        factory.start_typing(&origin(Some(7), 5, None));
        factory.start_typing(&origin(Some(7), 0, None));

        assert_eq!(
            api.calls(),
            vec![RecordedCall::RunTyping { channel: SerenityChannelId::new(5) }],
            "the channel-less origin gets a dead guard, never a bridge"
        );
    }

    /// The undeliverable ports fail every call outright - they carry no
    /// transport at all, so nothing can reach the API through them.
    #[tokio::test]
    async fn undeliverable_ports_fail_every_call() {
        let api = delivery_api(vec![]);

        assert!(matches!(
            UndeliverableChatOutput.send(OutboundMessage::text("hi")).await,
            Err(OutboundError::Send(_))
        ));
        assert!(matches!(
            UndeliverableChatStream.begin(OutboundMessage::text("hi")).await,
            Err(OutboundError::Send(_))
        ));
        assert!(matches!(
            UndeliverableChatStream.update(MessageId(1), "hi".to_owned()).await,
            Err(OutboundError::Send(_))
        ));
        assert!(matches!(
            UndeliverableReactionPort.add_reaction(MessageId(42), "👍").await,
            Err(OutboundError::Reaction(_))
        ));

        assert!(api.calls().is_empty(), "nothing is wired - nothing can be called");
    }
}
