use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serenity::all::{
    ChannelId as SerenityChannelId, CreateMessage, EditMessage, EmojiId,
    GuildId as SerenityGuildId, Http, MessageId as SerenityMessageId, MessageReference,
    MessageReferenceKind, ReactionType,
};
use serenity::async_trait;
use tokio_util::sync::CancellationToken;

use crate::kernel::{
    models::{ChannelId, Embed, MessageId, Origin, OutboundError, OutboundMessage},
    spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard, ReactionPort,
        UndeliverableReactionPort,
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
pub struct SerenityChatOutputFactory {
    http: Arc<Http>,
}

impl SerenityChatOutputFactory {
    /// Takes the shared REST client: serenity rate limiting is per `Http`,
    /// so every driven Discord caller must share one instance.
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

impl ChatOutputFactoryPort for SerenityChatOutputFactory {
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
        if let Some(token) = &origin.reply_token {
            return Arc::new(InteractionFollowupOutput {
                http: Arc::clone(&self.http),
                token: token.clone(),
                first_followup_used: AtomicBool::new(false),
            });
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
        let http = Arc::clone(&self.http);
        let channel = SerenityChannelId::new(origin.channel_id.get());
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            let typing = http.start_typing(channel);
            cancel.cancelled().await;
            drop(typing);
        });
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
        })
    }
}

struct SerenityChatOutput {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl ChatOutputPort for SerenityChatOutput {
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
            .map_err(|err| OutboundError::Send(err.to_string()))?;
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
/// guild's current emoji list (one REST fetch per bare-name token -
/// reactions are rare and capped, and the factory holds no gateway cache).
struct SerenityReaction {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
    guild_id: SerenityGuildId,
}

#[async_trait]
impl ReactionPort for SerenityReaction {
    async fn add_reaction(&self, message_id: MessageId, emoji: &str) -> Result<(), OutboundError> {
        let reaction = self.resolve(emoji).await?;
        self.http
            .create_reaction(self.channel_id, SerenityMessageId::new(message_id.get()), &reaction)
            .await
            .map_err(|err| OutboundError::Reaction(err.to_string()))
    }
}

impl SerenityReaction {
    /// Maps a raw protocol token onto a Discord reaction type. Fully
    /// qualified forms (`<:name:id>`, `<a:name:id>`, `:name:id`) build the
    /// custom reaction directly; bare `:name:` needs the guild emoji list;
    /// anything else is a Unicode emoji (tokens the plugin already
    /// validated - an unparseable token here fails per-token, by design).
    async fn resolve(&self, emoji: &str) -> Result<ReactionType, OutboundError> {
        let parsed = parse_reaction_token(emoji);
        match parsed {
            ParsedReaction::Custom { animated, name, id } => {
                Ok(custom_reaction(animated, &name, &id))
            }
            ParsedReaction::Name(name) => {
                let emojis = self
                    .http
                    .get_emojis(self.guild_id)
                    .await
                    .map_err(|err| OutboundError::Reaction(err.to_string()))?;
                emojis
                    .into_iter()
                    .find(|known| known.name.as_str() == name.as_str())
                    .map(|known| ReactionType::Custom {
                        animated: known.animated,
                        id: known.id,
                        name: Some(known.name),
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
    // Digit-validated by the parsers; a failed parse cannot be reached.
    let id = id.parse::<u64>().map(EmojiId::new).unwrap_or_else(|_| EmojiId::new(0));
    ReactionType::Custom { animated, id, name: Some(name.to_owned()) }
}

/// Progressive-rendering output: creates the message on `begin`, then edits
/// it in place as content arrives. Content-only - embeds are ignored in a
/// message that exists to be overwritten.
struct SerenityChatStream {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl ChatStreamPort for SerenityChatStream {
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
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(MessageId(created.id.get()))
    }

    async fn update(&self, message: MessageId, content: String) -> Result<(), OutboundError> {
        self.http
            .edit_message(
                self.channel_id,
                serenity::all::MessageId::new(message.get()),
                &EditMessage::new().content(denormalize_mention_tags(&content)),
                Vec::new(),
            )
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
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

struct InteractionFollowupOutput {
    http: Arc<Http>,
    token: String,
    /// Whether the first-followup slot has been consumed. The first followup
    /// of an interaction edits the deferred ephemeral original response and
    /// inherits its ephemeral state regardless of the flags it carries, so a
    /// PUBLIC first reply must be routed around that slot (see [`send`]).
    /// Fresh per interaction: one output instance is built per event origin.
    first_followup_used: AtomicBool,
}

#[async_trait]
impl ChatOutputPort for InteractionFollowupOutput {
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
            return self.post(&body).await;
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
            match self
                .http
                .create_followup_message(&self.token, &placeholder_body(), Vec::new())
                .await
            {
                Ok(placeholder) => {
                    if let Err(err) =
                        self.http.delete_followup_message(&self.token, placeholder.id).await
                    {
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
}

impl InteractionFollowupOutput {
    /// Posts one followup body for this interaction.
    async fn post(&self, body: &serde_json::Value) -> Result<(), OutboundError> {
        self.http
            .create_followup_message(&self.token, body, Vec::new())
            .await
            .map(|_: serenity::all::Message| ())
            .map_err(|err| OutboundError::Send(err.to_string()))
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
        // Anything with a non-ASCII character is a Unicode emoji.
        assert_eq!(parse_reaction_token("🤓"), ParsedReaction::Unicode);
        assert_eq!(parse_reaction_token("👍🏽"), ParsedReaction::Unicode);
        // Malformed tokens fail per-token (the plugin's per-item rule); a
        // name-shaped token like `:x:` stays resolvable - the API has the
        // final say on whether the emoji exists.
        for bad in ["word", "::", ":bad name:", "<nope>", "<:bad>", "42", ":id:abc"] {
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
}
