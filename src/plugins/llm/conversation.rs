//! Conversation domain logic: the record schema, capture/trigger rules,
//! context assembly, and reply splitting. Pure functions - no I/O - so the
//! rules stay unit-testable in isolation from storage and providers.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::completion_port::{ChatMessage, ChatRole};
use super::model::{CaptureMode, ChannelConfig, ConversationState};
use super::providers::{LlmSettings, SummaryPlacement};

/// Slot placeholder for the summary placements that keep a dedicated
/// message (the default `SystemTurn`, and `AssistantTurn`): the position
/// always exists, keeping turn positions stable (early messages carry more
/// weight with most models) and the prompt prefix byte-stable for provider
/// prompt caches. `SystemSuffix` merges the summary instead - no slot, no
/// placeholder.
pub const NO_EARLIER_CONTEXT: &str = "(no earlier context)";

/// Default rendering of user turns in the context.
pub const DEFAULT_TURN_TEMPLATE: &str = "{sender}: {message}";

/// The file name is a fake placeholder: the model only needs the "this was
/// an image" hint - the description lives in the markdown alt-text slot.
/// Multi-image messages number the placeholders so the model can refer to
/// them separately.
const IMAGE_PLACEHOLDER: &str = "image.png";

/// Renders a record's images as markdown image references, one per line:
/// `![description](image.png)`, undescribed ones as `![image](image.png)`.
#[must_use]
pub fn render_images(images: &[RecordImage]) -> String {
    let mut rendered = String::new();
    for (index, image) in images.iter().enumerate() {
        rendered.push('\n');
        let name = if index == 0 {
            IMAGE_PLACEHOLDER.to_owned()
        } else {
            format!("image_{}.png", index + 1)
        };
        let alt = image.description.as_deref().unwrap_or("image");
        let _ = write!(rendered, "![{alt}]({name})");
    }
    rendered
}

/// A record's content as it enters the prompt: text plus rendered images.
/// The single source of turn rendering - context assembly and compaction
/// transcripts render identically.
#[must_use]
pub fn record_content(record: &ConversationRecord) -> String {
    if record.images.is_empty() {
        return record.content.clone();
    }
    format!("{}{}", record.content, render_images(&record.images))
}

/// One image attached to a captured user message: the recognition
/// description when it succeeded, `None` when the image could not be
/// described (feature off at capture, endpoint failure, over the per-message
/// cap). Records are immutable once appended, so the rendered prompt stays
/// byte-stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordImage {
    #[serde(default)]
    pub description: Option<String>,
}

/// One captured conversation entry - the payload of a `guild_records` row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationRecord {
    /// Platform message id when known: user captures carry the inbound
    /// message id, assistant captures the handle of the reply's first
    /// message (from `ChatStreamPort::begin`) - reply-to detection needs it.
    #[serde(default)]
    pub message_id: Option<u64>,
    pub role: RecordRole,
    /// Author display name at capture time (user turns only).
    #[serde(default)]
    pub author: Option<String>,
    pub content: String,
    /// Message this one replies to, when the platform reported the
    /// reference.
    #[serde(default)]
    pub reply_to: Option<u64>,
    /// Capture time, unix seconds.
    pub captured_at: u64,
    /// Images attached to the message, in attachment order (user turns
    /// only; empty on records from before image recognition existed).
    #[serde(default)]
    pub images: Vec<RecordImage>,
}

/// Who spoke a captured record. The bot's own turns are recorded at send
/// time - the adapter never delivers bot messages back through the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordRole {
    User,
    Assistant,
}

/// Whether the message enters the channel's conversation history. In
/// `bot_related` mode a message qualifies when it mentions the bot or
/// replies into the captured conversation (a reply chain that contains the
/// bot); in `all_messages` mode everything does. Only the live window
/// (records after the cutoff) is considered: replying to a long-compacted
/// bot message needs a mention.
pub fn should_capture(
    mode: CaptureMode,
    mentions_bot: bool,
    reply_to: Option<u64>,
    live: &[ConversationRecord],
) -> bool {
    match mode {
        CaptureMode::AllMessages => true,
        CaptureMode::BotRelated => {
            mentions_bot
                || reply_to
                    .is_some_and(|id| live.iter().any(|record| record.message_id == Some(id)))
        }
    }
}

/// Whether the message should make the bot answer: an explicit mention, or
/// a direct reply to one of the bot's own turns. Replies between users
/// inside the conversation are captured but do not trigger.
pub fn should_trigger(
    mentions_bot: bool,
    reply_to: Option<u64>,
    live: &[ConversationRecord],
) -> bool {
    mentions_bot
        || reply_to.is_some_and(|id| {
            live.iter()
                .any(|record| record.role == RecordRole::Assistant && record.message_id == Some(id))
        })
}

/// Per-turn framing cost estimate (role markers, separators) added on top
/// of the content's estimated tokens.
const TURN_TOKEN_OVERHEAD: u64 = 8;

/// Completion space kept out of a model-window-derived prompt budget when
/// the channel sets no explicit `max_tokens`.
const DEFAULT_COMPLETION_RESERVE: u64 = 1024;

/// Resolves the effective prompt-side token budget:
///
/// 1. the channel's explicit `context_budget_tokens` override;
/// 2. the model's declared `context_window` minus the completion reserve
///    (channel `max_tokens` or a default - the answer needs room) and a 10%
///    margin for estimator error;
/// 3. `None` - message-count filling only. This is also forced while the
///    channel is uncalibrated (no reported usage yet): token filling on a
///    guessed ratio could silently truncate the context.
pub(crate) fn resolve_budget(
    config: &ChannelConfig,
    settings: &LlmSettings,
    calibrated: bool,
) -> Option<u64> {
    if let Some(explicit) = config.context_budget_tokens {
        return Some(u64::from(explicit));
    }
    if !calibrated {
        return None;
    }
    let window = settings.models.get(&config.model)?.context_window?;
    let reserve = u64::from(config.params.max_tokens.unwrap_or(DEFAULT_COMPLETION_RESERVE as u32));
    Some(window.saturating_sub(reserve).saturating_sub(window / 10))
}

/// Estimated token cost of one piece of prompt text (framing + content).
fn estimated_tokens(text: &str, tokens_per_char: f64) -> u64 {
    // estimator: precision loss is fine.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let cost = TURN_TOKEN_OVERHEAD + (text.chars().count() as f64 * tokens_per_char) as u64;
    cost
}

/// Assembles the LLM context: the system prompt, the always-present summary
/// slot (a separate second message by default - see `SummaryPlacement` for
/// the merge modes), then the live window. User turns render via the channel
/// template; assistant turns pass through raw (the role already says who spoke).
///
/// The whole prompt side (system + summary + turns) counts against the
/// resolved budget, and turns fill NEWEST-FIRST under that budget and
/// `history_depth`, whichever bites first. The newest turn is always
/// included - a reply must at least see what it answers. Without a budget
/// (uncalibrated, no model window, no channel override) only `history_depth`
/// applies.
pub fn assemble_context(
    config: &ChannelConfig,
    settings: &LlmSettings,
    state: &ConversationState,
    records: &[ConversationRecord],
    tokens_per_char: f64,
    budget: Option<u64>,
) -> Vec<ChatMessage> {
    let depth = usize::try_from(config.history_depth).unwrap_or(usize::MAX);
    // Undeclared models run the default placement (a separate summary slot)
    // - same capability contract as reasoning/context_window.
    let placement = settings
        .models
        .get(&config.model)
        .map_or(SummaryPlacement::default(), |model| model.summary_placement);
    let mut system =
        config.system_prompt.clone().unwrap_or_else(|| settings.default_system_prompt.clone());
    let summary_slot = match &state.summary {
        Some(summary) => format!("Earlier conversation summary:\n{summary}"),
        None => NO_EARLIER_CONTEXT.to_owned(),
    };

    // The fixed prompt side is part of the budget: the endpoint bills it as
    // prompt tokens just like the turns. `SystemSuffix` folds the summary
    // into the system message; the slot modes keep it as its own message
    // (placeholder included - the slot is always present there).
    let mut used = estimated_tokens(&system, tokens_per_char);
    let mut messages = Vec::new();
    match placement {
        SummaryPlacement::SystemSuffix => {
            if state.summary.is_some() {
                used += estimated_tokens(&summary_slot, tokens_per_char);
                system.push_str("\n\n");
                system.push_str(&summary_slot);
            }
            messages.push(ChatMessage::text(ChatRole::System, system));
        }
        SummaryPlacement::SystemTurn => {
            used += estimated_tokens(&summary_slot, tokens_per_char);
            messages.push(ChatMessage::text(ChatRole::System, system));
            messages.push(ChatMessage::text(ChatRole::System, summary_slot));
        }
        SummaryPlacement::AssistantTurn => {
            used += estimated_tokens(&summary_slot, tokens_per_char);
            messages.push(ChatMessage::text(ChatRole::System, system));
            messages.push(ChatMessage::text(ChatRole::Assistant, summary_slot));
        }
    }

    let mut count = 0usize;
    while count < records.len() && count < depth {
        let Some(record) = records.get(records.len() - count - 1) else {
            break;
        };
        let cost = estimated_tokens(&record_content(record), tokens_per_char);
        if budget.is_some_and(|budget| used + cost > budget) && count > 0 {
            break;
        }
        used += cost;
        count += 1;
    }
    let (_, window) = records.split_at(records.len() - count);

    let template = config.turn_template.as_deref().unwrap_or(DEFAULT_TURN_TEMPLATE);
    for record in window {
        let message = match record.role {
            RecordRole::User => ChatMessage::text(
                ChatRole::User,
                template
                    .replace("{sender}", record.author.as_deref().unwrap_or("user"))
                    .replace("{message}", &record_content(record)),
            ),
            RecordRole::Assistant => ChatMessage::text(ChatRole::Assistant, record.content.clone()),
        };
        messages.push(message);
    }
    messages
}

/// Builds the compaction request's messages: the compaction prompt, then a
/// transcript combining the previous summary (if any) with the chunk being
/// folded in - the model returns the cumulative replacement summary.
pub fn compaction_input(
    prompt: &str,
    previous_summary: Option<&str>,
    records: &[ConversationRecord],
) -> Vec<ChatMessage> {
    let mut transcript = String::new();
    if let Some(previous) = previous_summary {
        transcript.push_str("Previous summary:\n");
        transcript.push_str(previous);
        transcript.push_str("\n\n");
    }
    transcript.push_str("New messages:\n");
    for record in records {
        match record.role {
            RecordRole::User => writeln!(
                transcript,
                "{}: {}",
                record.author.as_deref().unwrap_or("user"),
                record_content(record)
            ),
            RecordRole::Assistant => writeln!(transcript, "assistant: {}", record.content),
        }
        .expect("writing to a String expected to be infallible");
    }
    vec![ChatMessage::text(ChatRole::System, prompt), ChatMessage::text(ChatRole::User, transcript)]
}

/// Splits a reply into platform-sized chunks on line boundaries: a line
/// that does not fit the current chunk starts a new one; a single line
/// longer than the limit is hard-cut (the platform rejects oversized
/// messages outright). Measured in characters, not bytes.
pub fn split_reply(content: &str, max_length: usize) -> Vec<String> {
    let max = max_length.max(1);
    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in content.split('\n') {
        // An overlong line degrades into limit-sized pieces.
        let mut rest = line.to_owned();
        loop {
            let line_len = rest.chars().count();
            if line_len <= max {
                let joiner = usize::from(!current.is_empty());
                if current.chars().count() + joiner + line_len <= max {
                    if joiner == 1 {
                        current.push('\n');
                    }
                    current.push_str(&rest);
                } else {
                    if !current.is_empty() {
                        chunks.push(std::mem::take(&mut current));
                    }
                    current = rest;
                }
                break;
            }
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            chunks.push(rest.chars().take(max).collect());
            rest = rest.chars().skip(max).collect();
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::llm::providers::{ModelSettings, SummaryPlacement};

    /// `LlmSettings` with model `m` declared at the given placement - so the
    /// assembly resolves it the way a real configuration would.
    fn settings_with_placement(placement: SummaryPlacement) -> LlmSettings {
        LlmSettings {
            models: std::collections::BTreeMap::from([(
                "m".to_owned(),
                ModelSettings {
                    reasoning: false,
                    context_window: None,
                    summary_placement: placement,
                },
            )]),
            ..LlmSettings::default()
        }
    }

    fn user_record(message_id: u64, author: &str, content: &str) -> ConversationRecord {
        ConversationRecord {
            message_id: Some(message_id),
            role: RecordRole::User,
            author: Some(author.to_owned()),
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
            images: Vec::new(),
        }
    }

    fn assistant_record(message_id: u64, content: &str) -> ConversationRecord {
        ConversationRecord {
            message_id: Some(message_id),
            role: RecordRole::Assistant,
            author: None,
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
            images: Vec::new(),
        }
    }

    #[test]
    fn bot_related_captures_mentions_and_conversation_replies() {
        let live = vec![user_record(10, "alice", "hi"), assistant_record(11, "hello!")];

        assert!(should_capture(CaptureMode::BotRelated, true, None, &live));
        assert!(should_capture(CaptureMode::BotRelated, false, Some(10), &live));
        assert!(should_capture(CaptureMode::BotRelated, false, Some(11), &live));
        // Reply to an unknown message: unrelated to the bot, not captured.
        assert!(!should_capture(CaptureMode::BotRelated, false, Some(99), &live));
        assert!(!should_capture(CaptureMode::BotRelated, false, None, &live));
        // All-messages mode captures regardless.
        assert!(should_capture(CaptureMode::AllMessages, false, None, &live));
    }

    #[test]
    fn triggers_are_mentions_and_replies_to_bot_turns() {
        let live = vec![user_record(10, "alice", "hi"), assistant_record(11, "hello!")];

        assert!(should_trigger(true, None, &live));
        assert!(should_trigger(false, Some(11), &live));
        // A user-to-user reply inside the conversation does not trigger.
        assert!(!should_trigger(false, Some(10), &live));
        assert!(!should_trigger(false, None, &live));
        // An unknown reply target cannot be the bot.
        assert!(!should_trigger(false, Some(99), &live));
    }

    #[test]
    fn context_has_fixed_schema_and_templated_turns() {
        let config = ChannelConfig::assigned("m".to_owned());
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        let records = vec![
            user_record(10, "alice", "hello"),
            assistant_record(11, "hi alice"),
            ConversationRecord {
                message_id: Some(12),
                role: RecordRole::User,
                author: None,
                content: "no name".to_owned(),
                reply_to: None,
                captured_at: 0,
                images: Vec::new(),
            },
        ];

        let messages = assemble_context(&config, &settings, &state, &records, 0.25, None);

        // Default placement (SystemTurn): system prompt, always-present
        // summary slot (placeholder without a summary), then the turns.
        assert_eq!(messages.len(), 5);
        assert_eq!(
            messages.first().map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::System, "You are a helpful chat assistant."))
        );
        assert_eq!(
            messages.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::System, NO_EARLIER_CONTEXT))
        );
        assert_eq!(
            messages.get(2).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::User, "alice: hello"))
        );
        assert_eq!(
            messages.get(3).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::Assistant, "hi alice"))
        );
        // Missing author falls back to a generic sender.
        assert_eq!(
            messages.get(4).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::User, "user: no name"))
        );
    }

    #[test]
    fn summary_replaces_the_placeholder_and_prompt_overrides_apply() {
        let config = ChannelConfig {
            system_prompt: Some("custom prompt".to_owned()),
            ..ChannelConfig::assigned("m".to_owned())
        };
        let settings = LlmSettings::default();
        let state = ConversationState {
            summary: Some("the gist".to_owned()),
            cutoff_seq: 4,
            cutoff_at: Some(1_717_000_000),
        };

        let messages = assemble_context(&config, &settings, &state, &[], 0.25, None);

        assert_eq!(messages.first().map(|m| m.content.as_str()), Some("custom prompt"));
        assert_eq!(
            messages.get(1).map(|m| m.content.as_str()),
            Some("Earlier conversation summary:\nthe gist")
        );
        assert_eq!(messages.len(), 2);
    }

    /// `SystemSuffix` merges the summary into the end of the system prompt:
    /// one message, no placeholder, no separate slot.
    #[test]
    fn system_suffix_placement_merges_the_summary_into_the_prompt() {
        let config = ChannelConfig {
            system_prompt: Some("custom prompt".to_owned()),
            ..ChannelConfig::assigned("m".to_owned())
        };
        let settings = settings_with_placement(SummaryPlacement::SystemSuffix);
        let state = ConversationState {
            summary: Some("the gist".to_owned()),
            cutoff_seq: 4,
            cutoff_at: Some(1_717_000_000),
        };
        let records = vec![user_record(10, "alice", "hello")];

        let messages = assemble_context(&config, &settings, &state, &records, 0.25, None);

        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages.first().map(|m| m.content.as_str()),
            Some("custom prompt\n\nEarlier conversation summary:\nthe gist")
        );
        assert_eq!(
            messages.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::User, "alice: hello"))
        );

        // Without a summary there is no placeholder either - just the prompt.
        let bare = assemble_context(
            &config,
            &settings,
            &ConversationState::default(),
            &records,
            0.25,
            None,
        );
        assert_eq!(bare.len(), 2);
        assert_eq!(bare.first().map(|m| m.content.as_str()), Some("custom prompt"));
    }

    /// `SystemTurn` (the default) keeps the separate summary slot: second
    /// system message, placeholder present even without a summary. Declared
    /// explicitly here to pin the placement resolution path.
    #[test]
    fn system_turn_placement_keeps_the_separate_slot() {
        let config = ChannelConfig::assigned("m".to_owned());
        let settings = settings_with_placement(SummaryPlacement::SystemTurn);
        let state = ConversationState {
            summary: Some("the gist".to_owned()),
            cutoff_seq: 4,
            cutoff_at: Some(1_717_000_000),
        };
        let records = vec![user_record(10, "alice", "hello")];

        let with_summary = assemble_context(&config, &settings, &state, &records, 0.25, None);
        assert_eq!(
            with_summary.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::System, "Earlier conversation summary:\nthe gist"))
        );

        let without_summary = assemble_context(
            &config,
            &settings,
            &ConversationState::default(),
            &records,
            0.25,
            None,
        );
        assert_eq!(
            without_summary.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::System, NO_EARLIER_CONTEXT))
        );
    }

    /// `AssistantTurn` posts the summary (or the placeholder) as the
    /// assistant's own message before the live window.
    #[test]
    fn assistant_turn_placement_posts_the_summary_as_assistant() {
        let config = ChannelConfig::assigned("m".to_owned());
        let settings = settings_with_placement(SummaryPlacement::AssistantTurn);
        let state = ConversationState {
            summary: Some("the gist".to_owned()),
            cutoff_seq: 4,
            cutoff_at: Some(1_717_000_000),
        };
        let records = vec![user_record(10, "alice", "hello")];

        let with_summary = assemble_context(&config, &settings, &state, &records, 0.25, None);
        assert_eq!(
            with_summary.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::Assistant, "Earlier conversation summary:\nthe gist"))
        );

        let without_summary = assemble_context(
            &config,
            &settings,
            &ConversationState::default(),
            &records,
            0.25,
            None,
        );
        assert_eq!(
            without_summary.get(1).map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::Assistant, NO_EARLIER_CONTEXT))
        );
    }

    #[test]
    fn custom_turn_template_is_substituted() {
        let config = ChannelConfig {
            turn_template: Some("<{sender}> {message}".to_owned()),
            ..ChannelConfig::assigned("m".to_owned())
        };
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        let records = vec![user_record(10, "alice", "hello")];

        let messages = assemble_context(&config, &settings, &state, &records, 0.25, None);

        assert_eq!(messages.get(2).map(|m| m.content.as_str()), Some("<alice> hello"));
    }

    #[test]
    fn images_render_as_markdown_references_in_user_turns() {
        let config = ChannelConfig::assigned("m".to_owned());
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        let mut record = user_record(10, "alice", "look at this");
        record.images = vec![
            RecordImage { description: Some("a tabby cat on a keyboard".to_owned()) },
            RecordImage { description: None },
        ];

        let messages = assemble_context(&config, &settings, &state, &[record], 0.25, None);

        assert_eq!(
            messages.get(2).map(|m| m.content.as_str()),
            Some(concat!(
                "alice: look at this\n",
                "![a tabby cat on a keyboard](image.png)\n",
                "![image](image_2.png)"
            ))
        );
    }

    #[test]
    fn image_descriptions_count_toward_the_context_budget() {
        let config = ChannelConfig::assigned("m".to_owned());
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        let mut padded = user_record(10, "alice", "old");
        padded.images = vec![RecordImage { description: Some("x".repeat(4000)) }];
        let records = vec![padded, user_record(11, "bob", "newest")];

        // A tight budget drops the image-padded record first - descriptions
        // are real prompt bytes and must not ride for free.
        let messages = assemble_context(&config, &settings, &state, &records, 0.25, Some(300));

        assert_eq!(messages.len(), 3); // system + placeholder + the newest turn
        assert!(messages.get(2).is_some_and(|m| m.content.ends_with("bob: newest")));
    }

    #[test]
    fn compaction_transcript_renders_images_like_the_context() {
        let mut record = user_record(10, "alice", "look");
        record.images = vec![RecordImage { description: Some("a dog".to_owned()) }];

        let messages = compaction_input("summarize", None, &[record]);

        assert_eq!(
            messages.get(1).map(|m| m.content.as_str()),
            Some("New messages:\nalice: look\n![a dog](image.png)\n")
        );
    }

    #[test]
    fn records_without_images_field_deserialize_from_old_logs() {
        let record: ConversationRecord = serde_json::from_value(serde_json::json!({
            "message_id": 10,
            "role": "user",
            "author": "alice",
            "content": "pre-feature message",
            "captured_at": 0
        }))
        .expect("old record shape expected to deserialize");
        assert!(record.images.is_empty());
    }

    /// Serializes the assembled context the way a provider prompt cache sees
    /// it: role + content per line, in order. Prefix stability of this string
    /// across message intake is the cache-viability contract.
    fn render(messages: &[ChatMessage]) -> String {
        let mut rendered = String::new();
        for message in messages {
            let role = match message.role {
                ChatRole::System => "system",
                ChatRole::User => "user",
                ChatRole::Assistant => "assistant",
            };
            let _ = writeln!(rendered, "{role}:{}", message.content);
        }
        rendered
    }

    fn no_compaction_config() -> ChannelConfig {
        let mut config = ChannelConfig::assigned("p/m".to_owned());
        config.compaction_enabled = false;
        config.history_depth = 100;
        config
    }

    /// Cache viability without compaction: appending captured user turns and
    /// produced bot turns only ever EXTENDS the assembled context - every
    /// earlier byte stays put (append-only growth), and the fixed slots
    /// (system prompt + summary placeholder) never move or change, so the
    /// provider's prompt cache keeps hitting.
    #[test]
    fn context_grows_append_only_within_depth() {
        let config = no_compaction_config();
        let settings = LlmSettings::default();
        let state = ConversationState::default();

        let steps = [
            user_record(1, "alice", "first question"),
            user_record(2, "bob", "second question"),
            assistant_record(3, "first answer"),
            user_record(4, "alice", "third — with ünicode ✓"),
            assistant_record(5, "second answer"),
        ];

        let mut previous = render(&assemble_context(&config, &settings, &state, &[], 0.0, None));
        for (index, _) in steps.iter().enumerate() {
            let visible = steps.get(..=index).expect("index below steps length");
            let current = render(&assemble_context(&config, &settings, &state, visible, 0.0, None));
            assert!(
                current.starts_with(previous.as_str()),
                "step {index} rewrote the context prefix:\n\
                 --- previous ---\n{previous}\n--- current ---\n{current}"
            );
            previous = current;
        }

        // Depth 100 and no budget: all five turns made it in after the two
        // fixed slots (system prompt + summary placeholder).
        assert_eq!(previous.lines().count(), 7);
    }

    /// Record metadata (message ids, reply targets, capture timestamps) and
    /// state metadata (summary bookkeeping, cutoff timestamp) are storage
    /// bookkeeping - none of it may leak into the rendered prompt, or every
    /// new capture would rewrite prefix bytes and kill the cache. User turns
    /// render to exactly the template over author+content; assistant turns
    /// pass through verbatim.
    #[test]
    fn rendered_context_carries_no_dynamic_record_fields() {
        let config = no_compaction_config();
        let settings = LlmSettings::default();

        let mut user = user_record(987_654_321, "alice", "what time is it");
        user.captured_at = 1_735_689_600;
        user.reply_to = Some(555);
        let mut bot = assistant_record(999_999_999, "noon");
        bot.captured_at = 1_735_689_601;
        let records = vec![user, bot];

        let state = ConversationState {
            summary: Some("earlier facts".to_owned()),
            cutoff_seq: 777_777,
            cutoff_at: Some(1_735_600_000),
        };

        let messages = assemble_context(&config, &settings, &state, &records, 0.0, None);
        assert_eq!(messages.len(), 4, "two fixed slots + two turns");
        assert!(matches!(messages.first().expect("system expected").role, ChatRole::System));
        assert!(matches!(messages.get(1).expect("summary expected").role, ChatRole::System));

        let rendered = render(&messages);
        assert!(rendered.contains("Earlier conversation summary:\nearlier facts"));
        assert!(rendered.contains("user:alice: what time is it\n"));
        assert!(rendered.contains("assistant:noon\n"));

        for leaked in ["987654321", "1735689600", "555", "999999999", "777777", "1735600000"] {
            assert!(
                !rendered.contains(leaked),
                "dynamic field {leaked} leaked into the prompt:\n{rendered}"
            );
        }
    }

    #[test]
    fn token_budget_fills_newest_first() {
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        // Ratio 1.0 -> per-turn costs: aaaa=12, bb=10, cccccc=14. The fixed
        // slots (system 33 chars + placeholder 20 chars, +8 overhead each)
        // consume 69 of the budget before any turn.
        let records = vec![
            user_record(1, "a1", "aaaa"),
            user_record(2, "a2", "bb"),
            user_record(3, "a3", "cccccc"),
        ];
        let config = ChannelConfig {
            history_depth: 10,
            context_budget_tokens: Some(93),
            ..ChannelConfig::assigned("m".to_owned())
        };

        let budget = resolve_budget(&config, &settings, true);
        let messages = assemble_context(&config, &settings, &state, &records, 1.0, budget);

        // 69 fixed + 14 (cccccc) + 10 (bb) = 93 exactly; aaaa would exceed.
        assert_eq!(messages.len(), 4);
        assert!(messages.get(2).expect("turn expected").content.contains("a2: bb"));
        assert!(messages.get(3).expect("turn expected").content.contains("a3: cccccc"));
    }

    #[test]
    fn token_budget_never_drops_the_newest_turn() {
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        let records = vec![user_record(1, "a1", "a very long message indeed")];
        let config = ChannelConfig {
            context_budget_tokens: Some(1),
            ..ChannelConfig::assigned("m".to_owned())
        };

        let budget = resolve_budget(&config, &settings, true);
        let messages = assemble_context(&config, &settings, &state, &records, 1.0, budget);

        // A reply must at least see what it answers.
        assert_eq!(messages.len(), 3);
        assert!(
            messages
                .get(2)
                .expect("turn expected")
                .content
                .contains("a1: a very long message indeed")
        );
    }

    #[test]
    fn model_window_derives_the_budget_but_only_when_calibrated() {
        let state = ConversationState::default();
        let mut settings = LlmSettings::default();
        settings.models.insert(
            "local/gemma".to_owned(),
            ModelSettings { reasoning: false, context_window: Some(2000), ..Default::default() },
        );
        // No channel override -> budget = window 2000 - reserve 1024 - 10%
        // margin (200) = 776. Ratio 1.0, fixed slots 68 -> 300-char turns
        // cost 308 each: two fit (68+308+308 = 684), the third (992) exceeds.
        let records = vec![
            user_record(1, "a1", &"x".repeat(300)),
            user_record(2, "a2", &"x".repeat(300)),
            user_record(3, "a3", &"x".repeat(300)),
            user_record(4, "a4", &"x".repeat(300)),
        ];
        let config = ChannelConfig {
            history_depth: 10,
            ..ChannelConfig::assigned("local/gemma".to_owned())
        };

        let budget = resolve_budget(&config, &settings, true);
        let calibrated = assemble_context(&config, &settings, &state, &records, 1.0, budget);
        assert_eq!(calibrated.len(), 4, "two fixed slots + two budgeted turns");

        // Uncalibrated: no usage data, so filling falls back to the message
        // limit - all four turns are present despite the declared window.
        let budget = resolve_budget(&config, &settings, false);
        let uncalibrated = assemble_context(&config, &settings, &state, &records, 1.0, budget);
        assert_eq!(uncalibrated.len(), 6);
    }

    #[test]
    fn resolve_budget_prefers_channel_overrides_model_and_gates_on_calibration() {
        let mut settings = LlmSettings::default();
        settings.models.insert(
            "local/gemma".to_owned(),
            ModelSettings { reasoning: false, context_window: Some(8000), ..Default::default() },
        );
        let mut config = ChannelConfig::assigned("local/gemma".to_owned());

        // No override, uncalibrated: message-count filling.
        assert_eq!(resolve_budget(&config, &settings, false), None);
        // Calibrated: window 8000 - reserve 1024 - 10% (800) = 6176.
        assert_eq!(resolve_budget(&config, &settings, true), Some(6176));

        // Explicit channel budget wins and does not need calibration.
        config.context_budget_tokens = Some(1500);
        assert_eq!(resolve_budget(&config, &settings, false), Some(1500));

        // Undeclared model: no window, no budget.
        let other = ChannelConfig::assigned("unknown/model".to_owned());
        assert_eq!(resolve_budget(&other, &settings, true), None);
    }

    #[test]
    fn compaction_input_carries_prompt_previous_summary_and_transcript() {
        let records = vec![
            user_record(10, "alice", "hello"),
            assistant_record(11, "hi alice"),
            ConversationRecord {
                message_id: Some(12),
                role: RecordRole::User,
                author: None,
                content: "who is there".to_owned(),
                reply_to: None,
                captured_at: 0,
                images: Vec::new(),
            },
        ];

        let messages = compaction_input("summarize", Some("old gist"), &records);

        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages.first().map(|m| (m.role, m.content.as_str())),
            Some((ChatRole::System, "summarize"))
        );
        let transcript = messages.get(1).map(|m| m.content.clone()).expect("transcript expected");
        assert!(transcript.contains("Previous summary:\nold gist"));
        assert!(transcript.contains("New messages:\n"));
        assert!(transcript.contains("alice: hello\n"));
        assert!(transcript.contains("assistant: hi alice\n"));
        assert!(transcript.contains("user: who is there\n"));
    }

    #[test]
    fn replies_fit_in_one_chunk() {
        assert_eq!(split_reply("hello world", 100), vec!["hello world"]);
        assert!(split_reply("", 100).is_empty());
    }

    #[test]
    fn lines_move_whole_to_the_next_chunk() {
        // "two" does not fit after "one" -> moves to chunk two, whole.
        let chunks = split_reply("one\ntwo", 5);
        assert_eq!(chunks, vec!["one", "two"]);
    }

    #[test]
    fn lines_are_packed_greedily() {
        let chunks = split_reply("aa\nbb\ncc", 6);
        assert_eq!(chunks, vec!["aa\nbb", "cc"]);
    }

    #[test]
    fn overlong_lines_are_hard_cut() {
        let chunks = split_reply("abcdef", 4);
        assert_eq!(chunks, vec!["abcd", "ef"]);
        // Hard cut across a newline boundary: the remainder continues the
        // line-by-line flow.
        let chunks = split_reply("ab\ncdefgh", 4);
        assert_eq!(chunks, vec!["ab", "cdef", "gh"]);
    }

    #[test]
    fn multibyte_characters_survive_splitting() {
        // 6 chars but 12 bytes; measured in characters.
        let chunks = split_reply("😀😀😀😀😀😀", 4);
        assert_eq!(chunks, vec!["😀😀😀😀", "😀😀"]);
    }

    #[test]
    fn zero_limit_degrades_to_one() {
        assert_eq!(split_reply("abc", 0), vec!["a", "b", "c"]);
    }
}
