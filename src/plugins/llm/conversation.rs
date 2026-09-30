//! Conversation domain logic: the record schema, capture/trigger rules,
//! context assembly, and reply splitting. Pure functions - no I/O - so the
//! rules stay unit-testable in isolation from storage and providers.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::completion_port::{ChatMessage, ChatRole};
use super::model::{CaptureMode, ChannelConfig, ConversationState};
use super::providers::LlmSettings;

/// Slot-2 placeholder: the summary position always exists, keeping turn
/// positions stable (early messages carry more weight with most models) and
/// the prompt prefix byte-stable for provider prompt caches.
pub const NO_EARLIER_CONTEXT: &str = "(no earlier context)";

/// Default rendering of user turns in the context.
pub const DEFAULT_TURN_TEMPLATE: &str = "{sender}: {message}";

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

/// Assembles the LLM context: fixed schema - the system prompt, then the
/// always-present summary slot (compacted context or placeholder), then the
/// live window. User turns render via the channel template; assistant turns
/// pass through raw (the role already says who spoke).
///
/// The window fills NEWEST-FIRST under two caps, whichever bites first:
/// `history_depth` (message count) and, when the channel sets
/// `context_budget_tokens`, the estimated token budget (per-turn cost =
/// framing + content chars x `tokens_per_char`, calibrated from the
/// endpoint's own usage reports). The newest turn is always included - a
/// reply must at least see what it answers.
pub fn assemble_context(
    config: &ChannelConfig,
    settings: &LlmSettings,
    state: &ConversationState,
    records: &[ConversationRecord],
    tokens_per_char: f64,
) -> Vec<ChatMessage> {
    let budget = config.context_budget_tokens.map(u64::from);
    let depth = usize::try_from(config.history_depth).unwrap_or(usize::MAX);
    let mut count = 0usize;
    let mut used: u64 = 0;
    while count < records.len() && count < depth {
        let Some(record) = records.get(records.len() - count - 1) else {
            break;
        };
        // Estimator: precision loss is fine.
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let cost =
            TURN_TOKEN_OVERHEAD + (record.content.chars().count() as f64 * tokens_per_char) as u64;
        if budget.is_some_and(|budget| used + cost > budget) && count > 0 {
            break;
        }
        used += cost;
        count += 1;
    }
    let (_, window) = records.split_at(records.len() - count);

    let mut messages = Vec::new();
    messages.push(ChatMessage {
        role: ChatRole::System,
        content: config
            .system_prompt
            .clone()
            .unwrap_or_else(|| settings.default_system_prompt.clone()),
    });
    messages.push(ChatMessage {
        role: ChatRole::System,
        content: match &state.summary {
            Some(summary) => format!("Earlier conversation summary:\n{summary}"),
            None => NO_EARLIER_CONTEXT.to_owned(),
        },
    });
    let template = config.turn_template.as_deref().unwrap_or(DEFAULT_TURN_TEMPLATE);
    for record in window {
        let message = match record.role {
            RecordRole::User => ChatMessage {
                role: ChatRole::User,
                content: template
                    .replace("{sender}", record.author.as_deref().unwrap_or("user"))
                    .replace("{message}", &record.content),
            },
            RecordRole::Assistant => {
                ChatMessage { role: ChatRole::Assistant, content: record.content.clone() }
            }
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
                record.content
            ),
            RecordRole::Assistant => writeln!(transcript, "assistant: {}", record.content),
        }
        .expect("writing to a String expected to be infallible");
    }
    vec![
        ChatMessage { role: ChatRole::System, content: prompt.to_owned() },
        ChatMessage { role: ChatRole::User, content: transcript },
    ]
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

    fn user_record(message_id: u64, author: &str, content: &str) -> ConversationRecord {
        ConversationRecord {
            message_id: Some(message_id),
            role: RecordRole::User,
            author: Some(author.to_owned()),
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
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
            },
        ];

        let messages = assemble_context(&config, &settings, &state, &records, 0.25);

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

        let messages = assemble_context(&config, &settings, &state, &[], 0.25);

        assert_eq!(messages.first().map(|m| m.content.as_str()), Some("custom prompt"));
        assert_eq!(
            messages.get(1).map(|m| m.content.as_str()),
            Some("Earlier conversation summary:\nthe gist")
        );
        assert_eq!(messages.len(), 2);
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

        let messages = assemble_context(&config, &settings, &state, &records, 0.25);

        assert_eq!(messages.get(2).map(|m| m.content.as_str()), Some("<alice> hello"));
    }

    #[test]
    fn token_budget_fills_newest_first() {
        let settings = LlmSettings::default();
        let state = ConversationState::default();
        // Ratio 1.0 + overhead 8 -> per-turn costs: aaaa=12, bb=10, cccccc=14.
        let records = vec![
            user_record(1, "a1", "aaaa"),
            user_record(2, "a2", "bb"),
            user_record(3, "a3", "cccccc"),
        ];
        let config = ChannelConfig {
            history_depth: 10,
            context_budget_tokens: Some(24),
            ..ChannelConfig::assigned("m".to_owned())
        };

        let messages = assemble_context(&config, &settings, &state, &records, 1.0);

        // Newest-first: cccccc (14) + bb (10) fill the budget exactly; aaaa
        // would exceed it and is dropped.
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

        let messages = assemble_context(&config, &settings, &state, &records, 1.0);

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
