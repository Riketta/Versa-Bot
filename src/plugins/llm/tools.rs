//! The LLM plugin's inline tool-call protocol. A model asks for a tool by
//! emitting a text marker anywhere in its answer: `[[name: payload]]`. The
//! extractor is generic; each supported tool is a dispatch arm with its own
//! payload grammar and degradation rules. A text protocol (not the OpenAI
//! `tools` wire API) is deliberate: it works on every OpenAI-compatible
//! endpoint regardless of chat-template tool support, keeps the completion
//! wire shape and conversation records untouched (the prefix stays
//! byte-stable for provider caches), and reuses the reasoning-strip
//! precedent for boundary parsing.
//!
//! Frozen protocol rules (AGENTS.md is authoritative):
//! - R1 shape: `[[name: payload]]` - lowercase snake name, payload is
//!   everything up to the first `]]` (payloads must not contain `]]`).
//! - R2 unknown names pass through untouched (visible, debug-logged): old
//!   parsers never eat future tools' markers.
//! - R3 markers are never revealed live (see [`MarkerHold`], the streaming
//!   suppressor); the final edit is the authoritative cleaned text.
//! - R4 the cleaned content is what is delivered and recorded - markers are
//!   ephemeral side effects, history stays plain user/assistant turns.
//! - R5 degradation is per-tool and per-item; a tool failure never fails the
//!   answer.
//! - R6 an unclosed marker stays visible as literal text (honest garbage
//!   beats silently eaten prose), mirroring the mid-text unclosed `<think>`.

use std::collections::HashSet;

/// The generic extracted form of one marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCall {
    pub name: String,
    pub payload: String,
}

/// The one tool taught today: emoji reactions on the message the bot is
/// replying to.
pub(crate) const REACT_TOOL: &str = "react";

/// Constant instruction block appended to the system prompt when a channel
/// has reactions enabled. Frozen bytes: the prompt prefix must stay stable
/// for provider prompt caches (a toggle flip costs one cache miss, the same
/// as any system prompt edit).
pub(crate) const REACT_TOOL_PROMPT: &str = "[[tool: reactions]]
You may react to the message you are replying to. To react, emit a marker line in your answer:
[[react: emoji]]
- Unicode (🤓) and custom emojis both work, separated by spaces.
- Custom emojis: bare (:name:) or the exact form the conversation shows (<:name:id>, <a:name:id>) - copying that exact form is the most reliable.
- Usually skip it, or pick ONE emoji that fits best; never more than 3.
- The marker is removed from your answer before it is shown; never mention it in text.";

/// Appended to the silent-react chime call only. That invocation is a
/// reaction decision, not a reply: without a dedicated instruction the
/// model answers conversationally, the prose is discarded, and markers
/// stay rare - the roll would mostly waste the call. Templates that drop
/// later system turns simply degrade to the ordinary context.
pub(crate) const REACT_CHIME_PROMPT: &str = "[[tool: reactions]]
This is not a reply turn: you are only choosing a reaction to the newest message.
Respond with a single [[react: emoji]] marker - or with nothing at all when no reaction fits.
Do not write conversational text; any prose is thrown away.";

/// Hard cap on markers processed per message - a runaway model cannot spin
/// the extractor (the react emoji cap applies after this).
const MAX_TOOL_CALLS: usize = 8;

/// Generic marker extraction per rules R1-R6. Returns the cleaned content
/// (markers consumed by known tools removed, unknown-name markers left in
/// place) plus every well-formed marker found. Unclosed markers stay in the
/// text. Callers dispatch on `name`; unknown names were already left in the
/// content by the extractor itself.
pub(crate) fn extract_tool_calls(content: &str) -> (String, Vec<ToolCall>) {
    if !content.contains("[[") {
        return (content.to_owned(), Vec::new());
    }
    // Pass 1: find the byte spans of consumed (known-tool) markers. Unknown
    // names and malformed shapes produce no span - their text stays put.
    let mut calls: Vec<ToolCall> = Vec::new();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut cursor = 0usize;
    let mut capped = false;
    while let Some(found) = content.get(cursor..).and_then(|rest| rest.find("[[")) {
        let abs = cursor + found;
        let candidate = content.get(abs..).unwrap_or("");
        match parse_marker(candidate) {
            Some((call, after)) => {
                let end = abs + (candidate.len() - after.len());
                if is_known_tool(&call.name) {
                    // The cap bounds parsing work, not correctness: markers
                    // beyond it are consumed but not recorded.
                    if calls.len() < MAX_TOOL_CALLS {
                        calls.push(call);
                    } else {
                        capped = true;
                    }
                    spans.push((abs, end));
                }
                cursor = end;
            }
            // R6: unclosed or malformed - literal text. Rescan after the
            // opening brackets so a later real marker is still found.
            None => {
                cursor = abs + 2;
            }
        }
    }
    if capped {
        tracing::debug!(
            cap = MAX_TOOL_CALLS,
            "tool marker cap reached - excess markers consumed unrecorded"
        );
    }
    if spans.is_empty() {
        return (content.to_owned(), calls);
    }
    // Pass 2: remove the spans, healing each junction - a marker that sat
    // alone on its line leaves no blank line; an inline marker keeps the
    // original single spacing (none when the model wrote none).
    let cleaned = remove_spans(content, &spans);
    (cleaned.trim().to_owned(), calls)
}

/// Removes whitespace-separated runs of spans as one, so adjacent markers
/// collapse into a single junction.
fn remove_spans(content: &str, spans: &[(usize, usize)]) -> String {
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for &(start, end) in spans {
        match merged.last_mut() {
            Some(last)
                if content
                    .get(last.1..start)
                    .is_some_and(|gap| gap.chars().all(|ch| ch == ' ' || ch == '\t')) =>
            {
                last.1 = end;
            }
            _ => merged.push((start, end)),
        }
    }

    let mut out = String::with_capacity(content.len());
    let mut cursor = 0usize;
    for (start, end) in &merged {
        out.push_str(content.get(cursor..*start).unwrap_or(""));
        // Left side of the junction: did the marker start its own line?
        let trimmed = out.trim_end_matches([' ', '\t']);
        let own_line_left = trimmed.is_empty() || trimmed.ends_with('\n');
        let gap_left = out.len() - trimmed.len();
        out.truncate(trimmed.len());
        // Right side: skip the marker's trailing spaces/tabs, then drop one
        // line break when the marker owned its line (the blank line must not
        // survive); inline, keep one space iff either side had a gap.
        let after = content.get(*end..).unwrap_or("");
        let after_ws = after.trim_start_matches([' ', '\t']);
        let gap_right = after.len() - after_ws.len();
        let line_break = after_ws
            .strip_prefix("\r\n")
            .map(|rest| (rest, 2))
            .or_else(|| after_ws.strip_prefix('\n').map(|rest| (rest, 1)));
        if own_line_left {
            if let Some((_rest, width)) = line_break {
                // The blank line collapses: `out` stays trimmed and the
                // final tail append continues after the dropped break.
                cursor = end + gap_right + width;
                continue;
            }
        } else {
            let right_is_line_break = after_ws.starts_with('\n') || after_ws.starts_with("\r\n");
            if !right_is_line_break && (gap_left > 0 || gap_right > 0) {
                out.push(' ');
            }
        }
        cursor = end + gap_right;
    }
    out.push_str(content.get(cursor..).unwrap_or(""));
    out
}

/// Parses one marker candidate at the start of `rest` (which begins with
/// `[[`). Returns the call plus the remainder after the closing `]]`, or
/// `None` when the shape is malformed or unclosed (R6).
fn parse_marker(rest: &str) -> Option<(ToolCall, &str)> {
    let body = rest.strip_prefix("[[")?;
    let mut name = String::new();
    let chars = body.char_indices();
    for (index, ch) in chars {
        if name.is_empty() && ch.is_ascii_lowercase() {
            name.push(ch);
        } else if !name.is_empty() && (ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        {
            name.push(ch);
        } else if ch == ':' && !name.is_empty() {
            let payload_start = index + 1;
            let payload_and_rest = body.get(payload_start..)?;
            let close = payload_and_rest.find("]]")?;
            let payload = payload_and_rest.get(..close).unwrap_or("").trim().to_owned();
            let after = payload_and_rest.get(close + 2..).unwrap_or("");
            return Some((ToolCall { name, payload }, after));
        } else {
            return None;
        }
    }
    None
}

/// Tools this build knows. Unknown names stay visible (R2).
fn is_known_tool(name: &str) -> bool {
    name == REACT_TOOL
}

/// Parses a react marker's payload into emoji tokens (R5 - per-item): valid
/// tokens keep their order, invalid ones are dropped individually. A token
/// is valid when it is a Discord custom form (`:name:`, `:name:id`,
/// `:name:id:`, `<:name:id>`, `<a:name:id>`) or contains at least one
/// non-ASCII character (covers every real emoji, including multi-codepoint
/// clusters with skin tones, ZWJ sequences and flags, while rejecting prose
/// words). Drops are debug-logged - a silent empty reaction list is
/// undiagnosable from the channel alone.
pub(crate) fn react_tokens(payload: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for token in payload.split_whitespace() {
        if is_reactable(token) {
            tokens.push(token.to_owned());
        } else {
            tracing::debug!(%token, "reaction token dropped - unrecognized emoji form");
        }
    }
    tokens
}

/// Union of the react markers' tokens, deduplicated in order and capped at
/// the plugin's `react_max_per_message` - the per-message safety net behind
/// the prompt's "never more than 3" guidance (Discord's own hard cap is 20).
pub(crate) fn react_tokens_from(calls: &[ToolCall], max: usize) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for call in calls.iter().filter(|call| call.name == REACT_TOOL) {
        for token in react_tokens(&call.payload) {
            if seen.insert(token.clone()) && tokens.len() < max {
                tokens.push(token);
            }
        }
    }
    tokens
}

fn is_reactable(token: &str) -> bool {
    if token.starts_with('<') {
        return parse_qualified_custom(token).is_some();
    }
    if token.starts_with(':') {
        return parse_custom_name(token).is_some();
    }
    token.chars().any(|ch| !ch.is_ascii())
}

/// `<:name:id>` / `<a:name:id>` - the fully qualified Discord form (the
/// shape inbound messages carry), directly usable without resolution.
fn parse_qualified_custom(token: &str) -> Option<()> {
    let body = token.strip_prefix('<')?.strip_suffix('>')?;
    let (animated, rest) = match body.strip_prefix("a:") {
        Some(rest) => (true, rest),
        None => (false, body.strip_prefix(':')?),
    };
    let _ = animated;
    validate_custom(rest)
}

/// `:name:`, `:name:id` or `:name:id:` - the bare custom form and the
/// trailing-colon blend models slip into between the bare and qualified
/// shapes; the adapter resolves the name against the origin guild's emojis
/// or sends the qualified id straight to Discord.
fn parse_custom_name(token: &str) -> Option<()> {
    let body = token.strip_prefix(':')?;
    // `:name:id:` - only strip the trailing colon when what remains is the
    // qualified form; bare `:name:` keeps its plain path.
    let body = match body.strip_suffix(':') {
        Some(stripped) if stripped.contains(':') => stripped,
        _ => body,
    };
    validate_custom(body)
}

/// Validates `name:` or `name:id` (id digits only, name Discord-shaped).
fn validate_custom(rest: &str) -> Option<()> {
    let (name, id) = rest.split_once(':')?;
    if id.contains(':') {
        return None;
    }
    if id.chars().next().is_some_and(|ch| !ch.is_ascii_digit()) {
        return None;
    }
    if name.is_empty() || !name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }
    Some(())
}

/// Bound on the streaming hold-back buffer: a candidate held longer than
/// this is released as literal text (a real marker with a >1 KiB payload is
/// a misbehaving model; honest garbage, the authoritative strip still
/// removes it from the final edit).
const MARKER_HOLD_LIMIT: usize = 1024;

/// Streaming half of the protocol (R3): never reveals a well-formed marker
/// in the live reveal. Holds back any `[[` + lowercase candidate until it
/// resolves - a complete `]]` closes and DROPS the held span (the final
/// authoritative edit shows whatever the extraction keeps), a non-marker
/// shape or the buffer bound releases it as literal text. Markers split
/// across delta boundaries stay hidden. Unknown-name markers are dropped
/// from the reveal too: the reveal may lag the final edit ("only ever
/// behind, never wrong"), which the authoritative text then corrects.
#[derive(Default)]
pub(crate) struct MarkerHold {
    held: String,
    /// `false` while passing text through, `true` from the first `[`.
    holding: bool,
}

impl MarkerHold {
    /// Consumes one delta chunk and returns the text safe to reveal.
    pub(crate) fn push(&mut self, delta: &str) -> String {
        let mut out = String::new();
        for ch in delta.chars() {
            if self.holding {
                self.held.push(ch);
                self.resolve(&mut out);
            } else if ch == '[' {
                self.holding = true;
                self.held.push(ch);
            } else {
                out.push(ch);
            }
        }
        out
    }

    /// Flushes at end of stream: an unclosed candidate is literal text (R6).
    pub(crate) fn finish(&mut self) -> String {
        std::mem::take(&mut self.held)
    }

    /// Re-evaluates the held candidate: releases it as text, drops it as a
    /// closed marker, or keeps holding.
    fn resolve(&mut self, out: &mut String) {
        let Some(after) = self.held.strip_prefix("[[") else {
            // A single `[` still waiting for its pair; anything else here is
            // already non-marker shape.
            if self.held.chars().count() > 1 {
                self.release(out);
            }
            return;
        };
        if after.is_empty() {
            return; // still just `[[` - the name decides.
        }
        if !after.chars().next().is_some_and(|ch| ch.is_ascii_lowercase()) {
            self.release(out);
            return;
        }
        if after.contains("]]") {
            // Closed marker - dropped from the reveal.
            self.held.clear();
            self.holding = false;
            return;
        }
        if self.held.chars().count() >= MARKER_HOLD_LIMIT {
            tracing::debug!(
                limit = MARKER_HOLD_LIMIT,
                "marker hold limit hit - candidate released as text"
            );
            self.release(out);
        }
    }

    /// Emits the held span as literal text and returns to pass-through.
    fn release(&mut self, out: &mut String) {
        out.push_str(&std::mem::take(&mut self.held));
        self.holding = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(content: &str) -> (String, Vec<ToolCall>) {
        extract_tool_calls(content)
    }

    #[test]
    fn content_without_markers_is_untouched() {
        assert_eq!(extract("plain text").0, "plain text");
        assert_eq!(extract("").0, "");
        let (cleaned, calls) = extract("[[[[[");
        assert_eq!(cleaned, "[[[[[");
        assert!(calls.is_empty());
    }

    #[test]
    fn own_line_marker_is_removed_without_a_blank_line() {
        let (cleaned, calls) = extract("nice message\n[[react: 🤓]]\nbye");
        assert_eq!(cleaned, "nice message\nbye");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls.first().map(|call| call.payload.as_str()), Some("🤓"));
    }

    #[test]
    fn inline_marker_joins_with_one_space() {
        let (cleaned, _) = extract("totally [[react: 🤓]] agree");
        assert_eq!(cleaned, "totally agree");
    }

    #[test]
    fn marker_at_content_edges_is_removed() {
        let (cleaned, _) = extract("[[react: 🤓]]\nthe answer\n[[react: 🐻]]");
        assert_eq!(cleaned, "the answer");
    }

    #[test]
    fn multiple_markers_are_all_extracted_in_order() {
        let (_, calls) = extract("[[react: 🤓]]\ntext\n[[react: 🐻 :robot:]]\n[[react: 🤓]]");
        assert_eq!(calls.len(), 3);
        assert_eq!(calls.first().map(|call| call.payload.as_str()), Some("🤓"));
        assert_eq!(calls.get(1).map(|call| call.payload.as_str()), Some("🐻 :robot:"));
    }

    #[test]
    fn empty_payload_marker_is_consumed_as_a_noop() {
        let (cleaned, calls) = extract("answer\n[[react:]]\nmore");
        assert_eq!(cleaned, "answer\nmore");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls.first().map(|call| call.payload.as_str()), Some(""));
    }

    #[test]
    fn no_space_after_colon_is_accepted() {
        let (cleaned, _) = extract("ok[[react:🤓]]!");
        assert_eq!(cleaned, "ok!");
    }

    #[test]
    fn unknown_tool_name_stays_visible() {
        let (cleaned, calls) = extract("look\n[[draw: a lighthouse]]\nend");
        assert_eq!(cleaned, "look\n[[draw: a lighthouse]]\nend");
        assert!(calls.is_empty());
    }

    #[test]
    fn unclosed_marker_stays_visible() {
        let (cleaned, calls) = extract("answer [[react: 🤓 and then the model stopped");
        assert_eq!(cleaned, "answer [[react: 🤓 and then the model stopped");
        assert!(calls.is_empty());
    }

    #[test]
    fn malformed_shapes_stay_visible_but_do_not_block_later_markers() {
        let (cleaned, calls) = extract("[[React: 🤓]]\n[[reactx ok\n[[react: 🐻]]");
        assert!(cleaned.contains("[[React: 🤓]]"));
        assert!(cleaned.contains("[[reactx ok"));
        assert_eq!(calls.len(), 1);
        assert_eq!(calls.first().map(|call| call.payload.as_str()), Some("🐻"));
    }

    #[test]
    fn uppercase_and_bracket_drift_stay_visible() {
        for drift in ["[react: 🤓]", "[[REACT: 🤓]]", "[[react]]", "[[react 🤓]]"] {
            let (cleaned, calls) = extract(drift);
            assert_eq!(cleaned, drift, "drift `{drift}` must stay visible");
            assert!(calls.is_empty(), "drift `{drift}` must not parse");
        }
    }

    #[test]
    fn marker_cap_bounds_extraction() {
        let content = "[[react: a]]".repeat(MAX_TOOL_CALLS + 5);
        let (_, calls) = extract(&content);
        assert_eq!(calls.len(), MAX_TOOL_CALLS);
    }

    #[test]
    fn payload_extends_to_the_first_double_bracket() {
        let (cleaned, calls) = extract("a\n[[react: 🤓]]trailing]]]]\nb");
        assert_eq!(cleaned, "a\ntrailing]]]]\nb");
        assert_eq!(calls.first().map(|call| call.payload.as_str()), Some("🤓"));
    }

    #[test]
    fn unicode_emoji_tokens_pass() {
        assert_eq!(react_tokens("🤓 🐻 👍🏽 👩‍🚀 🇺🇦 ☺️"), vec!["🤓", "🐻", "👍🏽", "👩‍🚀", "🇺🇦", "☺️"]);
    }

    #[test]
    fn custom_form_tokens_pass() {
        assert_eq!(
            react_tokens(":dorkiS: :robot_: <:bot:123> <a:spin:456> :id:789 :blend:42:"),
            vec![":dorkiS:", ":robot_:", "<:bot:123>", "<a:spin:456>", ":id:789", ":blend:42:"]
        );
    }

    #[test]
    fn invalid_tokens_are_dropped_individually() {
        let tokens = react_tokens("word :bad name: :: :ok: <nope> 42 🤓 :blend:x: :blend:1:2:");
        assert_eq!(tokens, vec![":ok:", "🤓"]);
    }

    #[test]
    fn react_tokens_from_unions_dedupes_and_caps() {
        let calls = vec![
            ToolCall { name: "react".to_owned(), payload: "🤓 🐻".to_owned() },
            ToolCall { name: "other".to_owned(), payload: "👻".to_owned() },
            ToolCall { name: "react".to_owned(), payload: "🐻 :dorkiS:".to_owned() },
        ];
        assert_eq!(react_tokens_from(&calls, 3), vec!["🤓", "🐻", ":dorkiS:"]);
        assert_eq!(react_tokens_from(&calls, 2), vec!["🤓", "🐻"]);
        assert_eq!(react_tokens_from(&[], 3), Vec::<String>::new());
    }

    #[test]
    fn marker_hold_never_reveals_closed_markers() {
        let reveal = |deltas: &[&str]| {
            let mut hold = MarkerHold::default();
            let mut out = String::new();
            for delta in deltas {
                out.push_str(&hold.push(delta));
            }
            out.push_str(&hold.finish());
            out
        };
        // One chunk.
        assert_eq!(reveal(&["hi [[react: 🤓]] bye"]), "hi  bye");
        // Split mid-marker.
        assert_eq!(reveal(&["a [[re", "act: 🤓]] b"]), "a  b");
        // Unknown-name markers are hidden from the reveal too.
        assert_eq!(reveal(&["x [[draw: lighthouse]] y"]), "x  y");
        // Unclosed candidate flushes as literal at finish (R6).
        assert_eq!(reveal(&["a [[react: 🤓"]), "a [[react: 🤓");
        // Non-marker brackets pass through.
        assert_eq!(reveal(&["a [b] [[42]] c"]), "a [b] [[42]] c");
        assert_eq!(reveal(&["[["]), "[[");
    }

    #[test]
    fn marker_hold_bound_releases_runaway_candidates() {
        let mut hold = MarkerHold::default();
        let mut out = String::new();
        out.push_str(&hold.push("[[react: "));
        let long = "x".repeat(MARKER_HOLD_LIMIT + 8);
        out.push_str(&hold.push(&long));
        assert!(out.contains('x'), "overlong candidate must be released as text");
    }
}
