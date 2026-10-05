//! System-prompt templates: `{{token}}` substitution with request-scoped
//! values (bot identity, time, platform, guild, model). Applies to the
//! system and compaction prompts only - channel overrides and operator
//! defaults alike; image-recognition prompts stay token-free so vision
//! payloads stay byte-identical across channels (provider caches).
//!
//! Rendering is request-scoped: `PromptVars` is built once per request and
//! every token sees the same snapshot. Tokens unknown to the renderer stay
//! literal (`{{oops}}` remains visible - typos are self-diagnosing), and
//! there is no escape syntax by design: prompts quoting the turn template's
//! single-brace fields (`{sender}`) are untouched.

use super::providers::LlmSettings;

/// Every token the renderer resolves. `/llm_set_prompt` rejects unknown
/// tokens with this list; boot warns about them in operator defaults.
pub(crate) const VALID_TOKENS: &[&str] =
    &["bot", "bot_name", "bot_id", "date", "weekday", "hour", "platform", "guild_name", "model"];

const OPEN: &str = "{{";
const CLOSE: &str = "}}";

/// Values substituted into one request's prompts. Time is snapshotted once,
/// so all time tokens agree even across an hour boundary mid-request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromptVars {
    /// `{{bot_name}}`: configured or adapter-discovered name; empty when
    /// unknown.
    pub bot_name: Option<String>,
    /// `{{bot_id}}`: configured or adapter-discovered id; empty when
    /// unknown.
    pub bot_id: Option<String>,
    /// `{{bot}}`: the identity in its graceful combined form - `name (id)`,
    /// whichever part exists, or empty. The bare-name/id tokens exist for
    /// authors who want to place the parts themselves.
    pub bot: String,
    /// `{{date}}`: `YYYY-MM-DD` at the configured UTC offset.
    pub date: String,
    /// `{{weekday}}`: English weekday name at the configured UTC offset.
    pub weekday: String,
    /// `{{hour}}`: the current hour bucket `HH:00-HH:59` - deliberately
    /// minute-free, so provider prompt caches rebuild at most once an hour.
    pub hour: String,
    /// `{{platform}}`: adapter-provided display name (`Discord`).
    pub platform: String,
    /// `{{guild_name}}`: current guild name; empty when the event carries
    /// none.
    pub guild_name: String,
    /// `{{model}}`: the model executing the prompt (the channel's chat
    /// model, or the compaction model for the compaction prompt).
    pub model: String,
}

impl PromptVars {
    /// Snapshots the request-scoped values. `timestamp_secs` is unix time;
    /// `offset_minutes` shifts date/weekday/hour away from UTC.
    pub fn new(
        bot_name: Option<String>,
        bot_id: Option<String>,
        timestamp_secs: i64,
        offset_minutes: i16,
        platform: &str,
        guild_name: Option<&str>,
        model: &str,
    ) -> Self {
        // The combined identity degrades gracefully: whatever part is known
        // is shown, and an unknown identity renders empty.
        let bot = match (&bot_name, &bot_id) {
            (Some(name), Some(id)) => format!("{name} ({id})"),
            (Some(name), None) => name.clone(),
            (None, Some(id)) => id.clone(),
            (None, None) => String::new(),
        };
        let (date, weekday, hour) = civil_parts(timestamp_secs, offset_minutes);
        Self {
            bot_name,
            bot_id,
            bot,
            date,
            weekday,
            hour,
            platform: platform.to_owned(),
            guild_name: guild_name.unwrap_or_default().to_owned(),
            model: model.to_owned(),
        }
    }

    /// Value for a token; `None` = unknown token (kept literal). Known
    /// tokens always resolve - an absent value is an empty string, never a
    /// literal `{{bot_name}}` left behind.
    fn resolve(&self, token: &str) -> Option<&str> {
        match token {
            "bot_name" => Some(self.bot_name.as_deref().unwrap_or_default()),
            "bot_id" => Some(self.bot_id.as_deref().unwrap_or_default()),
            "bot" => Some(&self.bot),
            "date" => Some(&self.date),
            "weekday" => Some(&self.weekday),
            "hour" => Some(&self.hour),
            "platform" => Some(&self.platform),
            "guild_name" => Some(&self.guild_name),
            "model" => Some(&self.model),
            _ => None,
        }
    }
}

/// Substitutes `{{token}}` occurrences with the request's values. Unknown
/// tokens and text outside well-formed `{{...}}` pairs pass through byte
/// for byte - a prompt without tokens renders as itself.
pub fn render_prompt(template: &str, vars: &PromptVars) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find(OPEN) {
        // `find` returns a match start - always a char boundary.
        out.push_str(rest.get(..start).unwrap_or_default());
        // "{{" is ASCII, so the bytes after it are on a boundary too.
        let Some(after_open) = rest.get(start + OPEN.len()..) else {
            break;
        };
        if let Some(end) = after_open.find(CLOSE) {
            let token = after_open.get(..end).unwrap_or_default();
            if let Some(value) = vars.resolve(token) {
                out.push_str(value);
            } else {
                out.push_str(OPEN);
                out.push_str(token);
                out.push_str(CLOSE);
            }
            rest = after_open.get(end + CLOSE.len()..).unwrap_or_default();
        } else {
            // Unclosed "{{": the rest is literal, braces included.
            out.push_str(rest.get(start..).unwrap_or_default());
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

/// Tokens appearing in the template that the renderer does not know.
/// Deduplicated, first-appearance order. Only well-formed `{{...}}` pairs
/// are scanned; a stray `{{` is not a token.
pub fn unknown_tokens(template: &str) -> Vec<&str> {
    let mut unknown: Vec<&str> = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find(OPEN) {
        let Some(after_open) = rest.get(start + OPEN.len()..) else {
            break;
        };
        match after_open.find(CLOSE) {
            Some(end) => {
                let token = after_open.get(..end).unwrap_or_default();
                if !VALID_TOKENS.contains(&token) && !unknown.contains(&token) {
                    unknown.push(token);
                }
                rest = after_open.get(end + CLOSE.len()..).unwrap_or_default();
            }
            None => break,
        }
    }
    unknown
}

/// Boot check for operator-configured defaults: unknown tokens warn (naming
/// each one) but never abort - a prompt is content, and unknown tokens
/// render literally and visibly in the context.
pub fn warn_unknown_prompt_tokens(settings: &LlmSettings) {
    for (kind, template) in [
        ("system", settings.default_system_prompt.as_str()),
        ("compaction", settings.default_compaction_prompt.as_str()),
    ] {
        for token in unknown_tokens(template) {
            tracing::warn!(
                prompt = kind,
                token,
                "unknown template token - it renders literally; valid: {}",
                VALID_TOKENS.join(", ")
            );
        }
    }
}

/// Civil time parts at the given UTC offset: `YYYY-MM-DD`, English weekday,
/// hour bucket. All three derive from the same shifted instant, so they
/// never disagree across a midnight or year boundary.
fn civil_parts(timestamp_secs: i64, offset_minutes: i16) -> (String, String, String) {
    let utc = time::OffsetDateTime::from_unix_timestamp(timestamp_secs)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let local = utc.checked_add(time::Duration::minutes(i64::from(offset_minutes))).unwrap_or(utc);
    let (year, month, day) = local.to_calendar_date();
    let (year, month, day) = (year, u8::from(month), day);
    let hour = local.hour();
    (
        format!("{year:04}-{month:02}-{day:02}"),
        weekday_name(local.weekday()).to_owned(),
        format!("{hour:02}:00-{hour:02}:59"),
    )
}

fn weekday_name(weekday: time::Weekday) -> &'static str {
    match weekday {
        time::Weekday::Monday => "Monday",
        time::Weekday::Tuesday => "Tuesday",
        time::Weekday::Wednesday => "Wednesday",
        time::Weekday::Thursday => "Thursday",
        time::Weekday::Friday => "Friday",
        time::Weekday::Saturday => "Saturday",
        time::Weekday::Sunday => "Sunday",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-01-31 23:30:00 UTC (a Saturday, one half hour before a month
    /// and weekday boundary - good for offset-shift assertions).
    const JAN_31_2330_UTC: i64 = 1_769_902_200;

    #[test]
    fn every_token_resolves() {
        let vars = PromptVars::new(
            Some("VersaBot".to_owned()),
            Some("42".to_owned()),
            JAN_31_2330_UTC,
            0,
            "Discord",
            Some("Crafters"),
            "zai/glm",
        );
        let rendered = render_prompt(
            "{{bot}}|{{bot_name}}|{{bot_id}}|{{date}}|{{weekday}}|{{hour}}|{{platform}}|\
             {{guild_name}}|{{model}}",
            &vars,
        );
        assert_eq!(
            rendered,
            "VersaBot (42)|VersaBot|42|2026-01-31|Saturday|23:00-23:59|Discord|Crafters|zai/glm"
        );
    }

    #[test]
    fn bot_identity_degrades_part_by_part() {
        let both = PromptVars::new(
            Some("VersaBot".to_owned()),
            Some("42".to_owned()),
            JAN_31_2330_UTC,
            0,
            "Test",
            None,
            "m",
        );
        assert_eq!(render_prompt("[{{bot}}]", &both), "[VersaBot (42)]");

        let name_only = PromptVars::new(
            Some("VersaBot".to_owned()),
            None,
            JAN_31_2330_UTC,
            0,
            "Test",
            None,
            "m",
        );
        assert_eq!(render_prompt("[{{bot}}]", &name_only), "[VersaBot]");
        assert_eq!(render_prompt("[{{bot_id}}]", &name_only), "[]");

        let id_only =
            PromptVars::new(None, Some("42".to_owned()), JAN_31_2330_UTC, 0, "Test", None, "m");
        assert_eq!(render_prompt("[{{bot}}]", &id_only), "[42]");

        let neither = PromptVars::new(None, None, JAN_31_2330_UTC, 0, "Test", None, "m");
        assert_eq!(render_prompt("[{{bot}}]", &neither), "[]");
    }

    #[test]
    fn offset_shifts_date_across_midnight() {
        // UTC 23:30 Saturday + 180 minutes = Sunday, next month.
        let vars = PromptVars::new(None, None, JAN_31_2330_UTC, 180, "Test", None, "m");
        assert_eq!(vars.date, "2026-02-01");
        assert_eq!(vars.weekday, "Sunday");
        assert_eq!(vars.hour, "02:00-02:59");
        // UTC stays exact at offset 0.
        let utc = PromptVars::new(None, None, JAN_31_2330_UTC, 0, "Test", None, "m");
        assert_eq!(utc.date, "2026-01-31");
        assert_eq!(utc.weekday, "Saturday");
        assert_eq!(utc.hour, "23:00-23:59");
    }

    #[test]
    fn unknown_tokens_stay_literal() {
        let vars = PromptVars::new(
            Some("VersaBot".to_owned()),
            None,
            JAN_31_2330_UTC,
            0,
            "Test",
            None,
            "m",
        );
        assert_eq!(render_prompt("{{oops}} {{nme}}", &vars), "{{oops}} {{nme}}");
        // Empty tokens are not tokens either.
        assert_eq!(render_prompt("{{}}", &vars), "{{}}");
        // An unclosed brace keeps the rest byte for byte.
        assert_eq!(render_prompt("hi {{bot", &vars), "hi {{bot");
        // Single braces (turn-template syntax) are not substituted.
        assert_eq!(
            render_prompt("[{sender}](<@{user_id}>): {message}", &vars),
            "[{sender}](<@{user_id}>): {message}"
        );
    }

    #[test]
    fn token_free_prompts_render_as_themselves() {
        let vars = PromptVars::default();
        assert_eq!(render_prompt("plain prompt", &vars), "plain prompt");
    }

    #[test]
    fn repeated_and_adjacent_tokens_resolve_independently() {
        let vars = PromptVars::new(
            Some("A".to_owned()),
            Some("B".to_owned()),
            JAN_31_2330_UTC,
            0,
            "Test",
            None,
            "m",
        );
        assert_eq!(render_prompt("{{bot_name}}{{bot_id}} {{bot_name}}", &vars), "AB A");
    }

    #[test]
    fn unknown_token_scan_dedupes_and_skips_unclosed() {
        assert_eq!(unknown_tokens("{{a}} {{b}} {{a}} {{"), vec!["a", "b"]);
        assert!(unknown_tokens("no tokens here").is_empty());
        assert!(unknown_tokens("{{bot}} {{date}}").is_empty());
    }

    #[test]
    fn warn_helper_covers_both_defaults() {
        // Compile-level smoke: the helper reads exactly the two defaults.
        let settings = LlmSettings {
            default_system_prompt: "You are {{bot}} {{wat}}.".to_owned(),
            ..LlmSettings::default()
        };
        assert_eq!(unknown_tokens(&settings.default_system_prompt), vec!["wat"]);
        assert!(unknown_tokens(&settings.default_compaction_prompt).is_empty());
    }
}
