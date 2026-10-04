//! `/llm_*` command handlers: channel assignment and service-channel
//! assignment. Meaning lives in the owning plugin; handlers run inside the
//! pipeline with the event itself, so channel-anchored assignment ("run me
//! in the channel to assign") needs no platform channel types.

use std::sync::Arc;

use async_trait::async_trait;

use crate::common::command_reply;
use crate::kernel::{
    models::{Embed, MessageId, OutboundMessage, RequestContext},
    plugin_ports::{CommandArgs, CommandHandler},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::chat_engine::ChatEngine;
use super::conversation::ConversationRecord;
use super::llm_plugin::ChannelLocks;
use super::model::{
    CaptureMode, ChannelConfig, ConversationState, NAMESPACE, SERVICE_CHANNEL_KEY, UsageStats,
    channel_config_key, channel_state_key, channel_stats_key, default_random_cooldown,
    records_namespace, unix_now,
};
use super::providers::{LlmSettings, ModelSettings};
use super::vision::DEFAULT_IMAGE_PROMPT;

/// Discord's hard cap for one text message - the reply splitter must never
/// produce chunks beyond it (the platform rejects them outright).
pub const DISCORD_MESSAGE_LIMIT: usize = 2000;

/// Recognized `/llm_set` keys, in display order. Doubles as the Discord
/// choices dropdown for the `key` argument.
pub(super) const SET_KEYS: &[&str] = &[
    "model",
    "temperature",
    "top_p",
    "top_k",
    "min_p",
    "frequency_penalty",
    "presence_penalty",
    "max_tokens",
    "reasoning_effort",
    "depth",
    "context_budget",
    "streaming",
    "random_chance",
    "random_cooldown",
    "capture_mode",
    "compaction",
    "compaction_model",
    "compaction_prompt",
    "images",
    "image_model",
    "image_prompt",
    "react",
    "random_chance",
    "random_cooldown",
    "random_react_chance",
    "capture_mode",
    "compaction",
    "compaction_model",
    "compaction_prompt",
    "max_length",
    "turn_template",
];

/// Loads the channel's config for mutation, replying (and returning `None`)
/// when the context is a DM or the channel is unassigned.
async fn load_assigned_config(
    event: &RequestContext,
    services: &KernelServices,
) -> anyhow::Result<Option<ChannelConfig>> {
    let Some(storage) = &services.guild_storage else {
        services
            .chat_output
            .send(command_reply("This command only works inside a server."))
            .await?;
        return Ok(None);
    };
    let Some(raw) =
        storage.get(NAMESPACE, &channel_config_key(event.origin.channel_id.get())).await?
    else {
        services
            .chat_output
            .send(command_reply("LLM chat is not assigned to this channel."))
            .await?;
        return Ok(None);
    };
    Ok(Some(serde_json::from_value(raw)?))
}

async fn save_config(
    event: &RequestContext,
    services: &KernelServices,
    config: ChannelConfig,
) -> anyhow::Result<()> {
    let Some(storage) = &services.guild_storage else {
        return Ok(()); // unreachable: load_assigned_config already guarded
    };
    storage
        .set(
            NAMESPACE,
            &channel_config_key(event.origin.channel_id.get()),
            serde_json::to_value(&config)?,
        )
        .await?;
    Ok(())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

/// Applies one `/llm_set` mutation. Pure so the key grammar stays unit-
/// testable: `Ok(message)` describes the change, `Err(usage)` is the reply
/// for an unknown key or malformed value.
fn apply_set(config: &mut ChannelConfig, key: &str, value: &str) -> Result<String, String> {
    let cleared = matches!(value, "clear" | "none" | "default");
    if let Some(result) = apply_numeric(config, key, value, cleared) {
        return result;
    }
    if let Some(result) = apply_optional_field(config, key, value, cleared) {
        return result;
    }
    if let Some(result) = apply_flag(config, key, value) {
        return result;
    }
    match key {
        "model" if !cleared && !value.is_empty() => {
            config.model = value.to_string();
            Ok(format!("`model` set to `{value}`."))
        }
        "reasoning_effort" => {
            if cleared {
                config.params.reasoning_effort = None;
                return Ok(
                    "`reasoning_effort` cleared (no reasoning parameter is sent - the provider's \
                     default applies)."
                        .to_owned(),
                );
            }
            if value == "off" {
                // Distinct from a reset: `off` is a real choice. Thinking-
                // switch providers (glm_thinking style) get an explicit
                // `thinking: disabled`; effort-style endpoints have no off
                // wire value, so their default applies - on Z.ai GLM that
                // default is heavy thinking (GLM-5.3: minimum is `low`).
                config.params.reasoning_effort = Some("off".to_owned());
                return Ok(
                    "`reasoning_effort` off: thinking-switch providers get an explicit disable; \
                     effort-style endpoints have no off value, so their default applies (on Z.ai \
                     GLM the default is heavy thinking - `low` is the minimum for GLM-5.3)."
                        .to_owned(),
                );
            }
            config.params.reasoning_effort = Some(value.to_owned());
            Ok(format!(
                "`reasoning_effort` set to `{value}` (sent only if the model declares reasoning support)."
            ))
        }
        "capture_mode" => {
            let mode: CaptureMode =
                serde_json::from_value(serde_json::json!(value)).map_err(|_| {
                    format!("`capture_mode` expects bot_related or all_messages, got `{value}`.")
                })?;
            config.capture_mode = mode;
            Ok(format!("`capture_mode` set to `{value}`."))
        }
        "model" | "depth" => {
            Err(format!("`{key}` cannot be cleared - assign a value or use `/llm_unassign`."))
        }
        other => Err(format!("Unknown key `{other}`. Keys: {}.", SET_KEYS.join(", "))),
    }
}

/// On/off settings. `None` = the key is not a flag (caller continues
/// matching). A recognized flag key with an unparseable value claims the key
/// and answers with usage - falling through would misreport it as unknown.
fn apply_flag(
    config: &mut ChannelConfig,
    key: &str,
    value: &str,
) -> Option<Result<String, String>> {
    if !matches!(key, "streaming" | "compaction" | "images" | "react") {
        return None;
    }
    let Some(enabled) = parse_bool(value) else {
        return Some(Err(format!("`{key}` expects on or off, got `{value}`.")));
    };
    match key {
        "streaming" => {
            config.streaming = enabled;
            Some(Ok(format!("`streaming` turned {}.", if enabled { "on" } else { "off" })))
        }
        "compaction" => {
            config.compaction_enabled = enabled;
            Some(Ok(format!("`compaction` turned {}.", if enabled { "on" } else { "off" })))
        }
        "images" => {
            config.images = enabled;
            Some(Ok(format!(
                "`images` turned {} (recognition needs an operator-configured image model).",
                if enabled { "on" } else { "off" }
            )))
        }
        "react" => {
            config.react = enabled;
            Some(Ok(format!(
                "`react` turned {} (the model may add emoji reactions to the message it replies to).",
                if enabled { "on" } else { "off" }
            )))
        }
        _ => None,
    }
}

/// Numeric settings: sampling floats, token counts, depth, budget, chance.
/// `None` = the key is not numeric (caller continues matching).
fn apply_numeric(
    config: &mut ChannelConfig,
    key: &str,
    value: &str,
    cleared: bool,
) -> Option<Result<String, String>> {
    match key {
        "temperature" | "top_p" | "top_k" | "min_p" | "frequency_penalty" | "presence_penalty" => {
            if cleared {
                set_float(config, key, None);
                return Some(Ok(format!("`{key}` cleared (provider default).")));
            }
            match value.parse::<f64>() {
                // NaN/infinity would serialize as JSON null (breaking the
                // provider request and silently vanishing on reload) -
                // reject instead of storing a silently broken value.
                Ok(parsed) if parsed.is_finite() => {
                    set_float(config, key, Some(parsed));
                    Some(Ok(format!("`{key}` set to {parsed}.")))
                }
                _ => Some(Err(format!("`{key}` expects a finite number, got `{value}`."))),
            }
        }
        "max_tokens" => {
            if cleared {
                config.params.max_tokens = None;
                return Some(Ok(format!("`{key}` cleared (provider default).")));
            }
            match value.parse::<u32>() {
                Ok(parsed) => {
                    config.params.max_tokens = Some(parsed);
                    Some(Ok(format!("`{key}` set to {parsed}.")))
                }
                Err(_) => Some(Err(format!("`{key}` expects a whole number, got `{value}`."))),
            }
        }
        "depth" => {
            if cleared {
                return Some(Err(format!(
                    "`{key}` cannot be cleared - assign a value or use `/llm_unassign`."
                )));
            }
            match value.parse::<u32>() {
                Ok(0) => Some(Err("`depth` must be at least 1.".to_owned())),
                Ok(parsed) => {
                    config.history_depth = parsed;
                    Some(Ok(format!("`depth` set to {parsed} messages.")))
                }
                Err(_) => Some(Err(format!("`depth` expects a whole number, got `{value}`."))),
            }
        }
        "context_budget" => {
            if cleared {
                config.context_budget_tokens = None;
                return Some(Ok(format!("`{key}` cleared (count-only filling).")));
            }
            match value.parse::<u32>() {
                Ok(0) => Some(Err(format!(
                    "`{key}` must be at least 1 (0 would keep only the newest turn)."
                ))),
                Ok(parsed) => {
                    config.context_budget_tokens = Some(parsed);
                    Some(Ok(format!("`{key}` set to {parsed} tokens.")))
                }
                Err(_) => {
                    Some(Err(format!("`{key}` expects a whole number of tokens, got `{value}`.")))
                }
            }
        }
        "random_chance" => {
            if cleared {
                config.random_chance_percent = 0.0;
                return Some(Ok("`random_chance` cleared (chime-ins off).".to_owned()));
            }
            match value.parse::<f64>() {
                // Non-finite clamps to NaN, not to the range bounds.
                Ok(parsed) if parsed.is_finite() => {
                    let clamped = parsed.clamp(0.0, 100.0);
                    config.random_chance_percent = clamped;
                    Some(Ok(format!("`random_chance` set to {clamped}.")))
                }
                _ => Some(Err(format!(
                    "`random_chance` expects a finite number (percent), got `{value}`."
                ))),
            }
        }
        "random_react_chance" => {
            if cleared {
                config.random_react_chance_percent = 0.0;
                return Some(
                    Ok("`random_react_chance` cleared (silent reactions off).".to_owned()),
                );
            }
            match value.parse::<f64>() {
                Ok(parsed) if parsed.is_finite() => {
                    let clamped = parsed.clamp(0.0, 100.0);
                    config.random_react_chance_percent = clamped;
                    Some(Ok(format!("`random_react_chance` set to {clamped}.")))
                }
                _ => Some(Err(format!(
                    "`random_react_chance` expects a finite number (percent), got `{value}`."
                ))),
            }
        }
        "random_cooldown" => {
            if cleared {
                let default = default_random_cooldown();
                config.random_cooldown_secs = default;
                return Some(Ok(format!("`{key}` cleared (default {default} seconds).")));
            }
            match value.parse::<u64>() {
                Ok(parsed) => {
                    config.random_cooldown_secs = parsed;
                    Some(Ok(format!("`{key}` set to {parsed} seconds.")))
                }
                Err(_) => {
                    Some(Err(format!("`{key}` expects a whole number of seconds, got `{value}`.")))
                }
            }
        }
        _ => None,
    }
}

/// Optional text/size fields that `clear` resets to plugin defaults.
/// `None` = the key is not one of them (caller continues matching).
fn apply_optional_field(
    config: &mut ChannelConfig,
    key: &str,
    value: &str,
    cleared: bool,
) -> Option<Result<String, String>> {
    match key {
        // Text/size fields sharing the set/clear shape.
        "compaction_model" | "compaction_prompt" | "image_model" | "image_prompt" => {
            if cleared {
                match key {
                    "compaction_model" => config.compaction_model = None,
                    "image_model" => config.image_model = None,
                    "image_prompt" => config.image_prompt = None,
                    _ => config.compaction_prompt = None,
                }
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            match key {
                "compaction_model" => {
                    config.compaction_model = Some(value.to_owned());
                    Some(Ok(format!("`{key}` set to `{value}`.")))
                }
                "image_model" => {
                    config.image_model = Some(value.to_owned());
                    Some(Ok(format!(
                        "`{key}` set to `{value}` (describes images for this channel)."
                    )))
                }
                "image_prompt" => {
                    config.image_prompt = Some(value.to_owned());
                    Some(Ok(format!("`{key}` updated (descriptions follow this instruction).")))
                }
                _ => {
                    config.compaction_prompt = Some(value.to_owned());
                    Some(Ok(format!("`{key}` updated.")))
                }
            }
        }
        "max_length" => {
            if cleared {
                config.max_length = None;
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            match value.parse::<usize>() {
                Ok(0) => Some(Err(format!("`{key}` must be at least 1."))),
                // Discord rejects longer text messages outright - a bigger
                // limit would only turn split replies into undelivered
                // chunks.
                Ok(n) if n > DISCORD_MESSAGE_LIMIT => Some(Err(format!(
                    "`{key}` cannot exceed {DISCORD_MESSAGE_LIMIT} (Discord's message limit)."
                ))),
                Ok(characters) => {
                    config.max_length = Some(characters);
                    Some(Ok(format!("`{key}` set to {characters} characters.")))
                }
                Err(_) => Some(Err(format!("`{key}` expects a whole number, got `{value}`."))),
            }
        }
        "turn_template" => Some(if cleared {
            config.turn_template = None;
            Ok(format!("`{key}` cleared (`[{{sender}}](<@{{user_id}}>): {{message}}` applies)."))
        } else if !value.contains("{sender}") || !value.contains("{message}") {
            Err(format!("`{key}` must contain `{{sender}}` and `{{message}}`, got `{value}`."))
        } else {
            config.turn_template = Some(value.to_owned());
            Ok(format!("`{key}` set to `{value}`."))
        }),
        _ => None,
    }
}

fn set_float(config: &mut ChannelConfig, key: &str, value: Option<f64>) {
    let params = &mut config.params;
    match key {
        "temperature" => params.temperature = value,
        "top_p" => params.top_p = value,
        "top_k" => params.top_k = value,
        "min_p" => params.min_p = value,
        "frequency_penalty" => params.frequency_penalty = value,
        "presence_penalty" => params.presence_penalty = value,
        _ => {}
    }
}

/// `/llm_assign`: assigns the chat bot to the channel the command is run in,
/// with the given provider/model reference. Re-assigning retunes in place.
/// Discord caps one option's `choices` at 25 entries.
pub(super) const MAX_DISCORD_CHOICES: usize = 25;

/// Declared model refs for the `/llm_assign` dropdown: registry order
/// (sorted), truncated to Discord's choice cap. The dropdown is discovery
/// only - runtime validation enforces the full registry regardless of the
/// truncation.
pub(super) fn model_choices(settings: &LlmSettings) -> Vec<String> {
    let mut refs: Vec<String> = settings.models.keys().cloned().collect();
    if refs.len() > MAX_DISCORD_CHOICES {
        tracing::warn!(
            total = refs.len(),
            cap = MAX_DISCORD_CHOICES,
            "declared models exceed Discord's choice cap - dropdown truncated; \
             runtime validation still accepts every declared ref"
        );
        refs.truncate(MAX_DISCORD_CHOICES);
    }
    refs
}

/// Assignments accept only operator-declared models: guild admins choose
/// among declared refs, so guild config can never introduce an endpoint or
/// an alias. `Err(reply)` is the ephemeral correction.
pub(super) fn validate_model_ref(settings: &LlmSettings, model: &str) -> Result<(), String> {
    if settings.models.contains_key(model) {
        return Ok(());
    }
    let declared: Vec<&str> = settings.models.keys().map(String::as_str).collect();
    let list = if declared.is_empty() {
        "nothing is declared in `[llm.models]` - ask the operator".to_owned()
    } else {
        format!("Declared models: {}.", declared.join(", "))
    };
    Err(format!("Unknown model `{model}` - only declared models can be assigned. {list}"))
}

/// One catalog line: the ref plus the declared capabilities that actually
/// change behavior (reasoning acceptance, context window).
fn model_catalog_line(reference: &str, model: &ModelSettings) -> String {
    let mut parts = vec![format!("`{reference}`")];
    if model.reasoning {
        parts.push("reasoning".to_owned());
    }
    if let Some(window) = model.context_window {
        parts.push(format!("{window} token context"));
    }
    parts.join(" - ")
}

/// `/llm_assign`: the channel the command is run in becomes a chat channel
/// for the given model. Only operator-declared models are legal - the
/// registry is the guild-facing isolation boundary, so a typo cannot
/// silently run with default capabilities.
pub(super) struct AssignLlmHandler {
    engine: Arc<ChatEngine>,
}

impl AssignLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for AssignLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };
        let Some(model) = args.get("model") else {
            services
                .chat_output
                .send(command_reply("Usage: `/llm_assign model` - e.g. `local/gemma`."))
                .await?;
            return Ok(());
        };
        if let Err(reply) = validate_model_ref(self.engine.settings(), model) {
            services.chat_output.send(command_reply(reply)).await?;
            return Ok(());
        }

        let config = ChannelConfig::assigned(model.to_owned());
        storage
            .set(
                NAMESPACE,
                &channel_config_key(event.origin.channel_id.get()),
                serde_json::to_value(&config)?,
            )
            .await?;

        services
            .chat_output
            .send(command_reply(format!("LLM chat assigned to this channel (model `{model}`).")))
            .await?;
        Ok(())
    }
}

/// `/llm_unassign`: removes the channel's chat configuration (idempotent -
/// conversation records and state are kept, only the assignment goes).
pub(super) struct UnassignLlmHandler;

#[async_trait]
impl CommandHandler for UnassignLlmHandler {
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

        storage.delete(NAMESPACE, &channel_config_key(event.origin.channel_id.get())).await?;

        services.chat_output.send(command_reply("LLM chat unassigned for this channel.")).await?;
        Ok(())
    }
}

/// `/llm_cutoff`: resets the channel's conversation context - the cutoff
/// moves past every existing record and the summary clears, so the next
/// answer starts fresh. Stored history is kept (records are never deleted);
/// this is a context reset, not a history wipe. The mutation runs under the
/// channel's processing lock: an in-flight engine run must not commit an
/// older state (compaction) over the fresh cutoff.
pub(super) struct CutoffLlmHandler {
    locks: Arc<ChannelLocks>,
}

impl CutoffLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>) -> Self {
        Self { locks }
    }
}

#[async_trait]
impl CommandHandler for CutoffLlmHandler {
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
        let channel = self.locks.lock_for(&event.origin);
        let _channel = channel.lock().await;

        let channel_id = event.origin.channel_id.get();
        let records_ns = records_namespace(channel_id);
        // The cutoff moves past the NEWEST SEQUENCE (read from the log
        // itself, not inferred from the count - sequence numbering stays
        // correct even if retention/deletion ever exists). Safe only under
        // the channel lock (no concurrent appends).
        let newest =
            storage.list_last(&records_ns, 1).await?.first().map_or(0, |record| record.seq);
        let state =
            ConversationState { summary: None, cutoff_seq: newest, cutoff_at: Some(unix_now()) };
        storage
            .set(NAMESPACE, &channel_state_key(channel_id), serde_json::to_value(&state)?)
            .await?;

        services
            .chat_output
            .send(command_reply(
                "Context cleared: this channel starts a fresh conversation (stored history is kept).",
            ))
            .await?;
        Ok(())
    }
}

/// Truncates for display, character-counted.
fn preview(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else {
        format!("{}…", text.chars().take(max_chars).collect::<String>())
    }
}

/// The token-usage lines of `/llm_status`: the calibrated live-window
/// estimate and the last request's reported usage (both appear only once
/// the endpoint has reported real usage - before that the default ratio is
/// an uncalibrated guess and showing it would be noise), plus the last
/// response's complete time, which is kept for every completed answer.
async fn usage_lines(
    storage: &Arc<dyn GuildStorage>,
    records_ns: &str,
    state: &ConversationState,
    live: u64,
    stats: &UsageStats,
) -> UsageLines {
    let last_response = stats.last_timing.map(|timing| {
        format!(
            "Last response: {} ({})",
            format_response_ms(timing.total_ms),
            if timing.endpoint_reported { "endpoint" } else { "measured" },
        )
    });
    let Some(last) = stats.last else {
        return UsageLines { estimate: None, last_request: None, last_response };
    };

    let mut context_chars = 0u64;
    let limit = u32::try_from(live).unwrap_or(u32::MAX);
    for stored in storage.list_after(records_ns, state.cutoff_seq, limit).await.unwrap_or_default()
    {
        if let Ok(record) = serde_json::from_value::<ConversationRecord>(stored.payload) {
            let counted = record.content.chars().count();
            // estimator: precision loss is fine
            #[allow(clippy::cast_precision_loss)]
            let counted = counted as u64;
            context_chars += counted;
        }
    }
    // estimator: precision loss is fine
    #[allow(clippy::cast_precision_loss)]
    let estimated = stats.tokens_per_char * context_chars as f64 + 8.0 * live as f64;
    // The budget the engine actually enforced last time (channel override
    // or model-window derived); absent while filling is count-only.
    let estimate = Some(match stats.last_budget {
        Some(budget) => format!("Est. context: ~{estimated:.0} / {budget} tokens"),
        None => format!("Est. context: ~{estimated:.0} tokens"),
    });

    let cached =
        last.cached_tokens.map(|cached| format!(" (+{cached} cached)")).unwrap_or_default();
    let reasoning = last
        .reasoning_tokens
        .map(|reasoning| format!(" ({reasoning} reasoning)"))
        .unwrap_or_default();
    let last_request = Some(format!(
        "Last request: {} prompt / {} completion{reasoning} / {} total tokens{cached}",
        last.prompt_tokens, last.completion_tokens, last.total_tokens
    ));
    UsageLines { estimate, last_request, last_response }
}

/// Renders a response time for `/llm_status`: whole milliseconds below ten
/// seconds, one-decimal seconds from there.
fn format_response_ms(total_ms: u64) -> String {
    if total_ms >= 10_000 {
        // precision loss is fine for a status line
        #[allow(clippy::cast_precision_loss)]
        let seconds = total_ms as f64 / 1000.0;
        format!("{seconds:.1} s")
    } else {
        format!("{total_ms} ms")
    }
}

/// The optional `/llm_status` lines fed by calibration data.
struct UsageLines {
    estimate: Option<String>,
    last_request: Option<String>,
    last_response: Option<String>,
}

/// `/llm_status`: inspect the channel's chat configuration and conversation
/// state. Ephemeral where the platform allows (slash invocations), since the
/// summary preview and the system-prompt head may be considered sensitive.
pub(super) struct StatusLlmHandler {
    engine: Arc<ChatEngine>,
}

impl StatusLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

/// Short identity fingerprint of the active system prompt: lets an admin
/// verify "same prompt as before / as that channel / as the file I uploaded"
/// without printing the whole text. NOT cryptographic - std's `DefaultHasher`,
/// stable only within one process; equality checks only.
fn prompt_fingerprint(text: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    format!("#{:016x}", hasher.finish())
}

/// `/llm_status` label of the channel's capture mode.
fn capture_label(mode: CaptureMode) -> &'static str {
    match mode {
        CaptureMode::BotRelated => "bot_related",
        CaptureMode::AllMessages => "all_messages",
    }
}

/// The effective reasoning display: the three-state contract made visible
/// (value sent as-is / explicit off / nothing sent).
fn reasoning_label(effort: Option<&str>) -> String {
    match effort {
        Some("off") => "off (explicit disable where the provider supports one)".to_owned(),
        Some(effort) => effort.to_owned(),
        None => "provider default (nothing sent)".to_owned(),
    }
}

/// The effective image-recognition display: on/off, the resolved model and
/// the resolved prompt length (override -> plugin default -> built-in), so
/// an admin can see which mechanism is active without printing prompts.
fn images_label(config: &ChannelConfig, settings: &LlmSettings) -> String {
    if !config.images {
        return "off".to_owned();
    }
    let model = config
        .image_model
        .clone()
        .or_else(|| settings.image_model.clone())
        .map_or_else(|| "no image model configured".to_owned(), |model| format!("`{model}`"));
    let prompt = config
        .image_prompt
        .clone()
        .or_else(|| settings.image_prompt.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE_PROMPT.to_owned());
    format!("on ({model}, {}-char prompt)", prompt.chars().count())
}

/// `/llm_status` label of the reaction tool state: off, or on with the
/// silent-react chime chance when it is enabled.
fn react_label(config: &ChannelConfig) -> String {
    if !config.react {
        "off".to_owned()
    } else if config.random_react_chance_percent > 0.0 {
        format!("on · {:.1}% silent-react", config.random_react_chance_percent)
    } else {
        "on".to_owned()
    }
}

/// `/llm_status` label of the chime-in roll chance and cooldown: the
/// percent plus the per-channel minimum interval, or `off` at zero chance.
fn chime_label(chance: f64, cooldown_secs: u64) -> String {
    if chance > 0.0 {
        format!("{chance:.1}% · {cooldown_secs}s cooldown")
    } else {
        "off".to_owned()
    }
}

#[async_trait]
impl CommandHandler for StatusLlmHandler {
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

        let channel_id = event.origin.channel_id.get();
        let Some(raw) = storage.get(NAMESPACE, &channel_config_key(channel_id)).await? else {
            services
                .chat_output
                .send(command_reply("LLM chat is not assigned to this channel."))
                .await?;
            return Ok(());
        };
        let config: ChannelConfig = serde_json::from_value(raw)?;
        let state: ConversationState = storage
            .get(NAMESPACE, &channel_state_key(channel_id))
            .await?
            .and_then(|raw| serde_json::from_value(raw).ok())
            .unwrap_or_default();

        let records_ns = records_namespace(channel_id);
        let live = storage.count_after(&records_ns, state.cutoff_seq).await?;
        let total = storage.count_after(&records_ns, 0).await?;
        let first_link = match storage.list_after(&records_ns, state.cutoff_seq, 1).await?.first() {
            Some(stored) => serde_json::from_value::<ConversationRecord>(stored.payload.clone())
                .ok()
                .and_then(|record| record.message_id)
                .and_then(|message_id| {
                    services.chat_output_factory.message_link(
                        &event.origin,
                        event.origin.channel_id,
                        MessageId(message_id),
                    )
                }),
            None => None,
        };
        let usage_stats: UsageStats = storage
            .get(NAMESPACE, &channel_stats_key(channel_id))
            .await?
            .and_then(|raw| serde_json::from_value(raw).ok())
            .unwrap_or_default();
        let usage = usage_lines(storage, &records_ns, &state, live, &usage_stats).await;

        // The effective prompt: what the model will actually see as slot 1,
        // override or not - with length, identity fingerprint and a head
        // preview so an admin can verify the version without printing it all.
        let (prompt_source, prompt_text) = match &config.system_prompt {
            Some(prompt) => ("channel override", prompt.as_str()),
            None => ("plugin default", self.engine.settings().default_system_prompt.as_str()),
        };
        let prompt_head = preview(prompt_text, 200);
        let summary = match &state.summary {
            Some(summary) => preview(summary, 200),
            None => "none".to_owned(),
        };
        let context_start = first_link.unwrap_or_else(|| "no messages after the cutoff".to_owned());
        let reasoning = reasoning_label(config.params.reasoning_effort.as_deref());
        let images = images_label(&config, self.engine.settings());
        let reactions = react_label(&config);
        let mut description = format!(
            "Model: `{}`
Reasoning: {reasoning}
Prompt: {prompt_source} ({} chars, {})
Prompt head: {prompt_head}
Context: {live}/{} messages ({total} kept)
Compaction: {}
Images: {images}
Reactions: {reactions}
Capture: {} · Chime-ins: {}
Summary: {summary}
Context start: {context_start}",
            config.model,
            prompt_text.chars().count(),
            prompt_fingerprint(prompt_text),
            config.history_depth,
            if config.compaction_enabled { "on" } else { "off" },
            capture_label(config.capture_mode),
            chime_label(config.random_chance_percent, config.random_cooldown_secs),
        );
        if let Some(line) = usage.estimate {
            description.push('\n');
            description.push_str(&line);
        }
        if let Some(line) = usage.last_request {
            description.push('\n');
            description.push_str(&line);
        }
        if let Some(line) = usage.last_response {
            description.push('\n');
            description.push_str(&line);
        }

        services
            .chat_output
            .send(
                OutboundMessage::embed(Embed { title: "LLM status".to_owned(), description })
                    .ephemeral(),
            )
            .await?;
        Ok(())
    }
}

/// `/llm_admin`: makes the channel the command is run in the guild's service
/// channel - LLM errors and service notices are reported there (last write
/// wins, one per guild).
pub(super) struct AssignServiceChannelHandler;

#[async_trait]
impl CommandHandler for AssignServiceChannelHandler {
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
                SERVICE_CHANNEL_KEY,
                serde_json::json!(event.origin.channel_id.get().to_string()),
            )
            .await?;

        services
            .chat_output
            .send(command_reply(
                "Service channel assigned: LLM errors and notices will be reported here.",
            ))
            .await?;
        Ok(())
    }
}

/// `/llm_admin_clear`: clears the guild's service channel (idempotent).
pub(super) struct ClearServiceChannelHandler;

#[async_trait]
impl CommandHandler for ClearServiceChannelHandler {
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

        storage.delete(NAMESPACE, SERVICE_CHANNEL_KEY).await?;

        services.chat_output.send(command_reply("Service channel cleared.")).await?;
        Ok(())
    }
}

/// `/llm_models`: ephemeral catalog of the operator-declared models - the
/// complete legal assignment set. Read-only: no channel lock, no storage.
pub(super) struct ModelsLlmHandler {
    engine: Arc<ChatEngine>,
}

impl ModelsLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for ModelsLlmHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let settings = self.engine.settings();
        let lines: Vec<String> = settings
            .models
            .iter()
            .map(|(reference, model)| model_catalog_line(reference, model))
            .collect();
        let body = if lines.is_empty() {
            "No models are declared in `[llm.models]` - the bot cannot be assigned to a \
             channel until the operator declares at least one."
                .to_owned()
        } else {
            format!("Declared models:\n{}", lines.join("\n"))
        };
        services.chat_output.send(command_reply(body)).await?;
        Ok(())
    }
}

/// `/llm_set`: tunes one setting of the channel's chat configuration by
/// `key`/`value`. Values of `clear`/`none`/`default` reset the setting to
/// its default; malformed values are answered with usage and never saved.
/// The read-modify-write runs under the channel's processing lock so
/// concurrent admin commands cannot lose an update.
pub(super) struct SetLlmHandler {
    locks: Arc<ChannelLocks>,
    engine: Arc<ChatEngine>,
}

impl SetLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>, engine: Arc<ChatEngine>) -> Self {
        Self { locks, engine }
    }
}

#[async_trait]
impl CommandHandler for SetLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        // The lock spans load-apply-save: two admins setting values on the
        // same channel must not lose one update.
        let channel = self.locks.lock_for(&event.origin);
        let _channel = channel.lock().await;
        let Some(mut config) = load_assigned_config(event, services).await? else {
            return Ok(());
        };
        let (Some(key), Some(value)) = (args.get("key"), args.get("value")) else {
            services
                .chat_output
                .send(command_reply(format!(
                    "Usage: `/llm_set key value`. Keys: {}.",
                    SET_KEYS.join(", ")
                )))
                .await?;
            return Ok(());
        };
        // Same registry boundary as `/llm_assign`: only declared models are
        // legal - chat, image, AND compaction refs alike. Clear-words are
        // not assignments - `apply_set` answers them with its own "cannot
        // be cleared" usage.
        if matches!(key, "model" | "image_model" | "compaction_model")
            && !matches!(value, "clear" | "none" | "default")
            && let Err(reply) = validate_model_ref(self.engine.settings(), value)
        {
            services.chat_output.send(command_reply(reply)).await?;
            return Ok(());
        }

        match apply_set(&mut config, key, value) {
            Ok(message) => {
                save_config(event, services, config).await?;
                services.chat_output.send(command_reply(message)).await?;
            }
            Err(usage) => {
                services.chat_output.send(command_reply(usage)).await?;
            }
        }
        Ok(())
    }
}

/// `/llm_prompt`: sets the channel's system prompt (long free text); the
/// value `clear` falls back to the plugin-wide default. Like `/llm_set`, the
/// read-modify-write runs under the channel's processing lock.
pub(super) struct PromptLlmHandler {
    locks: Arc<ChannelLocks>,
}

impl PromptLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>) -> Self {
        Self { locks }
    }
}

#[async_trait]
impl CommandHandler for PromptLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let channel = self.locks.lock_for(&event.origin);
        let _channel = channel.lock().await;
        let Some(mut config) = load_assigned_config(event, services).await? else {
            return Ok(());
        };
        let Some(prompt) = args.get("prompt") else {
            services
                .chat_output
                .send(command_reply(
                    "Usage: `/llm_prompt text` - or `/llm_prompt clear` to fall back to the default.",
                ))
                .await?;
            return Ok(());
        };

        let message = if prompt == "clear" {
            config.system_prompt = None;
            "System prompt cleared: the default applies again."
        } else {
            config.system_prompt = Some(prompt.to_owned());
            "System prompt updated."
        };
        save_config(event, services, config).await?;
        services.chat_output.send(command_reply(message)).await?;
        Ok(())
    }
}

/// `/llm_prompt_file`: sets the channel's system prompt from an uploaded
/// attachment - for prompts longer than Discord's inline option limit.
/// The adapter normalizes the attachment to its Discord CDN URL; this
/// handler re-validates the pinned host (the CDN is the only network peer
/// guild input may ever point the bot at - no arbitrary URL fetches),
/// downloads with the configured byte cap, and stores the decoded text.
/// Like every state-mutating LLM command, it runs under the channel's
/// processing lock.
pub(super) struct PromptFileLlmHandler {
    locks: Arc<ChannelLocks>,
    client: reqwest::Client,
    max_prompt_file_bytes: u64,
}

impl PromptFileLlmHandler {
    pub(super) fn new(
        locks: Arc<ChannelLocks>,
        client: reqwest::Client,
        max_prompt_file_bytes: u64,
    ) -> Self {
        Self { locks, client, max_prompt_file_bytes }
    }

    /// Downloads and decodes the attachment with the configured byte cap.
    /// The body is STREAM-read: the cap aborts the transfer instead of
    /// buffering a lying or chunked response whole. `Err` carries the
    /// user-facing reason; transport details go to logs.
    async fn download(&self, url: &str) -> Result<String, String> {
        let mut response = self.client.get(url).send().await.map_err(|err| {
            tracing::warn!(%err, "prompt file download failed");
            "the attachment could not be downloaded".to_owned()
        })?;
        if !response.status().is_success() {
            return Err(format!("the attachment host returned HTTP {}", response.status()));
        }
        if let Some(len) = response.content_length()
            && len > self.max_prompt_file_bytes
        {
            return Err(format!(
                "the attachment exceeds the limit ({len} > {} bytes, `max_prompt_file_bytes`)",
                self.max_prompt_file_bytes
            ));
        }
        let limit = usize::try_from(self.max_prompt_file_bytes).unwrap_or(usize::MAX);
        let mut body: Vec<u8> = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|err| {
            tracing::warn!(%err, "prompt file download failed");
            "the attachment could not be downloaded".to_owned()
        })? {
            body.extend_from_slice(&chunk);
            if body.len() > limit {
                return Err(format!(
                    "the attachment exceeds the limit ({} bytes or more > {}, \
                     `max_prompt_file_bytes`)",
                    body.len(),
                    self.max_prompt_file_bytes
                ));
            }
        }
        let text = String::from_utf8(body)
            .map_err(|_| "the attachment is not valid UTF-8 text".to_owned())?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err("the attachment is empty".to_owned());
        }
        Ok(trimmed.to_owned())
    }
}

/// The attachment argument must name Discord's CDN - the only network peer
/// guild input may ever point the bot at. Rejected before any network I/O.
fn validate_prompt_file_url(url: &str) -> Result<(), String> {
    const CDN_PREFIX: &str = "https://cdn.discordapp.com/";
    if url.starts_with(CDN_PREFIX) {
        Ok(())
    } else {
        Err("the `file` argument must be this command's own attachment - pick the \
             uploaded file in the slash command UI."
            .to_owned())
    }
}

#[async_trait]
impl CommandHandler for PromptFileLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(url) = args.get("file") else {
            services
                .chat_output
                .send(command_reply(
                    "Usage: `/llm_prompt_file file` - attach a .txt/.md file with the prompt text.",
                ))
                .await?;
            return Ok(());
        };
        if let Err(usage) = validate_prompt_file_url(url) {
            services.chat_output.send(command_reply(usage)).await?;
            return Ok(());
        }

        // Fetch the attachment BEFORE taking the channel lock: the download
        // is slow network I/O and must not freeze the channel's chat for up
        // to the client timeout. Only the config read-modify-write needs the
        // lock.
        let prompt = match self.download(url).await {
            Ok(prompt) => prompt,
            Err(reason) => {
                services
                    .chat_output
                    .send(command_reply(format!("Could not load the attachment: {reason}")))
                    .await?;
                return Ok(());
            }
        };

        let channel = self.locks.lock_for(&event.origin);
        let _channel = channel.lock().await;
        let Some(mut config) = load_assigned_config(event, services).await? else {
            return Ok(());
        };
        let characters = prompt.chars().count();
        config.system_prompt = Some(prompt);
        save_config(event, services, config).await?;
        services
            .chat_output
            .send(command_reply(format!("System prompt set from file ({characters} characters).")))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{GuildId, Platform};
    use crate::kernel::spi_ports::StoragePort;
    use crate::plugins::llm::RecordRole;
    use crate::plugins::llm::completion_port::{ResponseTiming, TokenUsage};
    use crate::test_support::InMemoryStorage;

    /// The attachment argument must name Discord's CDN - https and the exact
    /// pinned host. Anything else (other hosts, other schemes, lookalike
    /// domains) is rejected before any network I/O: no arbitrary URL fetches.
    #[test]
    fn prompt_file_url_must_be_the_discord_cdn() {
        assert!(
            validate_prompt_file_url(
                "https://cdn.discordapp.com/attachments/1/2/prompt.txt?ex=1&is=2&hm=3"
            )
            .is_ok()
        );
        assert!(validate_prompt_file_url("http://cdn.discordapp.com/a.txt").is_err());
        assert!(validate_prompt_file_url("https://cdn.discordapp.com.evil.com/a.txt").is_err());
        assert!(validate_prompt_file_url("https://example.com/prompt.txt").is_err());
    }

    /// Minimal one-shot TCP server (the same pattern as the provider
    /// tests): swallows the request head, writes the scripted bytes, closes
    /// the connection. Deliberately no HTTP framework.
    async fn raw_http_server(script: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            socket.write_all(&script).await.expect("write");
        });
        (format!("http://{addr}/attachment.md"), handle)
    }

    fn prompt_file_handler(cap: u64) -> PromptFileLlmHandler {
        PromptFileLlmHandler::new(ChannelLocks::new(), reqwest::Client::new(), cap)
    }

    /// The download happy path and the HTTP-status branch: 200 decodes the
    /// body (trimmed), a non-2xx surfaces the status in the reply.
    #[tokio::test]
    async fn prompt_file_download_success_and_status_error() {
        let handler = prompt_file_handler(10_000);

        let (url, server) =
            raw_http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n  prompt".to_vec()).await;
        let prompt = handler.download(&url).await.expect("download expected to succeed");
        let _ = server.await;
        assert_eq!(prompt, "prompt");

        let (url, server) =
            raw_http_server(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec()).await;
        let err = handler.download(&url).await.expect_err("404 expected to fail");
        let _ = server.await;
        assert!(err.contains("404"), "{err}");
    }

    /// The size cap aborts a close-delimited body mid-transfer - a missing
    /// or lying Content-Length cannot make the handler buffer the whole
    /// body (the streaming cap is the point of this test).
    #[tokio::test]
    async fn prompt_file_download_cap_aborts_oversized_body() {
        let handler = prompt_file_handler(64);
        let script = format!("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\n{}", "x".repeat(500));
        let (url, server) = raw_http_server(script.into_bytes()).await;

        let err = handler.download(&url).await.expect_err("oversized body expected to fail");
        let _ = server.await;

        assert!(err.contains("exceeds the limit"), "{err}");
    }

    #[tokio::test]
    async fn prompt_file_download_rejects_non_utf8() {
        let handler = prompt_file_handler(10_000);
        let mut script = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n".to_vec();
        script.extend_from_slice(&[0xFF, 0xFE]);
        let (url, server) = raw_http_server(script).await;

        let err = handler.download(&url).await.expect_err("invalid UTF-8 expected to fail");
        let _ = server.await;

        assert!(err.contains("UTF-8"), "{err}");
    }

    #[test]
    fn float_params_set_and_clear() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let message = apply_set(&mut config, "temperature", "0.7").expect("set expected");
        assert!(message.contains("0.7"));
        assert_eq!(config.params.temperature, Some(0.7));

        apply_set(&mut config, "top_k", "40").expect("set expected");
        assert_eq!(config.params.top_k, Some(40.0));

        apply_set(&mut config, "temperature", "clear").expect("clear expected");
        assert_eq!(config.params.temperature, None);

        assert!(apply_set(&mut config, "top_p", "abc").is_err());
    }

    #[test]
    fn bool_keys_accept_on_off_words() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "streaming", "on").expect("on expected");
        assert!(config.streaming);
        apply_set(&mut config, "streaming", "off").expect("off expected");
        assert!(!config.streaming);
        apply_set(&mut config, "compaction", "false").expect("false expected");
        assert!(!config.compaction_enabled);
        apply_set(&mut config, "images", "on").expect("on expected");
        assert!(config.images);
        apply_set(&mut config, "images", "off").expect("off expected");
        assert!(!config.images);
        apply_set(&mut config, "react", "on").expect("on expected");
        assert!(config.react);
        apply_set(&mut config, "react", "off").expect("off expected");
        assert!(!config.react);
        assert!(apply_set(&mut config, "streaming", "maybe").is_err());
    }

    /// Image settings follow the same set/clear grammar as their compaction
    /// counterparts: the model is a validated ref (handler-side), the prompt
    /// is free text with `clear` falling back to the plugin default.
    #[test]
    fn image_keys_set_and_clear() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "image_model", "local/vision").expect("set expected");
        assert_eq!(config.image_model.as_deref(), Some("local/vision"));
        apply_set(&mut config, "image_prompt", "Describe in Russian.").expect("set expected");
        assert_eq!(config.image_prompt.as_deref(), Some("Describe in Russian."));

        apply_set(&mut config, "image_model", "clear").expect("clear expected");
        assert_eq!(config.image_model, None);
        apply_set(&mut config, "image_prompt", "default").expect("clear expected");
        assert_eq!(config.image_prompt, None);
    }

    /// `off` is an explicit choice with its own acknowledgment and its own
    /// stored state (`Some("off")`) - thinking-switch providers render a
    /// real wire disable from it. The reset words keep the "cleared" reply
    /// and store `None` (provider default, nothing sent).
    #[test]
    fn reasoning_effort_distinguishes_off_from_reset() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let set = apply_set(&mut config, "reasoning_effort", "low").expect("set expected");
        assert!(set.contains("set to `low`"));
        assert_eq!(config.params.reasoning_effort.as_deref(), Some("low"));

        let off = apply_set(&mut config, "reasoning_effort", "off").expect("off expected");
        assert!(off.contains("`reasoning_effort` off"));
        assert!(off.contains("explicit disable"));
        assert_eq!(config.params.reasoning_effort.as_deref(), Some("off"));

        let cleared = apply_set(&mut config, "reasoning_effort", "clear").expect("clear expected");
        assert!(cleared.contains("cleared"));
        assert_eq!(config.params.reasoning_effort, None);
    }

    #[test]
    fn numeric_keys_validate_ranges() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "depth", "0").unwrap_err();
        apply_set(&mut config, "depth", "50").expect("depth expected");
        assert_eq!(config.history_depth, 50);

        apply_set(&mut config, "random_chance", "250").expect("clamp expected");
        assert!((config.random_chance_percent - 100.0).abs() < f64::EPSILON);
        apply_set(&mut config, "random_chance", "clear").expect("clear expected");
        assert!((config.random_chance_percent).abs() < f64::EPSILON);

        apply_set(&mut config, "random_react_chance", "250").expect("clamp expected");
        assert!((config.random_react_chance_percent - 100.0).abs() < f64::EPSILON);
        apply_set(&mut config, "random_react_chance", "clear").expect("clear expected");
        assert!((config.random_react_chance_percent).abs() < f64::EPSILON);

        apply_set(&mut config, "random_cooldown", "30").expect("cooldown expected");
        assert_eq!(config.random_cooldown_secs, 30);
        apply_set(&mut config, "random_cooldown", "clear").expect("cooldown clear expected");
        assert_eq!(config.random_cooldown_secs, 5);
        apply_set(&mut config, "random_cooldown", "not-a-number").unwrap_err();

        apply_set(&mut config, "max_tokens", "not-a-number").unwrap_err();

        // A zero budget would keep only the newest turn - rejected like
        // `depth` 0, not accepted as "no budget".
        apply_set(&mut config, "context_budget", "0").unwrap_err();
        apply_set(&mut config, "context_budget", "4096").expect("budget expected");
        assert_eq!(config.context_budget_tokens, Some(4096));
    }

    #[test]
    fn max_length_cannot_exceed_the_platform_limit() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "max_length", "2000").expect("limit value expected");
        assert_eq!(config.max_length, Some(2000));
        // Beyond Discord's cap the platform rejects the chunks outright -
        // rejected here so replies never turn into undelivered garbage.
        apply_set(&mut config, "max_length", "2001").unwrap_err();
        assert_eq!(config.max_length, Some(2000));
    }

    #[test]
    fn capture_mode_and_template_validate() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "capture_mode", "all_messages").expect("mode expected");
        assert_eq!(config.capture_mode, CaptureMode::AllMessages);
        apply_set(&mut config, "capture_mode", "chaos").unwrap_err();

        apply_set(&mut config, "turn_template", "<{sender}> {message}").expect("template expected");
        assert_eq!(config.turn_template.as_deref(), Some("<{sender}> {message}"));
        apply_set(&mut config, "turn_template", "no placeholders").unwrap_err();
        apply_set(&mut config, "turn_template", "clear").expect("clear expected");
        assert_eq!(config.turn_template, None);
    }

    #[test]
    fn unknown_keys_and_required_fields_are_rejected() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let unknown = apply_set(&mut config, "vibes", "maximum").unwrap_err();
        assert!(unknown.contains("Unknown key"));
        assert!(apply_set(&mut config, "model", "clear").is_err());
        assert!(apply_set(&mut config, "depth", "clear").is_err());
        apply_set(&mut config, "model", "zai/glm-5.3-flash").expect("model expected");
        assert_eq!(config.model, "zai/glm-5.3-flash");
    }

    /// A recognized flag key with a bad value answers usage - it must not
    /// fall through to "Unknown key".
    #[test]
    fn flag_with_invalid_value_reports_usage_not_unknown_key() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let err = apply_set(&mut config, "streaming", "maybe").unwrap_err();
        assert!(err.contains("on or off"), "unexpected reply: {err}");
        assert!(!err.contains("Unknown key"), "unexpected reply: {err}");
        assert!(!config.streaming, "the failed set must not mutate");
    }

    #[test]
    fn float_keys_reject_non_finite_values() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        assert!(apply_set(&mut config, "temperature", "NaN").is_err());
        assert!(apply_set(&mut config, "top_p", "inf").is_err());
        assert!(apply_set(&mut config, "min_p", "-inf").is_err());
        assert_eq!(config.params.temperature, None, "nothing may be stored");

        // NaN would clamp to NaN, not to the range bounds; the default stays.
        let before = config.random_chance_percent;
        assert!(apply_set(&mut config, "random_chance", "NaN").is_err());
        assert!((config.random_chance_percent - before).abs() < f64::EPSILON);
    }

    fn settings_with_models(refs: &[&str]) -> LlmSettings {
        let mut settings = LlmSettings::default();
        for reference in refs {
            settings.models.insert((*reference).to_owned(), ModelSettings::default());
        }
        settings
    }

    /// The registry boundary: declared refs pass; anything else is rejected
    /// with a correction that lists the legal set.
    #[test]
    fn only_declared_models_validate() {
        let settings = settings_with_models(&["zai/glm-5.3", "local/gemma"]);
        assert!(validate_model_ref(&settings, "zai/glm-5.3").is_ok());

        let err = validate_model_ref(&settings, "zai/typo").unwrap_err();
        assert!(err.contains("Unknown model `zai/typo`"));
        // BTreeMap order: the legal set renders sorted.
        assert!(err.contains("local/gemma, zai/glm-5.3"));
    }

    #[test]
    fn empty_registry_names_the_config_section() {
        let err = validate_model_ref(&LlmSettings::default(), "zai/glm").unwrap_err();
        assert!(err.contains("[llm.models]"));
    }

    /// Dropdown truncation is deterministic (sorted registry order) and
    /// capped at Discord's limit.
    #[test]
    fn dropdown_choices_truncate_to_discords_cap() {
        let mut settings = LlmSettings::default();
        for index in 0..30 {
            settings.models.insert(format!("p/m{index}"), ModelSettings::default());
        }

        let choices = model_choices(&settings);
        assert_eq!(choices.len(), MAX_DISCORD_CHOICES);
        assert_eq!(choices.first().expect("choice expected"), "p/m0");
    }

    #[test]
    fn catalog_line_lists_declared_capabilities() {
        let plain = ModelSettings::default();
        assert_eq!(model_catalog_line("p/m", &plain), "`p/m`");

        let full =
            ModelSettings { reasoning: true, context_window: Some(4096), ..Default::default() };
        assert_eq!(model_catalog_line("p/m", &full), "`p/m` - reasoning - 4096 token context");
    }

    fn user_record(content: &str) -> serde_json::Value {
        serde_json::to_value(ConversationRecord {
            message_id: Some(1),
            role: RecordRole::User,
            author: Some("alice".to_owned()),
            sender_id: None,
            guild_name: None,
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
            images: Vec::new(),
        })
        .expect("record expected to serialize")
    }

    /// Uncalibrated channels (no endpoint usage ever reported) show no
    /// estimate lines - the default ratio would be noise, not data.
    #[tokio::test]
    async fn usage_lines_stay_hidden_without_calibration() {
        let storage = InMemoryStorage::new();
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let lines = usage_lines(
            &guild,
            &records_namespace(2),
            &ConversationState::default(),
            0,
            &UsageStats::default(),
        )
        .await;

        assert!(lines.estimate.is_none());
        assert!(lines.last_request.is_none());
        assert!(lines.last_response.is_none());
    }

    /// The response-time line shows on its own - without usage stats too
    /// (endpoints that report neither), and with the source named.
    #[tokio::test]
    async fn usage_lines_render_response_time_with_and_without_usage() {
        let storage = InMemoryStorage::new();
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let timed = UsageStats {
            last_timing: Some(ResponseTiming::reported(50_237)),
            ..UsageStats::default()
        };
        let lines =
            usage_lines(&guild, &records_namespace(2), &ConversationState::default(), 0, &timed)
                .await;
        assert_eq!(lines.last_response.as_deref(), Some("Last response: 50.2 s (endpoint)"));
        assert!(lines.estimate.is_none());
        assert!(lines.last_request.is_none());

        let measured = UsageStats {
            last_timing: Some(ResponseTiming::measured(950)),
            ..UsageStats::default()
        };
        let lines =
            usage_lines(&guild, &records_namespace(2), &ConversationState::default(), 0, &measured)
                .await;
        assert_eq!(lines.last_response.as_deref(), Some("Last response: 950 ms (measured)"));
    }

    /// Calibrated: the estimate line carries the enforced budget, the last
    /// request line the usage block incl. cached tokens. Without a budget or
    /// cached tokens the variants degrade cleanly.
    #[tokio::test]
    async fn usage_lines_render_estimate_and_last_request() {
        let storage = InMemoryStorage::new();
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));
        // Two live turns of 10 chars each after the cutoff.
        for content in ["aaaaaaaaaa", "bbbbbbbbbb"] {
            guild
                .append(&records_namespace(2), user_record(content))
                .await
                .expect("append expected to succeed");
        }
        let stats = UsageStats {
            last: Some(TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 10,
                total_tokens: 110,
                cached_tokens: Some(40),
                reasoning_tokens: Some(6),
            }),
            last_timing: Some(ResponseTiming::reported(527_000)),
            tokens_per_char: 1.0,
            last_budget: Some(500),
        };

        let lines =
            usage_lines(&guild, &records_namespace(2), &ConversationState::default(), 2, &stats)
                .await;

        // est = 1.0 tokens/char * 20 chars + 8 per-turn overhead * 2 turns.
        assert_eq!(lines.estimate.as_deref(), Some("Est. context: ~36 / 500 tokens"));
        assert_eq!(
            lines.last_request.as_deref(),
            Some(
                "Last request: 100 prompt / 10 completion (6 reasoning) / 110 total tokens (+40 cached)"
            )
        );
        assert_eq!(lines.last_response.as_deref(), Some("Last response: 527.0 s (endpoint)"));

        let plain = UsageStats { last_budget: None, ..stats };
        let mut bare = stats.last.expect("usage expected");
        bare.cached_tokens = None;
        bare.reasoning_tokens = None;
        let plain = UsageStats { last: Some(bare), ..plain };
        let lines =
            usage_lines(&guild, &records_namespace(2), &ConversationState::default(), 2, &plain)
                .await;
        assert_eq!(lines.estimate.as_deref(), Some("Est. context: ~36 tokens"));
        assert_eq!(
            lines.last_request.as_deref(),
            Some("Last request: 100 prompt / 10 completion / 110 total tokens")
        );
    }
}
