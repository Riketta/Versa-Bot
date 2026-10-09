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
    CaptureMode, ChannelConfig, ConversationState, EmojiInject, EmojiWhitelistEntry,
    GUILD_EMOJI_WHITELIST_KEY, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY, UsageStats,
    channel_config_key, channel_emoji_whitelist_key, channel_state_key, channel_state_undo_key,
    channel_stats_key, default_random_cooldown, parse_whitelist, records_namespace, unix_now,
};
use super::prompts;
use super::providers::{LlmSettings, ModelSettings};
use super::usage_total::Dimension;
use super::vision::DEFAULT_IMAGE_PROMPT;

/// Recognized `/llm_set` keys, in display order. Doubles as the Discord
/// choices dropdown for the `key` argument. Channel prompts are NOT keys -
/// they have their own command (`/llm_set_prompt`), which also keeps this
/// list well under Discord's 25-choice cap.
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
    "context_messages",
    "context_tokens",
    "capture",
    "compaction",
    "compaction_model",
    "images",
    "image_model",
    "react",
    "react_emoji_inject",
    "streaming",
    "random_reply_chance",
    "random_cooldown",
    "random_react_chance",
    "split_length",
    "turn_template",
];

/// Loads the channel's config for mutation, replying (and returning `None`)
/// when the context is a DM or the channel is unassigned.
async fn load_assigned_config(
    event: &RequestContext,
    services: &KernelServices,
    min_split_length: usize,
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
    Ok(Some(ChannelConfig::from_stored(raw, min_split_length)?))
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
fn apply_set(
    config: &mut ChannelConfig,
    key: &str,
    value: &str,
    message_limit: Option<usize>,
    min_split_length: usize,
) -> Result<String, String> {
    let cleared = matches!(value, "clear" | "none" | "default");
    if let Some(result) = apply_numeric(config, key, value, cleared) {
        return result;
    }
    if let Some(result) =
        apply_optional_field(config, key, value, cleared, message_limit, min_split_length)
    {
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
        "capture" => {
            let mode: CaptureMode =
                serde_json::from_value(serde_json::json!(value)).map_err(|_| {
                    format!("`capture` expects bot_related or all_messages, got `{value}`.")
                })?;
            config.capture = mode;
            Ok(format!("`capture` set to `{value}`."))
        }
        "react_emoji_inject" => {
            let mode: EmojiInject =
                serde_json::from_value(serde_json::json!(value)).map_err(|_| {
                    format!("`react_emoji_inject` expects none, all, or whitelist, got `{value}`.")
                })?;
            config.react_emoji_inject = mode;
            let mut reply = format!(
                "`react_emoji_inject` set to `{value}` (the react tool's prompt lists this server's \
                 custom emojis)."
            );
            if mode != EmojiInject::None && !config.react {
                reply
                    .push_str(" Note: `react` is off - enable it for the list to reach the model.");
            }
            Ok(reply)
        }
        "model" | "context_messages" => {
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
        "context_messages" => {
            if cleared {
                return Some(Err(format!(
                    "`{key}` cannot be cleared - assign a value or use `/llm_unassign`."
                )));
            }
            match value.parse::<u32>() {
                Ok(0) => Some(Err("`context_messages` must be at least 1.".to_owned())),
                Ok(parsed) => {
                    config.context_messages = parsed;
                    Some(Ok(format!("`context_messages` set to {parsed} messages.")))
                }
                Err(_) => {
                    Some(Err(format!("`context_messages` expects a whole number, got `{value}`.")))
                }
            }
        }
        "context_tokens" => {
            if cleared {
                config.context_tokens = None;
                return Some(Ok(format!("`{key}` cleared (count-only filling).")));
            }
            match value.parse::<u32>() {
                Ok(0) => Some(Err(format!(
                    "`{key}` must be at least 1 (0 would keep only the newest turn)."
                ))),
                Ok(parsed) => {
                    config.context_tokens = Some(parsed);
                    Some(Ok(format!("`{key}` set to {parsed} tokens.")))
                }
                Err(_) => {
                    Some(Err(format!("`{key}` expects a whole number of tokens, got `{value}`.")))
                }
            }
        }
        "random_reply_chance" => {
            if cleared {
                config.random_reply_chance_percent = 0.0;
                return Some(Ok("`random_reply_chance` cleared (chime-ins off).".to_owned()));
            }
            match value.parse::<f64>() {
                // Non-finite clamps to NaN, not to the range bounds.
                Ok(parsed) if parsed.is_finite() => {
                    let clamped = parsed.clamp(0.0, 100.0);
                    config.random_reply_chance_percent = clamped;
                    Some(Ok(format!("`random_reply_chance` set to {clamped}.")))
                }
                _ => Some(Err(format!(
                    "`random_reply_chance` expects a finite number (percent), got `{value}`."
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
    message_limit: Option<usize>,
    min_split_length: usize,
) -> Option<Result<String, String>> {
    match key {
        // Model refs sharing the set/clear shape. The text prompts left the
        // key set for `/llm_set_prompt` - only model refs remain here.
        "compaction_model" | "image_model" => {
            if cleared {
                match key {
                    "compaction_model" => config.compaction_model = None,
                    _ => config.image_model = None,
                }
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            match key {
                "compaction_model" => {
                    config.compaction_model = Some(value.to_owned());
                    Some(Ok(format!("`{key}` set to `{value}`.")))
                }
                _ => {
                    config.image_model = Some(value.to_owned());
                    Some(Ok(format!(
                        "`{key}` set to `{value}` (describes images for this channel)."
                    )))
                }
            }
        }
        "split_length" => {
            if cleared {
                config.split_length = None;
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            match value.parse::<usize>() {
                // Tiny chunks would flood the channel and starve the send
                // rate limits - the floor is operator policy (announced at
                // boot) enforced here.
                Ok(n) if n < min_split_length => Some(Err(format!(
                    "`{key}` must be at least {min_split_length} - smaller chunks would flood \
                     the channel."
                ))),
                // The platform rejects longer text messages outright - a
                // bigger limit would only turn split replies into
                // undelivered chunks. Platforms declaring no cap accept
                // anything.
                Ok(n)
                    if let Some(cap) = message_limit
                        && n > cap =>
                {
                    Some(Err(format!(
                        "`{key}` cannot exceed {cap} (the platform's message limit)."
                    )))
                }
                Ok(characters) => {
                    config.split_length = Some(characters);
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
/// Cap on one command argument's `choices` dropdown. The name is
/// platform-neutral because the cap is a platform fact, not plugin logic -
/// but the value is Discord's (25 entries per option) on purpose: every
/// deployment serves a single chat provider, and plugins may focus that
/// provider until a second adapter ever exists.
pub(super) const MAX_CHOICES: usize = 25;

/// Declared model refs for the `/llm_assign` dropdown: registry order
/// (sorted), truncated to Discord's choice cap. The dropdown is discovery
/// only - runtime validation enforces the full registry regardless of the
/// truncation.
pub(super) fn model_choices(settings: &LlmSettings) -> Vec<String> {
    let mut refs: Vec<String> = settings.models.keys().cloned().collect();
    if refs.len() > MAX_CHOICES {
        tracing::warn!(
            total = refs.len(),
            cap = MAX_CHOICES,
            "declared models exceed Discord's choice cap - dropdown truncated; \
             runtime validation still accepts every declared ref"
        );
        refs.truncate(MAX_CHOICES);
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
pub(super) struct UnassignLlmHandler {
    locks: Arc<ChannelLocks>,
}

impl UnassignLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>) -> Self {
        Self { locks }
    }
}

#[async_trait]
impl CommandHandler for UnassignLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        // The delete joins the same per-channel serialization as the other
        // state-mutating commands: an in-flight engine run must not answer
        // once the unassignment has landed.
        let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
        let _channel = channel.lock().await;
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
/// answer starts fresh. Stored history is kept (records only leave storage
/// through `/llm_forget`); this is a context reset, not a history wipe. The
/// mutation runs under the channel's processing lock: an in-flight engine
/// run must not commit an older state (compaction) over the fresh cutoff.
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
        let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
        let _channel = channel.lock().await;

        let channel_id = event.origin.channel_id.get();
        let records_ns = records_namespace(channel_id);
        // Stash the state document exactly as it is: `/llm_cutoff_undo`
        // restores it verbatim (summary and cutoff position). JSON `null`
        // records "no state document existed" - undo then removes the state
        // again. Written under the channel lock like the cutoff itself.
        let previous = storage.get(NAMESPACE, &channel_state_key(channel_id)).await?;
        storage
            .set(
                NAMESPACE,
                &channel_state_undo_key(channel_id),
                previous.unwrap_or(serde_json::Value::Null),
            )
            .await?;
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
        tracing::info!(
            channel = channel_id,
            cutoff = newest,
            "conversation context reset by moderator"
        );

        services
            .chat_output
            .send(command_reply(
                "Context cleared: this channel starts a fresh conversation (stored history is kept).",
            ))
            .await?;
        Ok(())
    }
}

/// `/llm_cutoff_undo`: restores the state document stashed by the last
/// `/llm_cutoff` in this channel - the exact previous context (summary and
/// cutoff position); records were never touched, so nothing else changes.
/// One level deep: every cutoff overwrites the stash, every undo consumes
/// it. Same channel lock as the cutoff - no in-flight run can interleave.
pub(super) struct CutoffUndoLlmHandler {
    locks: Arc<ChannelLocks>,
}

impl CutoffUndoLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>) -> Self {
        Self { locks }
    }
}

#[async_trait]
impl CommandHandler for CutoffUndoLlmHandler {
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
        let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
        let _channel = channel.lock().await;

        let channel_id = event.origin.channel_id.get();
        let Some(previous) = storage.get(NAMESPACE, &channel_state_undo_key(channel_id)).await?
        else {
            services
                .chat_output
                .send(command_reply(
                    "Nothing to undo: no /llm_cutoff was run in this channel (or it was \
                     already undone).",
                ))
                .await?;
            return Ok(());
        };
        if previous == serde_json::Value::Null {
            // The channel had no state document before the cutoff: restore
            // that absence rather than a default-shaped document.
            storage.delete(NAMESPACE, &channel_state_key(channel_id)).await?;
        } else {
            storage.set(NAMESPACE, &channel_state_key(channel_id), previous).await?;
        }
        storage.delete(NAMESPACE, &channel_state_undo_key(channel_id)).await?;
        tracing::debug!(channel = channel_id, "llm cutoff undone - previous context restored");

        services
            .chat_output
            .send(command_reply(
                "Context restored: the channel is back to its state before the last \
                 /llm_cutoff.",
            ))
            .await?;
        Ok(())
    }
}

/// `/llm_forget`: removes every stored history record of this channel that
/// carries the given Discord message id - the one command that truly
/// deletes history (a cutoff hides, this removes; a tombstone would keep
/// the content in storage, defeating the moderation purpose). Runs under
/// the channel's processing lock: no in-flight engine run may hold the
/// record in its prompt while it goes away.
pub(super) struct ForgetLlmHandler {
    locks: Arc<ChannelLocks>,
    engine: Arc<ChatEngine>,
}

impl ForgetLlmHandler {
    pub(super) fn new(locks: Arc<ChannelLocks>, engine: Arc<ChatEngine>) -> Self {
        Self { locks, engine }
    }
}

#[async_trait]
impl CommandHandler for ForgetLlmHandler {
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
        let raw = args.get("message_id").unwrap_or_default();
        let Ok(message_id) = raw.parse::<u64>() else {
            services
                .chat_output
                .send(command_reply(format!(
                    "\"{raw}\" is not a message id - right-click a message and use \
                     Copy Message ID."
                )))
                .await?;
            return Ok(());
        };

        let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
        let _channel = channel.lock().await;

        let channel_id = event.origin.channel_id.get();
        let seqs = self.engine.forget_message(storage, channel_id, message_id).await?;
        if seqs.is_empty() {
            services
                .chat_output
                .send(command_reply(format!(
                    "No stored history entry carries message id {message_id}."
                )))
                .await?;
            return Ok(());
        }
        // Honesty caveat: records below the compaction cutoff already fed
        // the committed summary - deletion cannot unwind that text.
        let below_cutoff = storage
            .get(NAMESPACE, &channel_state_key(channel_id))
            .await?
            .and_then(|raw| serde_json::from_value::<ConversationState>(raw).ok())
            .is_some_and(|state| seqs.iter().any(|seq| *seq <= state.cutoff_seq));
        let caveat = if below_cutoff {
            " It was already folded into this channel's summary; the summary text still \
             reflects it."
        } else {
            ""
        };
        services
            .chat_output
            .send(command_reply(format!(
                "Removed {} stored history entr{} for message id {message_id}.{caveat}",
                seqs.len(),
                if seqs.len() == 1 { "y" } else { "ies" },
            )))
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
    window: usize,
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

    // The estimate covers the engine's operational window (depth + compaction
    // tail, post-cutoff) - the same shape the engine assembles and calibrates
    // against, not the whole uncompacted log, which is unbounded when
    // compaction is off or failing.
    let limit = u32::try_from(window.max(1)).unwrap_or(u32::MAX);
    let mut context_chars = 0u64;
    let mut counted = 0u64;
    let mut skipped = 0u64;
    let stored = match storage.list_last(records_ns, limit).await {
        Ok(stored) => stored,
        Err(err) => {
            tracing::debug!(%err, "usage estimate degraded - record log unreadable");
            Vec::new()
        }
    };
    for stored in stored {
        if stored.seq <= state.cutoff_seq {
            continue;
        }
        match serde_json::from_value::<ConversationRecord>(stored.payload) {
            Ok(record) => {
                // estimator: precision loss is fine
                #[allow(clippy::cast_precision_loss)]
                let chars = record.content.chars().count() as u64;
                context_chars += chars;
                counted += 1;
            }
            Err(_) => skipped += 1,
        }
    }
    if skipped > 0 {
        tracing::debug!(skipped, "usage estimate skipped malformed records");
    }
    // estimator: precision loss is fine
    #[allow(clippy::cast_precision_loss)]
    let estimated = stats.tokens_per_char * context_chars as f64 + 8.0 * counted as f64;
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
fn react_label(config: &ChannelConfig, emoji_inject: Option<&str>) -> String {
    if !config.react && config.react_emoji_inject == EmojiInject::None {
        return "off".to_owned();
    }
    let mut parts = Vec::new();
    parts.push(if config.react {
        "on".to_owned()
    } else {
        "off (the emoji list would not reach the model)".to_owned()
    });
    match config.react_emoji_inject {
        EmojiInject::None => {}
        EmojiInject::All => parts.push("emoji inject all".to_owned()),
        EmojiInject::Whitelist => parts.push(match emoji_inject {
            Some(detail) => format!("emoji inject whitelist ({detail})"),
            None => "emoji inject whitelist (no list)".to_owned(),
        }),
    }
    if config.react && config.random_react_chance_percent > 0.0 {
        parts.push(format!("{:.1}% silent-react", config.random_react_chance_percent));
    }
    parts.join(" · ")
}

/// Effective whitelist source for the status line: which scope's list would
/// apply (`guild, 3` / `channel, 1`) - the channel's own list when non-empty,
/// otherwise the guild-wide one. `None` = neither list exists.
async fn emoji_inject_detail(channel_id: u64, services: &KernelServices) -> Option<String> {
    let storage = services.guild_storage.as_ref()?;
    let channel = storage
        .get(NAMESPACE, &channel_emoji_whitelist_key(channel_id))
        .await
        .ok()
        .flatten()
        .map(parse_whitelist);
    if let Some(list) = channel.filter(|list| !list.is_empty()) {
        return Some(format!("channel, {}", list.len()));
    }
    let guild =
        storage.get(NAMESPACE, GUILD_EMOJI_WHITELIST_KEY).await.ok().flatten().map(parse_whitelist);
    guild.filter(|list| !list.is_empty()).map(|list| format!("guild, {}", list.len()))
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

/// Digit grouping for token counts (`1234567` -> `1,234,567`) - totals get
/// large enough that raw digits are hard to read at a glance.
fn grouped(count: u64) -> String {
    let digits = count.to_string();
    let groups: Vec<String> = digits
        .as_bytes()
        .rchunks(3)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect();
    groups.into_iter().rev().collect::<Vec<_>>().join(",")
}

/// One rendered usage line: requests plus the prompt/completion split.
fn usage_line(label: &str, total: &Dimension) -> String {
    format!("{label}: {}", usage_counts(total))
}

/// The counts without a label - the per-model list prefixes them with the
/// model ref instead of a label.
fn usage_counts(total: &Dimension) -> String {
    format!(
        "{} requests, {} prompt / {} completion tokens",
        grouped(total.requests),
        grouped(total.prompt_tokens),
        grouped(total.completion_tokens)
    )
}

/// `/llm_usage`: this server's token usage - all-time totals (per model)
/// and today's aggregate. Cross-guild numbers are operator information:
/// they surface only through the owner-tier `/llm_usage_global` (ephemeral),
/// never in a guild-facing reply.
pub(super) struct UsageLlmHandler {
    engine: Arc<ChatEngine>,
}

impl UsageLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for UsageLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(guild_id) = event.origin.guild_id else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };
        // The snapshot seeds lazily, so a fresh process still serves the
        // stored history.
        let (all_time, today) = self.engine.usage().guild_snapshot(guild_id.get()).await;
        let text = if all_time.total.requests == 0 {
            "No LLM usage recorded for this server yet.".to_owned()
        } else {
            let mut lines = vec![
                "**LLM usage for this server**".to_owned(),
                usage_line("All time", &all_time.total),
                usage_line("Today", &today),
            ];
            if !all_time.models.is_empty() {
                lines.push(String::new());
                lines.push("By model (all time):".to_owned());
                for (model, total) in &all_time.models {
                    lines.push(format!("- `{model}` \u{b7} {}", usage_counts(total)));
                }
            }
            lines.join("\n")
        };
        services.chat_output.send(command_reply(text)).await?;
        Ok(())
    }
}

/// Cap on the per-server ranking: a deployment serving many guilds must
/// not grow one reply without bound - the long tail is the small one.
pub(super) const GLOBAL_GUILD_CAP: usize = 10;

/// `/llm_usage_global`: every server's token usage. Operator information,
/// therefore owner-tier: the auth gate enforces the tier (bot owners come
/// from the deployment config) - this handler never checks identity. The
/// reply is ephemeral - visible to the invoking owner alone - so
/// cross-guild numbers never reach a guild-visible surface. The guild
/// guard is defense in depth, not decoration: DMs bypass the auth gate
/// entirely, so a DM invocation could not be tier-checked at all.
pub(super) struct GlobalUsageLlmHandler {
    engine: Arc<ChatEngine>,
}

impl GlobalUsageLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for GlobalUsageLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if event.origin.guild_id.is_none() {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        }
        // The snapshot seeds lazily, so a fresh process still serves the
        // stored history.
        let (snapshot, today) = self.engine.usage().global_snapshot().await;
        let text = if snapshot.total.requests == 0 {
            "No LLM usage recorded yet.".to_owned()
        } else {
            let mut lines = vec![
                "**LLM usage across all servers**".to_owned(),
                usage_line("All time", &snapshot.total),
                usage_line("Today", &today),
            ];
            if !snapshot.models.is_empty() {
                lines.push(String::new());
                lines.push("By model (all time):".to_owned());
                for (model, total) in &snapshot.models {
                    lines.push(format!("- `{model}` \u{b7} {}", usage_counts(total)));
                }
            }
            if !snapshot.guilds.is_empty() {
                lines.push(String::new());
                lines.push("By server (all time):".to_owned());
                let mut guilds = snapshot.guilds.clone();
                guilds.sort_by(|a, b| b.1.requests.cmp(&a.1.requests));
                let shown = guilds.len().min(GLOBAL_GUILD_CAP);
                for (guild_id, dim) in guilds.iter().take(shown) {
                    lines.push(format!("- `{guild_id}` \u{b7} {}", usage_counts(dim)));
                }
                let hidden = guilds.len().saturating_sub(shown);
                if hidden > 0 {
                    lines.push(format!("...and {hidden} more"));
                }
            }
            lines.join("\n")
        };
        services.chat_output.send(command_reply(text)).await?;
        Ok(())
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
        let config = ChannelConfig::from_stored(raw, self.engine.settings().min_split_length)?;
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
        // The estimate window: what the engine would actually load for the
        // next message (assembly depth + compaction tail).
        let depth = usize::try_from(config.context_messages).unwrap_or(usize::MAX);
        let keep_tail =
            usize::try_from(self.engine.settings().compaction_keep_tail).unwrap_or(usize::MAX);
        let window = depth.saturating_add(keep_tail).max(1);
        let usage = usage_lines(storage, &records_ns, &state, window, &usage_stats).await;

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
        let emoji_inject = emoji_inject_detail(event.origin.channel_id.get(), services).await;
        let reactions = react_label(&config, emoji_inject.as_deref());
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
            config.context_messages,
            if config.compaction_enabled { "on" } else { "off" },
            capture_label(config.capture),
            chime_label(config.random_reply_chance_percent, config.random_cooldown_secs),
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

/// `/llm_emoji_whitelist`: manages the emoji entries the `whitelist` inject
/// mode filters against - the `scope` argument picks the guild-wide baseline
/// or the channel's own override (a non-empty channel list replaces the
/// baseline for that channel). Names are validated against the guild's
/// actual custom emojis at add time, so the list only ever contains
/// reactable entries; each entry may carry a short description the menu
/// renders next to the emoji's wire form (`desc` sets, updates, or clears
/// it). Lock-free like `/llm_admin`: rare moderator writes, last one wins.
pub(super) struct EmojiWhitelistLlmHandler;

/// Longest accepted emoji description, in characters: a short hint for the
/// model, not documentation.
const EMOJI_DESCRIPTION_LIMIT: usize = 100;

/// The optional `description` argument, trimmed; `None` = empty or absent.
/// `Err` carries the rejection notice when over the cap.
fn normalize_description(raw: Option<&str>) -> Result<Option<String>, String> {
    match raw.map(str::trim).filter(|text| !text.is_empty()) {
        None => Ok(None),
        Some(text) if text.chars().count() > EMOJI_DESCRIPTION_LIMIT => Err(format!(
            "`description` is too long ({} characters, the maximum is {EMOJI_DESCRIPTION_LIMIT}).",
            text.chars().count()
        )),
        Some(text) => Ok(Some(text.to_owned())),
    }
}

/// Whitelist names as one backticked, comma-separated run.
fn render_names(list: &[EmojiWhitelistEntry]) -> String {
    list.iter().map(|entry| format!("`{}`", entry.name)).collect::<Vec<_>>().join(", ")
}

/// The `list` reply: one line per entry, the description (if any) after an
/// em dash. Empty-string descriptions (hand-edited docs) count as none -
/// the same reading the menu applies.
fn whitelist_list_reply(scope: &str, list: &[EmojiWhitelistEntry]) -> String {
    if list.is_empty() {
        return format!("The {scope} emoji whitelist is empty.");
    }
    let lines: Vec<String> = list
        .iter()
        .map(|entry| match entry.description.as_deref().filter(|text| !text.is_empty()) {
            Some(description) => format!("- `{}` \u{2014} {description}", entry.name),
            None => format!("- `{}`", entry.name),
        })
        .collect();
    let noun = if list.len() == 1 { "entry" } else { "entries" };
    format!("The {scope} emoji whitelist ({} {noun}):\n{}", list.len(), lines.join("\n"))
}

#[async_trait]
impl CommandHandler for EmojiWhitelistLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        const USAGE: &str = "Usage: `/llm_emoji_whitelist action:<add|remove|list|clear|desc> \
             scope:<guild|channel> name:<emoji> [description:<text>]` - `name` is required for \
             add, remove, and desc; `description` (up to 100 characters) sets or updates the \
             hint, empty clears it.";
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };
        let (Some(action), Some(scope)) = (args.get("action"), args.get("scope")) else {
            services.chat_output.send(command_reply(USAGE)).await?;
            return Ok(());
        };
        let key = match scope {
            "guild" => GUILD_EMOJI_WHITELIST_KEY.to_owned(),
            "channel" => channel_emoji_whitelist_key(event.origin.channel_id.get()),
            other => {
                services
                    .chat_output
                    .send(command_reply(format!("Unknown scope `{other}` - use guild or channel.")))
                    .await?;
                return Ok(());
            }
        };
        let mut list: Vec<EmojiWhitelistEntry> =
            storage.get(NAMESPACE, &key).await?.map(parse_whitelist).unwrap_or_default();
        match action {
            "add" | "remove" => {
                let Some(name) = args.get("name") else {
                    services.chat_output.send(command_reply(USAGE)).await?;
                    return Ok(());
                };
                if action == "add" {
                    let known =
                        services.chat_output_factory.reactable_emojis(&event.origin).list().await;
                    if !known.iter().any(|emoji| emoji.name == name) {
                        services
                            .chat_output
                            .send(command_reply(format!(
                                "`{name}` is not a custom emoji of this server - use its exact name."
                            )))
                            .await?;
                        return Ok(());
                    }
                    if list.iter().any(|entry| entry.name == name) {
                        services
                            .chat_output
                            .send(command_reply(format!(
                                "`{name}` is already on the {scope} whitelist ({} entries).",
                                list.len()
                            )))
                            .await?;
                        return Ok(());
                    }
                    // The cap check trails the name checks: an invalid name
                    // with an oversized hint should name the real problem.
                    let description = match normalize_description(args.get("description")) {
                        Ok(description) => description,
                        Err(notice) => {
                            services.chat_output.send(command_reply(notice)).await?;
                            return Ok(());
                        }
                    };
                    list.push(EmojiWhitelistEntry {
                        name: name.to_owned(),
                        description: description.clone(),
                    });
                    list.sort_by(|a, b| a.name.cmp(&b.name));
                    storage.set(NAMESPACE, &key, serde_json::json!(list)).await?;
                    let mut reply = format!(
                        "Added `{name}` - the {scope} whitelist now has {} entries: {}.",
                        list.len(),
                        render_names(&list)
                    );
                    if let Some(text) = &description {
                        reply.push_str(&format!("\n`{name}` description: {text}"));
                    }
                    services.chat_output.send(command_reply(reply)).await?;
                } else {
                    let Some(position) = list.iter().position(|entry| entry.name == name) else {
                        services
                            .chat_output
                            .send(command_reply(format!(
                                "`{name}` is not on the {scope} whitelist."
                            )))
                            .await?;
                        return Ok(());
                    };
                    list.remove(position);
                    storage.set(NAMESPACE, &key, serde_json::json!(list)).await?;
                    services
                        .chat_output
                        .send(command_reply(format!(
                            "Removed `{name}` - the {scope} whitelist now has {} entries: {}.",
                            list.len(),
                            render_names(&list)
                        )))
                        .await?;
                }
            }
            "desc" => {
                let Some(name) = args.get("name") else {
                    services.chat_output.send(command_reply(USAGE)).await?;
                    return Ok(());
                };
                let description = match normalize_description(args.get("description")) {
                    Ok(description) => description,
                    Err(notice) => {
                        services.chat_output.send(command_reply(notice)).await?;
                        return Ok(());
                    }
                };
                let Some(entry) = list.iter_mut().find(|entry| entry.name == name) else {
                    services
                        .chat_output
                        .send(command_reply(format!("`{name}` is not on the {scope} whitelist.")))
                        .await?;
                    return Ok(());
                };
                match description {
                    Some(text) => {
                        entry.description = Some(text.clone());
                        storage.set(NAMESPACE, &key, serde_json::json!(list)).await?;
                        services
                            .chat_output
                            .send(command_reply(format!(
                                "Updated the description of `{name}`: {text}"
                            )))
                            .await?;
                    }
                    None => {
                        if entry.description.take().is_some() {
                            storage.set(NAMESPACE, &key, serde_json::json!(list)).await?;
                            services
                                .chat_output
                                .send(command_reply(format!(
                                    "Cleared the description of `{name}`."
                                )))
                                .await?;
                        } else {
                            services
                                .chat_output
                                .send(command_reply(format!(
                                    "`{name}` has no description to clear."
                                )))
                                .await?;
                        }
                    }
                }
            }
            "list" => {
                services
                    .chat_output
                    .send(command_reply(whitelist_list_reply(scope, &list)))
                    .await?;
            }
            "clear" => {
                storage.delete(NAMESPACE, &key).await?;
                services
                    .chat_output
                    .send(command_reply(format!("The {scope} emoji whitelist cleared.")))
                    .await?;
            }
            other => {
                services
                    .chat_output
                    .send(command_reply(format!(
                        "Unknown action `{other}` - use add, remove, list, clear, or desc."
                    )))
                    .await?;
            }
        }
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
        let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
        let _channel = channel.lock().await;
        let Some(mut config) =
            load_assigned_config(event, services, self.engine.settings().min_split_length).await?
        else {
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

        match apply_set(
            &mut config,
            key,
            value,
            services.platform_info.message_limit(),
            self.engine.settings().min_split_length,
        ) {
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

/// `/llm_get`: reads back a channel's current setting values - one key, or
/// every key when the argument is omitted. Read-only twin of `/llm_set`:
/// same moderator tier, same ephemeral visibility (config details are
/// nobody else's business), no channel lock (a single document read; a
/// concurrent `/llm_set` lands before or after it, both are fine).
pub(super) struct GetLlmHandler {
    engine: Arc<ChatEngine>,
}

impl GetLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for GetLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(config) =
            load_assigned_config(event, services, self.engine.settings().min_split_length).await?
        else {
            return Ok(());
        };
        match args.get("key") {
            Some(key) => match current_value(&config, key, self.engine.settings()) {
                Some(value) => {
                    services.chat_output.send(command_reply(format!("`{key}`: {value}"))).await?;
                }
                None => {
                    services
                        .chat_output
                        .send(command_reply(format!(
                            "Unknown key `{key}`. Keys: {}.",
                            SET_KEYS.join(", ")
                        )))
                        .await?;
                }
            },
            // No key: every setting, one line each. Long free-text values
            // render as shapes - the full text is one `/llm_get key` away.
            None => {
                let lines: Vec<String> = SET_KEYS
                    .iter()
                    .map(|key| {
                        let value = current_value(&config, key, self.engine.settings())
                            .unwrap_or_else(|| "?".to_owned());
                        format!("`{key}`: {value}")
                    })
                    .collect();
                services
                    .chat_output
                    .send(command_reply(format!("Channel settings:\n{}", lines.join("\n"))))
                    .await?;
            }
        }
        Ok(())
    }
}

/// `/llm_dump`: every `/llm_set` key with its effective value at once, in
/// one copy-pasteable code fence. Read-only sibling of `/llm_get` - same
/// tier, same value renderer, one document read, no channel lock. Lines
/// whose value differs from a fresh `/llm_assign` are marked `*` (one
/// comparison against a baseline config shared by every key - no per-key
/// default table to drift). Prompts are never dumped - not even as a
/// preview; the dump only says whether one is set and how big it is, the
/// text is one argument-free `/llm_set_prompt kind` away. The fence is
/// plain three backticks - Discord renders deeper fences with a literal
/// tick at each end - so rendered values (channel-managed text,
/// templates) that may themselves contain code fences are sanitized
/// first: any backtick run of three or more degrades to a doubled tick
/// and can never close the block early.
pub(super) struct DumpLlmHandler {
    engine: Arc<ChatEngine>,
}

impl DumpLlmHandler {
    pub(super) fn new(engine: Arc<ChatEngine>) -> Self {
        Self { engine }
    }
}

/// A rendered value past this many characters degrades to a head preview
/// in the dump (long custom templates) - every key plus the prompt shapes
/// must stay inside one Discord message, and the full text is one focused
/// read command away.
const DUMP_VALUE_PREVIEW: usize = 120;

/// Dump shape of one prompt: set-or-not and size only - never the text.
fn prompt_shape(value: Option<&String>) -> String {
    value.map_or_else(
        || "<plugin default>".to_owned(),
        |text| format!("<set, {} chars>", text.chars().count()),
    )
}

/// Collapse backtick runs of three or more down to a doubled tick so a
/// rendered value can never close the dump's three-backtick fence early.
/// Single and doubled ticks are literal inside a fence and pass through.
fn fence_safe(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut ticks = 0usize;
    for ch in value.chars() {
        if ch == '`' {
            if ticks < 2 {
                out.push('`');
            }
            ticks += 1;
        } else {
            ticks = 0;
            out.push(ch);
        }
    }
    out
}

#[async_trait]
impl CommandHandler for DumpLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(config) =
            load_assigned_config(event, services, self.engine.settings().min_split_length).await?
        else {
            return Ok(());
        };
        // A fresh assignment is the default baseline. Built with the
        // channel's own model, so `model` itself never reads as customized -
        // it IS the assignment.
        let baseline = ChannelConfig::assigned(config.model.clone());
        let mut lines: Vec<String> = SET_KEYS
            .iter()
            .map(|key| {
                let value = current_value(&config, key, self.engine.settings())
                    .map(|value| fence_safe(&value))
                    .unwrap_or_else(|| "?".to_owned());
                let marker = if current_value(&baseline, key, self.engine.settings())
                    .map(|value| fence_safe(&value))
                    .as_deref()
                    == Some(value.as_str())
                {
                    ""
                } else {
                    "* "
                };
                let value = if value.chars().count() > DUMP_VALUE_PREVIEW {
                    format!(
                        "{}… ({} chars total)",
                        preview(&value, DUMP_VALUE_PREVIEW),
                        value.chars().count()
                    )
                } else {
                    value
                };
                format!("{marker}{key} = {value}")
            })
            .collect();
        // The three prompts are never dumped - set-or-not and size only;
        // the text is one `/llm_set_prompt kind` away.
        for kind in [PromptKind::System, PromptKind::Compaction, PromptKind::Image] {
            let marker = if kind.field(&config).is_some() { "* " } else { "" };
            lines.push(format!(
                "{marker}{} = {}",
                kind.field_name(),
                prompt_shape(kind.field(&config).as_ref())
            ));
        }
        services
            .chat_output
            .send(command_reply(format!(
                "Channel settings ({} keys + 3 prompts; `*` differs from the default):\n```\n{}\n```",
                SET_KEYS.len(),
                lines.join("\n")
            )))
            .await?;
        Ok(())
    }
}

/// The current value of one `/llm_set` key, rendered for `/llm_get`.
/// `None` = unknown key. Defaults render as the EFFECTIVE value (what the
/// engine would use now), so an admin never has to guess what "default"
/// means; long free-text values stay full unless they would blow the reply
/// budget, where they degrade to a head preview plus a length note.
fn current_value(config: &ChannelConfig, key: &str, settings: &LlmSettings) -> Option<String> {
    let not_sent = || "not sent (provider default)".to_owned();
    let on_off = |enabled: bool| if enabled { "on" } else { "off" }.to_owned();
    match key {
        "model" => Some(config.model.clone()),
        "temperature" | "top_p" | "top_k" | "min_p" | "frequency_penalty" | "presence_penalty" => {
            Some(
                sampling_value(&config.params, key)
                    .map_or_else(not_sent, |value| format!("{value}")),
            )
        }
        "max_tokens" => {
            Some(config.params.max_tokens.map_or_else(not_sent, |value| format!("{value}")))
        }
        "reasoning_effort" => Some(
            config
                .params
                .reasoning_effort
                .clone()
                .map_or_else(not_sent, |value| format!("`{value}`")),
        ),
        "context_messages" => Some(format!("{} messages", config.context_messages)),
        "context_tokens" => Some(
            config
                .context_tokens
                .map_or_else(|| "auto".to_owned(), |tokens| format!("{tokens} tokens")),
        ),
        "streaming" => Some(on_off(config.streaming)),
        "react" => Some(on_off(config.react)),
        "react_emoji_inject" => Some(config.react_emoji_inject.as_str().to_owned()),
        "capture" => Some(match config.capture {
            CaptureMode::BotRelated => "bot_related".to_owned(),
            CaptureMode::AllMessages => "all_messages".to_owned(),
        }),
        "compaction" => Some(on_off(config.compaction_enabled)),
        "compaction_model" => Some(
            config
                .compaction_model
                .clone()
                .unwrap_or_else(|| format!("the channel's chat model (`{}`)", config.model)),
        ),
        "images" => Some(on_off(config.images)),
        "image_model" => Some(config.image_model.clone().unwrap_or_else(|| {
            settings.image_model.clone().map_or_else(
                || "off (no operator image model)".to_owned(),
                |model| format!("plugin `image_model` (`{model}`)"),
            )
        })),
        "random_reply_chance" => Some(format!("{:.1}%", config.random_reply_chance_percent)),
        "random_cooldown" => Some(format!("{} seconds", config.random_cooldown_secs)),
        "random_react_chance" => Some(format!("{:.1}%", config.random_react_chance_percent)),
        "split_length" => Some(config.split_length.map_or_else(
            || format!("{} (max_message_length)", settings.max_message_length),
            |limit| format!("{limit}"),
        )),
        "turn_template" => Some(config.turn_template.clone().map_or_else(
            || format!("`{}` (plugin default)", super::conversation::DEFAULT_TURN_TEMPLATE),
            |template| format!("`{template}`"),
        )),
        _ => None,
    }
}

/// Read half of [`set_float`]: the sampling parameter a `/llm_set` key
/// addresses, `None` when unset (not sent to the provider).
fn sampling_value(params: &GenParams, key: &str) -> Option<f64> {
    match key {
        "temperature" => params.temperature,
        "top_p" => params.top_p,
        "top_k" => params.top_k,
        "min_p" => params.min_p,
        "frequency_penalty" => params.frequency_penalty,
        "presence_penalty" => params.presence_penalty,
        _ => None,
    }
}

/// Long free-text values stay full while they fit a Discord reply; beyond
/// that they degrade to a head preview plus a length note (same shapes-not-
/// contents convention as the audit log).
fn shape_or_preview(text: &str) -> String {
    const FULL_UP_TO: usize = 1200;
    if text.chars().count() <= FULL_UP_TO {
        return format!("`{text}`");
    }
    format!("`{}` ... ({} chars total)", preview(text, FULL_UP_TO), text.chars().count())
}

/// The channel prompts `/llm_set_prompt` manages, selected by `kind`.
#[derive(Clone, Copy)]
pub(super) enum PromptKind {
    /// The bot's persona - the conversation's system prompt.
    System,
    /// The summarizer instruction used by compaction.
    Compaction,
    /// The image-recognition instruction.
    Image,
}

impl PromptKind {
    /// The `kind` argument grammar - also the Discord choices dropdown.
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "system" => Some(Self::System),
            "compaction" => Some(Self::Compaction),
            "image" => Some(Self::Image),
            _ => None,
        }
    }

    /// The config field this kind addresses (read).
    fn field(self, config: &ChannelConfig) -> &Option<String> {
        match self {
            Self::System => &config.system_prompt,
            Self::Compaction => &config.compaction_prompt,
            Self::Image => &config.image_prompt,
        }
    }

    /// The config field this kind addresses (write).
    fn field_mut(self, config: &mut ChannelConfig) -> &mut Option<String> {
        match self {
            Self::System => &mut config.system_prompt,
            Self::Compaction => &mut config.compaction_prompt,
            Self::Image => &mut config.image_prompt,
        }
    }

    /// Key-style name used in replies (`system_prompt` etc.), mirroring the
    /// `/llm_set` acknowledgment shape.
    fn field_name(self) -> &'static str {
        match self {
            Self::System => "system_prompt",
            Self::Compaction => "compaction_prompt",
            Self::Image => "image_prompt",
        }
    }

    /// The effective prompt for read-back: the custom text (shape/preview)
    /// or what the plugin default contributes.
    fn current(self, config: &ChannelConfig, settings: &LlmSettings) -> String {
        if let Some(text) = self.field(config) {
            return shape_or_preview(text);
        }
        match self {
            Self::System => format!(
                "<plugin default, {} chars>",
                settings.default_system_prompt.chars().count()
            ),
            Self::Compaction => format!(
                "<plugin default, {} chars>",
                settings.default_compaction_prompt.chars().count()
            ),
            Self::Image => settings.image_prompt.clone().map_or_else(
                || "<built-in default>".to_owned(),
                |prompt| format!("<plugin default, {} chars>", prompt.chars().count()),
            ),
        }
    }
}

/// Applies one `/llm_set_prompt` mutation. Pure so the grammar stays unit-
/// testable: `Ok(message)` describes the change, `Err(usage)` is the reply
/// for a malformed value. `None` clears to the plugin default.
fn apply_prompt(
    config: &mut ChannelConfig,
    kind: PromptKind,
    value: Option<&str>,
) -> Result<String, String> {
    match value {
        None => {
            *kind.field_mut(config) = None;
            Ok(format!("`{}` cleared (the plugin default applies).", kind.field_name()))
        }
        Some(text) => {
            if text.trim().is_empty() {
                return Err(
                    "`prompt` cannot be empty - give text, `clear`, or attach a file.".to_owned()
                );
            }
            validate_prompt_text(kind, text)?;
            *kind.field_mut(config) = Some(text.to_owned());
            Ok(format!("`{}` updated.", kind.field_name()))
        }
    }
}

/// System and compaction prompts are template-rendered per request; a typo
/// must not sit unnoticed in guild storage, so unknown tokens are rejected
/// at set time with the valid list. Image prompts are not rendered
/// (token-free by contract) and pass validation untouched.
fn validate_prompt_text(kind: PromptKind, text: &str) -> Result<(), String> {
    if matches!(kind, PromptKind::Image) {
        return Ok(());
    }
    let unknown = prompts::unknown_tokens(text);
    if unknown.is_empty() {
        return Ok(());
    }
    let listed =
        unknown.iter().map(|token| format!("{{{{{token}}}}}")).collect::<Vec<_>>().join(", ");
    Err(format!(
        "unknown template token(s) {listed} - valid: {}.",
        prompts::VALID_TOKENS.join(", ")
    ))
}

/// `/llm_set_prompt`: one command for every channel prompt - system persona,
/// compaction and image instructions. `prompt` sets inline (`clear`/`none`/
/// `default` restores the plugin default), `file` sets from an uploaded
/// attachment (long prompts, formatting preserved), and giving neither reads
/// the current value back. Mutations run under the channel's processing
/// lock; the file download happens before it (slow network I/O must not
/// freeze the channel's chat), and the read-back takes no lock at all (a
/// single document read, like `/llm_get`).
pub(super) struct SetPromptLlmHandler {
    locks: Arc<ChannelLocks>,
    engine: Arc<ChatEngine>,
    client: reqwest::Client,
    max_prompt_file_bytes: u64,
}

impl SetPromptLlmHandler {
    pub(super) fn new(
        locks: Arc<ChannelLocks>,
        engine: Arc<ChatEngine>,
        client: reqwest::Client,
        max_prompt_file_bytes: u64,
    ) -> Self {
        Self { locks, engine, client, max_prompt_file_bytes }
    }
}

#[async_trait]
impl CommandHandler for SetPromptLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        const USAGE: &str = "Usage: `/llm_set_prompt kind prompt` - kind is `system` (persona), \
             `compaction` or `image`; give `prompt` text (`clear` restores the default), attach \
             `file` for long prompts, or give neither to show the current value.";
        let Some(kind_text) = args.get("kind") else {
            services.chat_output.send(command_reply(USAGE)).await?;
            return Ok(());
        };
        let Some(kind) = PromptKind::parse(kind_text) else {
            services
                .chat_output
                .send(command_reply(format!(
                    "Unknown kind `{kind_text}`. Kinds: system, compaction, image."
                )))
                .await?;
            return Ok(());
        };
        let value = args.get("prompt");
        let file = args.get("file");

        // File path: download BEFORE the channel lock (see the struct doc).
        if let Some(url) = file {
            if value.is_some() {
                services
                    .chat_output
                    .send(command_reply("Give either `prompt` or `file`, not both."))
                    .await?;
                return Ok(());
            }
            if let Err(usage) = validate_prompt_file_url(url) {
                services.chat_output.send(command_reply(usage)).await?;
                return Ok(());
            }
            let prompt =
                match download_prompt_file(&self.client, self.max_prompt_file_bytes, url).await {
                    Ok(prompt) => prompt,
                    Err(reason) => {
                        services
                            .chat_output
                            .send(command_reply(format!("Could not load the attachment: {reason}")))
                            .await?;
                        return Ok(());
                    }
                };
            let characters = prompt.chars().count();
            if let Err(usage) = validate_prompt_text(kind, &prompt) {
                services.chat_output.send(command_reply(usage)).await?;
                return Ok(());
            }
            let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
            let _channel = channel.lock().await;
            let Some(mut config) =
                load_assigned_config(event, services, self.engine.settings().min_split_length)
                    .await?
            else {
                return Ok(());
            };
            *kind.field_mut(&mut config) = Some(prompt);
            save_config(event, services, config).await?;
            services
                .chat_output
                .send(command_reply(format!(
                    "`{}` set from file ({} characters).",
                    kind.field_name(),
                    characters
                )))
                .await?;
            return Ok(());
        }

        match value {
            // Mutation: read-modify-write under the channel lock.
            Some(text) => {
                let cleared = matches!(text, "clear" | "none" | "default");
                let channel = self.locks.lock_for(services.platform_info.slug(), &event.origin);
                let _channel = channel.lock().await;
                let Some(mut config) =
                    load_assigned_config(event, services, self.engine.settings().min_split_length)
                        .await?
                else {
                    return Ok(());
                };
                match apply_prompt(&mut config, kind, (!cleared).then_some(text)) {
                    Ok(message) => {
                        save_config(event, services, config).await?;
                        services.chat_output.send(command_reply(message)).await?;
                    }
                    Err(usage) => {
                        services.chat_output.send(command_reply(usage)).await?;
                    }
                }
            }
            // Read-back: no channel lock (single document read, like
            // `/llm_get`).
            None => {
                let Some(config) =
                    load_assigned_config(event, services, self.engine.settings().min_split_length)
                        .await?
                else {
                    return Ok(());
                };
                services
                    .chat_output
                    .send(command_reply(format!(
                        "`{}`: {}",
                        kind.field_name(),
                        kind.current(&config, self.engine.settings())
                    )))
                    .await?;
            }
        }
        Ok(())
    }
}

/// Downloads and decodes a `/llm_set_prompt` attachment with the configured
/// byte cap. The body is STREAM-read: the cap aborts the transfer instead
/// of buffering a lying or chunked response whole. `Err` carries the
/// user-facing reason; transport details go to logs.
async fn download_prompt_file(
    client: &reqwest::Client,
    max_prompt_file_bytes: u64,
    url: &str,
) -> Result<String, String> {
    let mut response = client.get(url).send().await.map_err(|err| {
        tracing::warn!(%err, "prompt file download failed");
        "the attachment could not be downloaded".to_owned()
    })?;
    if !response.status().is_success() {
        return Err(format!("the attachment host returned HTTP {}", response.status()));
    }
    if let Some(len) = response.content_length()
        && len > max_prompt_file_bytes
    {
        return Err(format!(
            "the attachment exceeds the limit ({len} > {max_prompt_file_bytes} bytes, \
             `max_prompt_file_bytes`)"
        ));
    }
    let limit = usize::try_from(max_prompt_file_bytes).unwrap_or(usize::MAX);
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
                max_prompt_file_bytes
            ));
        }
    }
    let text =
        String::from_utf8(body).map_err(|_| "the attachment is not valid UTF-8 text".to_owned())?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("the attachment is empty".to_owned());
    }
    Ok(trimmed.to_owned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::GuildId;
    use crate::kernel::spi_ports::StoragePort;
    use crate::plugins::llm::RecordRole;
    use crate::plugins::llm::completion_port::{ResponseTiming, TokenUsage};
    use crate::test_support::InMemoryStorage;

    /// The cap tests exercise: what [`TestPlatformInfo`] serves, mirroring
    /// the wired adapter.
    const PLATFORM_LIMIT: Option<usize> = Some(2000);

    /// The default reply-chunk floor, as an untouched `[llm]` config yields.
    const CHUNK_FLOOR: usize = 100;

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

    async fn download(cap: u64, url: &str) -> Result<String, String> {
        download_prompt_file(&reqwest::Client::new(), cap, url).await
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

    /// The download happy path and the HTTP-status branch: 200 decodes the
    /// body (trimmed), a non-2xx surfaces the status in the reply.
    #[tokio::test]
    async fn prompt_file_download_success_and_status_error() {
        let (url, server) =
            raw_http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n  prompt".to_vec()).await;
        let prompt = download(10_000, &url).await.expect("download expected to succeed");
        let _ = server.await;
        assert_eq!(prompt, "prompt");

        let (url, server) =
            raw_http_server(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec()).await;
        let err = download(10_000, &url).await.expect_err("404 expected to fail");
        let _ = server.await;
        assert!(err.contains("404"), "{err}");
    }

    /// The size cap aborts a close-delimited body mid-transfer - a missing
    /// or lying Content-Length cannot make the handler buffer the whole
    /// body (the streaming cap is the point of this test).
    #[tokio::test]
    async fn prompt_file_download_cap_aborts_oversized_body() {
        let script = format!("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\n{}", "x".repeat(500));
        let (url, server) = raw_http_server(script.into_bytes()).await;

        let err = download(64, &url).await.expect_err("oversized body expected to fail");
        let _ = server.await;

        assert!(err.contains("exceeds the limit"), "{err}");
    }

    #[tokio::test]
    async fn prompt_file_download_rejects_non_utf8() {
        let mut script = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n".to_vec();
        script.extend_from_slice(&[0xFF, 0xFE]);
        let (url, server) = raw_http_server(script).await;

        let err = download(10_000, &url).await.expect_err("invalid UTF-8 expected to fail");
        let _ = server.await;

        assert!(err.contains("UTF-8"), "{err}");
    }

    /// Description normalization trims, clears on empty/absent, and
    /// enforces the character cap (100, boundary inclusive).
    #[test]
    fn description_normalization_trims_caps_and_clears() {
        assert_eq!(normalize_description(None).ok().flatten(), None);
        assert_eq!(normalize_description(Some("  ")).ok().flatten(), None);
        assert_eq!(
            normalize_description(Some("  smug face ")).ok().flatten(),
            Some("smug face".to_owned())
        );
        let oversized = "x".repeat(EMOJI_DESCRIPTION_LIMIT + 1);
        assert!(normalize_description(Some(&oversized)).is_err(), "over the cap is rejected");
        let at_cap = "x".repeat(EMOJI_DESCRIPTION_LIMIT);
        assert_eq!(
            normalize_description(Some(&at_cap)).ok().flatten(),
            Some(at_cap),
            "exactly at the cap passes"
        );
    }

    /// The `list` reply renders descriptions after an em dash and stays
    /// one-per-line; the empty whitelist has its own wording. Empty-string
    /// descriptions (hand-edited docs) read as none - no dangling em dash -
    /// and the count pluralizes.
    #[test]
    fn whitelist_list_reply_renders_descriptions_and_empty_scope() {
        assert_eq!(whitelist_list_reply("guild", &[]), "The guild emoji whitelist is empty.");
        let list = vec![
            EmojiWhitelistEntry {
                name: "dorkiS".to_owned(),
                description: Some("smug face".to_owned()),
            },
            EmojiWhitelistEntry { name: "ashuu".to_owned(), description: None },
        ];
        let reply = whitelist_list_reply("channel", &list);
        assert!(reply.contains("The channel emoji whitelist (2 entries):"), "{reply}");
        assert!(reply.contains("- `dorkiS` \u{2014} smug face"), "{reply}");
        assert!(reply.contains("- `ashuu`"), "{reply}");

        let blank = vec![EmojiWhitelistEntry {
            name: "dorkiS".to_owned(),
            description: Some(String::new()),
        }];
        let reply = whitelist_list_reply("guild", &blank);
        assert!(reply.contains("The guild emoji whitelist (1 entry):"), "{reply}");
        assert!(reply.contains("- `dorkiS`"), "{reply}");
        assert!(!reply.contains('\u{2014}'), "no dangling em dash: {reply}");
    }

    /// All three prompt kinds share the set/clear grammar: text stores,
    /// clearing resets to the plugin default, empty text is refused, and
    /// read-back renders custom text vs what the default contributes.
    #[test]
    fn prompt_kinds_set_clear_and_render() {
        let settings = LlmSettings::default();
        let mut config = ChannelConfig::assigned("m".to_owned());

        for kind in [PromptKind::System, PromptKind::Compaction, PromptKind::Image] {
            let set = apply_prompt(&mut config, kind, Some("You are a pirate.")).expect("set");
            assert!(set.contains("updated"), "{set}");
            assert_eq!(kind.field(&config).as_deref(), Some("You are a pirate."));

            let cleared = apply_prompt(&mut config, kind, None).expect("clear");
            assert!(cleared.contains("cleared"), "{cleared}");
            assert_eq!(kind.field(&config).as_deref(), None);
        }

        // Default states render what the plugin contributes.
        assert!(PromptKind::System.current(&config, &settings).contains("plugin default"));
        assert!(PromptKind::Compaction.current(&config, &settings).contains("plugin default"));
        assert!(PromptKind::Image.current(&config, &settings).contains("built-in default"));

        // Unknown kinds and empty text are rejected.
        assert!(PromptKind::parse("vibes").is_none());
        assert!(apply_prompt(&mut config, PromptKind::System, Some("   ")).is_err());
    }

    /// The set-time token gate: template-rendered kinds (system,
    /// compaction) reject unknown tokens, naming the offender together with
    /// the valid list; image prompts are token-free by contract and pass
    /// untouched - the same text that fails for a persona is a legal image
    /// instruction. The scanner itself is pinned in prompts.rs; this is the
    /// wrapper and its per-kind exemption.
    #[test]
    fn prompt_validation_rejects_unknown_tokens_except_image_prompts() {
        let err = validate_prompt_text(PromptKind::System, "You are {{wat}} today.")
            .expect_err("unknown token expected to be rejected");
        assert!(err.contains("{{wat}}"), "the offending token must be named: {err}");
        assert!(err.contains("valid:"), "the valid list must be included: {err}");

        assert!(validate_prompt_text(PromptKind::Compaction, "Summarize {{wat}}.").is_err());

        // No rendering happens for image prompts - unknown tokens are
        // literal text there, not errors.
        assert!(validate_prompt_text(PromptKind::Image, "You are {{wat}} today.").is_ok());
        // Known tokens pass for the rendered kinds.
        assert!(validate_prompt_text(PromptKind::System, "You are {{bot}} on {{date}}.").is_ok());
    }

    #[test]
    fn float_params_set_and_clear() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let message = apply_set(&mut config, "temperature", "0.7", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("set expected");
        assert!(message.contains("0.7"));
        assert_eq!(config.params.temperature, Some(0.7));

        apply_set(&mut config, "top_k", "40", PLATFORM_LIMIT, CHUNK_FLOOR).expect("set expected");
        assert_eq!(config.params.top_k, Some(40.0));

        apply_set(&mut config, "temperature", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clear expected");
        assert_eq!(config.params.temperature, None);

        assert!(apply_set(&mut config, "top_p", "abc", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
    }

    #[test]
    fn bool_keys_accept_on_off_words() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "streaming", "on", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("on expected");
        assert!(config.streaming);
        apply_set(&mut config, "streaming", "off", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("off expected");
        assert!(!config.streaming);
        apply_set(&mut config, "compaction", "false", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("false expected");
        assert!(!config.compaction_enabled);
        apply_set(&mut config, "images", "on", PLATFORM_LIMIT, CHUNK_FLOOR).expect("on expected");
        assert!(config.images);
        apply_set(&mut config, "images", "off", PLATFORM_LIMIT, CHUNK_FLOOR).expect("off expected");
        assert!(!config.images);
        apply_set(&mut config, "react", "on", PLATFORM_LIMIT, CHUNK_FLOOR).expect("on expected");
        assert!(config.react);
        apply_set(&mut config, "react", "off", PLATFORM_LIMIT, CHUNK_FLOOR).expect("off expected");
        assert!(!config.react);
        assert!(apply_set(&mut config, "streaming", "maybe", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
    }

    /// Image settings follow the same set/clear grammar as their compaction
    /// counterpart: the model is a validated ref (handler-side). The image
    /// and compaction PROMPTS left the key set for `/llm_set_prompt` - the
    /// tuner must refuse them, not silently set.
    #[test]
    fn image_keys_set_and_clear() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "image_model", "local/vision", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("set expected");
        assert_eq!(config.image_model.as_deref(), Some("local/vision"));

        apply_set(&mut config, "image_model", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clear expected");
        assert_eq!(config.image_model, None);

        assert!(
            apply_set(
                &mut config,
                "image_prompt",
                "Describe in Russian.",
                PLATFORM_LIMIT,
                CHUNK_FLOOR
            )
            .is_err()
        );
        assert!(
            apply_set(&mut config, "compaction_prompt", "Summarize.", PLATFORM_LIMIT, CHUNK_FLOOR)
                .is_err()
        );
    }

    /// `off` is an explicit choice with its own acknowledgment and its own
    /// stored state (`Some("off")`) - thinking-switch providers render a
    /// real wire disable from it. The reset words keep the "cleared" reply
    /// and store `None` (provider default, nothing sent).
    #[test]
    fn reasoning_effort_distinguishes_off_from_reset() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let set = apply_set(&mut config, "reasoning_effort", "low", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("set expected");
        assert!(set.contains("set to `low`"));
        assert_eq!(config.params.reasoning_effort.as_deref(), Some("low"));

        let off = apply_set(&mut config, "reasoning_effort", "off", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("off expected");
        assert!(off.contains("`reasoning_effort` off"));
        assert!(off.contains("explicit disable"));
        assert_eq!(config.params.reasoning_effort.as_deref(), Some("off"));

        let cleared =
            apply_set(&mut config, "reasoning_effort", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
                .expect("clear expected");
        assert!(cleared.contains("cleared"));
        assert_eq!(config.params.reasoning_effort, None);
    }

    #[test]
    fn numeric_keys_validate_ranges() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "context_messages", "0", PLATFORM_LIMIT, CHUNK_FLOOR).unwrap_err();
        apply_set(&mut config, "context_messages", "50", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("depth expected");
        assert_eq!(config.context_messages, 50);

        apply_set(&mut config, "random_reply_chance", "250", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clamp expected");
        assert!((config.random_reply_chance_percent - 100.0).abs() < f64::EPSILON);
        apply_set(&mut config, "random_reply_chance", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clear expected");
        assert!((config.random_reply_chance_percent).abs() < f64::EPSILON);

        apply_set(&mut config, "random_react_chance", "250", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clamp expected");
        assert!((config.random_react_chance_percent - 100.0).abs() < f64::EPSILON);
        apply_set(&mut config, "random_react_chance", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clear expected");
        assert!((config.random_react_chance_percent).abs() < f64::EPSILON);

        apply_set(&mut config, "random_cooldown", "30", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("cooldown expected");
        assert_eq!(config.random_cooldown_secs, 30);
        apply_set(&mut config, "random_cooldown", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("cooldown clear expected");
        assert_eq!(config.random_cooldown_secs, 5);
        apply_set(&mut config, "random_cooldown", "not-a-number", PLATFORM_LIMIT, CHUNK_FLOOR)
            .unwrap_err();

        apply_set(&mut config, "max_tokens", "not-a-number", PLATFORM_LIMIT, CHUNK_FLOOR)
            .unwrap_err();

        // A zero budget would keep only the newest turn - rejected like
        // `context_messages` 0, not accepted as "no budget".
        apply_set(&mut config, "context_tokens", "0", PLATFORM_LIMIT, CHUNK_FLOOR).unwrap_err();
        apply_set(&mut config, "context_tokens", "4096", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("budget expected");
        assert_eq!(config.context_tokens, Some(4096));
    }

    #[test]
    fn max_length_cannot_exceed_the_platform_limit() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "split_length", "2000", Some(2000), CHUNK_FLOOR)
            .expect("limit value expected");
        assert_eq!(config.split_length, Some(2000));
        // Beyond the cap the platform rejects the chunks outright - rejected
        // here so replies never turn into undelivered garbage.
        apply_set(&mut config, "split_length", "2001", Some(2000), CHUNK_FLOOR).unwrap_err();
        assert_eq!(config.split_length, Some(2000));

        // Below the reply-chunk floor: rejected - tiny chunks would flood
        // the channel and starve the send rate limits. The floor holds even
        // where the platform declares no cap.
        apply_set(&mut config, "split_length", "99", Some(2000), CHUNK_FLOOR).unwrap_err();
        assert_eq!(config.split_length, Some(2000));
        apply_set(&mut config, "split_length", "100", Some(2000), CHUNK_FLOOR)
            .expect("floor value expected");
        assert_eq!(config.split_length, Some(CHUNK_FLOOR));
        apply_set(&mut config, "split_length", "50", None, CHUNK_FLOOR).unwrap_err();
        assert_eq!(config.split_length, Some(CHUNK_FLOOR));
    }

    /// The floor is operator policy, not a hard-coded constant: `/llm_set`
    /// enforces whatever floor the composition root configured.
    #[test]
    fn max_length_floor_follows_the_configured_policy() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "split_length", "499", Some(2000), 500).unwrap_err();
        apply_set(&mut config, "split_length", "500", Some(2000), 500).expect("floor expected");
        assert_eq!(config.split_length, Some(500));
        apply_set(&mut config, "split_length", "500", None, 500).expect("no cap expected");
        assert_eq!(config.split_length, Some(500));
    }

    #[test]
    fn max_length_ignores_the_cap_when_the_platform_declares_none() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "split_length", "5000", None, CHUNK_FLOOR).expect("no cap expected");
        assert_eq!(config.split_length, Some(5000));
        apply_set(&mut config, "split_length", "clear", None, CHUNK_FLOOR).expect("clear expected");
        assert_eq!(config.split_length, None);
    }

    #[test]
    fn capture_and_template_validate() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        apply_set(&mut config, "capture", "all_messages", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("mode expected");
        assert_eq!(config.capture, CaptureMode::AllMessages);
        apply_set(&mut config, "capture", "chaos", PLATFORM_LIMIT, CHUNK_FLOOR).unwrap_err();

        apply_set(
            &mut config,
            "turn_template",
            "<{sender}> {message}",
            PLATFORM_LIMIT,
            CHUNK_FLOOR,
        )
        .expect("template expected");
        assert_eq!(config.turn_template.as_deref(), Some("<{sender}> {message}"));
        apply_set(&mut config, "turn_template", "no placeholders", PLATFORM_LIMIT, CHUNK_FLOOR)
            .unwrap_err();
        apply_set(&mut config, "turn_template", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("clear expected");
        assert_eq!(config.turn_template, None);
    }

    #[test]
    fn unknown_keys_and_required_fields_are_rejected() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let unknown =
            apply_set(&mut config, "vibes", "maximum", PLATFORM_LIMIT, CHUNK_FLOOR).unwrap_err();
        assert!(unknown.contains("Unknown key"));
        assert!(apply_set(&mut config, "model", "clear", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
        assert!(
            apply_set(&mut config, "context_messages", "clear", PLATFORM_LIMIT, CHUNK_FLOOR)
                .is_err()
        );
        apply_set(&mut config, "model", "zai/glm-5.3-flash", PLATFORM_LIMIT, CHUNK_FLOOR)
            .expect("model expected");
        assert_eq!(config.model, "zai/glm-5.3-flash");
    }

    /// A recognized flag key with a bad value answers usage - it must not
    /// fall through to "Unknown key".
    #[test]
    fn flag_with_invalid_value_reports_usage_not_unknown_key() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        let err =
            apply_set(&mut config, "streaming", "maybe", PLATFORM_LIMIT, CHUNK_FLOOR).unwrap_err();
        assert!(err.contains("on or off"), "unexpected reply: {err}");
        assert!(!err.contains("Unknown key"), "unexpected reply: {err}");
        assert!(!config.streaming, "the failed set must not mutate");
    }

    #[test]
    fn current_value_renders_set_and_default_states() {
        let settings = LlmSettings::default();
        let mut config = ChannelConfig::assigned("local/gemma".to_owned());

        // Default state renders the EFFECTIVE value - what the engine uses.
        assert_eq!(current_value(&config, "model", &settings).as_deref(), Some("local/gemma"));
        assert_eq!(
            current_value(&config, "temperature", &settings).as_deref(),
            Some("not sent (provider default)")
        );
        assert_eq!(
            current_value(&config, "reasoning_effort", &settings).as_deref(),
            Some("not sent (provider default)")
        );
        assert_eq!(
            current_value(&config, "context_messages", &settings).as_deref(),
            Some("100 messages")
        );
        assert_eq!(current_value(&config, "context_tokens", &settings).as_deref(), Some("auto"));
        assert_eq!(current_value(&config, "streaming", &settings).as_deref(), Some("off"));
        assert_eq!(current_value(&config, "capture", &settings).as_deref(), Some("bot_related"));
        assert_eq!(current_value(&config, "compaction", &settings).as_deref(), Some("on"));
        assert!(
            current_value(&config, "compaction_model", &settings)
                .expect("compaction_model expected")
                .contains("chat model")
        );
        assert_eq!(current_value(&config, "images", &settings).as_deref(), Some("off"));
        assert_eq!(current_value(&config, "react", &settings).as_deref(), Some("off"));
        assert_eq!(
            current_value(&config, "random_reply_chance", &settings).as_deref(),
            Some("2.0%")
        );
        assert_eq!(
            current_value(&config, "random_react_chance", &settings).as_deref(),
            Some("10.0%")
        );
        assert_eq!(
            current_value(&config, "split_length", &settings).as_deref(),
            Some("2000 (max_message_length)")
        );
        assert!(
            current_value(&config, "turn_template", &settings)
                .expect("turn_template expected")
                .contains("plugin default")
        );
        assert_eq!(current_value(&config, "nope", &settings), None);

        // Set state renders the stored value.
        config.params.temperature = Some(0.7);
        config.params.reasoning_effort = Some("off".to_owned());
        config.context_tokens = Some(4096);
        config.streaming = true;
        config.react = true;
        config.random_reply_chance_percent = 7.5;
        config.split_length = Some(500);
        assert_eq!(current_value(&config, "temperature", &settings).as_deref(), Some("0.7"));
        assert_eq!(current_value(&config, "reasoning_effort", &settings).as_deref(), Some("`off`"));
        assert_eq!(
            current_value(&config, "context_tokens", &settings).as_deref(),
            Some("4096 tokens")
        );
        assert_eq!(current_value(&config, "streaming", &settings).as_deref(), Some("on"));
        assert_eq!(current_value(&config, "react", &settings).as_deref(), Some("on"));
        assert_eq!(
            current_value(&config, "random_reply_chance", &settings).as_deref(),
            Some("7.5%")
        );
        assert_eq!(current_value(&config, "split_length", &settings).as_deref(), Some("500"));
    }

    /// The `react_emoji_inject` grammar: the three modes apply, `none` is a
    /// real value (not a clear), garbage claims the key, and setting a mode
    /// while `react` is off answers with the hint.
    #[test]
    fn react_emoji_inject_grammar_and_react_hint() {
        let mut config = ChannelConfig::assigned("local/gemma".to_owned());

        assert_eq!(
            apply_set(&mut config, "react_emoji_inject", "all", None, CHUNK_FLOOR)
                .expect("all applies"),
            "`react_emoji_inject` set to `all` (the react tool's prompt lists this server's \
             custom emojis). Note: `react` is off - enable it for the list to reach the model."
        );
        assert_eq!(config.react_emoji_inject, EmojiInject::All);

        config.react = true;
        assert_eq!(
            apply_set(&mut config, "react_emoji_inject", "whitelist", None, CHUNK_FLOOR)
                .expect("whitelist applies"),
            "`react_emoji_inject` set to `whitelist` (the react tool's prompt lists this server's \
             custom emojis)."
        );
        assert_eq!(config.react_emoji_inject, EmojiInject::Whitelist);
        assert!(
            !apply_set(&mut config, "react_emoji_inject", "none", None, CHUNK_FLOOR)
                .expect("none applies")
                .contains("Note:"),
            "no hint once react is on"
        );
        assert_eq!(config.react_emoji_inject, EmojiInject::None);

        assert!(apply_set(&mut config, "react_emoji_inject", "clear", None, CHUNK_FLOOR).is_err());
        assert!(
            apply_set(&mut config, "react_emoji_inject", "sometimes", None, CHUNK_FLOOR).is_err()
        );
    }

    /// The reactions status label: off stays plain, the inject mode shows
    /// with its effective whitelist source, and a react-off channel with an
    /// inject set says why nothing lands (the `whitelist` default included -
    /// its empty-list no-op is visible as "(no list)").
    #[test]
    fn react_label_covers_inject_modes() {
        let mut config = ChannelConfig::assigned("local/gemma".to_owned());
        config.react_emoji_inject = EmojiInject::None;
        assert_eq!(react_label(&config, None), "off");

        // The storage default: whitelist with no list anywhere - inert, but
        // the status still names the mode so admins can see it.
        let defaulted = ChannelConfig::assigned("local/gemma".to_owned());
        assert_eq!(defaulted.react_emoji_inject, EmojiInject::Whitelist);
        assert_eq!(
            react_label(&defaulted, None),
            "off (the emoji list would not reach the model) · emoji inject whitelist (no list)"
        );

        config.react_emoji_inject = EmojiInject::All;
        assert_eq!(
            react_label(&config, None),
            "off (the emoji list would not reach the model) · emoji inject all"
        );

        config.react = true;
        config.react_emoji_inject = EmojiInject::Whitelist;
        assert_eq!(
            react_label(&config, None),
            "on · emoji inject whitelist (no list) · 10.0% silent-react"
        );
        assert_eq!(
            react_label(&config, Some("guild, 3")),
            "on · emoji inject whitelist (guild, 3) · 10.0% silent-react"
        );

        config.random_react_chance_percent = 10.0;
        assert_eq!(
            react_label(&config, Some("channel, 1")),
            "on · emoji inject whitelist (channel, 1) · 10.0% silent-react"
        );
        // React off: the silent-react chance is irrelevant and hidden.
        config.react = false;
        assert_eq!(
            react_label(&config, Some("channel, 1")),
            "off (the emoji list would not reach the model) · emoji inject whitelist (channel, 1)"
        );
    }

    #[test]
    fn float_keys_reject_non_finite_values() {
        let mut config = ChannelConfig::assigned("m".to_owned());

        assert!(apply_set(&mut config, "temperature", "NaN", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
        assert!(apply_set(&mut config, "top_p", "inf", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
        assert!(apply_set(&mut config, "min_p", "-inf", PLATFORM_LIMIT, CHUNK_FLOOR).is_err());
        assert_eq!(config.params.temperature, None, "nothing may be stored");

        // NaN would clamp to NaN, not to the range bounds; the default stays.
        let before = config.random_reply_chance_percent;
        assert!(
            apply_set(&mut config, "random_reply_chance", "NaN", PLATFORM_LIMIT, CHUNK_FLOOR)
                .is_err()
        );
        assert!((config.random_reply_chance_percent - before).abs() < f64::EPSILON);
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
        assert_eq!(choices.len(), MAX_CHOICES);
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
        let guild = storage.guild_scoped("test", GuildId(1));

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
        let guild = storage.guild_scoped("test", GuildId(1));

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
        let guild = storage.guild_scoped("test", GuildId(1));
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
