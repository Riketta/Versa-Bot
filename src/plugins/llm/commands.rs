//! `/llm_*` command handlers: channel assignment and service-channel
//! assignment. Meaning lives in the owning plugin; handlers run inside the
//! pipeline with the event itself, so channel-anchored assignment ("run me
//! in the channel to assign") needs no platform channel types.

use std::sync::Arc;

use async_trait::async_trait;

use crate::kernel::{
    models::{Embed, MessageId, OutboundMessage, RequestContext},
    plugin_ports::{CommandArgs, CommandHandler},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::conversation::ConversationRecord;
use super::model::{
    CaptureMode, ChannelConfig, ConversationState, NAMESPACE, SERVICE_CHANNEL_KEY, UsageStats,
    channel_config_key, channel_state_key, channel_stats_key, records_namespace, unix_now,
};

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
            .send(OutboundMessage::text("This command only works inside a server."))
            .await?;
        return Ok(None);
    };
    let Some(raw) =
        storage.get(NAMESPACE, &channel_config_key(event.origin.channel_id.get())).await?
    else {
        services
            .chat_output
            .send(OutboundMessage::text("LLM chat is not assigned to this channel."))
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
            if cleared || value == "off" {
                config.params.reasoning_effort = None;
                return Ok("`reasoning_effort` cleared (no reasoning parameter sent).".to_owned());
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

/// On/off settings. `None` = the key is not a flag (caller continues matching).
fn apply_flag(
    config: &mut ChannelConfig,
    key: &str,
    value: &str,
) -> Option<Result<String, String>> {
    let enabled = parse_bool(value)?;
    match key {
        "streaming" => {
            config.streaming = enabled;
            Some(Ok(format!("`streaming` turned {}.", if enabled { "on" } else { "off" })))
        }
        "compaction" => {
            config.compaction_enabled = enabled;
            Some(Ok(format!("`compaction` turned {}.", if enabled { "on" } else { "off" })))
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
                Ok(parsed) => {
                    set_float(config, key, Some(parsed));
                    Some(Ok(format!("`{key}` set to {parsed}.")))
                }
                Err(_) => Some(Err(format!("`{key}` expects a number, got `{value}`."))),
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
                Ok(parsed) => {
                    let clamped = parsed.clamp(0.0, 100.0);
                    config.random_chance_percent = clamped;
                    Some(Ok(format!("`random_chance` set to {clamped}%.")))
                }
                Err(_) => {
                    Some(Err(format!("`random_chance` expects a number (percent), got `{value}`.")))
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
        // Both compaction fields share the same set/clear shape.
        "compaction_model" | "compaction_prompt" => {
            if cleared {
                if key == "compaction_model" {
                    config.compaction_model = None;
                } else {
                    config.compaction_prompt = None;
                }
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            if key == "compaction_model" {
                config.compaction_model = Some(value.to_owned());
                Some(Ok(format!("`{key}` set to `{value}`.")))
            } else {
                config.compaction_prompt = Some(value.to_owned());
                Some(Ok(format!("`{key}` updated.")))
            }
        }
        "max_length" => {
            if cleared {
                config.max_length = None;
                return Some(Ok(format!("`{key}` cleared (plugin default applies).")));
            }
            match value.parse::<usize>() {
                Ok(0) => Some(Err(format!("`{key}` must be at least 1."))),
                Ok(characters) => {
                    config.max_length = Some(characters);
                    Some(Ok(format!("`{key}` set to {characters} characters.")))
                }
                Err(_) => Some(Err(format!("`{key}` expects a whole number, got `{value}`."))),
            }
        }
        "turn_template" => Some(if cleared {
            config.turn_template = None;
            Ok(format!("`{key}` cleared (`{{sender}}: {{message}}` applies)."))
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
pub(super) struct AssignLlmHandler;

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
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };
        let Some(model) = args.get("model") else {
            services
                .chat_output
                .send(OutboundMessage::text("Usage: `/llm_assign model` - e.g. `local/gemma`."))
                .await?;
            return Ok(());
        };

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
            .send(OutboundMessage::text(format!(
                "LLM chat assigned to this channel (model `{model}`)."
            )))
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
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage.delete(NAMESPACE, &channel_config_key(event.origin.channel_id.get())).await?;

        services
            .chat_output
            .send(OutboundMessage::text("LLM chat unassigned for this channel."))
            .await?;
        Ok(())
    }
}

/// `/llm_cutoff`: resets the channel's conversation context - the cutoff
/// moves past every existing record and the summary clears, so the next
/// answer starts fresh. Stored history is kept (records are never deleted);
/// this is a context reset, not a history wipe.
pub(super) struct CutoffLlmHandler;

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
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        let channel_id = event.origin.channel_id.get();
        let records_ns = records_namespace(channel_id);
        // Per-scope sequence numbers are 1..=count, so the record count IS
        // the newest sequence - the cutoff moves past everything.
        let total = storage.count_after(&records_ns, 0).await?;
        let state =
            ConversationState { summary: None, cutoff_seq: total, cutoff_at: Some(unix_now()) };
        storage
            .set(NAMESPACE, &channel_state_key(channel_id), serde_json::to_value(&state)?)
            .await?;

        services
            .chat_output
            .send(OutboundMessage::text(
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
/// estimate and the last request's reported usage. Both appear only once
/// the endpoint has reported real usage - before that the default ratio is
/// an uncalibrated guess and showing it would be noise.
async fn usage_lines(
    storage: &Arc<dyn GuildStorage>,
    records_ns: &str,
    state: &ConversationState,
    live: u64,
    usage_stats: &UsageStats,
    config: &ChannelConfig,
) -> UsageLines {
    let Some(last) = usage_stats.last else {
        return UsageLines { estimate: None, last_request: None };
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
    let estimated = usage_stats.tokens_per_char * context_chars as f64 + 8.0 * live as f64;
    let estimate = Some(match config.context_budget_tokens {
        Some(budget) => format!("Est. context: ~{estimated:.0} / {budget} tokens"),
        None => format!("Est. context: ~{estimated:.0} tokens"),
    });

    let cached =
        last.cached_tokens.map(|cached| format!(" (+{cached} cached)")).unwrap_or_default();
    let last_request = Some(format!(
        "Last request: {} prompt / {} completion / {} total tokens{cached}",
        last.prompt_tokens, last.completion_tokens, last.total_tokens
    ));
    UsageLines { estimate, last_request }
}

/// The two optional `/llm_status` lines fed by calibration data.
struct UsageLines {
    estimate: Option<String>,
    last_request: Option<String>,
}

/// `/llm_status`: inspect the channel's chat configuration and conversation
/// state. Ephemeral where the platform allows (slash invocations), since the
/// summary preview may be considered sensitive.
pub(super) struct StatusLlmHandler;

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
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        let channel_id = event.origin.channel_id.get();
        let Some(raw) = storage.get(NAMESPACE, &channel_config_key(channel_id)).await? else {
            services
                .chat_output
                .send(OutboundMessage::text("LLM chat is not assigned to this channel."))
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
        let usage = usage_lines(storage, &records_ns, &state, live, &usage_stats, &config).await;

        let summary = match &state.summary {
            Some(summary) => preview(summary, 200),
            None => "none".to_owned(),
        };
        let context_start = first_link.unwrap_or_else(|| "no messages after the cutoff".to_owned());
        let mut description = format!(
            "Model: `{}`
Context: {live}/{} messages ({total} kept)
Compaction: {}
Summary: {summary}
Context start: {context_start}",
            config.model,
            config.history_depth,
            if config.compaction_enabled { "on" } else { "off" },
        );
        if let Some(line) = usage.estimate {
            description.push('\n');
            description.push_str(&line);
        }
        if let Some(line) = usage.last_request {
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
                .send(OutboundMessage::text("This command only works inside a server."))
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
            .send(OutboundMessage::text(
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
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage.delete(NAMESPACE, SERVICE_CHANNEL_KEY).await?;

        services.chat_output.send(OutboundMessage::text("Service channel cleared.")).await?;
        Ok(())
    }
}

/// `/llm_set`: tunes one setting of the channel's chat configuration by
/// `key`/`value`. Values of `clear`/`none`/`default` reset the setting to
/// its default; malformed values are answered with usage and never saved.
pub(super) struct SetLlmHandler;

#[async_trait]
impl CommandHandler for SetLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(mut config) = load_assigned_config(event, services).await? else {
            return Ok(());
        };
        let (Some(key), Some(value)) = (args.get("key"), args.get("value")) else {
            services
                .chat_output
                .send(OutboundMessage::text(format!(
                    "Usage: `/llm_set key value`. Keys: {}.",
                    SET_KEYS.join(", ")
                )))
                .await?;
            return Ok(());
        };

        match apply_set(&mut config, key, value) {
            Ok(message) => {
                save_config(event, services, config).await?;
                services.chat_output.send(OutboundMessage::text(message)).await?;
            }
            Err(usage) => {
                services.chat_output.send(OutboundMessage::text(usage)).await?;
            }
        }
        Ok(())
    }
}

/// `/llm_prompt`: sets the channel's system prompt (long free text); the
/// value `clear` falls back to the plugin-wide default.
pub(super) struct PromptLlmHandler;

#[async_trait]
impl CommandHandler for PromptLlmHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(mut config) = load_assigned_config(event, services).await? else {
            return Ok(());
        };
        let Some(prompt) = args.get("prompt") else {
            services
                .chat_output
                .send(OutboundMessage::text(
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
        services.chat_output.send(OutboundMessage::text(message)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(apply_set(&mut config, "streaming", "maybe").is_err());
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

        apply_set(&mut config, "max_tokens", "not-a-number").unwrap_err();
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
}
