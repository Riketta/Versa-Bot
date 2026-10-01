//! Operator-declared provider configuration and the OpenAI-compatible
//! completion adapter. Providers, endpoints, keys and model capabilities are
//! the bot operator's global domain - guild admins only pick among the
//! declared models, never configure endpoints (no SSRF-by-config, and no
//! cross-guild data path).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};

use super::completion_port::{
    ChatMessage, CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError, TokenUsage,
};
use super::model::GenParams;

/// Plugin-wide runtime settings, mapped from the global `[llm]` config
/// section at the composition root. Global runtime connections - not
/// hot-applied, same class as token/storage.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct LlmSettings {
    /// System prompt for channels without an override.
    pub default_system_prompt: String,
    /// Compaction instruction for channels without an override.
    pub default_compaction_prompt: String,
    /// Compaction model fallback; `None` = compact with the channel's chat
    /// model.
    pub compaction_model: Option<String>,
    /// Live messages kept after each compaction (the new window's seed).
    pub compaction_keep_tail: u32,
    /// Reply splitting limit (per-channel `max_length` overrides this).
    pub max_message_length: usize,
    /// Streaming edit cadence in milliseconds.
    pub stream_interval_ms: u64,
    /// Declared providers (`[llm.providers.<name>]`).
    pub providers: BTreeMap<String, ProviderSettings>,
    /// Declared model capabilities (`[llm.models."<provider/model>"]`).
    /// Undeclared models are usable but get default (no reasoning)
    /// capabilities.
    pub models: BTreeMap<String, ModelSettings>,
}

impl Default for LlmSettings {
    fn default() -> Self {
        Self {
            default_system_prompt: "You are a helpful chat assistant.".to_owned(),
            default_compaction_prompt:
                "Summarize the conversation above, preserving facts, decisions, names and open \
                 questions. Be concise."
                    .to_owned(),
            compaction_model: None,
            compaction_keep_tail: 10,
            max_message_length: 2000,
            stream_interval_ms: 2000,
            providers: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }
}

/// One declared provider (an OpenAI-compatible endpoint).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct ProviderSettings {
    /// Base URL, e.g. `https://api.z.ai/api/coding/paas/v4`.
    pub api_url: String,
    /// Environment variable holding the API key; absent = keyless (local
    /// llama.cpp). Keys never live in config files or guild storage.
    pub api_key_env: Option<String>,
    /// Optional per-provider proxy (the Discord proxy does not apply here).
    pub proxy: Option<String>,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
    pub reasoning_style: ReasoningStyle,
}

impl Default for ProviderSettings {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            api_key_env: None,
            proxy: None,
            timeout_secs: 120,
            reasoning_style: ReasoningStyle::default(),
        }
    }
}

/// How the reasoning/thinking parameter is rendered on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningStyle {
    /// `reasoning_effort: "<value>"` (`OpenAI` o-series, GLM coding endpoints).
    #[default]
    OpenaiEffort,
    /// `thinking: {"type": "enabled"}` (GLM boolean thinking switch; any
    /// non-`off` effort enables it).
    GlmThinking,
}

/// Capabilities of one declared model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ModelSettings {
    /// The model accepts a reasoning/thinking parameter. Per-channel
    /// `reasoning_effort` is ignored for models without this.
    pub reasoning: bool,
    /// Total context window in tokens. When declared, channels without an
    /// explicit `context_budget_tokens` fill their prompt up to this window
    /// minus the completion reserve and an estimator margin (token-budget
    /// filling needs calibrated usage data; before the first reported
    /// request, message-count filling applies).
    pub context_window: Option<u64>,
}

struct ProviderClient {
    settings: ProviderSettings,
    client: reqwest::Client,
    api_key: Option<String>,
}

/// The one adapter (for now): every OpenAI-compatible endpoint - `OpenAI`,
/// `Z.ai`, llama.cpp server, LM Studio, vLLM, `OpenRouter`. Anthropic-style
/// APIs become a second `LlmCompletionPort` adapter later.
pub struct OpenAiCompatibleAdapter {
    settings: Arc<LlmSettings>,
    providers: BTreeMap<String, ProviderClient>,
    /// Models queried once without a capability declaration; no per-request
    /// warning spam.
    warned_models: Mutex<HashSet<String>>,
}

impl OpenAiCompatibleAdapter {
    /// Builds one reqwest client per declared provider (connection pooling,
    /// per-provider proxy and timeout). API keys are resolved from the
    /// environment once, at construction.
    ///
    /// # Errors
    /// A declared `proxy` that reqwest cannot parse, a client that cannot
    /// build, or an `api_key_env` naming a missing environment variable.
    pub fn from_settings(settings: Arc<LlmSettings>) -> Result<Self, LlmError> {
        let mut providers = BTreeMap::new();
        for (name, provider) in &settings.providers {
            let mut builder =
                reqwest::Client::builder().timeout(Duration::from_secs(provider.timeout_secs));
            if let Some(proxy) = &provider.proxy {
                let proxy = reqwest::Proxy::all(proxy)
                    .map_err(|err| LlmError::Request(format!("provider `{name}` proxy: {err}")))?;
                builder = builder.proxy(proxy);
            }
            let client = builder
                .build()
                .map_err(|err| LlmError::Request(format!("provider `{name}` client: {err}")))?;
            let api_key = match &provider.api_key_env {
                Some(var) => {
                    let key = std::env::var(var).map_err(|_| {
                        // The most common misconfiguration is the key pasted
                        // into the field that must name the variable holding
                        // it - such values carry characters no settable env
                        // var name can (dots, slashes), so call it out.
                        let hint = if var.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                            ""
                        } else {
                            " - api_key_env must NAME the environment variable holding \
                             the key (e.g. VERSABOT_LLM_ZAI_KEY), not contain the key itself"
                        };
                        LlmError::Request(format!(
                            "provider `{name}`: env var `{var}` (api_key_env) is not set{hint}"
                        ))
                    })?;
                    Some(key).filter(|key| !key.is_empty())
                }
                None => None,
            };
            providers.insert(
                name.clone(),
                ProviderClient { settings: provider.clone(), client, api_key },
            );
        }
        Ok(Self { settings, providers, warned_models: Mutex::new(HashSet::new()) })
    }

    /// Capability of the referenced model; undeclared models get the default
    /// (no reasoning) and a one-time warning.
    fn reasoning_capability(&self, model_ref: &str) -> bool {
        if let Some(model) = self.settings.models.get(model_ref) {
            return model.reasoning;
        }
        if self.warned_models.lock().insert(model_ref.to_owned()) {
            tracing::warn!(
                model = model_ref,
                "model not declared in [llm.models] - assuming no reasoning support, \
                 per-channel reasoning_effort is ignored"
            );
        }
        false
    }
}

#[async_trait]
impl LlmCompletionPort for OpenAiCompatibleAdapter {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let (provider_name, model_name) = split_model_ref(&request.model)?;
        let provider = self
            .providers
            .get(provider_name)
            .ok_or_else(|| LlmError::UnknownProvider(provider_name.to_owned()))?;
        let supports_reasoning = self.reasoning_capability(&request.model);
        let body = completion_body(
            provider.settings.reasoning_style,
            model_name,
            &request.messages,
            &request.params,
            supports_reasoning,
        );

        let url = format!("{}/chat/completions", provider.settings.api_url.trim_end_matches('/'));
        let mut request_builder = provider.client.post(url).json(&body);
        if let Some(api_key) = &provider.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }
        let response =
            request_builder.send().await.map_err(|err| LlmError::Request(err.to_string()))?;
        let status = response.status();
        let text = response.text().await.map_err(|err| LlmError::Request(err.to_string()))?;
        if !status.is_success() {
            return Err(LlmError::Request(format!("HTTP {status}: {}", truncate(&text, 300))));
        }
        parse_completion_content(&text)
    }
}

/// Splits `provider/model` at the FIRST slash - provider ids never contain
/// one, model names may (GGUF file names).
fn split_model_ref(model_ref: &str) -> Result<(&str, &str), LlmError> {
    match model_ref.split_once('/') {
        Some((provider, model)) if !provider.is_empty() && !model.is_empty() => {
            Ok((provider, model))
        }
        _ => Err(LlmError::InvalidModelRef(model_ref.to_owned())),
    }
}

/// Builds the `chat/completions` request body. Absent params are omitted
/// (strict endpoints reject unknown fields); reasoning is rendered only when
/// the model declares support, in the provider's style.
fn completion_body(
    style: ReasoningStyle,
    model: &str,
    messages: &[ChatMessage],
    params: &GenParams,
    supports_reasoning: bool,
) -> Value {
    let GenParams {
        temperature,
        top_p,
        top_k,
        min_p,
        frequency_penalty,
        presence_penalty,
        max_tokens,
        reasoning_effort,
    } = params;

    let mut body = serde_json::Map::new();
    body.insert("model".to_owned(), json!(model));
    body.insert(
        "messages".to_owned(),
        json!(
            messages
                .iter()
                .map(|message| json!({"role": message.role.as_str(), "content": message.content}))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(value) = temperature {
        body.insert("temperature".to_owned(), json!(value));
    }
    if let Some(value) = top_p {
        body.insert("top_p".to_owned(), json!(value));
    }
    if let Some(value) = top_k {
        body.insert("top_k".to_owned(), json!(value));
    }
    if let Some(value) = min_p {
        body.insert("min_p".to_owned(), json!(value));
    }
    if let Some(value) = frequency_penalty {
        body.insert("frequency_penalty".to_owned(), json!(value));
    }
    if let Some(value) = presence_penalty {
        body.insert("presence_penalty".to_owned(), json!(value));
    }
    if let Some(value) = max_tokens {
        body.insert("max_tokens".to_owned(), json!(value));
    }
    if supports_reasoning
        && let Some(effort) = reasoning_effort
        && effort != "off"
    {
        match style {
            ReasoningStyle::OpenaiEffort => {
                body.insert("reasoning_effort".to_owned(), json!(effort));
            }
            ReasoningStyle::GlmThinking => {
                body.insert("thinking".to_owned(), json!({"type": "enabled"}));
            }
        }
    }
    Value::Object(body)
}

/// Extracts `choices[0].message.content` plus the optional `usage` block
/// from an OpenAI-compatible response, cutting model reasoning so it can
/// never reach a channel: separate `reasoning_content`/`reasoning` fields
/// are simply never read, and inline `<think>...</think>` blocks (the
/// interleaved-reasoning shape emitted by llama.cpp/LM Studio/vLLM) are
/// stripped from the content itself. This is also what keeps reasoning out
/// of the progressive reveal - the reveal only ever shows prefixes of the
/// returned content. A reasoning-only answer (empty after the strip) is no
/// answer: [`LlmError::EmptyResponse`].
fn parse_completion_content(text: &str) -> Result<CompletionResponse, LlmError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|err| LlmError::Request(format!("malformed JSON: {err}")))?;
    let content = value
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_str());
    let Some(content) = content.filter(|content| !content.is_empty()) else {
        return Err(LlmError::EmptyResponse);
    };
    let content = strip_think_blocks(content);
    if content.is_empty() {
        return Err(LlmError::EmptyResponse);
    }
    Ok(CompletionResponse { content, usage: parse_usage(&value) })
}

/// Cuts model reasoning from a completion so it can never reach a channel.
/// Separate `reasoning_content`/`reasoning` fields are never read in the
/// first place; inline `<think>` blocks follow three rules:
/// - a complete `<think>...</think>` pair anywhere is reasoning (leading
///   R1-style or interleaved mid-answer, GLM/Qwen thinking models) - cut;
/// - an opener that STARTS the content (nothing but whitespace before it,
///   chained blocks included) with no closer means the model never got
///   past thinking - everything from the opener on is dropped, and a
///   reasoning-only answer becomes empty (`EmptyResponse` upstream);
/// - an unclosed opener MID-TEXT is literal: answers may discuss the tag,
///   and cutting from it would truncate real content - it stays.
///
/// Whitespace around cuts is trimmed; content without any opener is
/// returned byte-identical. Residual false positive: prose containing both
/// tags in order still loses the span between them (same cost mainstream
/// reasoning UIs pay).
fn strip_think_blocks(content: &str) -> String {
    if !content.to_ascii_lowercase().contains("<think>") {
        return content.to_owned();
    }
    let lowered = content.to_ascii_lowercase();
    let mut kept = String::with_capacity(content.len());
    // Byte cursor into both strings. It only ever lands on ASCII-tag match
    // offsets, which are char boundaries (an ASCII byte cannot match inside
    // a multi-byte UTF-8 sequence, and `to_ascii_lowercase` preserves the
    // byte layout) - the `get` + `expect` makes that invariant explicit.
    let mut cursor = 0usize;
    loop {
        let Some(open) = lowered.get(cursor..).and_then(|rest| rest.find("<think>")) else {
            kept.push_str(content.get(cursor..).expect("tag match offsets are char boundaries"));
            break;
        };
        let open = cursor + open;
        let leading = content
            .get(cursor..open)
            .expect("tag match offsets are char boundaries")
            .trim()
            .is_empty();
        let after_open = open + "<think>".len();
        let closer = lowered
            .get(after_open..)
            .and_then(|rest| rest.find("</think>"))
            .map(|close| after_open + close + "</think>".len());
        match closer {
            // Complete pair anywhere: interleaved reasoning - cut it.
            Some(after_close) => {
                kept.push_str(
                    content
                        .get(cursor..open)
                        .expect("tag match offsets are char boundaries")
                        .trim_end(),
                );
                cursor = after_close;
            }
            // Unclosed opener at the start: the model never got past
            // thinking - no answer follows.
            None if leading => return kept.trim().to_owned(),
            // Unclosed opener mid-text: literal text, not reasoning. Keep
            // the tag itself and keep scanning after it.
            None => {
                kept.push_str(
                    content.get(cursor..after_open).expect("tag match offsets are char boundaries"),
                );
                cursor = after_open;
            }
        }
    }
    kept.trim().to_owned()
}

/// `usage` is optional in the `OpenAI` shape: endpoints that do not report
/// token stats simply get `None`. A partial `usage` block is treated as
/// absent rather than guessed at.
fn parse_usage(response: &Value) -> Option<TokenUsage> {
    let usage = response.get("usage")?;
    Some(TokenUsage {
        prompt_tokens: usage.get("prompt_tokens").and_then(Value::as_u64)?,
        completion_tokens: usage.get("completion_tokens").and_then(Value::as_u64)?,
        total_tokens: usage.get("total_tokens").and_then(Value::as_u64)?,
        cached_tokens: usage
            .get("prompt_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
    })
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else {
        text.chars().take(max_chars).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::llm::completion_port::ChatRole;

    #[test]
    fn model_refs_split_at_first_slash() {
        assert_eq!(split_model_ref("zai/glm-5.3-flash").ok(), Some(("zai", "glm-5.3-flash")));
        // GGUF model names contain slashes - only the first one separates.
        assert_eq!(
            split_model_ref("local/unsloth/gemma-4-26B-it-GGUF").ok(),
            Some(("local", "unsloth/gemma-4-26B-it-GGUF"))
        );
        assert!(matches!(split_model_ref("no-slash"), Err(LlmError::InvalidModelRef(_))));
        assert!(matches!(split_model_ref("/model"), Err(LlmError::InvalidModelRef(_))));
        assert!(matches!(split_model_ref("provider/"), Err(LlmError::InvalidModelRef(_))));
    }

    fn sample_messages() -> Vec<ChatMessage> {
        vec![
            ChatMessage { role: ChatRole::System, content: "be nice".to_owned() },
            ChatMessage { role: ChatRole::User, content: "alice: hi".to_owned() },
        ]
    }

    #[test]
    fn body_includes_only_set_params() {
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "glm-5.3-flash",
            &sample_messages(),
            &GenParams { temperature: Some(0.7), max_tokens: Some(512), ..GenParams::default() },
            false,
        );

        assert_eq!(body.get("model").and_then(Value::as_str), Some("glm-5.3-flash"));
        assert_eq!(body.get("temperature").and_then(Value::as_f64), Some(0.7));
        assert_eq!(body.get("max_tokens").and_then(Value::as_u64), Some(512));
        assert!(body.get("top_p").is_none());
        assert!(body.get("top_k").is_none());
        assert!(body.get("min_p").is_none());
        assert!(body.get("reasoning_effort").is_none());
        let messages = body.get("messages").and_then(Value::as_array).expect("messages expected");
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages.first().and_then(|m| m.get("role")).and_then(Value::as_str),
            Some("system")
        );
    }

    #[test]
    fn reasoning_rendered_only_when_supported() {
        let params =
            GenParams { reasoning_effort: Some("high".to_owned()), ..GenParams::default() };

        let body =
            completion_body(ReasoningStyle::OpenaiEffort, "m", &sample_messages(), &params, true);
        assert_eq!(body.get("reasoning_effort").and_then(Value::as_str), Some("high"));

        let body =
            completion_body(ReasoningStyle::OpenaiEffort, "m", &sample_messages(), &params, false);
        assert!(body.get("reasoning_effort").is_none());

        let body =
            completion_body(ReasoningStyle::GlmThinking, "m", &sample_messages(), &params, true);
        let thinking = body.get("thinking").expect("thinking expected");
        assert_eq!(thinking.get("type").and_then(Value::as_str), Some("enabled"));
    }

    #[test]
    fn effort_off_is_never_rendered() {
        let params = GenParams { reasoning_effort: Some("off".to_owned()), ..GenParams::default() };
        let body =
            completion_body(ReasoningStyle::OpenaiEffort, "m", &sample_messages(), &params, true);
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn completion_content_is_extracted() {
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"role":"assistant","content":"hello there"}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "hello there");
        // Endpoint without usage stats: absent, not zeroed.
        assert_eq!(response.usage, None);

        // Reasoning models may answer with null content - that is no answer.
        assert!(matches!(
            parse_completion_content(r#"{"choices":[{"message":{"content":null}}]}"#),
            Err(LlmError::EmptyResponse)
        ));
        assert!(matches!(
            parse_completion_content(r#"{"choices":[]}"#),
            Err(LlmError::EmptyResponse)
        ));
        assert!(matches!(parse_completion_content("not json"), Err(LlmError::Request(_))));
    }

    /// Reasoning output never reaches a channel: separate reasoning fields
    /// are ignored by construction, inline `<think>` blocks are cut, and a
    /// reasoning-only answer is `EmptyResponse` (silence policy takes over).
    #[test]
    fn reasoning_is_cut_from_responses() {
        // Separate field (GLM/DeepSeek shape): never read, content stands alone.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"The answer is 4.",
                "reasoning_content":"secret chain of thought"}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer is 4.");

        // Leading block (R1/llama.cpp shape): the trailing newline goes too.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"<think>reasoning here</think>\n\nThe answer is 4."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer is 4.");

        // Interleaved blocks, case-insensitive tags.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"One <think>a</think> two <THINK>b</THINK> three."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "One two three.");

        // Unclosed opener MID-TEXT is literal: answers may discuss the tag
        // without being truncated from it onward.
        for content in ["Visible<think>hidden forever", "Wrap it in <think> tags to reason."] {
            let response = parse_completion_content(&format!(
                r#"{{"choices":[{{"message":{{"content":"{content}"}}}}]}}"#
            ))
            .expect("response expected to parse");
            assert_eq!(response.content, content);
        }

        // Chained leading blocks are all reasoning.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"<think>a</think><think>b</think>The answer."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer.");

        // A leading unclosed block means the model never got past thinking:
        // a reasoning-only answer is empty and counts as no answer.
        assert!(matches!(
            parse_completion_content(
                r#"{"choices":[{"message":{"content":"<think>only reasoning"}}]}"#
            ),
            Err(LlmError::EmptyResponse)
        ));

        // Residual false positive, pinned on purpose: prose containing BOTH
        // tags in order loses the span between them.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"Compare <think> with </think> syntax."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "Compare syntax.");

        // No tag: byte-identical passthrough - even odd whitespace.
        let response =
            parse_completion_content(r#"{"choices":[{"message":{"content":"  keep\nthis  "}}]}"#)
                .expect("response expected to parse");
        assert_eq!(response.content, "  keep\nthis  ");
    }

    #[test]
    fn usage_is_parsed_when_the_endpoint_reports_it() {
        let response = parse_completion_content(
            r#"{
                "choices": [{"message": {"content": "ok"}}],
                "usage": {
                    "prompt_tokens": 1200,
                    "completion_tokens": 34,
                    "total_tokens": 1234,
                    "prompt_tokens_details": {"cached_tokens": 800}
                }
            }"#,
        )
        .expect("response expected to parse");
        assert_eq!(
            response.usage,
            Some(TokenUsage {
                prompt_tokens: 1200,
                completion_tokens: 34,
                total_tokens: 1234,
                cached_tokens: Some(800),
            })
        );

        // No cached-token breakdown: the core fields still parse.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#,
        )
        .expect("response expected to parse");
        assert_eq!(
            response.usage,
            Some(TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 2,
                total_tokens: 3,
                cached_tokens: None,
            })
        );

        // Partial usage blocks are treated as absent, not guessed at.
        let response = parse_completion_content(
            r#"{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1}}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.usage, None);
    }

    #[test]
    fn from_settings_fails_loud_on_missing_api_key_env() {
        let settings = LlmSettings {
            providers: BTreeMap::from([(
                "zai".to_owned(),
                ProviderSettings {
                    api_url: "https://example.invalid/v4".to_owned(),
                    api_key_env: Some("DEFINITELY_UNSET_VAR_123".to_owned()),
                    ..ProviderSettings::default()
                },
            )]),
            ..LlmSettings::default()
        };
        let result = OpenAiCompatibleAdapter::from_settings(Arc::new(settings));
        assert!(matches!(result, Err(LlmError::Request(_))));
    }

    /// A value that cannot be an env var name (dots, slashes) is almost
    /// always the key itself pasted into `api_key_env` - the error says so.
    #[test]
    fn from_settings_names_the_variable_not_the_key() {
        let settings = LlmSettings {
            providers: BTreeMap::from([(
                "zai".to_owned(),
                ProviderSettings {
                    api_url: "https://example.invalid/v4".to_owned(),
                    api_key_env: Some("0344272d.key.value".to_owned()),
                    ..ProviderSettings::default()
                },
            )]),
            ..LlmSettings::default()
        };
        let message = match OpenAiCompatibleAdapter::from_settings(Arc::new(settings)) {
            Err(LlmError::Request(message)) => message,
            Err(err) => panic!("expected a request error, got {err}"),
            Ok(_) => panic!("expected an error, but the adapter built"),
        };
        assert!(
            message.contains("must NAME the environment variable"),
            "hint missing from: {message}"
        );
    }

    #[test]
    fn from_settings_accepts_keyless_providers() {
        let settings = LlmSettings {
            providers: BTreeMap::from([(
                "local".to_owned(),
                ProviderSettings {
                    api_url: "http://192.168.1.35:8001/v1".to_owned(),
                    ..ProviderSettings::default()
                },
            )]),
            ..LlmSettings::default()
        };
        let result = OpenAiCompatibleAdapter::from_settings(Arc::new(settings));
        assert!(result.is_ok());
    }
}
