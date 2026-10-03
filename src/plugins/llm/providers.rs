//! Operator-declared provider configuration and the OpenAI-compatible
//! completion adapter. Providers, endpoints, keys and model capabilities are
//! the bot operator's global domain - guild admins only pick among the
//! declared models, never configure endpoints (no SSRF-by-config, and no
//! cross-guild data path).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::completion_port::{
    ChatMessage, CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError,
    ResponseTiming, TokenUsage,
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
    /// Cap for `/llm_prompt_file` attachment downloads, in bytes.
    pub max_prompt_file_bytes: u64,
    /// Image recognition model fallback (`provider/model`); `None` = image
    /// recognition off globally (channels can only toggle within that).
    pub image_model: Option<String>,
    /// Images are rescaled to this max side (aspect kept) before the
    /// recognition call; smaller images pass through untouched.
    pub image_max_side: u32,
    /// JPEG quality of the rescaled recognition payload.
    pub image_jpeg_quality: u8,
    /// Download cap per image attachment, in bytes; larger images are
    /// recorded undescribed.
    pub image_max_source_bytes: u64,
    /// Recognition prompt override; `None` = built-in default.
    pub image_prompt: Option<String>,
    /// Cap on images described per captured message; images beyond it are
    /// recorded undescribed.
    pub max_images_per_message: u32,
    /// Collapses runs of consecutive newlines in completions down to this
    /// many (applied at the adapter boundary, chat answers and compaction
    /// summaries alike); `None` leaves responses untouched.
    pub max_consecutive_newlines: Option<usize>,
    /// Diagnostic dump: log the raw request and response bodies of every
    /// completion at DEBUG level (stdout only, never shipped to Sentry).
    /// Off by default - the bodies carry full conversation content.
    pub log_raw_traffic: bool,
    /// Declared providers (`[llm.providers.<name]`).
    pub providers: BTreeMap<String, ProviderSettings>,
    /// Declared model capabilities (`[llm.models."<provider/model>"]`).
    /// This registry is also the legal assignment set: `/llm_assign` and
    /// `/llm_set model=` reject refs absent from here. Configs stored before
    /// that validation may still reference undeclared models - they run
    /// with default (no reasoning) capabilities, warn-once.
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
            max_prompt_file_bytes: 131_072,
            image_model: None,
            image_max_side: 512,
            image_jpeg_quality: 85,
            image_max_source_bytes: 8_388_608,
            image_prompt: None,
            max_images_per_message: 2,
            max_consecutive_newlines: None,
            log_raw_traffic: false,
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
    /// Provider-specific fields merged verbatim into every completion
    /// request body (llama.cpp `chat_template_kwargs` / `reasoning_budget`,
    /// vendor sampling extensions - knobs the adapter does not model).
    /// `model` and `messages` are engine-owned and cannot be overridden;
    /// other keys win over the standard rendering.
    pub extra_body: BTreeMap<String, Value>,
}

impl Default for ProviderSettings {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            api_key_env: None,
            proxy: None,
            timeout_secs: 120,
            reasoning_style: ReasoningStyle::default(),
            extra_body: BTreeMap::new(),
        }
    }
}

/// How the reasoning/thinking parameter is rendered on the wire.
///
/// Both styles only render when the model declares reasoning support;
/// `reasoning_effort: "off"` differs per style: the boolean switch style
/// sends an explicit disable, the effort style has no off wire value and
/// falls back to the provider default (which on Z.ai GLM is heavy thinking
/// - for GLM-5.3 series the minimum is `low`, thinking is forced).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningStyle {
    /// `reasoning_effort: "<value>"` (`OpenAI` o-series, Z.ai GLM). The
    /// effort scale has no universal off: omitted = provider default (Z.ai
    /// GLM defaults to `max`).
    #[default]
    OpenaiEffort,
    /// `thinking: {"type": "enabled"|"disabled"}` (GLM boolean thinking
    /// switch; any non-`off` effort enables it, `off` disables it). Not
    /// every model honors the disable - GLM-5.3 series thinks forcibly and
    /// is throttled via `reasoning_effort` instead.
    GlmThinking,
}

/// How the compaction summary enters the request context. Per-model because
/// chat templates disagree about context shapes: the default keeps the
/// long-standing separate summary slot (stable turn positions); models on
/// templates that silently drop later system messages should opt into
/// `system_suffix` - a dropped summary is silent context loss after every
/// compaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryPlacement {
    /// Separate second system message; a placeholder keeps the slot present
    /// when no summary exists yet. The default - the long-standing schema,
    /// with stable turn positions (early messages carry more weight with
    /// most models) and a byte-stable prompt prefix for provider caches.
    #[default]
    SystemTurn,
    /// Appended to the end of the system prompt: the one shape every chat
    /// template honors. The bytes before the summary stay stable, so
    /// provider prompt caches keep the system-prompt prefix across
    /// compactions. No placeholder exists in this mode - without a summary
    /// the context is just the system prompt.
    SystemSuffix,
    /// Assistant message before the live window; a placeholder keeps the
    /// slot present when no summary exists yet. For endpoints that mishandle
    /// merged prompts - the model reads the summary as its own words.
    AssistantTurn,
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
    /// How the compaction summary enters the context (see
    /// [`SummaryPlacement`]); undeclared models run the default slot schema.
    pub summary_placement: SummaryPlacement,
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
    /// Response normalization applied after reasoning stripping: collapses
    /// newline runs down to the configured maximum (operator option, off by
    /// default). Applied to the authoritative assembled content - the live
    /// reveal may briefly show more blank lines than the final edit keeps.
    fn normalize(&self, content: &str) -> String {
        match self.settings.max_consecutive_newlines {
            Some(max) => collapse_newlines(content, max),
            None => content.to_owned(),
        }
    }

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
            &provider.settings.extra_body,
        );

        let url = format!("{}/chat/completions", provider.settings.api_url.trim_end_matches('/'));
        let mut request_builder = provider.client.post(url).json(&body);
        if let Some(api_key) = &provider.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }
        let started = std::time::Instant::now();
        tracing::debug!(
            provider = provider_name,
            model = model_name,
            messages = request.messages.len(),
            "LLM request dispatched"
        );
        // Operator opt-in dump: what exactly went on the wire (proves which
        // reasoning/sampling parameters were actually sent). DEBUG stays on
        // stdout and never reaches Sentry; the body carries the full
        // conversation, hence the config gate.
        if self.settings.log_raw_traffic {
            tracing::debug!(
                provider = provider_name,
                model = model_name,
                body = %body,
                "LLM raw request"
            );
        }
        let response =
            request_builder.send().await.map_err(|err| LlmError::Request(err.to_string()))?;
        let status = response.status();
        let text = response.text().await.map_err(|err| LlmError::Request(err.to_string()))?;
        // Dumped before the status check so rejected requests are dumped too
        // - the response body is usually the only explanation an endpoint
        // gives (and the only place reasoning_content is ever visible).
        if self.settings.log_raw_traffic {
            tracing::debug!(
                provider = provider_name,
                model = model_name,
                status = %status,
                body = %text,
                "LLM raw response"
            );
        }
        if !status.is_success() {
            return Err(LlmError::Request(format!("HTTP {status}: {}", truncate(&text, 300))));
        }
        let mut parsed = parse_completion_content(&text, wall_ms(started))?;
        parsed.content = self.normalize(&parsed.content);
        tracing::debug!(
            provider = provider_name,
            model = model_name,
            status = %status,
            elapsed_ms = started.elapsed().as_millis(),
            response_ms = parsed.timing.total_ms,
            timed_by = if parsed.timing.endpoint_reported { "endpoint" } else { "adapter" },
            content_chars = parsed.content.chars().count(),
            "LLM response received"
        );
        Ok(parsed)
    }

    /// Streaming half of the port: `stream: true` over SSE, content deltas
    /// forwarded as they arrive, the reasoning-aware authoritative text and
    /// usage resolved once the stream ends.
    async fn complete_streaming(
        &self,
        request: CompletionRequest,
        deltas: mpsc::Sender<String>,
    ) -> Result<CompletionResponse, LlmError> {
        let (provider_name, model_name) = split_model_ref(&request.model)?;
        let provider = self
            .providers
            .get(provider_name)
            .ok_or_else(|| LlmError::UnknownProvider(provider_name.to_owned()))?;
        let supports_reasoning = self.reasoning_capability(&request.model);
        let mut body = completion_body(
            provider.settings.reasoning_style,
            model_name,
            &request.messages,
            &request.params,
            supports_reasoning,
            &provider.settings.extra_body,
        );
        if let Some(map) = body.as_object_mut() {
            map.insert("stream".to_owned(), json!(true));
        }

        let url = format!("{}/chat/completions", provider.settings.api_url.trim_end_matches('/'));
        let mut request_builder = provider.client.post(url).json(&body);
        if let Some(api_key) = &provider.api_key {
            request_builder = request_builder.bearer_auth(api_key);
        }
        let started = std::time::Instant::now();
        tracing::debug!(
            provider = provider_name,
            model = model_name,
            messages = request.messages.len(),
            "LLM request dispatched (stream)"
        );
        if self.settings.log_raw_traffic {
            tracing::debug!(
                provider = provider_name,
                model = model_name,
                body = %body,
                "LLM raw request"
            );
        }
        let response =
            request_builder.send().await.map_err(|err| LlmError::Request(err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.map_err(|err| LlmError::Request(err.to_string()))?;
            if self.settings.log_raw_traffic {
                tracing::debug!(
                    provider = provider_name,
                    model = model_name,
                    status = %status,
                    body = %text,
                    "LLM raw response"
                );
            }
            return Err(LlmError::Request(format!("HTTP {status}: {}", truncate(&text, 300))));
        }

        // SSE reading is byte-buffered: only complete lines are decoded, so
        // a multi-byte character split across TCP chunks stays intact. The
        // raw body is reassembled only for the operator diagnostic.
        let read = read_sse_stream(response, &deltas, self.settings.log_raw_traffic).await?;
        if self.settings.log_raw_traffic {
            tracing::debug!(
                provider = provider_name,
                model = model_name,
                body = %read.raw,
                "LLM raw response (stream)"
            );
        }
        if !read.done {
            // The endpoint vanished mid-stream: the assembled prefix is
            // incomplete by definition - it must never pass as a full
            // answer. The engine finalizes what was already revealed.
            return Err(LlmError::Request(format!(
                "stream ended without [DONE] after {} content chars",
                read.content.chars().count()
            )));
        }
        let content = strip_think_blocks(&read.content);
        let content = self.normalize(&content);
        if content.is_empty() {
            // A reasoning-only stream is no answer, same as non-streaming.
            return Err(LlmError::EmptyResponse);
        }
        let timing = match read.endpoint_ms {
            Some(ms) => ResponseTiming::reported(ms),
            None => ResponseTiming::measured(wall_ms(started)),
        };
        tracing::debug!(
            provider = provider_name,
            model = model_name,
            status = %status,
            elapsed_ms = started.elapsed().as_millis(),
            response_ms = timing.total_ms,
            timed_by = if timing.endpoint_reported { "endpoint" } else { "adapter" },
            content_chars = content.chars().count(),
            "LLM stream completed"
        );
        Ok(CompletionResponse { content, usage: read.usage, timing })
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
/// the model declares support, in the provider's style:
/// - `openai_effort`: `reasoning_effort: "<value>"`. Any effort renders as
///   sent; `off` is NOT rendered - the effort scale has no universal off
///   value, so off means "omit and let the provider default apply" (Z.ai
///   GLM defaults to `max`, GLM-5.3's minimum is `low`).
/// - `glm_thinking`: `thinking: {"type": ...}`. Non-`off` effort renders
///   `enabled`; `off` renders an explicit `disabled` (honored by GLM-4.5
///   through 5.2; GLM-5.3 series thinks forcibly regardless).
///
/// Finally, the provider's `extra_body` is merged in: operator-owned
/// passthrough for endpoint-specific knobs (llama.cpp template switches,
/// vendor sampling fields), with `${name}` template variables resolved per
/// request (see [`render_extra_body`]). `model`/`messages` cannot be
/// overridden; other keys win over the standard rendering.
fn completion_body(
    style: ReasoningStyle,
    model: &str,
    messages: &[ChatMessage],
    params: &GenParams,
    supports_reasoning: bool,
    extra_body: &BTreeMap<String, Value>,
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
        json!(messages.iter().map(wire_message).collect::<Vec<_>>()),
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
    if supports_reasoning && let Some(effort) = reasoning_effort {
        match style {
            ReasoningStyle::OpenaiEffort => {
                if effort != "off" {
                    body.insert("reasoning_effort".to_owned(), json!(effort));
                }
            }
            ReasoningStyle::GlmThinking => {
                let kind = if effort == "off" { "disabled" } else { "enabled" };
                body.insert("thinking".to_owned(), json!({"type": kind}));
            }
        }
    }
    // Operator passthrough, last so it can override the standard rendering.
    // `model`/`messages` stay engine-owned: a mistyped extra key must not be
    // able to swap the model or inject a foreign conversation.
    for (key, value) in extra_body {
        if key != "model" && key != "messages" {
            body.insert(key.clone(), render_extra_body(value, reasoning_effort.as_deref()));
        }
    }
    Value::Object(body)
}

/// Runtime values usable as `${name}` placeholders inside `extra_body`
/// template strings. They mirror the channel's reasoning setting so an
/// operator can route it into endpoint-specific knobs the adapter does not
/// model (llama.cpp `chat_template_kwargs`, vendor fields):
/// - `enable_reasoning`: boolean, `false` only when the channel's
///   `reasoning_effort` is `off`; `true` otherwise (an effort value, or
///   unset - "unset" means the provider default applies, which for thinking
///   templates is on).
/// - `reasoning_effort`: the channel's effort string (`"low"`, ...), or
///   `null` when unset or `off`.
///
/// Deliberately independent of the model's declared reasoning capability:
/// the operator decides per provider whether the knob applies at all.
fn template_value(name: &str, effort: Option<&str>) -> Option<Value> {
    match name {
        "enable_reasoning" => Some(json!(effort != Some("off"))),
        "reasoning_effort" => Some(match effort {
            Some(value) if value != "off" => json!(value),
            _ => Value::Null,
        }),
        _ => None,
    }
}

/// Renders `extra_body` for one request: `${name}` placeholders resolve at
/// runtime (see [`template_value`]). A string that is exactly one
/// placeholder takes the variable's typed JSON value - a boolean stays a
/// boolean, which is what llama.cpp template kwargs expect. Placeholders
/// embedded in longer strings substitute textually (string form; `null`
/// becomes empty). Unknown variables are left as-is with a warning: an
/// operator typo must show up in the logs, not vanish silently.
/// One message on the wire: plain string content normally; the `OpenAI`
/// multipart array (text part + `image_url` data-URL parts) only when the
/// message carries images - the image-recognition call's shape.
fn wire_message(message: &ChatMessage) -> Value {
    let role = json!(message.role.as_str());
    if message.images.is_empty() {
        return json!({"role": role, "content": message.content});
    }
    let mut parts = vec![json!({"type": "text", "text": message.content})];
    parts.extend(message.images.iter().map(|image| {
        json!({
            "type": "image_url",
            "image_url": {"url": format!("data:{};base64,{}", image.mime, image.data_base64)},
        })
    }));
    json!({"role": role, "content": parts})
}

fn render_extra_body(value: &Value, effort: Option<&str>) -> Value {
    match value {
        Value::String(text) => render_template_string(text, effort),
        Value::Array(items) => {
            Value::Array(items.iter().map(|item| render_extra_body(item, effort)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter().map(|(key, item)| (key.clone(), render_extra_body(item, effort))).collect(),
        ),
        other => other.clone(),
    }
}

fn render_template_string(text: &str, effort: Option<&str>) -> Value {
    if let Some(name) = text.strip_prefix("${").and_then(|rest| rest.strip_suffix('}')) {
        if let Some(value) = template_value(name, effort) {
            return value;
        }
        tracing::warn!(variable = name, "extra_body template variable unknown - left as-is");
        return json!(text);
    }
    json!(substitute_embedded(text, effort))
}

fn substitute_embedded(text: &str, effort: Option<&str>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some((before, tail)) = rest.split_once("${") {
        out.push_str(before);
        if let Some((name, remainder)) = tail.split_once('}') {
            out.push_str(&template_string_form(name, effort));
            rest = remainder;
        } else {
            // Unterminated placeholder: keep the literal text.
            out.push_str("${");
            rest = tail;
        }
    }
    out.push_str(rest);
    out
}

fn template_string_form(name: &str, effort: Option<&str>) -> String {
    match template_value(name, effort) {
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::String(value)) => value,
        Some(Value::Null) => String::new(),
        Some(other) => other.to_string(),
        None => {
            tracing::warn!(variable = name, "extra_body template variable unknown - left as-is");
            format!("${{{name}}}")
        }
    }
}

/// Extracts `choices[0].message.content` plus the optional `usage` and
/// `timings` blocks from an OpenAI-compatible response, cutting model
/// reasoning so it can never reach a channel: separate
/// `reasoning_content`/`reasoning` fields are simply never read, and inline
/// `<think>...</think>` blocks (the interleaved-reasoning shape emitted by
/// llama.cpp/LM Studio/vLLM) are stripped from the content itself. This is
/// also what keeps reasoning out of the progressive reveal - the reveal only
/// ever shows prefixes of the returned content. A reasoning-only answer
/// (empty after the strip) is no answer: [`LlmError::EmptyResponse`].
/// `measured_ms` is the adapter-measured round trip, used when the endpoint
/// publishes no timing data of its own.
fn parse_completion_content(text: &str, measured_ms: u64) -> Result<CompletionResponse, LlmError> {
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
    let timing = parse_timings(&value)
        .map_or_else(|| ResponseTiming::measured(measured_ms), ResponseTiming::reported);
    Ok(CompletionResponse { content, usage: parse_usage(&value), timing })
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

/// Incremental reasoning suppressor for STREAMED content deltas. The live
/// reveal must never show `<think>` text: without this, a channel with
/// `streaming` on would watch the model think (the raw deltas go straight
/// to the reveal), and a stream that died mid-thinking would deliver - and
/// record - thinking as the final partial answer. The authoritative final
/// content is stripped separately (see [`strip_think_blocks`]); this
/// filters only what the reveal shows.
///
/// Stream-safe semantics: an opener - leading or mid-text - suppresses
/// everything until a closer appears (possibly across delta boundaries);
/// whitespace right after a closer is skipped (mirroring the strip's trim
/// around cuts). The mid-text-unclosed-means-literal rule cannot be applied
/// prospectively, so such text stays hidden for the live reveal - the final
/// edit still shows it ("only ever behind, never wrong"), and a stream that
/// dies inside it loses it. A leading opener that never closes keeps the
/// reveal empty, which maps to the no-answer fallback exactly like the
/// authoritative `EmptyResponse`. Matching is case-sensitive on the
/// universal lowercase wire form; exotic casing is still cut from the
/// authoritative text.
#[derive(Default)]
pub(crate) struct DeltaThinkStripper {
    inside_think: bool,
    /// Held-back characters that may still turn into a tag boundary
    /// (`<...` while copying, `</...` while suppressing); emitted or
    /// dropped as soon as the next character resolves the candidate.
    held: String,
    /// Set when a think block closes: skip the whitespace run that follows
    /// the cut.
    trim_next: bool,
}

const OPEN_TAG: &str = "<think>";
const CLOSE_TAG: &str = "</think>";

impl DeltaThinkStripper {
    /// Consumes one delta and returns the text safe to reveal.
    pub(crate) fn push(&mut self, delta: &str) -> String {
        let mut out = String::new();
        for ch in delta.chars() {
            self.held.push(ch);
            self.resolve(&mut out);
        }
        out
    }

    /// Flushes the held-back characters at end of stream: literal when
    /// copying, discarded when the stream died inside a think block.
    pub(crate) fn finish(&mut self) -> String {
        let mut out = String::new();
        if !self.inside_think {
            let held = std::mem::take(&mut self.held);
            for ch in held.chars() {
                self.emit(&mut out, ch);
            }
        }
        self.held.clear();
        out
    }

    /// Resolves the held-back candidate: emits (copy mode) or drops
    /// (suppress mode) until the held bytes are a resolved tag or a
    /// possible tag prefix again.
    fn resolve(&mut self, out: &mut String) {
        loop {
            let (tag, suppressing) =
                if self.inside_think { (CLOSE_TAG, true) } else { (OPEN_TAG, false) };
            if self.held.starts_with(tag) {
                self.held.replace_range(..tag.len(), "");
                self.inside_think = !suppressing;
                self.trim_next = suppressing;
                continue;
            }
            // Still a possible tag prefix - wait for the next character.
            if tag.starts_with(self.held.as_str()) {
                return;
            }
            if suppressing {
                // Inside a think block: non-closer text is dropped, not
                // revealed - dropping the first character re-exposes the
                // rest for closer matching.
                self.held.remove(0);
            } else {
                let first = self.held.remove(0);
                self.emit(out, first);
            }
        }
    }

    fn emit(&mut self, out: &mut String, ch: char) {
        if self.trim_next {
            if ch.is_whitespace() {
                return;
            }
            self.trim_next = false;
        }
        out.push(ch);
    }
}

/// `usage` is optional in the `OpenAI` shape: endpoints that do not report
/// token stats simply get `None`. A partial `usage` block is treated as
/// absent rather than guessed at.
fn parse_usage(response: &Value) -> Option<TokenUsage> {
    parse_usage_value(response.get("usage")?)
}

/// Parses one `usage` object of the `OpenAI` shape (shared by the
/// single-shot response and the stream's final chunk).
fn parse_usage_value(usage: &Value) -> Option<TokenUsage> {
    Some(TokenUsage {
        prompt_tokens: usage.get("prompt_tokens").and_then(Value::as_u64)?,
        completion_tokens: usage.get("completion_tokens").and_then(Value::as_u64)?,
        total_tokens: usage.get("total_tokens").and_then(Value::as_u64)?,
        cached_tokens: usage
            .get("prompt_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
        reasoning_tokens: usage
            .get("completion_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64),
    })
}

/// Complete endpoint-side time from llama.cpp's `timings` extension block:
/// prompt processing plus generation. Fractional milliseconds; absent or
/// partial blocks yield `None` and the adapter's own measurement stands in.
fn parse_timings(response: &Value) -> Option<u64> {
    let timings = response.get("timings")?;
    let prompt_ms = timings.get("prompt_ms").and_then(Value::as_f64)?;
    let predicted_ms = timings.get("predicted_ms").and_then(Value::as_f64)?;
    // Fractional ms from the endpoint; rounded, clamped at zero (a sane
    // endpoint never reports a negative sum).
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some((prompt_ms + predicted_ms).round().max(0.0) as u64)
}

/// Adapter-measured round trip in whole milliseconds.
fn wall_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// One decoded `data:` payload of the `OpenAI` SSE stream: an owned content
/// delta and/or usage block and/or llama.cpp `timings` block. Reasoning
/// deltas are deliberately NOT represented - they are cut at this boundary
/// and never surface. Unknown shapes (keep-alives, empty choices) parse to
/// `None` and are ignored by the caller.
struct SseEvent {
    delta: Option<String>,
    usage: Option<TokenUsage>,
    endpoint_ms: Option<u64>,
}

fn parse_sse_data(data: &str) -> Option<SseEvent> {
    let value: Value = serde_json::from_str(data).ok()?;
    let delta = value
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("content"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let usage = value.get("usage").filter(|usage| usage.is_object()).and_then(parse_usage_value);
    let endpoint_ms = parse_timings(&value);
    Some(SseEvent { delta, usage, endpoint_ms })
}

/// What one SSE body yielded: the assembled content, the final `usage` and
/// `timings` blocks when reported, whether the stream terminated with
/// `[DONE]`, and the raw body (empty unless `log_raw`).
struct SseRead {
    content: String,
    usage: Option<TokenUsage>,
    endpoint_ms: Option<u64>,
    done: bool,
    raw: String,
}

/// Reads an `OpenAI` SSE body: forwards non-empty content deltas to `deltas`,
/// captures the final `usage` and `timings` blocks, and reassembles the raw
/// text when `log_raw` is set (operator diagnostic). Byte-buffered on
/// purpose: only complete lines are decoded, so a multi-byte character split
/// across TCP chunks stays intact.
async fn read_sse_stream(
    response: reqwest::Response,
    deltas: &mpsc::Sender<String>,
    log_raw: bool,
) -> Result<SseRead, LlmError> {
    let mut bytes = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut content = String::new();
    let mut usage: Option<TokenUsage> = None;
    let mut endpoint_ms: Option<u64> = None;
    let mut raw = String::new();
    let mut done = false;
    // The reveal never sees reasoning: deltas are filtered through the
    // incremental suppressor while `content` stays raw for the
    // authoritative strip below.
    let mut stripper = DeltaThinkStripper::default();
    while !done {
        let Some(chunk) = bytes.next().await else { break };
        let chunk = chunk.map_err(|err| LlmError::Request(format!("stream read failed: {err}")))?;
        buffer.extend_from_slice(&chunk);
        while let Some(pos) = buffer.iter().position(|&byte| byte == b'\n') {
            let line_bytes = buffer.drain(..=pos).collect::<Vec<u8>>();
            let line_len = line_bytes.len().saturating_sub(1); // the newline
            let decoded =
                String::from_utf8_lossy(line_bytes.get(..line_len).unwrap_or(&line_bytes));
            let line = decoded.trim_end_matches('\r');
            if log_raw {
                raw.push_str(line);
                raw.push('\n');
            }
            let Some(data) = line.strip_prefix("data:") else { continue };
            let data = data.trim();
            if data == "[DONE]" {
                done = true;
                break;
            }
            let Some(event) = parse_sse_data(data) else { continue };
            if event.usage.is_some() {
                usage = event.usage;
            }
            if event.endpoint_ms.is_some() {
                endpoint_ms = event.endpoint_ms;
            }
            if let Some(delta) = event.delta.filter(|delta| !delta.is_empty()) {
                content.push_str(&delta);
                // The engine consumes promptly - it throttles Discord edits,
                // not receives. A closed channel means the engine task died;
                // finish reading the authoritative response anyway.
                let visible = stripper.push(&delta);
                if !visible.is_empty() {
                    let _ = deltas.send(visible).await;
                }
            }
        }
    }
    let visible = stripper.finish();
    if !visible.is_empty() {
        let _ = deltas.send(visible).await;
    }
    Ok(SseRead { content, usage, endpoint_ms, done, raw })
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else {
        text.chars().take(max_chars).collect()
    }
}

/// Collapses runs of more than `max` consecutive newlines down to exactly
/// `max` - some models pad answers with blank lines, which wastes message
/// length and renders as huge gaps. Only `\n` runs are counted; a `\r` in
/// `\r\n` pairs breaks the run and passes through unchanged.
fn collapse_newlines(content: &str, max: usize) -> String {
    let mut out = String::with_capacity(content.len());
    let mut run = 0usize;
    for ch in content.chars() {
        if ch == '\n' {
            run += 1;
            if run > max {
                continue;
            }
        } else {
            run = 0;
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::llm::completion_port::{ChatRole, ImagePart};

    /// Feeds the stripper delta-by-delta and appends the finish flush -
    /// mirrors exactly what `read_sse_stream` does on the wire.
    fn strip_deltas(deltas: &[&str]) -> String {
        let mut stripper = DeltaThinkStripper::default();
        let mut out = String::new();
        for delta in deltas {
            out.push_str(&stripper.push(delta));
        }
        out.push_str(&stripper.finish());
        out
    }

    #[test]
    fn stripper_passes_plain_text_through() {
        assert_eq!(strip_deltas(&["hello ✓ world"]), "hello ✓ world");
        assert_eq!(strip_deltas(&["a < b and c > d"]), "a < b and c > d");
        assert_eq!(strip_deltas(&["", "more"]), "more");
    }

    #[test]
    fn stripper_cuts_leading_think_block_split_across_deltas() {
        let out = strip_deltas(&["<thi", "nk>reasoning </th", "ink>", "answer"]);
        assert_eq!(out, "answer");
    }

    #[test]
    fn stripper_cuts_mid_text_pair_and_surrounding_whitespace() {
        let out = strip_deltas(&["answer", "<think>hidden</think>", "\n\nmore"]);
        assert_eq!(out, "answermore");
    }

    #[test]
    fn stripper_leading_unclosed_emits_nothing() {
        let out = strip_deltas(&["<think>, reasoning forever"]);
        assert_eq!(out, "");
        // The reveal stayed empty -> begin is never called -> the engine's
        // no-reveal failure path fires, matching EmptyResponse semantics.
    }

    #[test]
    fn stripper_stream_dying_inside_mid_text_block_keeps_the_answer_prefix() {
        let out = strip_deltas(&["answer <think>and now hidden"]);
        assert_eq!(out, "answer ");
    }

    #[test]
    fn stripper_partial_tag_at_stream_end_is_literal() {
        // A trailing '<' that never resolves is literal text.
        assert_eq!(strip_deltas(&["hi <thi"]), "hi <thi");
    }

    #[test]
    fn stripper_handles_tag_lookalikes_and_repeated_blocks() {
        // The real tag starts at position 5 (`<thin<think>` contains a
        // genuine `<think>` from there) - identical to the authoritative
        // strip: literal prefix + suppressed span + tail.
        assert_eq!(strip_deltas(&["<thin<think>x</think>ok"]), "<thinok");
        assert_eq!(strip_deltas(&["<think>a</think>one<think>b</think>two"]), "onetwo");
    }

    /// Single-shot parse with a placeholder wall clock - timing assertions
    /// live in the dedicated tests below.
    fn parse_completion(text: &str) -> Result<CompletionResponse, LlmError> {
        parse_completion_content(text, 0)
    }

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
            ChatMessage::text(ChatRole::System, "be nice"),
            ChatMessage::text(ChatRole::User, "alice: hi"),
        ]
    }

    #[test]
    fn image_messages_render_multipart_content() {
        let messages = vec![ChatMessage {
            images: vec![ImagePart {
                mime: "image/jpeg".to_owned(),
                data_base64: "QUJD".to_owned(),
            }],
            ..ChatMessage::text(ChatRole::User, "describe this")
        }];
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "vision-model",
            &messages,
            &GenParams::default(),
            false,
            &BTreeMap::new(),
        );

        let content = body
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
            .expect("multipart content array expected");
        let text = content.first().expect("text part expected");
        let image = content.get(1).expect("image part expected");
        assert_eq!(content.len(), 2);
        assert_eq!(text.get("type").and_then(Value::as_str), Some("text"));
        assert_eq!(text.get("text").and_then(Value::as_str), Some("describe this"));
        assert_eq!(image.get("type").and_then(Value::as_str), Some("image_url"));
        assert_eq!(
            image.get("image_url").and_then(|image| image.get("url")).and_then(Value::as_str),
            Some("data:image/jpeg;base64,QUJD")
        );
    }

    #[test]
    fn text_only_messages_render_plain_content() {
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &GenParams::default(),
            false,
            &BTreeMap::new(),
        );
        let content = body
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .expect("plain string content expected");
        assert_eq!(content, "be nice");
    }

    #[test]
    fn body_includes_only_set_params() {
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "glm-5.3-flash",
            &sample_messages(),
            &GenParams { temperature: Some(0.7), max_tokens: Some(512), ..GenParams::default() },
            false,
            &BTreeMap::new(),
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

        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &params,
            true,
            &BTreeMap::new(),
        );
        assert_eq!(body.get("reasoning_effort").and_then(Value::as_str), Some("high"));

        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &params,
            false,
            &BTreeMap::new(),
        );
        assert!(body.get("reasoning_effort").is_none());

        let body = completion_body(
            ReasoningStyle::GlmThinking,
            "m",
            &sample_messages(),
            &params,
            true,
            &BTreeMap::new(),
        );
        let thinking = body.get("thinking").expect("thinking expected");
        assert_eq!(thinking.get("type").and_then(Value::as_str), Some("enabled"));
    }

    /// The operator passthrough rides into every body - and engine-owned
    /// fields cannot be hijacked from provider config.
    #[test]
    fn extra_body_passes_provider_fields_through() {
        let extra = BTreeMap::from([
            ("chat_template_kwargs".to_owned(), json!({"enable_thinking": false})),
            ("model".to_owned(), json!("hijack")),
        ]);
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &GenParams::default(),
            false,
            &extra,
        );
        let kwargs = body.get("chat_template_kwargs").expect("passthrough expected");
        assert_eq!(kwargs.get("enable_thinking").and_then(Value::as_bool), Some(false));
        assert_eq!(body.get("model").and_then(Value::as_str), Some("m"));
    }

    /// `${enable_reasoning}` routes the channel's off-switch into template
    /// kwargs as a real JSON boolean - the wire value llama.cpp expects.
    /// `off` is the only false; unset stays true (provider default = on).
    #[test]
    fn extra_body_template_renders_enable_reasoning() {
        let extra = BTreeMap::from([(
            "chat_template_kwargs".to_owned(),
            json!({"enable_thinking": "${enable_reasoning}"}),
        )]);
        let body_for = |effort: Option<&str>| {
            completion_body(
                ReasoningStyle::OpenaiEffort,
                "m",
                &sample_messages(),
                &GenParams { reasoning_effort: effort.map(str::to_owned), ..GenParams::default() },
                false,
                &extra,
            )
        };
        let flag = |body: &Value| {
            body.pointer("/chat_template_kwargs/enable_thinking").and_then(Value::as_bool)
        };

        assert_eq!(flag(&body_for(Some("off"))), Some(false));
        assert_eq!(flag(&body_for(Some("low"))), Some(true));
        assert_eq!(flag(&body_for(None)), Some(true));
    }

    /// `${reasoning_effort}` passes the effort through verbatim; unset and
    /// `off` render `null` (no meaningful effort exists for either).
    #[test]
    fn extra_body_template_renders_effort_variable() {
        let extra = BTreeMap::from([("vendor_effort".to_owned(), json!("${reasoning_effort}"))]);
        let body_for = |effort: Option<&str>| {
            completion_body(
                ReasoningStyle::OpenaiEffort,
                "m",
                &sample_messages(),
                &GenParams { reasoning_effort: effort.map(str::to_owned), ..GenParams::default() },
                false,
                &extra,
            )
        };

        assert_eq!(
            body_for(Some("high")).get("vendor_effort").and_then(Value::as_str),
            Some("high")
        );
        assert_eq!(body_for(None).get("vendor_effort"), Some(&Value::Null));
        assert_eq!(body_for(Some("off")).get("vendor_effort"), Some(&Value::Null));
    }

    /// Placeholders resolve inside nested arrays/objects, substitute
    /// textually when embedded in longer strings, and unknown variables are
    /// left as-is (with a log warning) instead of silently vanishing.
    #[test]
    fn extra_body_template_handles_nesting_and_unknown_variables() {
        let extra = BTreeMap::from([(
            "nested".to_owned(),
            json!([
                {"mixed": "thinking=${enable_reasoning}?"},
                "${enable_reasoning}",
                "${wizard}",
                42,
            ]),
        )]);
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &GenParams { reasoning_effort: Some("off".to_owned()), ..GenParams::default() },
            false,
            &extra,
        );
        let nested = body.get("nested").and_then(Value::as_array).expect("nested expected");
        assert_eq!(
            nested.first().and_then(|item| item.get("mixed")).and_then(Value::as_str),
            Some("thinking=false?")
        );
        assert_eq!(nested.get(1), Some(&json!(false)));
        assert_eq!(nested.get(2).and_then(Value::as_str), Some("${wizard}"));
        assert_eq!(nested.get(3), Some(&json!(42)));
    }

    /// `off` on the boolean-switch style is a real wire value: an explicit
    /// disable (honored by GLM-4.5 through 5.2; GLM-5.3 thinks forcibly).
    #[test]
    fn off_renders_explicit_disable_for_thinking_switch_style() {
        let params = GenParams { reasoning_effort: Some("off".to_owned()), ..GenParams::default() };
        let body = completion_body(
            ReasoningStyle::GlmThinking,
            "m",
            &sample_messages(),
            &params,
            true,
            &BTreeMap::new(),
        );
        let thinking = body.get("thinking").expect("explicit disable expected");
        assert_eq!(thinking.get("type").and_then(Value::as_str), Some("disabled"));
    }

    /// The effort scale has no universal off value (sending an unsupported
    /// level could hard-reject on strict endpoints), so `off` omits the
    /// parameter and the provider default applies - which on Z.ai GLM is
    /// heavy thinking. The docs say so; operators pick `low` instead.
    #[test]
    fn off_omits_the_parameter_for_effort_style() {
        let params = GenParams { reasoning_effort: Some("off".to_owned()), ..GenParams::default() };
        let body = completion_body(
            ReasoningStyle::OpenaiEffort,
            "m",
            &sample_messages(),
            &params,
            true,
            &BTreeMap::new(),
        );
        assert!(body.get("reasoning_effort").is_none());
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn completion_content_is_extracted() {
        let response = parse_completion(
            r#"{"choices":[{"message":{"role":"assistant","content":"hello there"}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "hello there");
        // Endpoint without usage stats: absent, not zeroed.
        assert_eq!(response.usage, None);

        // Reasoning models may answer with null content - that is no answer.
        assert!(matches!(
            parse_completion(r#"{"choices":[{"message":{"content":null}}]}"#),
            Err(LlmError::EmptyResponse)
        ));
        assert!(matches!(parse_completion(r#"{"choices":[]}"#), Err(LlmError::EmptyResponse)));
        assert!(matches!(parse_completion("not json"), Err(LlmError::Request(_))));
    }

    /// Reasoning output never reaches a channel: separate reasoning fields
    /// are ignored by construction, inline `<think>` blocks are cut, and a
    /// reasoning-only answer is `EmptyResponse` (silence policy takes over).
    #[test]
    fn reasoning_is_cut_from_responses() {
        // Separate field (GLM/DeepSeek shape): never read, content stands alone.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"The answer is 4.",
                "reasoning_content":"secret chain of thought"}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer is 4.");

        // Leading block (R1/llama.cpp shape): the trailing newline goes too.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"<think>reasoning here</think>\n\nThe answer is 4."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer is 4.");

        // Interleaved blocks, case-insensitive tags.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"One <think>a</think> two <THINK>b</THINK> three."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "One two three.");

        // Unclosed opener MID-TEXT is literal: answers may discuss the tag
        // without being truncated from it onward.
        for content in ["Visible<think>hidden forever", "Wrap it in <think> tags to reason."] {
            let response = parse_completion(&format!(
                r#"{{"choices":[{{"message":{{"content":"{content}"}}}}]}}"#
            ))
            .expect("response expected to parse");
            assert_eq!(response.content, content);
        }

        // Chained leading blocks are all reasoning.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"<think>a</think><think>b</think>The answer."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "The answer.");

        // A leading unclosed block means the model never got past thinking:
        // a reasoning-only answer is empty and counts as no answer.
        assert!(matches!(
            parse_completion(r#"{"choices":[{"message":{"content":"<think>only reasoning"}}]}"#),
            Err(LlmError::EmptyResponse)
        ));

        // Residual false positive, pinned on purpose: prose containing BOTH
        // tags in order loses the span between them.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"Compare <think> with </think> syntax."}}]}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.content, "Compare syntax.");

        // No tag: byte-identical passthrough - even odd whitespace.
        let response =
            parse_completion(r#"{"choices":[{"message":{"content":"  keep\nthis  "}}]}"#)
                .expect("response expected to parse");
        assert_eq!(response.content, "  keep\nthis  ");
    }

    #[test]
    fn usage_is_parsed_when_the_endpoint_reports_it() {
        let response = parse_completion(
            r#"{
                "choices": [{"message": {"content": "ok"}}],
                "usage": {
                    "prompt_tokens": 1200,
                    "completion_tokens": 34,
                    "total_tokens": 1234,
                    "prompt_tokens_details": {"cached_tokens": 800},
                    "completion_tokens_details": {"reasoning_tokens": 2050}
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
                reasoning_tokens: Some(2050),
            })
        );

        // No cached/reasoning breakdown: the core fields still parse.
        let response = parse_completion(
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
                reasoning_tokens: None,
            })
        );

        // Partial usage blocks are treated as absent, not guessed at.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1}}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.usage, None);
    }

    #[test]
    fn newline_runs_collapse_to_the_maximum() {
        assert_eq!(collapse_newlines("a\n\n\n\n\nb", 2), "a\n\nb");
        // Under and at the cap: untouched.
        assert_eq!(collapse_newlines("a\nb", 2), "a\nb");
        assert_eq!(collapse_newlines("a\n\nb", 2), "a\n\nb");
        // Max 1 = no blank lines at all.
        assert_eq!(collapse_newlines("a\n\n\n\nb", 1), "a\nb");
        // No newlines: unchanged.
        assert_eq!(collapse_newlines("plain text", 2), "plain text");
        // A `\r` breaks the run and passes through.
        assert_eq!(collapse_newlines("a\r\n\r\nb", 1), "a\r\n\r\nb");
    }

    /// A plain-JSON HTTP fixture for the single-shot path (the SSE fixture
    /// carries content-type headers only, so it serves both).
    fn json_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn collapsing_adapter(api_url: String, max: Option<usize>) -> OpenAiCompatibleAdapter {
        let settings = LlmSettings {
            max_consecutive_newlines: max,
            providers: BTreeMap::from([(
                "local".to_owned(),
                ProviderSettings { api_url, ..ProviderSettings::default() },
            )]),
            ..LlmSettings::default()
        };
        OpenAiCompatibleAdapter::from_settings(Arc::new(settings)).expect("adapter builds")
    }

    #[tokio::test]
    async fn newline_runs_collapse_when_the_option_is_set() {
        let body = r#"{"choices":[{"message":{"content":"a\n\n\n\n\nb"}}]}"#;
        let (api_url, server) = raw_http_server(json_response(body)).await;
        let adapter = collapsing_adapter(api_url, Some(2));

        let response = adapter.complete(stream_request()).await.expect("completion expected");
        server.await.expect("server task");

        assert_eq!(response.content, "a\n\nb");
    }

    #[tokio::test]
    async fn newline_runs_survive_when_the_option_is_absent() {
        let body = r#"{"choices":[{"message":{"content":"a\n\n\n\n\nb"}}]}"#;
        let (api_url, server) = raw_http_server(json_response(body)).await;
        let adapter = collapsing_adapter(api_url, None);

        let response = adapter.complete(stream_request()).await.expect("completion expected");
        server.await.expect("server task");

        assert_eq!(response.content, "a\n\n\n\n\nb");
    }

    /// The authoritative assembled stream content is collapsed - the live
    /// deltas may carry the raw runs until the final edit pins the clean
    /// text (same class as reasoning stripping).
    #[tokio::test]
    async fn streaming_final_content_collapses_newline_runs() {
        let script = String::new()
            + &sse_frame(r#"{"choices":[{"delta":{"content":"a\n"}}]}"#)
            + &sse_frame(r#"{"choices":[{"delta":{"content":"\n\n\nb"}}]}"#)
            + "data: [DONE]\n\n";
        let (api_url, server) = raw_http_server(sse_response(&script)).await;
        let adapter = collapsing_adapter(api_url, Some(1));

        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let response = adapter
            .complete_streaming(stream_request(), tx)
            .await
            .expect("stream expected to succeed");
        server.await.expect("server task");

        assert_eq!(response.content, "a\nb");
    }

    /// llama.cpp reports its own complete time (the `timings` block: prompt
    /// processing plus generation, fractional ms) - the recorded timing
    /// prefers it over the adapter's wall clock and marks it
    /// endpoint-reported.
    #[test]
    fn timing_prefers_endpoint_reported_values() {
        let response = parse_completion(
            r#"{
                "choices": [{"message": {"content": "ok"}}],
                "timings": {
                    "prompt_n": 218, "prompt_ms": 2468.306,
                    "predicted_n": 999, "predicted_ms": 47768.303
                }
            }"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.timing, ResponseTiming::reported(50_237));

        // Partial timings blocks are treated as absent, not guessed at.
        let response = parse_completion(
            r#"{"choices":[{"message":{"content":"ok"}}],"timings":{"prompt_ms":10.0}}"#,
        )
        .expect("response expected to parse");
        assert_eq!(response.timing, ResponseTiming::measured(0));
    }

    /// Without a `timings` block the complete time is the adapter-measured
    /// wall clock passed in by the caller.
    #[test]
    fn timing_falls_back_to_the_measured_wall_clock() {
        let response =
            parse_completion_content(r#"{"choices":[{"message":{"content":"ok"}}]}"#, 1234)
                .expect("response expected to parse");
        assert_eq!(response.timing, ResponseTiming::measured(1234));
    }

    /// llama.cpp also emits `timings` on the final SSE chunk - the stream
    /// path records the endpoint-reported time the same way.
    #[tokio::test]
    async fn streaming_uses_endpoint_timings_when_reported() {
        let script = String::new()
            + &sse_frame(r#"{"choices":[{"delta":{"content":"He"}}]}"#)
            + &sse_frame(
                r#"{"choices":[{"delta":{},"finish_reason":"stop"}],"timings":{"prompt_ms":1.5,"predicted_ms":2.4}}"#,
            )
            + "data: [DONE]\n\n";
        let (api_url, server) = raw_http_server(sse_response(&script)).await;
        let adapter = streaming_adapter(api_url);

        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let response = adapter
            .complete_streaming(stream_request(), tx)
            .await
            .expect("stream expected to succeed");
        server.await.expect("server task");

        assert_eq!(response.timing, ResponseTiming::reported(4));
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

    /// Minimal SSE fixture: a one-shot TCP server that swallows the request
    /// head, writes the scripted payload, and closes (EOF). Deliberately no
    /// HTTP framework - the adapter only needs status line + body.
    async fn raw_http_server(script: String) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 8192];
            let _ = socket.read(&mut buf).await;
            socket.write_all(script.as_bytes()).await.expect("write");
        });
        (format!("http://{addr}/v1"), handle)
    }

    /// A successful SSE response head + the given frames.
    fn sse_response(frames: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{frames}"
        )
    }

    /// A non-2xx status is a Request error carrying the status AND the body
    /// (truncated) - the body is usually the only explanation an endpoint
    /// gives for a rejection.
    #[tokio::test]
    async fn single_shot_non_2xx_is_an_error_with_status_and_body() {
        let script = "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\n\
                      Content-Length: 5\r\n\r\noops!"
            .to_owned();
        let (api_url, server) = raw_http_server(script).await;
        let adapter = streaming_adapter(api_url);

        let err = adapter.complete(stream_request()).await.expect_err("non-2xx expected to fail");
        server.await.expect("server task");

        assert!(matches!(err, LlmError::Request(_)));
        let rendered = err.to_string();
        assert!(rendered.contains("500"), "status expected in the error: {rendered}");
        assert!(rendered.contains("oops"), "body expected in the error: {rendered}");
    }

    fn streaming_adapter(api_url: String) -> OpenAiCompatibleAdapter {
        let settings = LlmSettings {
            providers: BTreeMap::from([(
                "local".to_owned(),
                ProviderSettings { api_url, ..ProviderSettings::default() },
            )]),
            ..LlmSettings::default()
        };
        OpenAiCompatibleAdapter::from_settings(Arc::new(settings)).expect("adapter builds")
    }

    fn sse_frame(payload: &str) -> String {
        format!("data: {payload}\n\n")
    }

    fn stream_request() -> CompletionRequest {
        CompletionRequest {
            model: "local/m".to_owned(),
            messages: sample_messages(),
            params: GenParams::default(),
        }
    }

    #[tokio::test]
    async fn streaming_forwards_content_deltas_and_final_usage() {
        let script = String::new()
            + &sse_frame(r#"{"choices":[{"delta":{"content":"He"}}]}"#)
            + &sse_frame(r#"{"choices":[{"delta":{"reasoning_content":"secret thoughts"}}]}"#)
            + &sse_frame(r#"{"choices":[{"delta":{"content":"y"}}]}"#)
            + &sse_frame(
                r#"{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}"#,
            )
            + "data: [DONE]\n\n";
        let (api_url, server) = raw_http_server(sse_response(&script)).await;
        let adapter = streaming_adapter(api_url);

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let response = adapter
            .complete_streaming(stream_request(), tx)
            .await
            .expect("stream expected to succeed");
        server.await.expect("server task");

        // Only content deltas surface - reasoning is cut at this boundary.
        let mut deltas = Vec::new();
        while let Ok(delta) = rx.try_recv() {
            deltas.push(delta);
        }
        assert_eq!(deltas, vec!["He".to_owned(), "y".to_owned()]);
        assert_eq!(response.content, "Hey");
        assert_eq!(
            response.usage,
            Some(TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 3,
                total_tokens: 13,
                cached_tokens: None,
                reasoning_tokens: None,
            })
        );
        // No `timings` block in the stream: the complete time is measured.
        assert!(!response.timing.endpoint_reported);
    }

    /// An abrupt end of stream (no `[DONE]`) is a failure: the assembled
    /// prefix is incomplete by definition and must not pass as an answer.
    #[tokio::test]
    async fn stream_without_done_marker_is_an_error() {
        let script = sse_response(&sse_frame(r#"{"choices":[{"delta":{"content":"par"}}]}"#));
        let (api_url, server) = raw_http_server(script).await;
        let adapter = streaming_adapter(api_url);

        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let result = adapter.complete_streaming(stream_request(), tx).await;
        server.await.expect("server task");

        let Err(LlmError::Request(message)) = result else {
            panic!("expected a stream failure, got {result:?}")
        };
        assert!(message.contains("without [DONE]"), "unexpected: {message}");
    }

    /// A non-success status before any SSE body is the regular request
    /// failure, carrying the classification-bearing status line.
    #[tokio::test]
    async fn streaming_error_status_is_reported() {
        let script = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 8\r\n\
                      Connection: close\r\n\r\nslow down"
            .to_owned();
        let (api_url, server) = raw_http_server(script).await;
        let adapter = streaming_adapter(api_url);

        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let result = adapter.complete_streaming(stream_request(), tx).await;
        server.await.expect("server task");

        let Err(LlmError::Request(message)) = result else {
            panic!("expected a request failure, got {result:?}")
        };
        assert!(message.contains("HTTP 429"), "unexpected: {message}");
    }
}
