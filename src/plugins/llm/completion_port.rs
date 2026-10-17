//! Plugin-internal driven port for LLM completions. The provider layer sits
//! behind this port: the conversation engine never knows which vendor speaks
//! on the other side. Non-OpenAI-compatible providers become additional
//! adapters behind the same trait.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::model::GenParams;

/// Role of one message in an LLM conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRole {
    System,
    User,
    Assistant,
}

impl ChatRole {
    /// Wire name used by OpenAI-compatible APIs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

/// One image attached to a chat message, base64-encoded as a data URL on
/// the wire. Only the image-recognition call ever carries these - chat and
/// compaction contexts are text-only by design (descriptions are baked into
/// the records instead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImagePart {
    /// MIME type of the encoded bytes, e.g. `image/jpeg`.
    pub mime: String,
    /// Image bytes, base64-encoded.
    pub data_base64: String,
}

/// One message of an LLM conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
    /// Images for vision-capable calls; empty renders plain-text content.
    pub images: Vec<ImagePart>,
}

impl ChatMessage {
    /// A plain-text message (the common shape - chat and compaction turns).
    #[must_use]
    pub fn text(role: ChatRole, content: impl Into<String>) -> Self {
        Self { role, content: content.into(), images: Vec::new() }
    }
}

/// A completion request: fully assembled context in, answer out. Context
/// assembly (system prompt, summary slot, turn rendering) is the engine's
/// job; the port is transport only.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    /// Provider-qualified model reference (`provider/model`).
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub params: GenParams,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionResponse {
    pub content: String,
    /// Provider-reported token usage; `None` when the endpoint does not
    /// provide usage stats (the field is optional in the shape).
    pub usage: Option<TokenUsage>,
    /// Complete response time: endpoint-reported when the provider
    /// publishes timing data, otherwise adapter-measured wall clock.
    pub timing: ResponseTiming,
}

/// How long one completion took end to end. `endpoint_reported` marks the
/// provider's own measurement (llama.cpp `timings`: prompt processing +
/// generation); without it the total is the adapter's wall clock around the
/// HTTP call - which additionally contains network transfer and JSON
/// encoding, so the two sources are not expected to agree. Stored per
/// channel as part of the usage stats, hence the serde derives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseTiming {
    pub total_ms: u64,
    pub endpoint_reported: bool,
}

impl ResponseTiming {
    /// Endpoint-reported complete time (`timings` block present).
    #[must_use]
    pub fn reported(total_ms: u64) -> Self {
        Self { total_ms, endpoint_reported: true }
    }

    /// Adapter-measured wall clock (endpoint publishes no timing data).
    #[must_use]
    pub fn measured(total_ms: u64) -> Self {
        Self { total_ms, endpoint_reported: false }
    }
}

/// Token accounting of one completion, as reported by the endpoint. Field
/// names mirror the wire object 1:1 (hence the shared `_tokens` postfix).
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    /// Prompt tokens served from the provider's cache, when the endpoint
    /// reports the breakdown - the number that tells whether the
    /// byte-stable-prefix design is actually hitting.
    #[serde(default)]
    pub cached_tokens: Option<u64>,
    /// Tokens the endpoint reports as reasoning/thinking output
    /// (`completion_tokens_details.reasoning_tokens`). A completion can cost
    /// thousands of reasoning tokens for a one-line answer - thinking
    /// models burn them silently; this is the receipt. Absent when the
    /// endpoint does not report the breakdown.
    #[serde(default)]
    pub reasoning_tokens: Option<u64>,
}

/// Coarse classification of one failed provider request, decided inside the
/// provider adapter. It drives the guild-presentable strings - the
/// service-channel embed ([`LlmError::classify`]) and the channel fallback
/// ([`LlmError::fallback_message`]) quote only these static phrases, never
/// status codes, endpoint bodies, or other dynamic provider data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFailureKind {
    /// HTTP 429: the endpoint's usage limit is exhausted.
    QuotaExhausted,
    /// HTTP 401/403: the endpoint rejected the bot's credentials.
    AuthRejected,
    /// HTTP 400/404/413/422: the endpoint rejected the request as invalid.
    InvalidRequest,
    /// HTTP 5xx: the endpoint reported a server-side failure.
    EndpointError,
    /// HTTP 408 or a transport timeout: no answer arrived in time.
    Timeout,
    /// Connect/DNS/body-read faults below the HTTP answer layer.
    Transport,
    /// Boot-time configuration failure (proxy, client build, API key env).
    Config,
    /// Anything unclassified: malformed JSON, a stream without `[DONE]`,
    /// an unmatched status code.
    Generic,
}

impl RequestFailureKind {
    /// Technical classification for the service-channel embed.
    #[must_use]
    pub fn classify(self) -> &'static str {
        match self {
            Self::QuotaExhausted => "the endpoint reported the usage limit is reached",
            Self::AuthRejected => {
                "the endpoint rejected the bot's credentials (check the bot configuration)"
            }
            Self::InvalidRequest => {
                "the endpoint rejected the request as invalid (check the bot configuration)"
            }
            Self::EndpointError => "the endpoint reported a server-side error",
            Self::Timeout => "the endpoint took too long to respond",
            Self::Transport => "the endpoint could not be reached",
            Self::Config => "the provider setup failed (check the bot configuration)",
            Self::Generic => "the endpoint failed in an unexpected way",
        }
    }

    /// User-facing sentence for the source-channel fallback. Self-sufficient
    /// advice per kind: an exhausted quota or a configuration problem never
    /// promises that a retry in a moment will succeed.
    #[must_use]
    pub fn fallback(self) -> &'static str {
        match self {
            Self::QuotaExhausted => {
                "I couldn't process that just now - the model's usage limit has been reached. \
                 Please try again later."
            }
            Self::AuthRejected => {
                "I couldn't process that just now - the bot's access to the model was rejected. \
                 Please contact the bot operator."
            }
            Self::InvalidRequest => {
                "I couldn't process that just now - the model rejected the request as invalid. \
                 This usually means a configuration problem on the bot's side."
            }
            Self::EndpointError => {
                "I couldn't process that just now - the model's endpoint is having server-side \
                 problems. Please try again later."
            }
            Self::Timeout => {
                "I couldn't process that just now - the model took too long to respond. \
                 Please try again."
            }
            Self::Transport => {
                "I couldn't process that just now - the language model is unreachable. \
                 Please try again in a moment."
            }
            Self::Config => {
                "I couldn't process that just now - the model is unavailable due to a \
                 configuration problem. Please contact the bot operator."
            }
            Self::Generic => {
                "I couldn't process that just now - something unexpected went wrong. \
                 Please try again in a moment."
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum LlmError {
    #[error("invalid model reference `{0}` - expected `provider/model`")]
    InvalidModelRef(String),
    #[error("unknown provider `{0}` - not declared in [llm.providers]")]
    UnknownProvider(String),
    #[error("provider request failed: {detail}")]
    Request { kind: RequestFailureKind, detail: String },
    #[error("provider returned no message content")]
    EmptyResponse,
}

impl LlmError {
    /// The coarse failure kind, when the error came from a provider request.
    /// Operator-side diagnostics: the error `Display` quotes only the reqwest
    /// detail (whose wording is identical for timeouts and connect faults),
    /// so logs need the kind alongside to tell them apart.
    #[must_use]
    pub fn failure_kind(&self) -> Option<RequestFailureKind> {
        match self {
            Self::Request { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// Guild-presentable classification for service-channel embeds: names
    /// what happened without quoting the endpoint's response body, which can
    /// carry operator-domain detail (account/project identifiers). The full
    /// error text stays in tracing.
    #[must_use]
    pub fn classify(&self) -> &'static str {
        match self {
            Self::InvalidModelRef(_) | Self::UnknownProvider(_) => {
                "the model is not available (check the bot configuration)"
            }
            Self::Request { kind, .. } => kind.classify(),
            Self::EmptyResponse => "the endpoint returned no content",
        }
    }

    /// User-facing fallback sentence for the source channel: static phrases
    /// only - no status codes, no endpoint data. The exact strings are
    /// pinned by tests so a rewording cannot smuggle detail past the
    /// privacy rule.
    #[must_use]
    pub fn fallback_message(&self) -> &'static str {
        match self {
            Self::InvalidModelRef(_) | Self::UnknownProvider(_) => {
                RequestFailureKind::Config.fallback()
            }
            Self::Request { kind, .. } => kind.fallback(),
            Self::EmptyResponse => {
                "I couldn't process that just now - the model returned an empty response. \
                 Please try again."
            }
        }
    }
}

/// Driven port: single-shot completion. Streaming renderers ride the chat
/// output ports, not this trait - the answer arrives as one string and is
/// delivered (and split) by the engine.
#[async_trait]
pub trait LlmCompletionPort: Send + Sync {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Streaming completion for channels with `streaming` enabled: forwards
    /// visible content deltas as they arrive and resolves to the FULL
    /// assembled response once the endpoint is done - the returned content
    /// (reasoning-stripped) and usage are authoritative, the deltas only
    /// drive the live reveal. Reasoning deltas are cut at the adapter
    /// boundary and never surface. The default implementation degrades to
    /// [`Self::complete`] with a single delta, so providers without SSE
    /// support work unchanged everywhere.
    async fn complete_streaming(
        &self,
        request: CompletionRequest,
        deltas: mpsc::Sender<String>,
    ) -> Result<CompletionResponse, LlmError> {
        let response = self.complete(request).await?;
        let _ = deltas.send(response.content.clone()).await;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `classify` produces the ONLY error detail allowed into guild-visible
    /// embeds (service-channel notices), and `fallback_message` the ONLY
    /// sentence allowed into the source channel: the exact strings are
    /// pinned so a rewording cannot smuggle endpoint- or operator-domain
    /// detail past the privacy rule. The full error text stays in tracing.
    #[test]
    fn classify_and_fallback_pin_every_variant() {
        let detail = "HTTP 401: account=secret-org project=hidden";
        let request = |kind| LlmError::Request { kind, detail: detail.to_owned() };

        let config_error = "the model is not available (check the bot configuration)";
        assert_eq!(LlmError::InvalidModelRef("ghost/m".to_owned()).classify(), config_error);
        assert_eq!(LlmError::UnknownProvider("ghost".to_owned()).classify(), config_error);
        assert_eq!(
            request(RequestFailureKind::QuotaExhausted).classify(),
            "the endpoint reported the usage limit is reached"
        );
        assert_eq!(
            request(RequestFailureKind::AuthRejected).classify(),
            "the endpoint rejected the bot's credentials (check the bot configuration)"
        );
        assert_eq!(
            request(RequestFailureKind::InvalidRequest).classify(),
            "the endpoint rejected the request as invalid (check the bot configuration)"
        );
        assert_eq!(
            request(RequestFailureKind::EndpointError).classify(),
            "the endpoint reported a server-side error"
        );
        assert_eq!(
            request(RequestFailureKind::Timeout).classify(),
            "the endpoint took too long to respond"
        );
        assert_eq!(
            request(RequestFailureKind::Transport).classify(),
            "the endpoint could not be reached"
        );
        assert_eq!(
            request(RequestFailureKind::Config).classify(),
            "the provider setup failed (check the bot configuration)"
        );
        assert_eq!(
            request(RequestFailureKind::Generic).classify(),
            "the endpoint failed in an unexpected way"
        );
        assert_eq!(LlmError::EmptyResponse.classify(), "the endpoint returned no content");

        // Fallback sentences: static, endpoint-detail-free, and distinct
        // where the advice differs (a quota limit must not promise that a
        // retry in a moment succeeds).
        assert_eq!(
            request(RequestFailureKind::QuotaExhausted).fallback_message(),
            "I couldn't process that just now - the model's usage limit has been reached. \
             Please try again later."
        );
        assert_eq!(
            request(RequestFailureKind::AuthRejected).fallback_message(),
            "I couldn't process that just now - the bot's access to the model was rejected. \
             Please contact the bot operator."
        );
        assert_eq!(
            request(RequestFailureKind::InvalidRequest).fallback_message(),
            "I couldn't process that just now - the model rejected the request as invalid. \
             This usually means a configuration problem on the bot's side."
        );
        assert_eq!(
            request(RequestFailureKind::EndpointError).fallback_message(),
            "I couldn't process that just now - the model's endpoint is having server-side \
             problems. Please try again later."
        );
        assert_eq!(
            request(RequestFailureKind::Timeout).fallback_message(),
            "I couldn't process that just now - the model took too long to respond. \
             Please try again."
        );
        assert_eq!(
            request(RequestFailureKind::Transport).fallback_message(),
            "I couldn't process that just now - the language model is unreachable. \
             Please try again in a moment."
        );
        assert_eq!(
            request(RequestFailureKind::Config).fallback_message(),
            LlmError::UnknownProvider("ghost".to_owned()).fallback_message()
        );
        assert_eq!(
            request(RequestFailureKind::Generic).fallback_message(),
            "I couldn't process that just now - something unexpected went wrong. \
             Please try again in a moment."
        );
        assert_eq!(
            LlmError::EmptyResponse.fallback_message(),
            "I couldn't process that just now - the model returned an empty response. \
             Please try again."
        );

        // No fallback sentence may quote the endpoint detail.
        for kind in [
            RequestFailureKind::QuotaExhausted,
            RequestFailureKind::AuthRejected,
            RequestFailureKind::InvalidRequest,
            RequestFailureKind::EndpointError,
            RequestFailureKind::Timeout,
            RequestFailureKind::Transport,
            RequestFailureKind::Config,
            RequestFailureKind::Generic,
        ] {
            let message = request(kind).fallback_message();
            assert!(!message.contains("secret-org"), "{kind:?}: {message}");
            assert!(!message.contains("401"), "{kind:?}: {message}");
        }
    }

    /// `failure_kind` exposes the request kind for operator logs (the error
    /// `Display` alone cannot tell a timeout from a connect fault) and is
    /// absent for errors that never reached a provider request.
    #[test]
    fn failure_kind_is_the_request_kind_when_present() {
        let request = |kind| LlmError::Request { kind, detail: "x".to_owned() };
        assert_eq!(
            request(RequestFailureKind::Timeout).failure_kind(),
            Some(RequestFailureKind::Timeout)
        );
        assert_eq!(
            request(RequestFailureKind::Transport).failure_kind(),
            Some(RequestFailureKind::Transport)
        );
        assert_eq!(LlmError::EmptyResponse.failure_kind(), None);
        assert_eq!(LlmError::InvalidModelRef("ghost/m".to_owned()).failure_kind(), None);
        assert_eq!(LlmError::UnknownProvider("ghost".to_owned()).failure_kind(), None);
    }
}
