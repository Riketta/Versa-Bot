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

#[derive(Debug, Clone, thiserror::Error)]
pub enum LlmError {
    #[error("invalid model reference `{0}` - expected `provider/model`")]
    InvalidModelRef(String),
    #[error("unknown provider `{0}` - not declared in [llm.providers]")]
    UnknownProvider(String),
    #[error("provider request failed: {0}")]
    Request(String),
    #[error("provider returned no message content")]
    EmptyResponse,
}

impl LlmError {
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
            Self::Request(_) => "the endpoint could not be reached or rejected the request",
            Self::EmptyResponse => "the endpoint returned no content",
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
    /// embeds (service-channel notices): the exact strings are pinned so a
    /// rewording cannot smuggle endpoint- or operator-domain detail past
    /// the privacy rule. The full error text stays in tracing.
    #[test]
    fn classify_covers_every_variant() {
        let config_error = "the model is not available (check the bot configuration)";
        assert_eq!(LlmError::InvalidModelRef("ghost/m".to_owned()).classify(), config_error);
        assert_eq!(LlmError::UnknownProvider("ghost".to_owned()).classify(), config_error);
        assert_eq!(
            LlmError::Request("HTTP 401: account=secret-org".to_owned()).classify(),
            "the endpoint could not be reached or rejected the request"
        );
        assert_eq!(LlmError::EmptyResponse.classify(), "the endpoint returned no content");
    }
}
