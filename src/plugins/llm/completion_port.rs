//! Plugin-internal driven port for LLM completions. The provider layer sits
//! behind this port: the conversation engine never knows which vendor speaks
//! on the other side. Non-OpenAI-compatible providers become additional
//! adapters behind the same trait.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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

/// One message of an LLM conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
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
}
