//! Plugin-internal driven port for LLM completions. The provider layer sits
//! behind this port: the conversation engine never knows which vendor speaks
//! on the other side. Non-OpenAI-compatible providers become additional
//! adapters behind the same trait.

use async_trait::async_trait;

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

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionResponse {
    pub content: String,
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

/// Driven port: single-shot completion. Streaming renderers ride the chat
/// output ports, not this trait - the answer arrives as one string and is
/// delivered (and split) by the engine.
#[async_trait]
pub trait LlmCompletionPort: Send + Sync {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError>;
}
