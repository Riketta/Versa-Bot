//! LLM chat plugin: per-channel chat bot with conversation history,
//! compaction, and progressive rendering. The plugin is its own hexagon:
//! `completion_port` + `providers` are its driven side (LLM providers are
//! adapters), `chat_engine`/`conversation` its application+domain, and the
//! middleware hook is its driving side into the kernel pipeline.

mod chat_engine;
mod commands;
mod completion_port;
mod conversation;
mod llm_plugin;
mod model;
mod providers;
mod rng;

pub use chat_engine::ChatEngine;
pub use completion_port::{
    ChatMessage, ChatRole, CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError,
};
pub use conversation::{ConversationRecord, RecordRole};
pub use llm_plugin::LlmPlugin;
pub use model::{
    CaptureMode, ChannelConfig, ConversationState, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY,
    channel_config_key, channel_state_key,
};
pub use providers::{
    LlmSettings, ModelSettings, OpenAiCompatibleAdapter, ProviderSettings, ReasoningStyle,
};
pub use rng::{RandRandom, RandomPort};
