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
mod prompts;
mod providers;
mod rng;
mod tools;
mod vision;

pub use chat_engine::ChatEngine;
pub use commands::DISCORD_MESSAGE_LIMIT;
pub use completion_port::{
    ChatMessage, ChatRole, CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError,
    ResponseTiming,
};
pub use conversation::{ConversationRecord, RecordRole};
pub use llm_plugin::LlmPlugin;
pub use model::{
    CaptureMode, ChannelConfig, ConversationState, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY,
    channel_config_key, channel_state_key,
};
pub use prompts::warn_unknown_prompt_tokens;
pub use providers::{
    LlmSettings, ModelSettings, OpenAiCompatibleAdapter, ProviderSettings, ReasoningStyle,
    SummaryPlacement,
};
pub use rng::{DeckRandom, RandRandom, RandomPort, RandomScope};
pub use vision::{ImageDescriber, ImageJob, ImageSource, VisionService};
