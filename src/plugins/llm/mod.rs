//! LLM chat plugin: per-channel chat bot with conversation history,
//! compaction, and progressive rendering. The admin commands and storage
//! schema are live; the conversation engine (capture, completion,
//! compaction, random replies) joins the middleware pipeline in the
//! following steps.

mod commands;
mod llm_plugin;
mod model;
mod rng;

pub use llm_plugin::LlmPlugin;
pub use model::{
    CaptureMode, ChannelConfig, ConversationState, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY,
    channel_config_key, channel_state_key,
};
pub use rng::{RandRandom, RandomPort};
