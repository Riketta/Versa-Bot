/// Best-effort panic payload extraction (payloads are opaque `Any`). Shared
/// by the execution boundaries that isolate plugin panics (event bus,
/// middleware pipeline).
pub(crate) fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

/// The standard reply to a slash command: ephemeral. Confirmations, usage
/// notices and error corrections stay between the bot and the invoking user
/// (admin commands carry configuration and policy details that are nobody
/// else's business, and public replies would only add channel noise). The
/// adapter honors the flag only on transactional invocations, which is all
/// command handlers ever answer. Channel-visible messages (LLM answers,
/// audit notices, fallbacks) are plain sends, not this.
#[must_use]
pub(crate) fn command_reply(text: impl Into<String>) -> crate::kernel::models::OutboundMessage {
    crate::kernel::models::OutboundMessage::text(text.into()).ephemeral()
}
