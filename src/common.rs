/// Best-effort panic payload extraction (payloads are opaque `Any`). Shared
/// by the execution boundaries that isolate plugin panics (event bus,
/// middleware pipeline).
///
/// Takes the `catch_unwind` box by reference - NOT `&dyn Any`: `Box<dyn Any>`
/// itself implements `Any`, so coercing `&Box<...>` to `&dyn Any` views the
/// box as the trait object (unsize coercion wins over deref coercion) and
/// every downcast against the payload silently fails. Through `&Box<...>`
/// the method call autoderefs to the actual payload.
pub(crate) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
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

/// UTF-16 code-unit length of `text` - the unit the strictest platforms
/// count limits in, so budgeting in it stays safe under both a chars and a
/// units cap (message splits, embed packing, nickname checks).
#[must_use]
pub(crate) fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

#[cfg(test)]
mod tests {
    use super::panic_message;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// Regression: the payload box must be inspected as the payload, not as
    /// the box - both payload shapes `panic!` can produce extract correctly.
    #[test]
    fn extracts_both_payload_shapes() {
        let formatted = catch_unwind(AssertUnwindSafe(|| panic!("boom {}", 42))).unwrap_err();
        assert_eq!(panic_message(&formatted), "boom 42");

        let literal = catch_unwind(AssertUnwindSafe(|| panic!("literal panic"))).unwrap_err();
        assert_eq!(panic_message(&literal), "literal panic");
    }
}
