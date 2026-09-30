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
