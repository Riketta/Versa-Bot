/// Platform-blind presence update: what the bot "is doing" right now.
/// Adapters map it onto the native mechanism (Discord: gateway presence
/// update) and degrade where the platform lacks the concept. Presence is a
/// global concern - it is not guild-scoped.
#[derive(Debug, Clone, Default)]
pub struct Presence {
    /// Current activity; `None` clears it.
    pub activity: Option<Activity>,
}

#[derive(Debug, Clone)]
pub struct Activity {
    pub kind: ActivityKind,
    pub name: String,
}

/// The small set of activity kinds that translate across platforms.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    Playing,
    Listening,
    Watching,
    Competing,
}

impl Presence {
    /// Presence showing as `Playing <name>`.
    #[must_use]
    pub fn playing(name: impl Into<String>) -> Self {
        Self { activity: Some(Activity { kind: ActivityKind::Playing, name: name.into() }) }
    }
}
