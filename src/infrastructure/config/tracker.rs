use serde::Deserialize;

use crate::plugins::tracker::DEFAULT_NOTICE_LIMIT;

/// Global `[tracker]` section: tuning for the audit notices the tracker
/// posts to the guild's configured audit channel (joins, leaves, invites,
/// message edits/deletions). Startup-only.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct TrackerConfig {
    /// Per-side character cap on quoted message text in edit/deletion
    /// notices. The plugin clamps the value so a fully loaded notice still
    /// fits the platform's message limit.
    pub message_notice_limit: usize,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self { message_notice_limit: DEFAULT_NOTICE_LIMIT }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_section_uses_the_plugin_default() {
        let config = serde_json::from_str::<TrackerConfig>("{}").expect("empty deserializes");
        assert_eq!(config.message_notice_limit, DEFAULT_NOTICE_LIMIT);
    }

    #[test]
    fn limit_deserializes() {
        let config = serde_json::from_str::<TrackerConfig>(r#"{ "message_notice_limit": 200 }"#)
            .expect("section expected to deserialize");
        assert_eq!(config.message_notice_limit, 200);
    }
}
