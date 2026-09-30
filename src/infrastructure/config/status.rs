use serde::Deserialize;

/// Bot status rotation: the presence activity cycles through `statuses`
/// every `interval_seconds`. Presence is a global concern, so this lives in
/// the bot configuration, not in guild storage. An absent section, an empty
/// list, or a zero interval disables the rotation. Both fields default when
/// the section is partial, so a half-written `[status]` deserializes to the
/// disabled state instead of failing the whole configuration.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StatusConfig {
    #[serde(default)]
    pub interval_seconds: u64,
    #[serde(default)]
    pub statuses: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A partial `[status]` section must not fail the whole configuration:
    /// zero interval + empty list is the disabled state.
    #[test]
    fn empty_section_deserializes_to_defaults() {
        let config =
            serde_json::from_str::<StatusConfig>("{}").expect("empty section deserializes");
        assert_eq!(config.interval_seconds, 0);
        assert!(config.statuses.is_empty());
    }

    #[test]
    fn partial_section_with_only_interval_deserializes() {
        let config = serde_json::from_str::<StatusConfig>(r#"{"interval_seconds": 30}"#)
            .expect("partial section deserializes");
        assert_eq!(config.interval_seconds, 30);
        assert!(config.statuses.is_empty());
    }
}
