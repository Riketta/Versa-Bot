use serde::Deserialize;

#[derive(Clone, Default, Deserialize, PartialEq)]
pub struct DiscordConfig {
    pub token: String,
    pub proxy: Option<String>,
}

impl std::fmt::Debug for DiscordConfig {
    // The token and the proxy URL are credentials: a derived `Debug` would
    // print them wherever a future `debug!(?config)` lands - stdout, log
    // files, and Sentry alike. Redacted unconditionally; `PartialEq` still
    // compares the real values, so config change detection is unaffected.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiscordConfig")
            .field("token", &redact(&self.token))
            .field("proxy", &self.proxy.as_deref().map(redact))
            .finish()
    }
}

/// Stable placeholder that hides the value but preserves its presence and
/// rough size, so a debug dump still shows whether the field is configured.
fn redact(value: &str) -> String {
    if value.is_empty() {
        "<empty>".to_owned()
    } else {
        format!("<redacted: {} chars>", value.chars().count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The redacted `Debug` must never carry the token or the proxy URL -
    /// one added `debug!(?config)` would otherwise leak both to stdout, log
    /// files, and Sentry.
    #[test]
    fn debug_hides_the_credentials() {
        let config = DiscordConfig {
            token: "super-secret-token".to_owned(),
            proxy: Some("http://user:pass@proxy.internal:8080".to_owned()),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-token"), "{rendered}");
        assert!(!rendered.contains("user:pass"), "{rendered}");
        assert!(rendered.contains("<redacted"), "{rendered}");
        assert!(rendered.contains("proxy"), "{rendered}");

        // Empty values stay distinguishable from unset-looking ones.
        let defaults = DiscordConfig::default();
        assert!(format!("{defaults:?}").contains("<empty>"));
    }

    /// The redaction is display-only: equality still sees the real values,
    /// which the config diff detection relies on.
    #[test]
    fn equality_compares_the_real_values() {
        let a = DiscordConfig { token: "one".to_owned(), proxy: None };
        let b = DiscordConfig { token: "two".to_owned(), proxy: None };
        assert_eq!(format!("{a:?}"), format!("{a:?}"));
        assert_ne!(a, b);
        assert_eq!(a, a.clone());
    }
}
