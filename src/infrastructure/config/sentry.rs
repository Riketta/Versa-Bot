use serde::Deserialize;

/// Sentry-protocol endpoint (`Sentry` SaaS or self-hosted `GlitchTip`).
/// The address to report to is encoded in the DSN host - no separate URL.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct SentryConfig {
    pub dsn: String,
    pub environment: Option<String>,
    /// Performance-transaction sample rate (0.0..=1.0). Absent = performance
    /// monitoring off (only error events are reported). `GlitchTip` suggests
    /// a low rate like 0.01 in production to keep data volume down.
    pub traces_sample_rate: Option<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_only_section_deserializes() {
        let config =
            serde_json::from_str::<SentryConfig>(r#"{ "dsn": "http://key@localhost:9000/1" }"#)
                .expect("dsn-only section deserializes");
        assert_eq!(config.dsn, "http://key@localhost:9000/1");
        assert_eq!(config.environment, None);
        assert_eq!(config.traces_sample_rate, None);
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<SentryConfig>(
            r#"{
                "dsn": "http://key@localhost:9000/1",
                "environment": "production",
                "traces_sample_rate": 0.01
            }"#,
        )
        .expect("full section deserializes");
        assert_eq!(config.environment.as_deref(), Some("production"));
        assert_eq!(config.traces_sample_rate, Some(0.01));
    }
}
