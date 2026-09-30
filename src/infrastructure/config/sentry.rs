use serde::Deserialize;

/// Sentry-protocol endpoint (Sentry SaaS or self-hosted GlitchTip).
/// The address to report to is encoded in the DSN host - no separate URL.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct SentryConfig {
    pub dsn: String,
    pub environment: Option<String>,
}
