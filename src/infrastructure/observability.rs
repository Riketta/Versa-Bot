use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Installs the global tracing subscriber: stdout fmt layer plus an optional
/// Sentry/GlitchTip layer. The Sentry endpoint is any Sentry-protocol DSN
/// (GlitchTip included), so "address" is just the DSN host - nothing else to
/// configure. Without a DSN the app logs to stdout only.
///
/// Returns the Sentry client guard; the caller must keep it alive for the
/// whole process lifetime, otherwise events are dropped on shutdown.
#[must_use]
pub fn init(
    sentry_dsn: Option<&str>,
    sentry_environment: Option<&str>,
) -> Option<sentry::ClientInitGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer());

    let guard = sentry_dsn
        .map(str::trim)
        .filter(|dsn| !dsn.is_empty())
        .map(|dsn| {
            // `ClientOptions` is `#[non_exhaustive]` - construct via defaults
            // and field assignment, not a struct literal.
            let mut options = sentry::ClientOptions::default();
            options.dsn = Some(dsn.parse().expect("sentry dsn must be a valid DSN"));
            // Cow<'static, str> - the borrowed &str outlives nothing here,
            // so take ownership first.
            options.environment = sentry_environment.map(String::from).map(Into::into);
            sentry::init(options)
        });

    match &guard {
        Some(_) => registry.with(sentry_tracing::layer()).init(),
        None => registry.init(),
    }

    guard
}
