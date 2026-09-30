use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Installs the global tracing subscriber: stdout fmt layer plus an optional
/// Sentry/GlitchTip layer. The Sentry endpoint is any Sentry-protocol DSN
/// (GlitchTip included), so "address" is just the DSN host - nothing else to
/// configure. Without a DSN - or with a malformed one, which is reported and
/// skipped instead of aborting startup - the app logs to stdout only.
/// `RUST_LOG` overrides everything; when unset, `debug` picks the fallback
/// verbosity.
///
/// Returns the Sentry client guard; the caller must keep it alive for the
/// whole process lifetime, otherwise events are dropped on shutdown.
#[must_use]
pub fn init(
    debug: bool,
    sentry_dsn: Option<&str>,
    sentry_environment: Option<&str>,
) -> Option<sentry::ClientInitGuard> {
    let fallback = if debug { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    let registry =
        tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer());

    // A malformed DSN must not abort startup: the tracing subscriber is not
    // installed yet, so warn on stderr and continue stdout-only.
    let guard =
        sentry_dsn.map(str::trim).filter(|dsn| !dsn.is_empty()).and_then(|dsn| match dsn.parse() {
            Ok(dsn) => {
                // `ClientOptions` is `#[non_exhaustive]` - construct via defaults
                // and field assignment, not a struct literal.
                let mut options = sentry::ClientOptions::default();
                options.dsn = Some(dsn);
                // Cow<'static, str> - the borrowed &str outlives nothing here,
                // so take ownership first.
                options.environment = sentry_environment.map(String::from).map(Into::into);
                Some(sentry::init(options))
            }
            Err(err) => {
                eprintln!("warning: invalid Sentry DSN ({err}) - Sentry reporting disabled");
                None
            }
        });

    match &guard {
        Some(_) => registry.with(sentry_tracing::layer()).init(),
        None => registry.init(),
    }

    guard
}
