use sentry_tracing::EventFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Fallback directives when `RUST_LOG` is unset. Target directives are
/// more specific than the global level and win for their crate.
const FALLBACK_INFO_FILTER: &str = "info";
const FALLBACK_DEBUG_FILTER: &str = "info,versa_bot=debug,reqwest=warn,hyper=warn,hyper_util=warn";

/// Installs the global tracing subscriber: stdout fmt layer plus an optional
/// Sentry/GlitchTip layer. The Sentry endpoint is any Sentry-protocol DSN
/// (GlitchTip included), so "address" is just the DSN host - nothing else to
/// configure. Without a DSN - or with a malformed one, which is reported and
/// skipped instead of aborting startup - the app logs to stdout only.
/// `RUST_LOG` overrides everything; when unset, `debug` picks the fallback
/// verbosity. The debug fallback keeps only the bot's own breadcrumbs at
/// debug and pins the third-party HTTP stack to warn: `reqwest`/`hyper`
/// emit per-request connect/frame lines whose span context embeds the
/// entire client dump - pure volume, no signal for operators.
///
/// Error events carry the crate release (`versa-bot@<version>`, via
/// `sentry::release_name!`) so the backend can group by release; performance
/// transactions are only sampled when `traces_sample_rate` (0.0..=1.0) is
/// configured - out-of-range values are reported and treated as off, in the
/// same spirit as a malformed DSN. Session tracking stays off (the SDK
/// default; `GlitchTip` does not support it).
///
/// Returns the Sentry client guard; the caller must keep it alive for the
/// whole process lifetime, otherwise events are dropped on shutdown.
#[must_use]
pub fn init(
    debug: bool,
    sentry_dsn: Option<&str>,
    sentry_environment: Option<&str>,
    sentry_traces_sample_rate: Option<f32>,
) -> Option<sentry::ClientInitGuard> {
    let fallback = if debug { FALLBACK_DEBUG_FILTER } else { FALLBACK_INFO_FILTER };
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
                options.release = sentry::release_name!();
                if let Some(rate) = sentry_traces_sample_rate {
                    if (0.0..=1.0).contains(&rate) {
                        options = options.traces_sample_rate(rate);
                    } else {
                        eprintln!(
                            "warning: sentry.traces_sample_rate {rate} is outside 0.0..=1.0 \
                             - performance monitoring disabled"
                        );
                    }
                }
                Some(sentry::init(options))
            }
            Err(err) => {
                eprintln!("warning: invalid Sentry DSN ({err}) - Sentry reporting disabled");
                None
            }
        });

    match &guard {
        Some(_) => registry
            .with(sentry_tracing::layer().event_filter(|metadata| match *metadata.level() {
                // Pinned contract: error-grade events surface as Issues (the
                // always-delivered path), warn/info ride along as log items,
                // debug/trace never ship - stdout only. Explicit so a default
                // change upstream cannot silently leak breadcrumbs.
                tracing::Level::ERROR => EventFilter::Event | EventFilter::Log,
                tracing::Level::WARN | tracing::Level::INFO => EventFilter::Log,
                tracing::Level::DEBUG | tracing::Level::TRACE => EventFilter::Ignore,
            }))
            .init(),
        None => registry.init(),
    }

    guard
}
