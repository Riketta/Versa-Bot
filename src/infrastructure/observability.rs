use sentry_tracing::EventFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use super::config::{LoggingConfig, Rotation};

/// Fallback directives when `RUST_LOG` is unset. Target directives are
/// more specific than the global level and win for their crate.
const FALLBACK_INFO_FILTER: &str = "info";
/// The debug fallback routes the `llm_raw_traffic` target explicitly: raw
/// traffic no longer carries the crate target (dedicated emit target in the
/// llm adapter), so `versa_bot=debug` alone would not match it.
const FALLBACK_DEBUG_FILTER: &str =
    "info,versa_bot=debug,llm_raw_traffic=debug,reqwest=warn,hyper=warn,hyper_util=warn";
/// File-layer default (`[logging].level` absent): flight-recorder mode - the
/// bot's own breadcrumbs at debug regardless of the stdout level, the
/// third-party HTTP stack pinned to warn. Deliberately without
/// `llm_raw_traffic`: full conversation bodies reach files only when the
/// operator names that target explicitly.
const FILE_DEFAULT_FILTER: &str = "info,versa_bot=debug,reqwest=warn,hyper=warn,hyper_util=warn";

/// Log file name prefix - `tracing-appender` appends the rotation date, so
/// daily files are `versa-bot.log.2026-10-05` and `never` yields
/// `versa-bot.log`.
const LOG_FILE_PREFIX: &str = "versa-bot.log";

/// Guards that must outlive the whole run: dropping the Sentry client guard
/// discards queued events, dropping the file worker guard flushes pending
/// log lines. Bound at `main`'s top level.
#[must_use]
pub struct ObservabilityGuards {
    /// Sentry SDK client guard; `None` without a configured DSN.
    pub sentry: Option<sentry::ClientInitGuard>,
    /// Non-blocking file-log worker; `None` without a `[logging]` section.
    pub file: Option<WorkerGuard>,
}

/// Installs the global tracing subscriber: a stdout fmt layer, an optional
/// file layer, and an optional Sentry/GlitchTip layer.
///
/// The stdout and Sentry layers share one filter: `RUST_LOG` overrides
/// everything; when unset, `debug` picks the fallback verbosity. The debug
/// fallback keeps only the bot's own breadcrumbs at debug and pins the
/// third-party HTTP stack to warn: `reqwest`/`hyper` emit per-request
/// connect/frame lines whose span context embeds the entire client dump -
/// pure volume, no signal for operators. The file layer filters
/// independently (its default is the flight-recorder verbosity, `RUST_LOG`
/// never affects it; see the `[logging]` config docs).
///
/// The Sentry endpoint is any Sentry-protocol DSN (GlitchTip included), so
/// "address" is just the DSN host - nothing else to configure. Without a DSN
/// - or with a malformed one, which is reported and skipped instead of
/// aborting startup - the app logs to stdout only. A misconfigured file
/// layer, in contrast, aborts startup: both sinks are operator-declared,
/// but a file directory is local and verifiable, so failing fast beats
/// silently running without the requested record.
///
/// Error events carry the crate release (`versa-bot@<version>`, via
/// `sentry::release_name!`) so the backend can group by release; performance
/// transactions are only sampled when `traces_sample_rate` (0.0..=1.0) is
/// configured - out-of-range values are reported and treated as off, in the
/// same spirit as a malformed DSN. Session tracking stays off (the SDK
/// default; `GlitchTip` does not support it).
///
/// Returns the guards the caller must keep alive for the whole process
/// lifetime, otherwise queued events are dropped on shutdown.
#[must_use]
pub fn init(
    debug: bool,
    logging: Option<&LoggingConfig>,
    sentry_dsn: Option<&str>,
    sentry_environment: Option<&str>,
    sentry_traces_sample_rate: Option<f32>,
) -> ObservabilityGuards {
    let fallback = if debug { FALLBACK_DEBUG_FILTER } else { FALLBACK_INFO_FILTER };
    let stdout_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    // An explicit `[logging].level` is used verbatim; a typo must not
    // silently change what is captured, so invalid directives abort startup.
    let file_filter = match logging.and_then(|logging| logging.level.as_deref()) {
        Some(directives) => EnvFilter::try_new(directives)
            .expect("config [logging].level expected to be valid EnvFilter directives"),
        None => EnvFilter::new(FILE_DEFAULT_FILTER),
    };

    let mut file_guard = None;
    let mut file_layer = None;
    if let Some(logging) = logging {
        let rotation = match logging.rotation {
            Rotation::Daily => tracing_appender::rolling::Rotation::DAILY,
            Rotation::Never => tracing_appender::rolling::Rotation::NEVER,
        };
        let appender = tracing_appender::rolling::RollingFileAppender::builder()
            .rotation(rotation)
            .filename_prefix(LOG_FILE_PREFIX)
            .build(&logging.dir)
            .expect("config [logging].dir expected to be a creatable and writable log directory");
        let (writer, guard) = tracing_appender::non_blocking(appender);
        file_guard = Some(guard);
        file_layer = Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(file_filter),
        );
    }

    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(stdout_filter.clone()))
        .with(file_layer);

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
            .with(
                sentry_tracing::layer()
                    .event_filter(|metadata| match *metadata.level() {
                        // Pinned contract: error-grade events surface as Issues (the
                        // always-delivered path), warn/info ride along as log items,
                        // debug/trace never ship - stdout only. Explicit so a default
                        // change upstream cannot silently leak breadcrumbs.
                        tracing::Level::ERROR => EventFilter::Event | EventFilter::Log,
                        tracing::Level::WARN | tracing::Level::INFO => EventFilter::Log,
                        tracing::Level::DEBUG | tracing::Level::TRACE => EventFilter::Ignore,
                    })
                    .with_filter(stdout_filter),
            )
            .init(),
        None => registry.init(),
    }

    ObservabilityGuards { sentry: guard, file: file_guard }
}
