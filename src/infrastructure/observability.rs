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
    let file_filter = EnvFilter::new(
        file_filter_directives(logging.and_then(|logging| logging.level.as_deref()))
            .expect("config [logging].level expected to be valid EnvFilter directives"),
    );

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
    let guard = effective_dsn(sentry_dsn).and_then(|dsn| match dsn.parse() {
        Ok(dsn) => {
            // `ClientOptions` is `#[non_exhaustive]` - construct via defaults
            // and field assignment, not a struct literal.
            let mut options = sentry::ClientOptions::default();
            options.dsn = Some(dsn);
            // Cow<'static, str> - the borrowed &str outlives nothing here,
            // so take ownership first.
            options.environment = sentry_environment.map(String::from).map(Into::into);
            options.release = sentry::release_name!();
            match (usable_sample_rate(sentry_traces_sample_rate), sentry_traces_sample_rate) {
                (Some(rate), _) => options = options.traces_sample_rate(rate),
                // Reported, not fatal: an out-of-range rate turns performance
                // monitoring off while error reporting itself stays on.
                (None, Some(configured)) => eprintln!(
                    "warning: sentry.traces_sample_rate {configured} is outside 0.0..=1.0 \
                     - performance monitoring disabled"
                ),
                (None, None) => {}
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

/// The file layer's filter directives: an explicit `[logging].level` is
/// used verbatim after validation, its absence picks the flight-recorder
/// default. The `Err` case is the deliberate fail-fast for invalid
/// directives - `init` turns it into the aborting `expect`, since a typo
/// must not silently change what is captured.
fn file_filter_directives(level: Option<&str>) -> Result<String, String> {
    match level {
        Some(directives) => EnvFilter::try_new(directives)
            .map(|_| directives.to_owned())
            .map_err(|err| err.to_string()),
        None => Ok(FILE_DEFAULT_FILTER.to_owned()),
    }
}

/// The DSN Sentry would parse: trimmed, with an empty or whitespace-only
/// value treated as absent - reporting disabled without a warning, since a
/// blank DSN is a configuration, not a misconfiguration. A non-empty value
/// that still fails the DSN parse is `init`'s warn-and-continue case.
fn effective_dsn(dsn: Option<&str>) -> Option<&str> {
    dsn.map(str::trim).filter(|dsn| !dsn.is_empty())
}

/// The sample rate `init` may hand to the Sentry client: an unset rate
/// stays `None` (SDK default), an in-range rate passes through, and an
/// out-of-range one degrades to `None` - performance monitoring off while
/// error reporting is unaffected. `init` emits the warning for the
/// degraded case, and only when a DSN is actually wired.
fn usable_sample_rate(rate: Option<f32>) -> Option<f32> {
    rate.filter(|rate| (0.0..=1.0).contains(rate))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A DSN that passes `sentry`'s parser - the valid shape of the Sentry
    /// decisions.
    const VALID_DSN: &str = "https://examplePublicKey@o0.ingest.sentry.io/0";

    #[test]
    fn explicit_file_level_is_used_verbatim() {
        assert_eq!(
            file_filter_directives(Some("info,versa_bot=trace")).as_deref(),
            Ok("info,versa_bot=trace")
        );
    }

    /// The deliberate fail-fast: invalid directives are an `Err` that
    /// `init` turns into the aborting `expect` - a typo must not silently
    /// change what is captured.
    #[test]
    fn invalid_file_level_is_the_fail_fast_error() {
        assert!(file_filter_directives(Some("versa_bot=not_a_level")).is_err());
    }

    /// Privacy contract: the file default is the flight-recorder filter
    /// WITHOUT the raw-traffic target - full conversation bodies reach
    /// files only when the operator names `llm_raw_traffic` explicitly in
    /// `[logging].level`.
    #[test]
    fn file_default_filter_excludes_the_raw_traffic_target() {
        let default = file_filter_directives(None).expect("default expected to be valid");
        assert_eq!(default, FILE_DEFAULT_FILTER);
        assert!(default.contains("versa_bot=debug"), "flight-recorder mode expected");
        assert!(!default.contains("llm_raw_traffic"));
    }

    #[test]
    fn absent_or_blank_dsn_disables_silently() {
        assert_eq!(effective_dsn(None), None);
        assert_eq!(effective_dsn(Some("")), None);
        assert_eq!(effective_dsn(Some("   ")), None);
    }

    #[test]
    fn dsn_is_trimmed_not_rejected() {
        let padded = format!("  {VALID_DSN}  ");
        assert_eq!(effective_dsn(Some(&padded)), Some(VALID_DSN));
    }

    /// The malformed-DSN branch: `init` warns on this parse `Err` and
    /// continues stdout-only - pinned on the pure decision that guards the
    /// enable path.
    #[test]
    fn malformed_dsn_fails_the_parse_guarding_the_enable_path() {
        assert!("wat".parse::<sentry::types::Dsn>().is_err());
        assert!(VALID_DSN.parse::<sentry::types::Dsn>().is_ok());
    }

    #[test]
    fn sample_rate_passes_only_in_range_values() {
        assert_eq!(usable_sample_rate(None), None);
        assert_eq!(usable_sample_rate(Some(0.0)), Some(0.0));
        assert_eq!(usable_sample_rate(Some(0.5)), Some(0.5));
        assert_eq!(usable_sample_rate(Some(1.0)), Some(1.0));
        // Out of range is off: reported and degraded by `init`.
        assert_eq!(usable_sample_rate(Some(-0.1)), None);
        assert_eq!(usable_sample_rate(Some(1.5)), None);
        assert_eq!(usable_sample_rate(Some(f32::NAN)), None);
    }
}
