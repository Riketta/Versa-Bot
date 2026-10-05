use serde::Deserialize;

/// File-log rotation policy (`rotation` in the `[logging]` section).
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Rotation {
    /// One file per day (`versa-bot.log.2026-10-05`).
    #[default]
    Daily,
    /// A single append-only file (`versa-bot.log`) - pair with an external
    /// rotator (logrotate) if retention matters.
    Never,
}

/// Optional persistent file logging (`[logging]` section). Startup-only:
/// the tracing subscriber is installed once at boot, so changes require a
/// restart. An absent section keeps stdout (and Sentry) only.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct LoggingConfig {
    /// Directory for the log files. Created at boot; an uncreatable or
    /// unwritable directory aborts startup - an operator-configured sink
    /// must not silently degrade to stdout-only.
    pub dir: String,
    /// Rotation policy. Default: daily files.
    #[serde(default)]
    pub rotation: Rotation,
    /// Independent filter directives for the file layer (EnvFilter syntax,
    /// used verbatim; invalid directives abort startup). `RUST_LOG` never
    /// affects the file. Default: flight-recorder mode - the bot's own
    /// breadcrumbs at debug regardless of the stdout level, the third-party
    /// HTTP stack pinned to warn. Full conversation dumps
    /// (`llm_raw_traffic`) are excluded unless the target is named here.
    #[serde(default)]
    pub level: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dir_only_section_deserializes() {
        let config = serde_json::from_str::<LoggingConfig>(r#"{ "dir": "logs" }"#)
            .expect("dir-only section deserializes");
        assert_eq!(config.dir, "logs");
        assert_eq!(config.rotation, Rotation::Daily);
        assert_eq!(config.level, None);
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<LoggingConfig>(
            r#"{
                "dir": "logs",
                "rotation": "never",
                "level": "info,versa_bot=debug"
            }"#,
        )
        .expect("full section deserializes");
        assert_eq!(config.dir, "logs");
        assert_eq!(config.rotation, Rotation::Never);
        assert_eq!(config.level.as_deref(), Some("info,versa_bot=debug"));
    }

    #[test]
    fn unknown_rotation_is_rejected() {
        assert!(
            serde_json::from_str::<LoggingConfig>(r#"{ "dir": "logs", "rotation": "weekly" }"#)
                .is_err()
        );
    }
}
