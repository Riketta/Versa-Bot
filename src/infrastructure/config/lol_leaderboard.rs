use serde::Deserialize;

/// Global `[lol_leaderboard]` section: the on-demand `/lol_leaderboard`
/// command's data source (aggregated ranked-leaderboard statistics).
/// Startup-only (same class as token/storage): the HTTP client and the
/// engine are built once at boot; changes require a restart.
///
/// An absent section - or an empty `regions` list - keeps the command in
/// "not configured" mode (it still registers and explains itself). There is
/// no per-guild setup: the command answers wherever it is invoked.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct LolLeaderboardConfig {
    /// Regions (servers) to aggregate: lowercase, without the trailing
    /// digit (`kr`, `euw`, `eun`, `na`, `jp`, `br`, `tr`, `tw`, `vn`,
    /// `sea`). Empty = plugin disabled.
    pub regions: Vec<String>,
    /// Minimum delay in seconds between two consecutive source requests.
    /// Requests are strictly sequential; this is rate-limit politeness.
    pub request_interval_secs: u64,
    /// Keep the cache warm with a background scheduler job (ticking at
    /// `cache_ttl_secs`, first run at boot): the command then serves
    /// whatever is cached, however old (the dump carries the data age),
    /// and never waits for a parse. `false` = on-demand mode: a stale
    /// cache re-parses before the reply. Startup-only.
    pub background_refresh: bool,
    /// Wall-clock budget in seconds for a command-driven (inline)
    /// refresh: regions past the budget keep serving their cached data
    /// (or are named as failed when they have none) instead of parking
    /// the invoker behind a slow source. `0` = unbounded. The background
    /// job ignores it. Startup-only.
    pub inline_refresh_budget_secs: u64,
    /// How many top-ranked players to parse per region (rounded up to
    /// whole source pages; the source may return fewer). Must be > 0
    /// (0 disables the plugin); values beyond 10 000 are capped at startup
    /// with a warning.
    pub parse_depth: u32,
    /// Player-count rows for the role-distribution tables (e.g. a
    /// "TOP 300" row). Values beyond `parse_depth` are clamped to it.
    pub display_buckets: Vec<u32>,
    /// Player pool (per region, highest ranked first) behind the
    /// champion-per-role tables. Clamped to `parse_depth`.
    pub champ_pool_depth: u32,
    /// Champions listed per role in the champion-per-role tables.
    pub champs_per_role: u32,
    /// How long cached leaderboard data stays fresh, in seconds. On demand
    /// only: a fresh cache answers instantly, a stale one triggers a
    /// re-parse before the reply.
    pub cache_ttl_secs: u64,
    /// Optional proxy for source requests (`socks5://` or `http://`); the
    /// Discord proxy does not apply here.
    pub proxy: Option<String>,
}

impl Default for LolLeaderboardConfig {
    fn default() -> Self {
        Self {
            regions: Vec::new(),
            request_interval_secs: 3,
            background_refresh: true,
            inline_refresh_budget_secs: 180,
            parse_depth: 1000,
            display_buckets: vec![300, 1000],
            champ_pool_depth: 1000,
            champs_per_role: 5,
            cache_ttl_secs: 18 * 3600,
            proxy: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_section_takes_defaults() {
        let config = serde_json::from_str::<LolLeaderboardConfig>("{}").expect("parses");
        assert!(config.regions.is_empty());
        assert_eq!(config.request_interval_secs, 3);
        assert!(config.background_refresh);
        assert_eq!(config.inline_refresh_budget_secs, 180);
        assert_eq!(config.parse_depth, 1000);
        assert_eq!(config.display_buckets, vec![300, 1000]);
        assert_eq!(config.champ_pool_depth, 1000);
        assert_eq!(config.champs_per_role, 5);
        assert_eq!(config.cache_ttl_secs, 18 * 3600);
        assert_eq!(config.proxy, None);
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<LolLeaderboardConfig>(
            r#"{
                "regions": ["kr", "euw"],
                "request_interval_secs": 2,
                "background_refresh": false,
                "inline_refresh_budget_secs": 60,
                "parse_depth": 500,
                "display_buckets": [100, 500],
                "champ_pool_depth": 300,
                "champs_per_role": 3,
                "cache_ttl_secs": 3600,
                "proxy": "socks5://127.0.0.1:1080"
            }"#,
        )
        .expect("parses");
        assert_eq!(config.regions, vec!["kr", "euw"]);
        assert_eq!(config.request_interval_secs, 2);
        assert!(!config.background_refresh);
        assert_eq!(config.inline_refresh_budget_secs, 60);
        assert_eq!(config.parse_depth, 500);
        assert_eq!(config.display_buckets, vec![100, 500]);
        assert_eq!(config.champ_pool_depth, 300);
        assert_eq!(config.champs_per_role, 3);
        assert_eq!(config.cache_ttl_secs, 3600);
        assert_eq!(config.proxy.as_deref(), Some("socks5://127.0.0.1:1080"));
    }
}
