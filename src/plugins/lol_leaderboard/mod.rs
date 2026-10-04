//! `LoL` leaderboard tracker: aggregated ranked-leaderboard statistics
//! (role distributions, most picked champions per role) from a pluggable
//! data source, dumped on demand by `/lol_leaderboard`.
//!
//! A `PluginPort`-only plugin (no middleware hook, no scheduler job, no
//! events): its driver is the command, and its only state is a
//! process-lifetime cache of world data - there is deliberately no guild
//! storage and no per-guild setup. Own hexagon inside: the engine depends
//! on the [`port::LeaderboardSourcePort`] boundary,
//! [`DeepLolSource`] is the one adapter.
//!
//! Disabled by default: without a `[lol_leaderboard]` config section (or
//! with no usable regions) the command still registers and explains
//! itself. Everything is startup-only; changes require a restart.

mod commands;
mod deeplol;
mod engine;
mod format;
mod port;
mod stats;

use std::sync::Arc;

use crate::kernel::{
    models::PluginError,
    plugin_ports::{AccessTier, CommandDescriptor, CommandRegistryPort, PluginPort},
};

pub use commands::LeaderboardHandler;
pub use deeplol::DeepLolSource;
pub use engine::{EngineSettings, LeaderboardEngine, ResolvedView, Snapshot};
pub use port::{LeaderboardPlayer, LeaderboardSourcePort, RegionLeaderboard, Role, SourceError};

/// The plugin facade: command registration (`init`); no lifecycle state.
pub struct LeaderboardPlugin {
    registry: Arc<dyn CommandRegistryPort>,
    engine: Arc<LeaderboardEngine>,
}

impl LeaderboardPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>, engine: Arc<LeaderboardEngine>) -> Self {
        Self { registry, engine }
    }
}

impl PluginPort for LeaderboardPlugin {
    fn name(&self) -> &'static str {
        "lol_leaderboard"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_leaderboard".to_owned(),
                description:
                    "LoL ranked leaderboard stats: role distribution and most picked champions"
                        .to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: false,
            },
            Arc::new(LeaderboardHandler::new(Arc::clone(&self.engine))),
        );
        Ok(())
    }

    fn start(&self) -> Result<(), PluginError> {
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::engine::test_support::FakeSource;
    use super::*;
    use crate::kernel::plugin_ports::CommandRegistryPort;
    use crate::test_support::{CapturingCommandRegistry, assert_descriptions_fit_discord};
    use std::time::Duration;

    fn engine(regions: &[&str]) -> Arc<LeaderboardEngine> {
        let source = FakeSource::ungated();
        let keys: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            Duration::from_secs(3600),
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::new(source, settings))
    }

    #[test]
    fn registers_one_command_within_discord_limits() {
        let registry = Arc::new(CapturingCommandRegistry::default());
        let plugin = LeaderboardPlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            engine(&["kr"]),
        );
        plugin.init().expect("init");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        let descriptor = descriptors.first().expect("one descriptor");
        assert_eq!(descriptor.name, "lol_leaderboard");
        assert_eq!(descriptor.plugin_id, "lol_leaderboard");
        assert_eq!(descriptor.required_tier, Some(AccessTier::User));
        assert!(!descriptor.guild_only);
        assert!(descriptor.arguments.is_empty());
        assert!(registry.handler("lol_leaderboard").is_some());
        assert_descriptions_fit_discord(&descriptors);
    }

    #[test]
    fn engine_without_usable_regions_is_unconfigured() {
        assert!(!engine(&["mars"]).is_configured());
        assert!(engine(&["kr"]).is_configured());
    }
}
