use serde::Deserialize;

/// Global `[lol_store]` section: the local League client link and the store
/// watcher's cadence. Startup-only (same class as token/storage): the
/// engine and its scheduler job are built once at boot.
///
/// Absent section - or an empty `lockfile_path` - keeps the watcher
/// disabled (commands still register and explain themselves). Per guild the
/// tracker additionally stays off until `/lol_store_enable` + a channel
/// assignment.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct LolStoreConfig {
    /// Path to the League client's `lockfile` (contains the per-start port
    /// and token; re-read every poll, so client restarts self-heal). Empty
    /// = watcher disabled.
    pub lockfile_path: String,
    /// Host the client's API listens on. The port always comes from the
    /// lockfile. Trust note: the adapter accepts the client's self-signed
    /// certificate and authenticates with the lockfile token - keep this on
    /// a trusted segment only (the default loopback for a same-machine
    /// client, or the WSL/host bridge address when the bot runs in WSL).
    pub address: String,
    /// Poll cadence in seconds. The four sources are cheap local HTTP
    /// calls; rotations change daily at most.
    pub poll_secs: u64,
    /// Announce new sales.
    pub announce_sales: bool,
    /// Announce newly listed skins.
    pub announce_new_skins: bool,
    /// Announce Mythic Shop rotation changes.
    pub announce_mythic_rotation: bool,
    /// Announce Your Shop event starts.
    pub announce_yourshop: bool,
    /// Maximum store watches per user per guild (`/lol_store_watch`).
    pub watch_user_cap: u32,
    /// Maximum store watches per guild.
    pub watch_guild_cap: u32,
}

impl Default for LolStoreConfig {
    fn default() -> Self {
        Self {
            lockfile_path: String::new(),
            address: "127.0.0.1".to_owned(),
            poll_secs: 300,
            announce_sales: true,
            announce_new_skins: true,
            announce_mythic_rotation: true,
            announce_yourshop: true,
            watch_user_cap: 20,
            watch_guild_cap: 300,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_section_disables_by_default() {
        let config =
            serde_json::from_str::<LolStoreConfig>("{}").expect("empty section deserializes");
        assert_eq!(config.lockfile_path, "");
        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.poll_secs, 300);
        assert!(config.announce_sales);
        assert!(config.announce_new_skins);
        assert!(config.announce_mythic_rotation);
        assert!(config.announce_yourshop);
        assert_eq!(config.watch_user_cap, 20);
        assert_eq!(config.watch_guild_cap, 300);
        // The documented defaults must not drift from the plugin's own.
        assert_eq!(config.watch_user_cap, crate::plugins::lol_store::DEFAULT_USER_CAP);
        assert_eq!(config.watch_guild_cap, crate::plugins::lol_store::DEFAULT_GUILD_CAP);
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<LolStoreConfig>(
            r#"{
                "lockfile_path": "E:/Games/Riot Games/League of Legends/lockfile",
                "address": "192.168.1.10",
                "poll_secs": 60,
                "announce_sales": false,
                "watch_user_cap": 5,
                "watch_guild_cap": 50
            }"#,
        )
        .expect("section expected to deserialize");
        assert_eq!(config.lockfile_path, "E:/Games/Riot Games/League of Legends/lockfile");
        assert_eq!(config.address, "192.168.1.10");
        assert_eq!(config.poll_secs, 60);
        assert!(!config.announce_sales);
        assert!(config.announce_new_skins);
        assert!(config.announce_mythic_rotation);
        assert!(config.announce_yourshop);
        assert_eq!(config.watch_user_cap, 5);
        assert_eq!(config.watch_guild_cap, 50);
    }
}
