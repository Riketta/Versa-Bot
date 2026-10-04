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
    /// lockfile.
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
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<LolStoreConfig>(
            r#"{
                "lockfile_path": "E:/Games/Riot Games/League of Legends/lockfile",
                "address": "192.168.1.10",
                "poll_secs": 60,
                "announce_sales": false
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
    }
}
