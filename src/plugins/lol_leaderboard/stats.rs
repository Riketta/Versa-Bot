//! Pure aggregation of a [`Snapshot`] into display statistics. Provider-
//! blind (only neutral DTOs enter) and hole-tolerant: a player without a
//! role leaves the role tables, a player without champion data leaves the
//! champion tables - percentages are always computed over what is actually
//! present, and the coverage line in the formatted output tells the reader
//! exactly how much that is.
//!
//! Semantics of the pooled "average" section: each region contributes its
//! own top-bucket slice, and shares are the weighted totals across those
//! slices (summed counts over summed players - never an average of
//! percentages). A bucket row renders only when the parses actually cover
//! it, per region and - strictly - for every region of the pooled row.

use std::collections::HashMap;

use super::engine::Snapshot;
use super::port::{LeaderboardPlayer, RegionLeaderboard, Role};

/// One role-distribution row (a "TOP N" line).
#[derive(Debug, Clone, PartialEq)]
pub struct RoleRow {
    pub bucket: u32,
    /// Share in percent per role, in display order; sums to ~100 over all
    /// roles with at least one known player.
    pub shares: Vec<(Role, f64)>,
}

/// Role distribution for one scope: the pooled average or a single region.
#[derive(Debug, Clone, PartialEq)]
pub struct RoleSection {
    /// `"average"` or a region key.
    pub scope: String,
    pub rows: Vec<RoleRow>,
}

/// One champion inside a role block.
#[derive(Debug, Clone, PartialEq)]
pub struct ChampEntry {
    pub name: String,
    /// Share of the role's players (with known champion data) in percent.
    pub percent: f64,
}

/// Most-picked champions for one role over the pooled champion pool.
#[derive(Debug, Clone, PartialEq)]
pub struct ChampBlock {
    pub role: Role,
    /// Denominator: the role's players with known champion data.
    pub players: usize,
    pub entries: Vec<ChampEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeaderboardStats {
    /// Players actually parsed across served regions.
    pub parsed: usize,
    /// Players a full parse would cover (configured regions x depth).
    pub requested: usize,
    pub region_count: usize,
    pub failures: Vec<String>,
    pub stale: bool,
    pub age_seconds: u64,
    pub role_sections: Vec<RoleSection>,
    pub champ_blocks: Vec<ChampBlock>,
}

/// Share in percent, guarded against the empty denominator.
fn percent(count: usize, total: usize) -> f64 {
    if total == 0 { 0.0 } else { count as f64 / total as f64 * 100.0 }
}

/// Role distribution over a slice: denominator is the players with a known
/// role.
fn role_shares(players: &[LeaderboardPlayer]) -> Vec<(Role, f64)> {
    let known: usize = players.iter().filter(|p| p.role.is_some()).count();
    Role::ALL
        .map(|role| {
            let count = players.iter().filter(|p| p.role == Some(role)).count();
            (role, percent(count, known))
        })
        .to_vec()
}

fn champion_placeholder(id: &str) -> String {
    format!("Champion #{id}")
}

/// Aggregates the snapshot. Never errors: partial data degrades section by
/// section, and an empty champ pool simply omits the champion tables.
#[must_use]
pub fn build(snapshot: &Snapshot) -> LeaderboardStats {
    let buckets = &snapshot.view.buckets;
    let pool = snapshot.view.champ_pool as usize;

    let mut role_sections: Vec<RoleSection> = Vec::new();

    // Pooled average first (matches the demo layout). Strict coverage:
    // every served region must cover the bucket, or the pooled row is
    // skipped - a silently biased average would be worse than a gap.
    if snapshot.regions.len() > 1 {
        let mut rows = Vec::new();
        for &bucket in buckets {
            let slice = bucket as usize;
            if snapshot.regions.iter().all(|region| region.players.len() >= slice) {
                let pooled: Vec<LeaderboardPlayer> = snapshot
                    .regions
                    .iter()
                    .flat_map(|region| {
                        region
                            .players
                            .get(..slice)
                            .expect("bucket covered by the coverage rule")
                            .iter()
                            .cloned()
                    })
                    .collect();
                rows.push(RoleRow { bucket, shares: role_shares(&pooled) });
            }
        }
        if !rows.is_empty() {
            role_sections.push(RoleSection { scope: "average".to_owned(), rows });
        }
    }

    // Per-region sections: a row renders only when that region's parse
    // covers it.
    for region in &snapshot.regions {
        let mut rows = Vec::new();
        for &bucket in buckets {
            let slice_len = bucket as usize;
            if region.players.len() >= slice_len {
                let slice =
                    region.players.get(..slice_len).expect("bucket covered by the coverage rule");
                rows.push(RoleRow { bucket, shares: role_shares(slice) });
            }
        }
        if !rows.is_empty() {
            role_sections.push(RoleSection { scope: region.region.clone(), rows });
        }
    }

    // Champion tables: pooled over every served region's top-pool slice
    // (the pool is a cap, not a promise - the coverage line carries the
    // truth about what was parsed).
    let champ_slice: Vec<&LeaderboardPlayer> = snapshot
        .regions
        .iter()
        .flat_map(|region: &RegionLeaderboard| region.players.iter().take(pool))
        .collect();
    let mut champ_blocks: Vec<ChampBlock> = Vec::new();
    if !champ_slice.is_empty() {
        for role in Role::ALL {
            let mut counts: HashMap<String, usize> = HashMap::new();
            let mut total = 0usize;
            for player in &champ_slice {
                if player.role != Some(role) {
                    continue;
                }
                if let Some(id) = &player.champ_id {
                    total += 1;
                    let name = snapshot
                        .champ_names
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| champion_placeholder(id));
                    *counts.entry(name).or_default() += 1;
                }
            }
            if total == 0 {
                continue;
            }
            let mut entries: Vec<ChampEntry> = counts
                .into_iter()
                .map(|(name, count)| ChampEntry { name, percent: percent(count, total) })
                .collect();
            // Deterministic order: share descending, name ascending on ties.
            entries
                .sort_by(|a, b| b.percent.total_cmp(&a.percent).then_with(|| a.name.cmp(&b.name)));
            entries.truncate(snapshot.view.champs_per_role as usize);
            champ_blocks.push(ChampBlock { role, players: total, entries });
        }
    }

    LeaderboardStats {
        parsed: snapshot.regions.iter().map(|region| region.players.len()).sum(),
        requested: snapshot.requested_players,
        region_count: snapshot.regions.len(),
        failures: snapshot.failures.clone(),
        stale: snapshot.stale,
        age_seconds: snapshot.age.as_secs(),
        role_sections,
        champ_blocks,
    }
}

#[cfg(test)]
mod tests {
    use super::super::engine::ResolvedView;
    use super::*;
    use std::time::Duration;

    fn view() -> ResolvedView {
        ResolvedView { buckets: vec![300, 1000], champ_pool: 1000, champs_per_role: 2 }
    }

    fn player(position: u32, role: Option<Role>, champ: Option<&str>) -> LeaderboardPlayer {
        LeaderboardPlayer { position, role, champ_id: champ.map(str::to_owned) }
    }

    fn region(key: &str, players: Vec<LeaderboardPlayer>) -> RegionLeaderboard {
        RegionLeaderboard { region: key.to_owned(), players }
    }

    /// A board of `len` players: roles cycle Top, Jungle, Mid, Bot,
    /// Support; champions cycle "1" (Annie), "2" (Olaf, from the fake's
    /// names map - unknown ids stay as they are).
    fn board(key: &str, len: u32) -> RegionLeaderboard {
        let players = (1..=len)
            .map(|position| {
                let role = Role::ALL
                    .iter()
                    .cycle()
                    .nth(position as usize - 1)
                    .copied()
                    .expect("position > 0");
                let champ = if position % 3 == 0 { None } else { Some("1") };
                player(position, Some(role), champ)
            })
            .collect();
        region(key, players)
    }

    fn names() -> HashMap<String, String> {
        HashMap::from([("1".to_owned(), "Annie".to_owned())])
    }

    fn snapshot(regions: Vec<RegionLeaderboard>) -> Snapshot {
        Snapshot {
            requested_players: 2000,
            regions,
            champ_names: names(),
            failures: Vec::new(),
            stale: false,
            age: Duration::from_secs(3600),
            view: view(),
        }
    }

    #[test]
    fn empty_denominators_stay_zero_not_nan() {
        let shares = role_shares(&[player(1, None, Some("1"))]);
        assert!(shares.iter().all(|(_, share)| *share == 0.0));
    }

    #[test]
    fn single_region_gets_only_its_own_section() {
        let stats = build(&snapshot(vec![board("kr", 1000)]));
        assert_eq!(stats.parsed, 1000);
        assert_eq!(stats.region_count, 1);
        assert_eq!(stats.role_sections.len(), 1);
        let section = stats.role_sections.first().expect("kr section");
        assert_eq!(section.scope, "kr");
        // Both buckets covered: rows TOP 300 and TOP 1000.
        assert_eq!(section.rows.iter().map(|r| r.bucket).collect::<Vec<_>>(), vec![300, 1000]);
        // Every fifth player is Top; 20% of the board, both rows equal.
        let row = section.rows.first().expect("two rows");
        let top = row.shares.first().expect("five roles");
        assert_eq!(top.0, Role::Top);
        assert!((top.1 - 20.0).abs() < 1e-9);
    }

    #[test]
    fn pooled_average_needs_every_region_to_cover_the_bucket() {
        // kr: 200 players, euw: 100 - with buckets [300, 1000] nothing is
        // covered: kr lacks both buckets, euw lacks both, and the pooled
        // average requires every region. No sections render at all.
        let kr = region("kr", (1..=200).map(|p| player(p, Some(Role::Top), None)).collect());
        let euw = region("euw", (1..=100).map(|p| player(p, Some(Role::Top), None)).collect());
        let stats = build(&snapshot(vec![kr, euw]));
        assert!(stats.role_sections.is_empty());

        // Shrink the bucket to 100 and the sections appear: kr and euw
        // cover it individually, and the pooled average pools the two
        // top-100 slices (equal sizes under the strict rule). kr's
        // top-100 is 60 Top + 40 Jungle.
        let kr = region(
            "kr",
            (1..=60)
                .map(|p| player(p, Some(Role::Top), None))
                .chain((61..=200).map(|p| player(p, Some(Role::Jungle), None)))
                .collect(),
        );
        let euw = region(
            "euw",
            (1..=30)
                .map(|p| player(p, Some(Role::Top), None))
                .chain((31..=100).map(|p| player(p, Some(Role::Jungle), None)))
                .collect(),
        );
        let mut snap = snapshot(vec![kr, euw]);
        snap.view.buckets = vec![100];
        let stats = build(&snap);
        assert_eq!(stats.role_sections.len(), 3); // average + kr + euw
        let average = stats.role_sections.first().expect("average section");
        assert_eq!(average.scope, "average");
        // Pooled slice: kr's 60 Top + 40 Jungle, euw's 30 Top + 70 Jungle
        // -> 90/200 and 110/200.
        let average_row = average.rows.first().expect("one row");
        let top = average_row.shares.first().expect("five roles");
        let jungle = average_row.shares.get(1).expect("five roles");
        assert!((top.1 - 45.0).abs() < 1e-9, "top share: {}", top.1);
        assert!((jungle.1 - 55.0).abs() < 1e-9, "jungle share: {}", jungle.1);
        // Individual regions show their own top-100 distribution.
        let kr_section = stats.role_sections.get(1).expect("kr section");
        let kr_row = kr_section.rows.first().expect("one row");
        assert!((kr_row.shares.first().expect("five roles").1 - 60.0).abs() < 1e-9);
        let euw_section = stats.role_sections.get(2).expect("euw section");
        let euw_row = euw_section.rows.first().expect("one row");
        assert!((euw_row.shares.first().expect("five roles").1 - 30.0).abs() < 1e-9);
    }

    #[test]
    fn bucket_rows_skip_uncovered_regions_but_keep_pools() {
        let snap = snapshot(vec![board("kr", 450), board("euw", 1000)]);
        let stats = build(&snap);
        let kr = stats.role_sections.iter().find(|s| s.scope == "kr").expect("kr section");
        // 450 parses cover TOP 300 but not TOP 1000.
        assert_eq!(kr.rows.iter().map(|r| r.bucket).collect::<Vec<_>>(), vec![300]);
        let euw = stats.role_sections.iter().find(|s| s.scope == "euw").expect("euw section");
        assert_eq!(euw.rows.iter().map(|r| r.bucket).collect::<Vec<_>>(), vec![300, 1000]);
    }

    #[test]
    fn champ_tables_count_only_players_with_role_and_champ() {
        let players = vec![
            player(1, Some(Role::Top), Some("1")),
            player(2, Some(Role::Top), Some("1")),
            player(3, Some(Role::Top), Some("99")), // unknown id -> placeholder
            player(4, Some(Role::Top), None),       // no champ data -> excluded
            player(5, None, Some("1")),             // no role -> excluded everywhere
        ];
        let stats = build(&snapshot(vec![region("kr", players)]));
        assert_eq!(stats.champ_blocks.len(), 1);
        let block = stats.champ_blocks.first().expect("one block");
        assert_eq!(block.role, Role::Top);
        assert_eq!(block.players, 3);
        assert_eq!(block.entries.len(), 2);
        assert!((block.entries.first().expect("2 entries").percent - 200.0 / 3.0).abs() < 1e-9);
        assert_eq!(block.entries.get(1).expect("2 entries").name, "Champion #99");
    }

    #[test]
    fn champ_pool_caps_per_region_slice_and_entries_cap_per_role() {
        let mut snap = snapshot(vec![board("kr", 1000)]);
        snap.view.champ_pool = 300;
        snap.view.champs_per_role = 1;
        let stats = build(&snap);
        // Board cycles 5 roles; a 300-player slice has 60 per role, of
        // which every third player (position % 3) carries no champion
        // data -> 40 count in the denominator. One champion per role.
        assert_eq!(stats.champ_blocks.len(), 5);
        for block in &stats.champ_blocks {
            assert_eq!(block.players, 40);
            assert_eq!(block.entries.len(), 1);
            assert!((block.entries.first().expect("1 entry").percent - 100.0).abs() < 1e-9);
        }
    }

    #[test]
    fn champ_section_omitted_when_no_champ_data_at_all() {
        let players: Vec<LeaderboardPlayer> = (1..=10)
            .map(|p| {
                let role = Role::ALL.iter().cycle().nth(p as usize - 1).copied().expect("p > 0");
                player(p, Some(role), None)
            })
            .collect();
        let stats = build(&snapshot(vec![region("kr", players)]));
        assert!(stats.champ_blocks.is_empty());
    }

    #[test]
    fn coverage_and_staleness_pass_through() {
        let mut snap = snapshot(vec![board("kr", 1000)]);
        snap.failures = vec!["na".to_owned()];
        snap.stale = true;
        snap.requested_players = 3000;
        let stats = build(&snap);
        assert_eq!(stats.parsed, 1000);
        assert_eq!(stats.requested, 3000);
        assert_eq!(stats.failures, vec!["na"]);
        assert!(stats.stale);
        assert_eq!(stats.age_seconds, 3600);
    }
}
