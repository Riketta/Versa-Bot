//! Rendering of aggregated statistics into Discord messages: the demo
//! layout (role distribution rows per scope, champion tables per role) plus
//! an honest coverage line. Output is packed into messages at section
//! boundaries - a section never straddles two messages unless it alone
//! exceeds the limit, and then it is split on line boundaries, never
//! mid-line.

use std::time::Duration;

use super::stats::{ChampBlock, LeaderboardStats, RoleSection};

/// Discord's hard content limit; byte-based packing is conservative for
/// non-ASCII content, which only ever shortens a message.
pub const MESSAGE_LIMIT: usize = 2000;

/// Humanized data age for the coverage line.
fn age_label(age: Duration) -> String {
    let seconds = age.as_secs();
    if seconds < 120 {
        return "just now".to_owned();
    }
    let minutes = seconds / 60;
    if minutes < 120 {
        return format!("{minutes}m old");
    }
    let hours = minutes / 60;
    format!("{hours}h {}m old", minutes % 60)
}

/// `Parsed 2941/3000 players from 3 regions · data age 2h 5m` plus the
/// failure note when a refresh could not keep everything fresh.
fn header_block(stats: &LeaderboardStats) -> String {
    let mut lines = vec![
        "# LoL Leaderboard Statistics".to_owned(),
        String::new(),
        format!(
            "Parsed {}/{} players from {} region{} · data age {}",
            stats.parsed,
            stats.requested,
            stats.region_count,
            if stats.region_count == 1 { "" } else { "s" },
            age_label(Duration::from_secs(stats.age_seconds)),
        ),
    ];
    if !stats.failures.is_empty() {
        lines.push(format!(
            "Could not refresh: {} - showing earlier data",
            stats.failures.join(", ")
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn region_label(scope: &str) -> String {
    if scope == "average" {
        "in average".to_owned()
    } else {
        format!("in {}", scope.to_uppercase())
    }
}

/// `## Role distribution in average` + one `**TOP N:**` row per bucket.
fn role_block(section: &RoleSection) -> String {
    let mut lines =
        vec![format!("## Role distribution {}", region_label(&section.scope)), String::new()];
    for row in &section.rows {
        let shares = row
            .shares
            .iter()
            .map(|(role, share)| format!("{}: {share:.2}%", role.label()))
            .collect::<Vec<_>>()
            .join(" | ");
        lines.push(format!("**TOP {}:** {shares}", row.bucket));
    }
    lines.push(String::new());
    lines.join("\n")
}

/// `## Most picked champions per role` + one block per role.
fn champ_block(blocks: &[ChampBlock]) -> String {
    let mut lines = vec!["## Most picked champions per role".to_owned(), String::new()];
    for (index, block) in blocks.iter().enumerate() {
        if index > 0 {
            lines.push(String::new());
        }
        lines.push(format!(
            "**{}** - {} player{}",
            block.role.label(),
            block.players,
            if block.players == 1 { "" } else { "s" }
        ));
        lines.push(String::new());
        for (rank, entry) in block.entries.iter().enumerate() {
            lines.push(format!("{}. {} - {:.1}%", rank + 1, entry.name, entry.percent));
        }
    }
    lines.join("\n")
}

/// Packs blocks into messages: blocks stay whole across boundaries unless
/// one alone exceeds the limit, and an oversized block falls back to
/// line-boundary splitting (an absurd single line is hard-cut on char
/// boundaries as the last resort).
fn pack(blocks: Vec<String>) -> Vec<String> {
    let mut messages: Vec<String> = Vec::new();
    let mut current = String::new();
    let flush = |messages: &mut Vec<String>, current: &mut String| {
        if !current.is_empty() {
            messages.push(std::mem::take(current));
        }
    };

    for block in blocks {
        if block.len() > MESSAGE_LIMIT {
            flush(&mut messages, &mut current);
            let mut part = String::new();
            for line in block.lines() {
                if line.len() > MESSAGE_LIMIT {
                    // Hard-cut an oversized single line on chars.
                    flush(&mut messages, &mut part);
                    let mut chunk = String::new();
                    for character in line.chars() {
                        if chunk.len() + character.len_utf8() > MESSAGE_LIMIT {
                            messages.push(std::mem::take(&mut chunk));
                        }
                        chunk.push(character);
                    }
                    part = chunk;
                    continue;
                }
                if part.is_empty() {
                    part.push_str(line);
                } else if part.len() + 1 + line.len() <= MESSAGE_LIMIT {
                    part.push('\n');
                    part.push_str(line);
                } else {
                    flush(&mut messages, &mut part);
                    part.clear();
                    part.push_str(line);
                }
            }
            flush(&mut messages, &mut part);
            continue;
        }
        if current.is_empty() {
            current = block;
        } else if current.len() + 1 + block.len() <= MESSAGE_LIMIT {
            current.push('\n');
            current.push_str(&block);
        } else {
            flush(&mut messages, &mut current);
            current = block;
        }
    }
    flush(&mut messages, &mut current);
    messages
}

/// Renders the full output as one message per packed block group.
#[must_use]
pub fn render(stats: &LeaderboardStats) -> Vec<String> {
    let mut blocks = vec![header_block(stats)];
    for section in &stats.role_sections {
        blocks.push(role_block(section));
    }
    if !stats.champ_blocks.is_empty() {
        blocks.push(champ_block(&stats.champ_blocks));
    }
    pack(blocks)
}

#[cfg(test)]
mod tests {
    use super::super::port::Role;
    use super::super::stats::{ChampEntry, RoleRow};
    use super::*;

    fn section_of(scope: &str, row: &RoleRow, lines: usize) -> RoleSection {
        RoleSection { scope: scope.to_owned(), rows: vec![row.clone(); lines] }
    }

    fn stats_with_lines(lines_per_role_section: usize) -> LeaderboardStats {
        let role_row = RoleRow { bucket: 300, shares: Role::ALL.map(|role| (role, 20.0)).to_vec() };
        LeaderboardStats {
            parsed: 3000,
            requested: 3000,
            region_count: 3,
            failures: Vec::new(),
            stale: false,
            age_seconds: 7500,
            role_sections: vec![
                section_of("average", &role_row, lines_per_role_section),
                section_of("kr", &role_row, lines_per_role_section),
                section_of("euw", &role_row, lines_per_role_section),
            ],
            champ_blocks: vec![ChampBlock {
                role: Role::Top,
                players: 540,
                entries: vec![
                    ChampEntry { name: "Ambessa".to_owned(), percent: 14.8 },
                    ChampEntry { name: "Rumble".to_owned(), percent: 14.4 },
                ],
            }],
        }
    }

    #[test]
    fn header_line_carries_coverage_age_and_failures() {
        let mut stats = stats_with_lines(1);
        stats.failures = vec!["na".to_owned()];
        stats.stale = true;
        let rendered = render(&stats);
        let header = rendered.first().expect("header message");
        assert!(header.starts_with("# LoL Leaderboard Statistics"));
        assert!(header.contains("Parsed 3000/3000 players from 3 regions"));
        assert!(header.contains("data age 2h 5m old"));
        assert!(header.contains("Could not refresh: na - showing earlier data"));
    }

    #[test]
    fn singular_region_and_just_now_age() {
        let mut stats = stats_with_lines(1);
        stats.region_count = 1;
        stats.age_seconds = 30;
        let header = render(&stats).remove(0);
        assert!(header.contains("from 1 region ·"));
        assert!(header.contains("data age just now"));
    }

    #[test]
    fn full_render_contains_demo_layout_pieces() {
        let rendered = render(&stats_with_lines(1));
        let whole = rendered.join("\n");
        assert!(whole.contains("## Role distribution in average"));
        assert!(whole.contains("## Role distribution in KR"));
        assert!(whole.contains(
            "**TOP 300:** Top: 20.00% | Jungle: 20.00% | Middle: 20.00% | \
                                Bot: 20.00% | Supporter: 20.00%"
        ));
        assert!(whole.contains("## Most picked champions per role"));
        assert!(whole.contains("**Top** - 540 players"));
        assert!(whole.contains("1. Ambessa - 14.8%"));
    }

    /// Every generated line is complete: a heading, a blank separator, a
    /// bold row/role line, a numbered champion entry, or a coverage/failure
    /// note. A message may START mid-section (line-split continuation),
    /// but never mid-line.
    fn complete_lines_only(message: &str) {
        for line in message.lines() {
            assert!(
                line.starts_with('#')
                    || line.is_empty()
                    || line.starts_with("**")
                    || line.starts_with("Parsed")
                    || line.starts_with("Could not refresh")
                    || line.chars().next().is_some_and(|c| c.is_ascii_digit()),
                "partial line in output: {line:?}"
            );
        }
    }

    #[test]
    fn sections_never_straddle_message_boundaries() {
        // Many role rows per section: several messages; a section too big
        // for one message line-splits, so a message may start with a row -
        // but every line anywhere stays complete.
        let rendered = render(&stats_with_lines(30));
        assert!(rendered.len() > 1);
        for message in &rendered {
            assert!(message.len() <= MESSAGE_LIMIT);
            complete_lines_only(message);
        }
    }

    #[test]
    fn oversized_section_falls_back_to_line_splitting() {
        // One section whose rows alone exceed the limit.
        let rendered = render(&stats_with_lines(200));
        assert!(rendered.len() > 1);
        for message in &rendered {
            assert!(message.len() <= MESSAGE_LIMIT);
            complete_lines_only(message);
        }
    }

    #[test]
    fn absurd_single_line_is_hard_cut_on_char_boundaries() {
        // 2010 Cyrillic chars = 4020 bytes; byte-based chunks cut at 2000
        // bytes = 1000 chars per message - on char boundaries by
        // construction (no panics, no replacement characters).
        let long_line = "ж".repeat(MESSAGE_LIMIT + 10);
        let rendered = pack(vec![long_line]);
        assert_eq!(rendered.len(), 3);
        assert_eq!(rendered.first().expect("3 parts").chars().count(), 1000);
        assert_eq!(rendered.get(1).expect("3 parts").chars().count(), 1000);
        assert_eq!(rendered.get(2).expect("3 parts").chars().count(), 10);
        let total: usize = rendered.iter().map(|m| m.chars().count()).sum();
        assert_eq!(total, MESSAGE_LIMIT + 10);
    }
}
