//! Plugin-internal randomness port. Deliberately not a kernel port: no
//! other consumer is foreseeable, and swapping the plain RNG for a
//! deck-style generator later is a plugin-internal adapter swap.
//!
//! Shape: methods return drawn VALUES and take a [`RandomScope`]; any state
//! behind them (a deck, counters) is per-scope and owned by the adapter.
//! Deck-style "fake random" is stateful and per-channel - draws must
//! balance out within one conversation, never globally - so the scope is
//! part of the contract, but state ownership stays inside the adapter and
//! call sites never touch it.

use std::collections::HashMap;

use parking_lot::Mutex;
use rand::Rng;
use rand::seq::SliceRandom;

/// Identifies one random sequence - one per channel. The platform joins
/// the key so id spaces of different platforms can never collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RandomScope {
    pub platform: &'static str,
    pub guild_id: u64,
    pub channel_id: u64,
}

/// Driven port for plugin-internal randomness.
pub trait RandomPort: Send + Sync {
    /// True with `percent` probability, drawn from `scope`'s sequence.
    /// `percent <= 0` is always false and `percent >= 100` always true -
    /// callers need no clamping.
    fn chance_percent(&self, scope: RandomScope, percent: f64) -> bool;
}

/// Plain RNG adapter over `rand`'s thread-local generator: stateless, the
/// scope is accepted for interface symmetry and ignored.
pub struct RandRandom;

impl RandomPort for RandRandom {
    fn chance_percent(&self, _scope: RandomScope, percent: f64) -> bool {
        if percent <= 0.0 {
            return false;
        }
        if percent >= 100.0 {
            return true;
        }
        rand::rng().random_bool(percent / 100.0)
    }
}

/// Deck-style adapter ("fake random"): per scope, draws come WITHOUT
/// replacement from a bag holding the rounded percent as hits - a 2% chance
/// fires exactly 2 times per 100 draws, instead of statistically clumping.
/// The bag rebuilds when exhausted or when the percent changes.
pub struct DeckRandom {
    decks: Mutex<HashMap<RandomScope, Deck>>,
}

#[derive(Debug)]
struct Deck {
    /// Rounded percent the bag was built for; rebuilds on change.
    built_for: u32,
    remaining: Vec<bool>,
}

impl DeckRandom {
    #[must_use]
    pub fn new() -> Self {
        Self { decks: Mutex::new(HashMap::new()) }
    }
}

impl Default for DeckRandom {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomPort for DeckRandom {
    fn chance_percent(&self, scope: RandomScope, percent: f64) -> bool {
        if percent <= 0.0 {
            return false;
        }
        if percent >= 100.0 {
            return true;
        }

        // Guards above clamp percent into (0, 100); rounding is exact.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rounded = percent.round() as u32;
        let mut decks = self.decks.lock();
        let deck =
            decks.entry(scope).or_insert(Deck { built_for: u32::MAX, remaining: Vec::new() });
        if deck.built_for != rounded || deck.remaining.is_empty() {
            let hits = usize::from(u16::try_from(rounded).unwrap_or(u16::MAX).min(100));
            let mut bag: Vec<bool> = std::iter::repeat_n(true, hits)
                .chain(std::iter::repeat_n(false, 100 - hits))
                .collect();
            bag.shuffle(&mut rand::rng());
            deck.built_for = rounded;
            deck.remaining = bag;
        }
        deck.remaining.pop().unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(platform: &'static str, channel_id: u64) -> RandomScope {
        RandomScope { platform, guild_id: 1, channel_id }
    }

    #[test]
    fn zero_and_negative_chance_never_fires() {
        let rng = RandRandom;
        assert!(!rng.chance_percent(scope("discord", 1), 0.0));
        assert!(!rng.chance_percent(scope("discord", 1), -0.5));
        let deck = DeckRandom::new();
        assert!(!deck.chance_percent(scope("discord", 1), 0.0));
        assert!(!deck.chance_percent(scope("discord", 1), -0.5));
    }

    #[test]
    fn full_chance_always_fires() {
        let rng = RandRandom;
        assert!(rng.chance_percent(scope("discord", 1), 100.0));
        assert!(rng.chance_percent(scope("discord", 1), 500.0));
        let deck = DeckRandom::new();
        assert!(deck.chance_percent(scope("discord", 1), 100.0));
        assert!(deck.chance_percent(scope("discord", 1), 500.0));
    }

    #[test]
    fn deck_yields_exactly_the_promised_hits_per_cycle() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope, 2.0) {
                hits += 1;
            }
        }
        // One full deck cycle: exactly 2 hits, regardless of shuffle order.
        assert_eq!(hits, 2);
    }

    #[test]
    fn deck_state_is_per_scope() {
        let deck = DeckRandom::new();

        // Drain scope A's bag (draws beyond 100 start a fresh cycle, so the
        // counts stay exact per full cycle); scope B's first cycle is
        // untouched by A's draws.
        let mut a_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope("discord", 1), 5.0) {
                a_hits += 1;
            }
        }
        assert_eq!(a_hits, 5);

        let mut b_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope("discord", 2), 5.0) {
                b_hits += 1;
            }
        }
        assert_eq!(b_hits, 5);
    }

    #[test]
    fn deck_state_is_per_platform() {
        // Same raw channel id, different platform: independent sequences.
        let deck = DeckRandom::new();

        let mut discord_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope("discord", 7), 5.0) {
                discord_hits += 1;
            }
        }
        assert_eq!(discord_hits, 5);

        let mut telegram_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope("telegram", 7), 5.0) {
                telegram_hits += 1;
            }
        }
        assert_eq!(telegram_hits, 5);
    }

    #[test]
    fn deck_rebuilds_when_percent_changes() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        // 2% deck holds 2 hits; switch to 50% mid-cycle: the bag is rebuilt
        // for the new percent, so the next 100 draws hold exactly 50 hits.
        deck.chance_percent(scope, 2.0);
        let mut hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope, 50.0) {
                hits += 1;
            }
        }
        assert_eq!(hits, 50);
    }
}
