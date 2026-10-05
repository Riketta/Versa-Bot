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

/// Identifies one random sequence - one per channel and purpose. The
/// platform joins the key so id spaces of different platforms can never
/// collide; the purpose separates a channel's parallel rolls (reply vs
/// silent react) so each draws from its own bag - they would interleave
/// and rebuild one another's decks otherwise, since their percents differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RandomScope {
    pub platform: &'static str,
    pub guild_id: u64,
    pub channel_id: u64,
    /// Roll purpose inside the channel (e.g. `"reply"`, `"react"`).
    pub purpose: &'static str,
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
/// replacement from a bag holding the promised hits, so fires balance out
/// over a cycle instead of statistically clumping. Low chances normalize
/// to a single hit - 2% decks as 1 hit per 50 cards, not 2 per 100 - so
/// hits cannot pair up mid-cycle and the spacing stays even (and
/// sub-percent chances become exact instead of rounding up to a 1-hit
/// floor). The single-hit size is `round(100/percent)`, so the delivered
/// rate drifts by at most half a card of rounding (imperceptible for
/// chime rolls); sizes cap at [`MAX_DECK_CARDS`], clamping chances below
/// 0.01% to it. Higher chances keep the exact rounded percent as hits per
/// 100 cards - a one-hit deck would distort the rate there (50% would
/// alternate strictly, 90% would always fire). The bag rebuilds when
/// exhausted or when the shape changes.
pub struct DeckRandom {
    decks: Mutex<HashMap<RandomScope, Deck>>,
}

/// Single-hit normalization applies while `round(100/percent)` reaches
/// this many cards; below it (percent above ~28%), a one-hit deck would
/// distort the rate past perception, so the exact hits-per-100 shape wins.
const MIN_SINGLE_HIT_DECK: u32 = 4;

/// Deck-size cap: chances below `100/MAX` percent clamp to it (one hit
/// per 10,000 cards), bounding the bag's memory.
const MAX_DECK_CARDS: u32 = 10_000;

#[derive(Debug)]
struct Deck {
    /// (hits, cards) the bag was built for; rebuilds on change.
    built_for: (u32, u32),
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

        // Guards above clamp percent into (0, 100); both casts are exact:
        // the kept size sits in [MIN_SINGLE_HIT_DECK, MAX_DECK_CARDS], and
        // the fallback percent rounds into [29, 100].
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let shape = if (100.0 / percent).round() >= f64::from(MIN_SINGLE_HIT_DECK) {
            (1, (100.0 / percent).round().min(f64::from(MAX_DECK_CARDS)) as u32)
        } else {
            (percent.round() as u32, 100)
        };

        let mut decks = self.decks.lock();
        let deck = decks.entry(scope).or_insert(Deck { built_for: (0, 0), remaining: Vec::new() });
        if deck.built_for != shape || deck.remaining.is_empty() {
            let (hits, cards) = shape;
            let mut bag: Vec<bool> = std::iter::repeat_n(true, hits as usize)
                .chain(std::iter::repeat_n(false, cards as usize - hits as usize))
                .collect();
            bag.shuffle(&mut rand::rng());
            deck.built_for = shape;
            deck.remaining = bag;
        }
        deck.remaining.pop().unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(platform: &'static str, channel_id: u64) -> RandomScope {
        RandomScope { platform, guild_id: 1, channel_id, purpose: "reply" }
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
        // Two normalized single-hit cycles of 50: exactly 2 hits, regardless
        // of shuffle order.
        assert_eq!(hits, 2);
    }

    /// The normalization pins the cycle SIZE, not just the average: 2% is
    /// one hit per 50 draws, not two hits per 100 that might pair up.
    #[test]
    fn single_hit_deck_cycle_is_the_normalized_size() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..50 {
            if deck.chance_percent(scope, 2.0) {
                hits += 1;
            }
        }
        assert_eq!(hits, 1);
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

    /// Reply and silent-react rolls share a channel but never a bag: each
    /// purpose's cycle holds exactly its own promised hits. A shared bag
    /// would be rebuilt on every alternating draw (the percents differ),
    /// collapsing the deck into plain-RNG behavior.
    #[test]
    fn deck_state_is_per_purpose() {
        let deck = DeckRandom::new();

        let mut reply_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(RandomScope { purpose: "reply", ..scope("discord", 1) }, 5.0) {
                reply_hits += 1;
            }
        }
        assert_eq!(reply_hits, 5);

        let mut react_hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(RandomScope { purpose: "react", ..scope("discord", 1) }, 10.0) {
                react_hits += 1;
            }
        }
        assert_eq!(react_hits, 10);
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

    /// Sub-percent chances resolve to exact single-hit decks (0.4% = 1 hit
    /// per 250 cards) - the old rounded-hits shape had to round them up to
    /// a 1-hit-per-100 floor, tripling the rate.
    #[test]
    fn sub_percent_chances_are_exact_one_hit_decks() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..250 {
            if deck.chance_percent(scope, 0.4) {
                hits += 1;
            }
        }
        assert_eq!(hits, 1);
    }

    /// Fractional percents normalize by scaling the deck: 2.5% is 1 hit per
    /// 40 cards - exact, no rounding to whole hits per 100.
    #[test]
    fn fractional_percent_normalizes_to_one_hit() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..40 {
            if deck.chance_percent(scope, 2.5) {
                hits += 1;
            }
        }
        assert_eq!(hits, 1);
    }

    /// Above the single-hit threshold the exact rounded percent rides as
    /// hits per 100 cards - a one-hit deck would distort the rate (90%
    /// would always fire).
    #[test]
    fn high_chance_keeps_the_exact_hits_per_100_deck() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..100 {
            if deck.chance_percent(scope, 90.0) {
                hits += 1;
            }
        }
        assert_eq!(hits, 90);
    }

    /// The deck-size cap bounds the bag: chances below 0.01% clamp to one
    /// hit per 10,000 cards.
    #[test]
    fn tiny_chances_clamp_to_the_deck_cap() {
        let deck = DeckRandom::new();
        let scope = scope("discord", 1);

        let mut hits = 0;
        for _ in 0..10_000 {
            if deck.chance_percent(scope, 0.001) {
                hits += 1;
            }
        }
        assert_eq!(hits, 1);
    }
}
