//! Plugin-internal randomness port. Deliberately not a kernel port: no
//! other consumer is foreseeable, and swapping the plain RNG for a
//! deck-style "fake random" (draw without replacement, to balance chime-ins
//! out over time) later is a plugin-internal adapter swap.

use rand::Rng;

/// Driven port for plugin-internal randomness.
pub trait RandomPort: Send + Sync {
    /// True with `percent` probability. `percent <= 0` is always false and
    /// `percent >= 100` always true - callers need no clamping.
    fn chance_percent(&self, percent: f64) -> bool;
}

/// Plain RNG adapter over `rand`'s thread-local generator.
pub struct RandRandom;

impl RandomPort for RandRandom {
    fn chance_percent(&self, percent: f64) -> bool {
        if percent <= 0.0 {
            return false;
        }
        if percent >= 100.0 {
            return true;
        }
        rand::rng().random_bool(percent / 100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_negative_chance_never_fires() {
        let rng = RandRandom;
        assert!(!rng.chance_percent(0.0));
        assert!(!rng.chance_percent(-0.5));
    }

    #[test]
    fn full_chance_always_fires() {
        let rng = RandRandom;
        assert!(rng.chance_percent(100.0));
        assert!(rng.chance_percent(500.0));
    }
}
