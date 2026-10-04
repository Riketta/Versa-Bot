// `unwrap` is denied outside tests (the lint table in Cargo.toml): tests
// arrange known-good fixtures and stay readable with it, production code
// handles the fallible paths explicitly.
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod common;
pub mod infrastructure;
pub mod kernel;
pub mod plugins;

#[cfg(test)]
pub mod test_support;
