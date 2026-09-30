use crate::kernel::plugin_ports::PluginPort;

/// Rotates bot status once in a while (`PluginPort`-only - it is not part of
/// the inbound pipeline). Placeholder until the scheduler port lands; the
/// wiring is deferred.
pub struct StatusRotatorPlugin;

impl PluginPort for StatusRotatorPlugin {
    fn name(&self) -> &'static str {
        "status_rotator"
    }
}
