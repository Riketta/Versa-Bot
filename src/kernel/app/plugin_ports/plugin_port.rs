use crate::kernel::models::PluginError;

pub trait PluginPort: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn init(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn start(&self) -> Result<(), PluginError> {
        Ok(())
    }
    fn stop(&self) -> Result<(), PluginError> {
        Ok(())
    }
}
