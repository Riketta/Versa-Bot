use async_trait::async_trait;

#[async_trait]
pub trait ConfigPort: Send + Sync {
    // fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T>;
    // fn get_or_default<T: serde::de::DeserializeOwned + Default>(&self, key: &str) -> T;
    // /// Subscribe to config change notifications for a given key prefix.
    // async fn on_change(&self, prefix: &str, handler: Arc<dyn ConfigChangeHandler>);
}

pub trait ConfigChangeHandler: Send + Sync {
    fn handle(&self, key: &str, new_value: &serde_json::Value);
}
