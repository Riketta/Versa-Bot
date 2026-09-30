use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use bon::bon;
use futures_util::FutureExt;
use tracing::Instrument;

use crate::common::panic_message;
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{GuildId, Origin, PluginError, RequestContext},
    plugin_ports::{EventBusPort, MiddlewarePluginPort, Next, PluginPort},
    services::KernelServices,
    spi_ports::{ChatOutputFactoryPort, StoragePort},
};

/// The kernel: assembles the middleware chain and runs it, but never knows
/// what is in it. Holds plugins behind `PluginPort`/`MiddlewarePluginPort`
/// contracts and routes plugin-to-plugin traffic via `EventBusPort`.
pub struct KernelService<E: EventBusPort> {
    plugins: Vec<Arc<dyn PluginPort>>,
    middleware: Vec<Arc<dyn MiddlewarePluginPort>>,
    /// Owned by the kernel, wired at the composition root; the same instance
    /// is cloned into plugins at construction. The kernel itself only routes.
    // TODO(discussed-later): kernel-side bus interaction (topic A).
    #[allow(dead_code)]
    event_bus: E,
    chat_output_factory: Arc<dyn ChatOutputFactoryPort>,
    storage: Arc<dyn StoragePort>,
}

#[bon]
impl<E: EventBusPort> KernelService<E> {
    #[builder]
    pub fn new(
        plugins: Vec<Arc<dyn PluginPort>>,
        middleware: Vec<Arc<dyn MiddlewarePluginPort>>,
        event_bus: E,
        chat_output_factory: Arc<dyn ChatOutputFactoryPort>,
        storage: Arc<dyn StoragePort>,
    ) -> Self {
        Self { plugins, middleware, event_bus, chat_output_factory, storage }
    }

    /// Kernel entrypoint: two-phase plugin lifecycle - `init` on all plugins
    /// first, then `start` on all, so every plugin is initialized before any
    /// starts. A plugin registered in both `plugins` and `middleware` (the
    /// same `Arc`, dual registration) is initialized/started once.
    ///
    /// # Errors
    /// Propagates the first plugin `init`/`start` failure; remaining plugins
    /// are neither initialized nor started.
    pub fn boot(&self) -> Result<(), PluginError> {
        let mut seen = HashSet::new();
        for plugin in &self.plugins {
            if seen.insert(plugin.name()) {
                plugin.init()?;
            }
        }

        let mut seen = HashSet::new();
        for plugin in &self.plugins {
            if seen.insert(plugin.name()) {
                plugin.start()?;
            }
        }

        tracing::info!("kernel booted");
        Ok(())
    }

    /// Ordered stop of all plugins, reverse registration order.
    pub fn shutdown(&self) {
        for plugin in self.plugins.iter().rev() {
            if let Err(err) = plugin.stop() {
                tracing::error!(plugin = plugin.name(), %err, "plugin stop failed");
            }
        }
    }

    /// Event-scoped service context: outbound ports and storage bound to the
    /// event's origin. DMs get no guild storage handle at all.
    fn scoped_services(&self, origin: &Origin) -> KernelServices {
        KernelServices {
            chat_output: self.chat_output_factory.chat_output(origin),
            chat_output_factory: Arc::clone(&self.chat_output_factory),
            guild_storage: origin
                .guild_id
                .map(|guild_id| self.storage.guild_scoped(origin.platform, guild_id)),
        }
    }
}

impl<E: EventBusPort> Drop for KernelService<E> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[async_trait]
impl<E: EventBusPort> RequestHandlerPort for KernelService<E> {
    /// Fire-and-forget middleware traversal: forward `pre` (chain-breakable),
    /// then backward `post` over the plugins that ran. An event no plugin
    /// handles yields no output - there is no "not found" default.
    ///
    /// Every event is traced with its origin as span fields, so stdout and
    /// Sentry/GlitchTip records correlate without plugin effort.
    async fn handle(&self, event: RequestContext) {
        let span = tracing::info_span!(
            "handle_event",
            platform = ?event.origin.platform,
            guild_id = event.origin.guild_id.map(GuildId::get),
            channel_id = event.origin.channel_id.get(),
            user_id = event.origin.user_id.get(),
        );

        self.process(event).instrument(span).await;
    }
}

impl<E: EventBusPort> KernelService<E> {
    /// Fire-and-forget middleware traversal with panic isolation. Hooks are
    /// fire-and-forget (they return `Next`, never `Result`) - plugins log
    /// their own recoverable failures; panics are the kernel's concern:
    ///
    /// - a `pre` that panics is logged (plugin + event) and treated as
    ///   `Stop`: the event does not flow to remaining plugins (fail closed)
    ///   and `post` still runs for the plugins that ran (the panicking one
    ///   included - only its call frame unwound, its state is intact);
    /// - a `post` that panics is logged and remaining posts still run -
    ///   one broken observer must not skip the others' cleanup.
    ///
    /// No plugin panic may reach the driving adapter's task.
    async fn process(&self, mut event: RequestContext) {
        let services = self.scoped_services(&event.origin);

        let mut ran = 0usize;
        let mut aborted = false;

        for step in &self.middleware {
            let outcome =
                std::panic::AssertUnwindSafe(step.pre(&mut event, &services)).catch_unwind().await;

            match outcome {
                Ok(Next::Continue) => ran += 1,
                Ok(Next::Stop) => {
                    ran += 1;
                    break;
                }
                Ok(Next::Abort) => {
                    aborted = true;
                    break;
                }
                Err(panic) => {
                    tracing::error!(
                        plugin = step.name(),
                        kind = ?event.kind,
                        panic = panic_message(&panic),
                        "middleware `pre` panicked - event dropped"
                    );
                    ran += 1;
                    break;
                }
            }
        }

        if aborted {
            return;
        }

        let Some(ran_steps) = self.middleware.get(..ran) else {
            return;
        };

        for step in ran_steps.iter().rev() {
            let outcome =
                std::panic::AssertUnwindSafe(step.post(&event, &services)).catch_unwind().await;
            if let Err(panic) = outcome {
                tracing::error!(
                    plugin = step.name(),
                    kind = ?event.kind,
                    panic = panic_message(&panic),
                    "middleware `post` panicked"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::OutboundMessage;
    use crate::kernel::models::{EventKind, Platform};
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use parking_lot::Mutex;
    use serde_json::json;

    struct PongPlugin {
        started: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PluginPort for PongPlugin {
        fn name(&self) -> &'static str {
            "pong"
        }

        fn start(&self) -> Result<(), PluginError> {
            self.started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl MiddlewarePluginPort for PongPlugin {
        async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
            if event.kind != EventKind::MessageReceived {
                return Next::Continue;
            }
            let _ = services.chat_output.send(OutboundMessage::text("pong")).await;
            Next::Stop
        }
    }

    /// Reads its own namespace from the event-scoped guild storage and
    /// replies with it - proves the handle reaches plugins guild-bound.
    struct GreeterPlugin;

    impl PluginPort for GreeterPlugin {
        fn name(&self) -> &'static str {
            "greeter"
        }
    }

    #[async_trait]
    impl MiddlewarePluginPort for GreeterPlugin {
        async fn pre(&self, _event: &mut RequestContext, services: &KernelServices) -> Next {
            let Some(storage) = &services.guild_storage else {
                return Next::Continue;
            };
            let greeting = storage
                .get("greeter", "greeting")
                .await
                .ok()
                .flatten()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "hello".to_owned());
            let _ = services.chat_output.send(OutboundMessage::text(greeting)).await;
            Next::Stop
        }
    }

    struct SilentPlugin;

    impl PluginPort for SilentPlugin {
        fn name(&self) -> &'static str {
            "silent"
        }
    }

    #[async_trait]
    impl MiddlewarePluginPort for SilentPlugin {}

    /// Records its hook invocations; can be told to panic in either hook -
    /// the fixture for the pipeline failure policy tests.
    struct RecorderPlugin {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
        panic_in_pre: bool,
        panic_in_post: bool,
    }

    impl PluginPort for RecorderPlugin {
        fn name(&self) -> &'static str {
            self.name
        }
    }

    #[async_trait]
    impl MiddlewarePluginPort for RecorderPlugin {
        async fn pre(&self, _event: &mut RequestContext, _services: &KernelServices) -> Next {
            self.log.lock().push(format!("pre:{}", self.name));
            if self.panic_in_pre {
                panic!("pre boom");
            }
            Next::Continue
        }

        async fn post(&self, _event: &RequestContext, _services: &KernelServices) {
            self.log.lock().push(format!("post:{}", self.name));
            if self.panic_in_post {
                panic!("post boom");
            }
        }
    }

    struct TestEventBus;

    impl EventBusPort for TestEventBus {
        fn publish(&self, _event: Arc<dyn crate::kernel::models::Event>) {}

        fn subscribe<E: crate::kernel::models::Event + 'static>(
            &self,
            _handler: Arc<dyn crate::kernel::plugin_ports::EventHandler<E>>,
        ) {
        }
    }

    fn test_origin() -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: crate::kernel::models::ChannelId(2),
            user_id: crate::kernel::models::UserId(3),
            message_id: Some(crate::kernel::models::MessageId(4)),
            reply_token: None,
        }
    }

    fn test_kernel(
        storage: Arc<dyn StoragePort>,
        plugins: Vec<Arc<dyn PluginPort>>,
        middleware: Vec<Arc<dyn MiddlewarePluginPort>>,
    ) -> (KernelService<TestEventBus>, Arc<RecordingChatOutput>) {
        let output = RecordingChatOutput::new();
        let kernel = KernelService::builder()
            .plugins(plugins)
            .middleware(middleware)
            .event_bus(TestEventBus)
            .chat_output_factory(RecordingChatOutputFactory::new(Arc::clone(&output)).boxed())
            .storage(storage)
            .build();
        (kernel, output)
    }

    // --- tests ---

    #[tokio::test]
    async fn pipeline_produces_output_and_short_circuits() {
        let pong =
            Arc::new(PongPlugin { started: Arc::new(std::sync::atomic::AtomicUsize::new(0)) });
        let (kernel, output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&pong) as Arc<dyn PluginPort>],
            vec![Arc::clone(&pong) as Arc<dyn MiddlewarePluginPort>],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "ping")).await;

        assert_eq!(output.messages(), ["pong"]);
    }

    #[tokio::test]
    async fn unhandled_event_yields_no_output() {
        let (kernel, output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::new(SilentPlugin)],
            vec![Arc::new(SilentPlugin) as Arc<dyn MiddlewarePluginPort>],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "ping")).await;

        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn boot_starts_each_dual_registered_plugin_once() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pong = Arc::new(PongPlugin { started: Arc::clone(&started) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&pong) as Arc<dyn PluginPort>],
            vec![Arc::clone(&pong) as Arc<dyn MiddlewarePluginPort>],
        );

        kernel.boot().expect("boot should succeed");

        assert_eq!(
            started.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "dual-registered plugin must start exactly once"
        );
    }

    #[tokio::test]
    async fn pipeline_hands_plugin_guild_scoped_storage() {
        let storage = Arc::new(InMemoryStorage::new());
        // Guild 1 is greeted by config; guild 2 has none and gets the default.
        storage.seed(Platform::Discord, GuildId(1), "greeter", "greeting", json!("privit"));

        let (kernel, output) = test_kernel(
            storage,
            vec![Arc::new(GreeterPlugin)],
            vec![Arc::new(GreeterPlugin) as Arc<dyn MiddlewarePluginPort>],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "hi")).await;

        let other_guild = Origin { guild_id: Some(GuildId(2)), ..test_origin() };
        kernel.handle(RequestContext::message_received(other_guild, "hi")).await;

        assert_eq!(output.messages(), ["privit", "hello"]);
    }

    /// Failure policy: a panicking `pre` drops the event (fail closed - the
    /// healthy plugin never sees it) while the broken plugin's own `post`
    /// still runs (only its call frame unwound, its state is intact).
    #[tokio::test]
    async fn panicking_pre_stops_chain_but_runs_post_of_ran_plugins() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let broken = Arc::new(RecorderPlugin {
            name: "broken",
            log: Arc::clone(&log),
            panic_in_pre: true,
            panic_in_post: false,
        });
        let healthy = Arc::new(RecorderPlugin {
            name: "healthy",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![],
            vec![
                Arc::clone(&broken) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&healthy) as Arc<dyn MiddlewarePluginPort>,
            ],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "hi")).await;

        let entries = log.lock().clone();
        assert_eq!(
            entries,
            vec!["pre:broken".to_owned(), "post:broken".to_owned()],
            "event must not flow past the panicking plugin, but its post must run"
        );
    }

    /// Failure policy: a panicking `post` must not skip the remaining posts
    /// (posts run in reverse order, so the broken one is last in the chain).
    #[tokio::test]
    async fn panicking_post_does_not_block_other_posts() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let healthy = Arc::new(RecorderPlugin {
            name: "healthy",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let broken = Arc::new(RecorderPlugin {
            name: "broken",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: true,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![],
            vec![
                Arc::clone(&healthy) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&broken) as Arc<dyn MiddlewarePluginPort>,
            ],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "hi")).await;

        let entries = log.lock().clone();
        assert_eq!(
            entries,
            vec![
                "pre:healthy".to_owned(),
                "pre:broken".to_owned(),
                "post:broken".to_owned(),
                "post:healthy".to_owned(),
            ],
            "healthy plugin's post must run despite the broken one panicking first"
        );
    }
}
