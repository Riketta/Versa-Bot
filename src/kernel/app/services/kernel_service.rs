use std::{
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use bon::bon;
use futures_util::FutureExt;
use parking_lot::Mutex;
use tracing::Instrument;

use crate::common::panic_message;
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{GuildId, Origin, PluginError, RequestContext},
    plugin_ports::{EventBusPort, MiddlewarePluginPort, Next, PluginPort},
    services::KernelServices,
    spi_ports::{
        ChatOutputFactoryPort, PlatformInfoPort, PluginStorage, PluginStoragePort, StoragePort,
    },
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
    /// Plugin-global document store, pre-bound to the deployment's platform
    /// slug at construction - the same instance every event's service
    /// context shares (aggregates only, never user content).
    plugin_storage: Arc<dyn PluginStorage>,
    /// The deployment's platform identity (adapter-owned values), stamped
    /// into every event's service context and storage binding.
    platform_info: Arc<dyn PlatformInfoPort>,
    /// One-shot guard: the explicit `shutdown()` and the `Drop` fallback
    /// together must stop plugins exactly once.
    shutdown_started: AtomicBool,
    /// One-shot guard for [`Self::boot`]: init/start must never re-run on
    /// already-live plugins (a second call is a warned no-op, mirroring
    /// shutdown's idempotence).
    boot_started: AtomicBool,
    /// Plugins that reached a successful `start()` under a successful boot -
    /// the only ones `shutdown` may ever stop. Drained by `shutdown`; a
    /// failed boot leaves it empty (rolled-back plugins were already stopped
    /// and a plugin that never started owes no stop).
    started: Mutex<Vec<Arc<dyn PluginPort>>>,
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
        plugin_storage: Arc<dyn PluginStoragePort>,
        platform_info: Arc<dyn PlatformInfoPort>,
    ) -> Self {
        Self {
            plugins,
            middleware,
            event_bus,
            chat_output_factory,
            storage,
            plugin_storage: plugin_storage.plugin_scoped(platform_info.slug()),
            platform_info,
            shutdown_started: AtomicBool::new(false),
            boot_started: AtomicBool::new(false),
            started: Mutex::new(Vec::new()),
        }
    }

    /// Kernel entrypoint: two-phase plugin lifecycle - `init` on all plugins
    /// first, then `start` on all, so every plugin is initialized before any
    /// starts. A plugin registered in both `plugins` and `middleware` (the
    /// same `Arc`, dual registration) is initialized/started once.
    ///
    /// Boot validates registration first (see [`Self::validated_plugins`]).
    /// If a `start` fails, the already-started plugins are stopped in reverse
    /// order before the error propagates; an `init`-phase failure needs no
    /// rollback - nothing has started yet.
    ///
    /// Hook ORDER is the composition root's policy: the kernel is
    /// meaning-blind and cannot know which plugin gates which, so a gating
    /// plugin (e.g. auth) must be registered before the plugins it gates.
    ///
    /// A failed boot rolls the one-shot guard back - the rollback already
    /// stopped everything that started, so `boot` may be retried.
    ///
    /// # Errors
    /// Propagates the first plugin `init`/`start` failure, or
    /// `PluginError::Invalid` on registration conflicts.
    pub fn boot(&self) -> Result<(), PluginError> {
        // One-shot like `shutdown`: re-running init/start on live plugins is
        // always a bug, and plugins that guard themselves must not have to.
        if self.boot_started.swap(true, Ordering::SeqCst) {
            tracing::warn!("kernel boot called twice - ignoring, plugins are already booted");
            return Ok(());
        }
        let outcome = self.boot_plugins();
        if outcome.is_err() {
            self.boot_started.store(false, Ordering::SeqCst);
        }
        outcome
    }

    /// The boot body - the one-shot guard bookkeeping stays in [`Self::boot`].
    fn boot_plugins(&self) -> Result<(), PluginError> {
        let plugins = self.validated_plugins()?;

        for plugin in &plugins {
            plugin.init()?;
        }

        let mut started: Vec<Arc<dyn PluginPort>> = Vec::new();
        for plugin in &plugins {
            match plugin.start() {
                Ok(()) => started.push(Arc::clone(plugin)),
                Err(err) => {
                    Self::rollback_started(&started);
                    return Err(err);
                }
            }
        }

        *self.started.lock() = started;
        tracing::info!("kernel booted");
        Ok(())
    }

    /// Registration validation, shared by `boot`: every middleware step must
    /// be the registered plugin's own instance (the same allocation - a
    /// distinct object sharing the name would intercept events with no
    /// lifecycle), and a name listed twice in `plugins` must be the same
    /// instance (a harmless duplicate registration) - a name collision across
    /// distinct instances is an `Invalid` configuration. Returns the plugins
    /// to boot in registration order, duplicates skipped.
    fn validated_plugins(&self) -> Result<Vec<Arc<dyn PluginPort>>, PluginError> {
        for step in &self.middleware {
            if !self
                .plugins
                .iter()
                .any(|plugin| plugin.name() == step.name() && Self::same_instance(plugin, step))
            {
                return Err(PluginError::Invalid(format!(
                    "middleware plugin `{}` is not dual-registered as the same plugin instance",
                    step.name()
                )));
            }
        }

        // A duplicated middleware entry (the same instance listed twice)
        // would silently run every event through it twice - refuse it.
        for (index, step) in self.middleware.iter().enumerate() {
            if self.middleware.get(..index).is_some_and(|earlier| {
                earlier.iter().any(|previous| Self::same_instance(previous, step))
            }) {
                return Err(PluginError::Invalid(format!(
                    "middleware plugin `{}` is listed twice",
                    step.name()
                )));
            }
        }

        let mut unique: Vec<Arc<dyn PluginPort>> = Vec::new();
        for plugin in &self.plugins {
            if let Some(registered) = unique.iter().find(|p| p.name() == plugin.name()) {
                if !Self::same_instance(registered, plugin) {
                    return Err(PluginError::Invalid(format!(
                        "plugin name `{}` is claimed by two distinct plugin instances",
                        plugin.name()
                    )));
                }
                continue;
            }
            unique.push(Arc::clone(plugin));
        }
        Ok(unique)
    }

    /// Instance identity via the fat-pointer data address of the `Arc`:
    /// dual registration must hand the kernel the same allocation (an `Arc`
    /// clone), not two objects that merely share a name.
    fn same_instance<T: ?Sized, U: ?Sized>(a: &Arc<T>, b: &Arc<U>) -> bool {
        std::ptr::eq(Arc::as_ptr(a).cast::<()>(), Arc::as_ptr(b).cast::<()>())
    }

    /// Boot rollback: stops already-started plugins in reverse order. Stop
    /// failures and panics are logged and skipped - a broken `stop` must
    /// not strand the other plugins' cleanup nor mask the original boot
    /// error.
    fn rollback_started(started: &[Arc<dyn PluginPort>]) {
        for plugin in started.iter().rev() {
            Self::stop_quietly(plugin);
        }
    }

    /// Stops one plugin, absorbing both `Err` and panics: a broken `stop`
    /// must never strand the other plugins' cleanup - the same isolation
    /// contract the pipeline hooks, bus handlers, jobs, and config
    /// subscribers honor.
    fn stop_quietly(plugin: &Arc<dyn PluginPort>) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plugin.stop())) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(plugin = plugin.name(), %err, "plugin stop failed"),
            Err(panic) => {
                tracing::error!(
                    plugin = plugin.name(),
                    panic = panic_message(&panic),
                    "plugin stop panicked"
                );
            }
        }
    }

    /// Ordered stop of the plugins that reached a successful `start()` under
    /// a successful boot, in reverse start order - exactly once each.
    /// Idempotent: an explicit `shutdown()` followed by the `Drop` fallback
    /// stops every started plugin exactly once, so `PluginPort::stop`
    /// implementations are never double-invoked by the kernel. A failed boot
    /// leaves nothing to stop: rolled-back plugins were stopped by the
    /// rollback, and a plugin that never started owes no stop.
    pub fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        // Collected before stopping: the reverse-order walk must survive
        // whatever a stopping plugin does - every stop is isolated in
        // [`Self::stop_quietly`].
        let started: Vec<_> = self.started.lock().drain(..).collect();
        for plugin in started.iter().rev() {
            Self::stop_quietly(plugin);
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
                .map(|guild_id| self.storage.guild_scoped(self.platform_info.slug(), guild_id)),
            plugin_storage: Arc::clone(&self.plugin_storage),
            platform_info: Arc::clone(&self.platform_info),
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
            platform = self.platform_info.slug(),
            kind = ?event.kind,
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

        tracing::debug!(kind = ?event.kind, "event entering middleware pipeline");

        let mut ran = 0usize;
        let mut aborted = false;
        let mut stopped_by: Option<&str> = None;

        for step in &self.middleware {
            let outcome =
                std::panic::AssertUnwindSafe(step.pre(&mut event, &services)).catch_unwind().await;

            match outcome {
                Ok(Next::Continue) => ran += 1,
                Ok(Next::Stop) => {
                    ran += 1;
                    stopped_by = Some(step.name());
                    break;
                }
                Ok(Next::Abort) => {
                    aborted = true;
                    stopped_by = Some(step.name());
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

        tracing::debug!(ran, aborted, ?stopped_by, "middleware pre-traversal finished");

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
    use crate::kernel::models::EventKind;
    use crate::kernel::models::OutboundMessage;
    use crate::kernel::plugin_ports::EventBusSubscription;
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

        fn stop(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("stop:{}", self.name));
            Ok(())
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

    /// Plugin whose `start` fails - the fixture for the boot rollback tests.
    /// Records `stop` calls the same way `RecorderPlugin` does.
    struct FailingStartPlugin {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl PluginPort for FailingStartPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn start(&self) -> Result<(), PluginError> {
            Err(PluginError::Start(format!("start failed: {}", self.name)))
        }

        fn stop(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("stop:{}", self.name));
            Ok(())
        }
    }

    /// Plugin whose `stop` panics - the fixture for the shutdown isolation
    /// tests. Records the call before panicking, like `RecorderPlugin`.
    struct PanickingStopPlugin {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl PluginPort for PanickingStopPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn stop(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("stop:{}", self.name));
            panic!("stop boom");
        }
    }

    /// Plugin whose `stop` returns `Err` - the fixture for the shutdown
    /// isolation tests. Records the call, same as `RecorderPlugin`.
    struct FailingStopPlugin {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl PluginPort for FailingStopPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn stop(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("stop:{}", self.name));
            Err(PluginError::Stop(format!("stop failed: {}", self.name)))
        }
    }

    /// Plugin whose `start` fails only on the first attempt - the fixture
    /// for the boot-retry test.
    struct OnceFailingStartPlugin {
        name: &'static str,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PluginPort for OnceFailingStartPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn start(&self) -> Result<(), PluginError> {
            let attempt = self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                Err(PluginError::Start(format!("start failed: {}", self.name)))
            } else {
                Ok(())
            }
        }
    }

    /// Plugin whose `pre` hard-stops the chain with `Abort` - the fixture
    /// for the hard-stop policy test.
    struct AbortingPlugin {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl PluginPort for AbortingPlugin {
        fn name(&self) -> &'static str {
            "aborter"
        }
    }

    #[async_trait]
    impl MiddlewarePluginPort for AbortingPlugin {
        async fn pre(&self, _event: &mut RequestContext, _services: &KernelServices) -> Next {
            self.log.lock().push("pre:aborter".to_owned());
            Next::Abort
        }
    }

    /// Logs every lifecycle call in order - the fixture for the two-phase
    /// boot contract tests.
    struct LifecyclePlugin {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
        fail_init: bool,
    }

    impl PluginPort for LifecyclePlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn init(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("init:{}", self.name));
            if self.fail_init {
                return Err(PluginError::Init(format!("init failed: {}", self.name)));
            }
            Ok(())
        }

        fn start(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("start:{}", self.name));
            Ok(())
        }

        fn stop(&self) -> Result<(), PluginError> {
            self.log.lock().push(format!("stop:{}", self.name));
            Ok(())
        }
    }

    struct TestEventBus;

    impl EventBusPort for TestEventBus {
        fn publish(&self, _event: Arc<dyn crate::kernel::models::Event>) {}

        fn subscribe<E: crate::kernel::models::Event + 'static>(
            &self,
            _handler: Arc<dyn crate::kernel::plugin_ports::EventHandler<E>>,
        ) -> EventBusSubscription {
            EventBusSubscription::new(Arc::new(|| {}))
        }
    }

    fn test_origin() -> Origin {
        Origin {
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
            .plugin_storage(Arc::new(crate::test_support::InMemoryPluginStorage::new())
                as Arc<dyn PluginStoragePort>)
            .platform_info(crate::test_support::test_platform_info())
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

    /// The boot one-shot guard: a second `boot` is a warned no-op - init and
    /// start never re-run on live plugins, and the started set is left
    /// untouched so shutdown still stops everything exactly once.
    #[tokio::test]
    async fn boot_twice_starts_plugins_once() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pong = Arc::new(PongPlugin { started: Arc::clone(&started) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&pong) as Arc<dyn PluginPort>],
            vec![Arc::clone(&pong) as Arc<dyn MiddlewarePluginPort>],
        );

        kernel.boot().expect("first boot should succeed");
        kernel.boot().expect("second boot is a warned no-op");

        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(kernel.started.lock().len(), 1, "started set must survive the second boot");
    }

    /// A panicking `stop` must not strand the remaining plugins' cleanup -
    /// the same isolation contract every other plugin boundary honors.
    #[tokio::test]
    async fn panicking_stop_does_not_strand_the_remaining_plugins() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let healthy = Arc::new(RecorderPlugin {
            name: "healthy",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let boomer = Arc::new(PanickingStopPlugin { name: "boomer", log: Arc::clone(&log) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&healthy) as Arc<dyn PluginPort>,
                Arc::clone(&boomer) as Arc<dyn PluginPort>,
            ],
            vec![],
        );
        kernel.boot().expect("boot should succeed");
        kernel.shutdown();

        // Reverse stop order: boomer (started last) panics first - healthy
        // must still be stopped, and the Drop fallback must not re-stop.
        assert_eq!(*log.lock(), ["stop:boomer".to_owned(), "stop:healthy".to_owned()]);
    }

    /// A `stop` returning `Err` is logged and skipped; the remaining
    /// plugins still stop.
    #[tokio::test]
    async fn failing_stop_still_stops_the_remaining_plugins() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let healthy = Arc::new(RecorderPlugin {
            name: "healthy",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let failing = Arc::new(FailingStopPlugin { name: "failing", log: Arc::clone(&log) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&healthy) as Arc<dyn PluginPort>,
                Arc::clone(&failing) as Arc<dyn PluginPort>,
            ],
            vec![],
        );
        kernel.boot().expect("boot should succeed");
        kernel.shutdown();

        assert_eq!(*log.lock(), ["stop:failing".to_owned(), "stop:healthy".to_owned()]);
    }

    /// Registration validation: a middleware step that is not registered as
    /// a plugin is an invalid kernel configuration - boot must refuse it.
    #[tokio::test]
    async fn middleware_only_plugin_fails_boot() {
        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![],
            vec![Arc::new(SilentPlugin) as Arc<dyn MiddlewarePluginPort>],
        );

        let err = kernel.boot().expect_err("middleware-only registration must fail boot");
        assert!(matches!(err, PluginError::Invalid(_)), "unexpected error: {err:?}");
    }

    /// Registration validation: two DISTINCT instances claiming the same name
    /// are an invalid configuration (a twice-registered SAME instance is the
    /// harmless dual registration, see the test above).
    #[tokio::test]
    async fn duplicate_name_fails_boot() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(RecorderPlugin {
            name: "duplicate",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let second = Arc::new(RecorderPlugin {
            name: "duplicate",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&first) as Arc<dyn PluginPort>,
                Arc::clone(&second) as Arc<dyn PluginPort>,
            ],
            vec![],
        );

        let err = kernel.boot().expect_err("distinct instances with one name must fail boot");
        assert!(matches!(err, PluginError::Invalid(_)), "unexpected error: {err:?}");

        let entries = log.lock().clone();
        assert!(entries.is_empty(), "validation must run no lifecycle hooks: {entries:?}");
    }

    /// Boot rollback: a `start` failure stops the already-started plugins in
    /// reverse order before the error propagates - the failed plugin itself
    /// is never stopped (it never started).
    #[tokio::test]
    async fn boot_failure_rolls_back_started_plugins() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let healthy = Arc::new(RecorderPlugin {
            name: "healthy",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let failing = Arc::new(FailingStartPlugin { name: "failing", log: Arc::clone(&log) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&healthy) as Arc<dyn PluginPort>,
                Arc::clone(&failing) as Arc<dyn PluginPort>,
            ],
            vec![],
        );

        let err = kernel.boot().expect_err("boot must propagate the start failure");
        assert!(matches!(err, PluginError::Start(_)), "unexpected error: {err:?}");

        let entries = log.lock().clone();
        assert_eq!(
            entries,
            vec!["stop:healthy".to_owned()],
            "the started plugin must be rolled back, the failed one must not be stopped"
        );
    }

    /// A failed boot must not poison the kernel: the one-shot guard rolls
    /// back, so a retry genuinely boots instead of silently reporting
    /// success over an unbooted kernel.
    #[tokio::test]
    async fn failed_boot_can_be_retried() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let flaky =
            Arc::new(OnceFailingStartPlugin { name: "flaky", attempts: Arc::clone(&attempts) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&flaky) as Arc<dyn PluginPort>],
            vec![],
        );

        kernel.boot().expect_err("first boot must fail");
        kernel.boot().expect("retry after a failed boot must genuinely boot");

        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "start must have run on both attempts"
        );
        assert_eq!(
            kernel.started.lock().len(),
            1,
            "the retried boot must register the started plugin"
        );
    }

    #[tokio::test]
    async fn pipeline_hands_plugin_guild_scoped_storage() {
        let storage = Arc::new(InMemoryStorage::new());
        // Guild 1 is greeted by config; guild 2 has none and gets the default.
        storage.seed("test", GuildId(1), "greeter", "greeting", json!("privit"));

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

    /// The explicit `shutdown()` plus the `Drop` fallback must stop plugins
    /// exactly once - `PluginPort::stop` is not required to be re-entrant.
    #[test]
    fn shutdown_is_idempotent() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let plugin = Arc::new(RecorderPlugin {
            name: "p",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&plugin) as Arc<dyn PluginPort>],
            vec![],
        );
        kernel.boot().expect("boot expected to succeed");

        kernel.shutdown();
        drop(kernel); // Drop runs shutdown again - a no-op by the guard.

        let entries = log.lock().clone();
        assert_eq!(entries, vec!["stop:p".to_owned()], "plugins must be stopped exactly once");
    }

    /// After a rolled-back boot (a later plugin's `start` failed), neither
    /// the explicit shutdown nor `Drop` may stop the already-rolled-back
    /// plugins a second time - and never-stated ones are not stopped at all.
    #[test]
    fn rollback_then_drop_never_double_stops() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let fine =
            Arc::new(LifecyclePlugin { name: "fine", log: Arc::clone(&log), fail_init: false });
        let broken = Arc::new(FailingStartPlugin { name: "broken", log: Arc::clone(&log) });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&fine) as Arc<dyn PluginPort>,
                Arc::clone(&broken) as Arc<dyn PluginPort>,
            ],
            vec![],
        );

        assert!(kernel.boot().is_err());
        drop(kernel);

        assert_eq!(
            log.lock().clone(),
            vec!["init:fine".to_owned(), "start:fine".to_owned(), "stop:fine".to_owned(),],
            "the rolled-back plugin is stopped exactly once by the rollback; 
             the never-started one is never stopped"
        );
    }

    /// `Abort` is the hard stop: remaining `pre` hooks are skipped AND no
    /// `post` runs at all - unlike `Stop`, which still runs the posts of the
    /// plugins that ran.
    #[tokio::test]
    async fn abort_skips_remaining_pre_and_all_post() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(RecorderPlugin {
            name: "first",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let aborter = Arc::new(AbortingPlugin { log: Arc::clone(&log) });
        let last = Arc::new(RecorderPlugin {
            name: "last",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&first) as Arc<dyn PluginPort>,
                Arc::clone(&aborter) as Arc<dyn PluginPort>,
                Arc::clone(&last) as Arc<dyn PluginPort>,
            ],
            vec![
                Arc::clone(&first) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&aborter) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&last) as Arc<dyn MiddlewarePluginPort>,
            ],
        );

        kernel.handle(RequestContext::message_received(test_origin(), "hi")).await;

        assert_eq!(
            log.lock().clone(),
            vec!["pre:first".to_owned(), "pre:aborter".to_owned()],
            "pre after the Abort and every post must be skipped"
        );
        assert!(output.messages().is_empty());
    }

    /// Two-phase lifecycle: every plugin initializes before any starts.
    #[test]
    fn init_runs_for_all_plugins_before_any_start() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let a = Arc::new(LifecyclePlugin { name: "a", log: Arc::clone(&log), fail_init: false });
        let b = Arc::new(LifecyclePlugin { name: "b", log: Arc::clone(&log), fail_init: false });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&a) as Arc<dyn PluginPort>, Arc::clone(&b) as Arc<dyn PluginPort>],
            vec![],
        );

        kernel.boot().expect("boot expected to succeed");

        assert_eq!(
            log.lock().clone(),
            vec![
                "init:a".to_owned(),
                "init:b".to_owned(),
                "start:a".to_owned(),
                "start:b".to_owned(),
            ],
            "every init must precede any start"
        );
    }

    /// An `init` failure fails boot with nothing started: no `start` ran, so
    /// no rollback (stop) is owed either.
    #[test]
    fn init_failure_fails_boot_without_starting_anything() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let healthy =
            Arc::new(LifecyclePlugin { name: "healthy", log: Arc::clone(&log), fail_init: false });
        let broken =
            Arc::new(LifecyclePlugin { name: "broken", log: Arc::clone(&log), fail_init: true });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&healthy) as Arc<dyn PluginPort>,
                Arc::clone(&broken) as Arc<dyn PluginPort>,
            ],
            vec![],
        );

        let result = kernel.boot();

        assert!(matches!(result, Err(PluginError::Init(_))));
        assert_eq!(
            log.lock().clone(),
            vec!["init:healthy".to_owned(), "init:broken".to_owned()],
            "no start and no stop may run after an init failure"
        );
        // Dropping the failed kernel must not stop plugins that never
        // started: a plugin owes a stop only after its own start.
        drop(kernel);
        assert_eq!(
            log.lock().clone(),
            vec!["init:healthy".to_owned(), "init:broken".to_owned()],
            "Drop after a failed boot must stop nothing"
        );
    }

    /// DM events carry no guild storage handle: a plugin reading its config
    /// via the scoped handle must degrade (Continue), not answer from any
    /// guild's data - there is none.
    #[tokio::test]
    async fn dm_origin_gets_no_guild_storage() {
        let greeter = Arc::new(GreeterPlugin);
        let pong =
            Arc::new(PongPlugin { started: Arc::new(std::sync::atomic::AtomicUsize::new(0)) });
        let (kernel, output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![
                Arc::clone(&greeter) as Arc<dyn PluginPort>,
                Arc::clone(&pong) as Arc<dyn PluginPort>,
            ],
            vec![
                Arc::clone(&greeter) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&pong) as Arc<dyn MiddlewarePluginPort>,
            ],
        );
        let dm_origin = Origin { guild_id: None, ..test_origin() };

        kernel.handle(RequestContext::message_received(dm_origin, "ping")).await;

        // The greeter had no storage handle to read a greeting from and
        // continued; only the pong plugin answered. With a guild origin the
        // greeter would have replied "hello" and stopped the chain.
        assert_eq!(output.messages(), ["pong"]);
    }

    /// A middleware step that merely shares a registered plugin's name but
    /// is a distinct instance is rejected: it would intercept events with no
    /// lifecycle behind it - the exact shape boot exists to prevent.
    #[test]
    fn middleware_masquerading_under_registered_name_fails_boot() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registered = Arc::new(RecorderPlugin {
            name: "x",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });
        let impostor = Arc::new(RecorderPlugin {
            name: "x",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&registered) as Arc<dyn PluginPort>],
            vec![Arc::clone(&impostor) as Arc<dyn MiddlewarePluginPort>],
        );

        let result = kernel.boot();
        assert!(
            matches!(result, Err(PluginError::Invalid(_))),
            "a name-shared impostor instance must fail boot validation"
        );
    }

    /// The same instance listed twice in the middleware chain would run
    /// every event through it twice - boot refuses the configuration.
    #[test]
    fn duplicate_middleware_entry_fails_boot() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let plugin = Arc::new(RecorderPlugin {
            name: "x",
            log: Arc::clone(&log),
            panic_in_pre: false,
            panic_in_post: false,
        });

        let (kernel, _output) = test_kernel(
            Arc::new(InMemoryStorage::new()),
            vec![Arc::clone(&plugin) as Arc<dyn PluginPort>],
            vec![
                Arc::clone(&plugin) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&plugin) as Arc<dyn MiddlewarePluginPort>,
            ],
        );

        let result = kernel.boot();
        assert!(
            matches!(result, Err(PluginError::Invalid(_))),
            "a duplicated middleware entry must fail boot validation"
        );
    }
}
