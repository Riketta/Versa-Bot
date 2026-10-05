/// Driven port identifying the platform this deployment serves. The kernel
/// never enumerates platforms: the values are adapter-owned - the wired chat
/// adapter declares its slug and display name (from one constant), and the
/// kernel only carries, stores, and hands them out.
///
/// The slug is the stable storage/telemetry key: it namespaces guild ids in
/// storage rows and must never change once a deployment has data on disk.
/// The display name is presentation, rendered by plugins in user-facing
/// text (e.g. prompt templates).
///
/// Deployments serve exactly one chat platform per process; if a second
/// adapter is ever wired into the same kernel, this port grows the lookup
/// (identity per origin slug) rather than the kernel growing a taxonomy.
pub trait PlatformInfoPort: Send + Sync + 'static {
    /// Stable storage/telemetry slug ("discord").
    fn slug(&self) -> &'static str;

    /// Presentation name ("Discord") for user-facing text.
    fn display_name(&self) -> &'static str;

    /// Hard cap on one outbound text message as enforced by the platform
    /// (`Some(2000)` on Discord); `None` when the platform declares no cap.
    /// Reply-splitting callers clamp their output to it - chunks beyond the
    /// cap would be rejected outright.
    fn message_limit(&self) -> Option<usize>;

    /// Hard cap on one embed's text payload (the description), as enforced
    /// by the platform (`Some(4096)` on Discord); `None` when the platform
    /// declares no cap. Embed-rendering callers clamp their bodies to it -
    /// an oversized embed is rejected outright.
    fn embed_limit(&self) -> Option<usize>;
}
