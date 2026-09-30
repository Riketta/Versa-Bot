pub trait InspectTrace {
    fn inspect_trace(self) -> Self;
}

impl<T, E> InspectTrace for Result<T, E>
where
    E: std::fmt::Debug,
{
    #[track_caller]
    fn inspect_trace(self) -> Self {
        self.inspect_err(|e| {
            tracing::trace!(target: "app::error", error = ?e, "Result::Err");
        })
    }
}
