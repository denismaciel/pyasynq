use std::future::Future;
use std::sync::LazyLock;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// One tokio runtime per process, created on first use. Its worker threads
/// never touch Python objects.
static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("pyasynq-tokio")
        .build()
        .expect("failed to start tokio runtime")
});

pub fn spawn<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    RUNTIME.spawn(fut)
}

/// Runs `fut` to completion on the runtime while the calling thread has
/// released the GIL, so other Python threads keep running meanwhile.
pub fn block_on<F>(py: Python<'_>, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    py.detach(|| RUNTIME.block_on(fut))
}
