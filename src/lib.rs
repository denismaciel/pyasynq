use pyo3::prelude::*;

mod aio;
pub mod base;
pub mod client;
pub mod engine;
mod py_base;
mod py_broker;
mod py_client;
mod py_engine;
mod py_errors;
mod py_inspector;
mod py_scheduler;
pub mod rdb;
mod runtime;
pub mod scheduler;

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

static LOG_RESET: std::sync::OnceLock<Option<pyo3_log::ResetHandle>> = std::sync::OnceLock::new();

/// Rust log records go to Python `logging` (loggers "pyasynq.*"). Levels
/// are cached; call this after reconfiguring logging.
#[pyfunction]
fn reset_log_cache() {
    if let Some(Some(h)) = LOG_RESET.get() {
        h.reset();
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    LOG_RESET.get_or_init(|| pyo3_log::try_init().ok());
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(reset_log_cache, m)?)?;
    py_engine::register(m)?;
    py_errors::register(m)?;
    py_base::register(m)?;
    py_client::register(m)?;
    py_broker::register(m)?;
    py_inspector::register(m)?;
    py_scheduler::register(m)?;
    Ok(())
}
