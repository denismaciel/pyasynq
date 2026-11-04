use pyo3::prelude::*;

mod aio;
pub mod base;
pub mod client;
mod py_base;
mod py_client;
pub mod rdb;
mod runtime;

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    py_base::register(m)?;
    py_client::register(m)?;
    Ok(())
}
