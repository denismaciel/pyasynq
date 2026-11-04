//! Bridge from tokio futures to asyncio awaitables.
//!
//! The Rust future runs on a tokio worker thread. When it finishes, we hand
//! the result back to the asyncio loop with `loop.call_soon_threadsafe`,
//! because asyncio futures may only be touched from their loop's thread.

use std::future::Future;
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::IntoPyObjectExt;
use tokio::task::AbortHandle;

use crate::runtime;

/// Wraps `fut` in an `asyncio.Future` bound to the running event loop.
/// Cancelling the asyncio future aborts the tokio task.
pub fn future_into_py<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_fut = event_loop.call_method0("create_future")?;

    let loop_ref = event_loop.unbind();
    let fut_ref = py_fut.clone().unbind();
    let handle = runtime::spawn(async move {
        let result = fut.await;
        Python::attach(|py| {
            let result = result.and_then(|v| v.into_py_any(py));
            let complete = SetResult(Mutex::new(Some(result)));
            // Fails only if the loop is already closed; nobody is waiting then.
            let _ = loop_ref.call_method1(py, "call_soon_threadsafe", (complete, fut_ref));
        });
    });

    py_fut.call_method1("add_done_callback", (AbortOnCancel(handle.abort_handle()),))?;
    Ok(py_fut)
}

/// Runs on the asyncio thread and resolves the future, unless it was
/// cancelled in the meantime.
#[pyclass]
struct SetResult(Mutex<Option<PyResult<Py<PyAny>>>>);

#[pymethods]
impl SetResult {
    fn __call__(&self, fut: &Bound<'_, PyAny>) -> PyResult<()> {
        if fut.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        match self.0.lock().unwrap().take() {
            Some(Ok(value)) => fut.call_method1("set_result", (value,))?,
            Some(Err(err)) => fut.call_method1("set_exception", (err.into_value(fut.py()),))?,
            None => return Ok(()),
        };
        Ok(())
    }
}

#[pyclass]
struct AbortOnCancel(AbortHandle);

#[pymethods]
impl AbortOnCancel {
    #[pyo3(signature = (*args))]
    fn __call__(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        if args.get_item(0)?.call_method0("cancelled")?.is_truthy()? {
            self.0.abort();
        }
        Ok(())
    }
}
