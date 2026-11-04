use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use crate::base::TaskMessage;
use crate::py_base::PyTaskMessage;
use crate::py_errors::rdb_err;
use crate::rdb::{Rdb, LEASE_DURATION};
use crate::runtime;

/// Low-level access to the task state transitions the worker performs.
/// Not part of the public API; used by tests and for exploring.
#[pyclass(name = "Broker", module = "pyasynq._core", frozen)]
pub struct PyBroker {
    rdb: Arc<Rdb>,
}

#[pymethods]
impl PyBroker {
    #[new]
    #[pyo3(signature = (url = "redis://127.0.0.1:6379/0"))]
    fn new(py: Python<'_>, url: &str) -> PyResult<Self> {
        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        Ok(Self { rdb: Arc::new(rdb) })
    }

    /// Returns `(message, lease_expires_at)` from the first non-paused,
    /// non-empty queue, or None. `lease` is in seconds.
    #[pyo3(signature = (*queues, lease = None))]
    fn dequeue(
        &self,
        py: Python<'_>,
        queues: Vec<String>,
        lease: Option<f64>,
    ) -> PyResult<Option<(PyTaskMessage, DateTime<Utc>)>> {
        let lease = lease.map_or(LEASE_DURATION, Duration::from_secs_f64);
        let res =
            runtime::block_on(py, self.rdb.dequeue_with_lease(&queues, lease)).map_err(rdb_err)?;
        Ok(res.map(|(msg, lease)| (msg.into(), lease)))
    }

    fn done(&self, py: Python<'_>, msg: PyRef<'_, PyTaskMessage>) -> PyResult<()> {
        let msg: TaskMessage = msg.clone().into();
        runtime::block_on(py, self.rdb.done(&msg)).map_err(rdb_err)
    }

    fn mark_as_complete(&self, py: Python<'_>, msg: PyRef<'_, PyTaskMessage>) -> PyResult<()> {
        let msg: TaskMessage = msg.clone().into();
        runtime::block_on(py, self.rdb.mark_as_complete(&msg)).map_err(rdb_err)
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBroker>()
}
