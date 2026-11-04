use std::sync::Arc;

use chrono::{DateTime, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::aio::future_into_py;
use crate::py_errors::rdb_err;
use crate::rdb::Rdb;
use crate::runtime;

/// Redis-backed counting semaphore shared by all workers (asynq `x/rate`).
#[pyclass(name = "Semaphore", module = "pyasynq._core", frozen, subclass)]
pub struct PySemaphore {
    rdb: Arc<Rdb>,
    #[pyo3(get)]
    scope: String,
    #[pyo3(get)]
    max_tokens: u32,
}

#[pymethods]
impl PySemaphore {
    #[new]
    #[pyo3(signature = (url, scope, max_tokens))]
    fn new(py: Python<'_>, url: &str, scope: String, max_tokens: u32) -> PyResult<Self> {
        if max_tokens < 1 {
            return Err(PyValueError::new_err("max_tokens cannot be less than 1"));
        }
        if scope.trim().is_empty() {
            return Err(PyValueError::new_err("scope should not be empty"));
        }
        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        Ok(Self {
            rdb: Arc::new(rdb),
            scope,
            max_tokens,
        })
    }

    fn _aacquire<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        deadline: DateTime<Utc>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (rdb, scope, max) = (self.rdb.clone(), self.scope.clone(), self.max_tokens);
        future_into_py(py, async move {
            rdb.semaphore_acquire(&scope, max, deadline, &task_id)
                .await
                .map_err(rdb_err)
        })
    }

    fn _arelease<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let (rdb, scope) = (self.rdb.clone(), self.scope.clone());
        future_into_py(py, async move {
            rdb.semaphore_release(&scope, &task_id)
                .await
                .map_err(rdb_err)
        })
    }

    fn acquire_sync(
        &self,
        py: Python<'_>,
        task_id: &str,
        deadline: DateTime<Utc>,
    ) -> PyResult<bool> {
        runtime::block_on(
            py,
            self.rdb
                .semaphore_acquire(&self.scope, self.max_tokens, deadline, task_id),
        )
        .map_err(rdb_err)
    }

    fn release_sync(&self, py: Python<'_>, task_id: &str) -> PyResult<bool> {
        runtime::block_on(py, self.rdb.semaphore_release(&self.scope, task_id)).map_err(rdb_err)
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySemaphore>()
}
