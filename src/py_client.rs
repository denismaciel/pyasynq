use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

use crate::client::{self, EnqueueOptions, TaskInfo};
use crate::rdb::{self, Rdb};
use crate::runtime;

create_exception!(
    _core,
    AsynqError,
    PyException,
    "Base class for pyasynq errors."
);
create_exception!(
    _core,
    DuplicateTaskError,
    AsynqError,
    "A task with the same uniqueness lock already exists."
);
create_exception!(
    _core,
    TaskIDConflictError,
    AsynqError,
    "A task with the given task_id already exists."
);
create_exception!(_core, RedisError, AsynqError, "Redis command failed.");

fn rdb_err(e: rdb::Error) -> PyErr {
    match e {
        rdb::Error::DuplicateTask => DuplicateTaskError::new_err(e.to_string()),
        rdb::Error::TaskIdConflict => TaskIDConflictError::new_err(e.to_string()),
        rdb::Error::Redis(_) => RedisError::new_err(e.to_string()),
    }
}

fn client_err(e: client::Error) -> PyErr {
    match e {
        client::Error::InvalidArgument(msg) => PyValueError::new_err(msg),
        client::Error::Rdb(e) => rdb_err(e),
    }
}

#[pyclass(name = "TaskInfo", module = "pyasynq._core", frozen)]
pub struct PyTaskInfo(TaskInfo);

#[pymethods]
impl PyTaskInfo {
    #[getter]
    fn id(&self) -> &str {
        &self.0.msg.id
    }
    #[getter]
    fn queue(&self) -> &str {
        &self.0.msg.queue
    }
    #[getter]
    fn r#type(&self) -> &str {
        &self.0.msg.r#type
    }
    #[getter]
    fn payload(&self) -> Vec<u8> {
        self.0.msg.payload.clone()
    }
    #[getter]
    fn headers(&self) -> HashMap<String, String> {
        self.0.msg.headers.clone()
    }
    #[getter]
    fn state(&self) -> &'static str {
        self.0.state.as_str()
    }
    #[getter]
    fn max_retry(&self) -> i32 {
        self.0.msg.retry
    }
    #[getter]
    fn retried(&self) -> i32 {
        self.0.msg.retried
    }
    #[getter]
    fn last_err(&self) -> Option<&str> {
        Some(self.0.msg.error_msg.as_str()).filter(|s| !s.is_empty())
    }
    #[getter]
    fn timeout(&self) -> Option<Duration> {
        positive_secs(self.0.msg.timeout)
    }
    #[getter]
    fn deadline(&self) -> Option<DateTime<Utc>> {
        Some(self.0.msg.deadline)
            .filter(|&t| t > 0)
            .and_then(|t| DateTime::from_timestamp(t, 0))
    }
    #[getter]
    fn group(&self) -> Option<&str> {
        Some(self.0.msg.group_key.as_str()).filter(|s| !s.is_empty())
    }
    #[getter]
    fn retention(&self) -> Option<Duration> {
        positive_secs(self.0.msg.retention)
    }
    #[getter]
    fn next_process_at(&self) -> Option<DateTime<Utc>> {
        self.0.next_process_at
    }

    fn __repr__(&self) -> String {
        format!(
            "TaskInfo(id={:?}, type={:?}, queue={:?}, state={:?})",
            self.0.msg.id,
            self.0.msg.r#type,
            self.0.msg.queue,
            self.0.state.as_str()
        )
    }
}

fn positive_secs(secs: i64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs as u64))
}

/// Enqueues tasks. Safe to share between threads.
#[pyclass(name = "Client", module = "pyasynq._core", frozen)]
pub struct PyClient {
    rdb: Arc<Rdb>,
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (url = "redis://127.0.0.1:6379/0"))]
    fn new(py: Python<'_>, url: &str) -> PyResult<Self> {
        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        Ok(Self { rdb: Arc::new(rdb) })
    }

    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        runtime::block_on(py, self.rdb.ping()).map_err(rdb_err)
    }

    /// Enqueues a task. `process_at`/`process_in` in the future schedule it
    /// instead of making it pending right away.
    #[pyo3(signature = (
        task_type, payload = None, *, queue = None, max_retry = None, task_id = None,
        timeout = None, deadline = None, unique = None, process_at = None,
        process_in = None, retention = None, group = None, headers = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        py: Python<'_>,
        task_type: String,
        payload: Option<Vec<u8>>,
        queue: Option<String>,
        max_retry: Option<i32>,
        task_id: Option<String>,
        timeout: Option<Duration>,
        deadline: Option<DateTime<Utc>>,
        unique: Option<Duration>,
        process_at: Option<DateTime<Utc>>,
        process_in: Option<Duration>,
        retention: Option<Duration>,
        group: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<PyTaskInfo> {
        let process_at = match (process_at, process_in) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "pass either process_at or process_in, not both",
                ))
            }
            (Some(t), None) => Some(t),
            (None, Some(d)) => Some(
                Utc::now()
                    + chrono::Duration::from_std(d)
                        .map_err(|_| PyValueError::new_err("process_in is too large"))?,
            ),
            (None, None) => None,
        };
        let opts = EnqueueOptions {
            queue,
            max_retry,
            task_id,
            timeout,
            deadline,
            unique_ttl: unique,
            process_at,
            retention,
            group,
            headers: headers.unwrap_or_default(),
        };
        let info = runtime::block_on(
            py,
            client::enqueue(&self.rdb, &task_type, payload.as_deref(), &opts),
        )
        .map_err(client_err)?;
        Ok(PyTaskInfo(info))
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add_class::<PyClient>()?;
    m.add_class::<PyTaskInfo>()?;
    m.add("AsynqError", py.get_type::<AsynqError>())?;
    m.add("DuplicateTaskError", py.get_type::<DuplicateTaskError>())?;
    m.add("TaskIDConflictError", py.get_type::<TaskIDConflictError>())?;
    m.add("RedisError", py.get_type::<RedisError>())?;
    Ok(())
}
