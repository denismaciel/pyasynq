use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::base::{self, TaskState};
use crate::client::{self, EnqueueOptions, TaskInfo};
use crate::py_base::Secs;
use crate::py_errors::rdb_err;
use crate::rdb::Rdb;
use crate::{aio, runtime};

fn client_err(e: client::Error) -> PyErr {
    match e {
        client::Error::InvalidArgument(msg) => PyValueError::new_err(msg),
        client::Error::Rdb(e) => rdb_err(e),
    }
}

#[pyclass(name = "TaskInfo", module = "pyasynq._core", frozen)]
pub struct PyTaskInfo(pub(crate) TaskInfo);

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
    /// Bytes written by the handler, while the task is retained.
    #[getter]
    fn result(&self) -> Option<Vec<u8>> {
        self.0.result.clone()
    }
    #[getter]
    fn completed_at(&self) -> Option<DateTime<Utc>> {
        Some(self.0.msg.completed_at)
            .filter(|&t| t > 0)
            .and_then(|t| DateTime::from_timestamp(t, 0))
    }
    #[getter]
    fn last_failed_at(&self) -> Option<DateTime<Utc>> {
        Some(self.0.msg.last_failed_at)
            .filter(|&t| t > 0)
            .and_then(|t| DateTime::from_timestamp(t, 0))
    }

    /// `TaskInfo[R]` in annotations names the task's decoded result type.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let alias = cls.py().import("types")?.getattr("GenericAlias")?;
        alias.call1((cls, item))
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

/// Arguments shared by `enqueue` and `aenqueue`, already validated.
pub(crate) struct EnqueueRequest {
    task_type: String,
    payload: Option<Vec<u8>>,
    pub(crate) opts: EnqueueOptions,
}

impl EnqueueRequest {
    async fn run(self, rdb: &Rdb) -> PyResult<PyTaskInfo> {
        client::enqueue(rdb, &self.task_type, self.payload.as_deref(), &self.opts)
            .await
            .map(PyTaskInfo)
            .map_err(client_err)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn enqueue_request(
    task_type: String,
    payload: Option<Vec<u8>>,
    queue: Option<String>,
    max_retry: Option<i32>,
    task_id: Option<String>,
    timeout: Option<Secs>,
    deadline: Option<DateTime<Utc>>,
    unique: Option<Secs>,
    process_at: Option<DateTime<Utc>>,
    process_in: Option<Secs>,
    retention: Option<Secs>,
    group: Option<String>,
    headers: Option<HashMap<String, String>>,
) -> PyResult<EnqueueRequest> {
    let (timeout, unique, retention) = (
        timeout.map(|s| s.0),
        unique.map(|s| s.0),
        retention.map(|s| s.0),
    );
    let process_at = match (process_at, process_in.map(|s| s.0)) {
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
    Ok(EnqueueRequest {
        task_type,
        payload,
        opts: EnqueueOptions {
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
        },
    })
}

/// Enqueues tasks. Safe to share between threads and event loops.
#[pyclass(name = "Client", module = "pyasynq._core", frozen, subclass)]
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

    fn _aping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rdb = self.rdb.clone();
        aio::future_into_py(py, async move { rdb.ping().await.map_err(rdb_err) })
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
        timeout: Option<Secs>,
        deadline: Option<DateTime<Utc>>,
        unique: Option<Secs>,
        process_at: Option<DateTime<Utc>>,
        process_in: Option<Secs>,
        retention: Option<Secs>,
        group: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<PyTaskInfo> {
        let req = enqueue_request(
            task_type, payload, queue, max_retry, task_id, timeout, deadline, unique, process_at,
            process_in, retention, group, headers,
        )?;
        runtime::block_on(py, req.run(&self.rdb))
    }

    /// Returns an `asyncio.Future`; `pyasynq.Client.aenqueue` wraps it in a
    /// coroutine.
    #[pyo3(signature = (
        task_type, payload = None, *, queue = None, max_retry = None, task_id = None,
        timeout = None, deadline = None, unique = None, process_at = None,
        process_in = None, retention = None, group = None, headers = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn _aenqueue<'py>(
        &self,
        py: Python<'py>,
        task_type: String,
        payload: Option<Vec<u8>>,
        queue: Option<String>,
        max_retry: Option<i32>,
        task_id: Option<String>,
        timeout: Option<Secs>,
        deadline: Option<DateTime<Utc>>,
        unique: Option<Secs>,
        process_at: Option<DateTime<Utc>>,
        process_in: Option<Secs>,
        retention: Option<Secs>,
        group: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = enqueue_request(
            task_type, payload, queue, max_retry, task_id, timeout, deadline, unique, process_at,
            process_in, retention, group, headers,
        )?;
        let rdb = self.rdb.clone();
        aio::future_into_py(py, async move { req.run(&rdb).await })
    }
}

type PgRow = (
    String,
    String,
    &'static str,
    Vec<u8>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
    Option<DateTime<Utc>>,
);

/// The `INSERT` that enqueues a task on the Postgres backend, for running
/// on the application's own connection inside its transaction. Returns the
/// SQL (`%s` or `$n` placeholders), its parameters, and the task's info.
#[pyfunction]
#[pyo3(signature = (
    task_type, payload = None, *, queue = None, max_retry = None, task_id = None,
    timeout = None, deadline = None, process_at = None, process_in = None,
    retention = None, group = None, headers = None, paramstyle = "format",
))]
#[allow(clippy::too_many_arguments)]
fn pg_enqueue_statement(
    task_type: String,
    payload: Option<Vec<u8>>,
    queue: Option<String>,
    max_retry: Option<i32>,
    task_id: Option<String>,
    timeout: Option<Secs>,
    deadline: Option<DateTime<Utc>>,
    process_at: Option<DateTime<Utc>>,
    process_in: Option<Secs>,
    retention: Option<Secs>,
    group: Option<String>,
    headers: Option<HashMap<String, String>>,
    paramstyle: &str,
) -> PyResult<(String, PgRow, PyTaskInfo)> {
    let req = enqueue_request(
        task_type, payload, queue, max_retry, task_id, timeout, deadline, None, process_at,
        process_in, retention, group, headers,
    )?;
    let msg = client::build_message(&req.task_type, req.payload.as_deref(), &req.opts)
        .map_err(client_err)?;
    let now = Utc::now();
    let (state, pending_since, process_at, grouped_at) = match req.opts.process_at {
        Some(t) if t > now => (TaskState::Scheduled, None, Some(t), None),
        _ if req.opts.group.is_some() => (TaskState::Aggregating, None, None, Some(now)),
        _ => (TaskState::Pending, Some(now), None, None),
    };
    let marks: Vec<String> = match paramstyle {
        "format" => vec!["%s".into(); 8],
        "numeric" => (1..=8).map(|i| format!("${i}")).collect(),
        _ => {
            return Err(PyValueError::new_err(
                "paramstyle must be \"format\" (psycopg) or \"numeric\" (asyncpg)",
            ))
        }
    };
    let sql = format!(
        "INSERT INTO pyasynq_tasks \
         (queue, id, state, msg, pending_since, process_at, group_key, grouped_at) \
         VALUES ({})",
        marks.join(", ")
    );
    let row = (
        msg.queue.clone(),
        msg.id.clone(),
        state.as_str(),
        base::encode_message(&msg),
        pending_since,
        process_at,
        req.opts.group.clone(),
        grouped_at,
    );
    let info = TaskInfo {
        msg,
        state,
        next_process_at: process_at.or(pending_since),
        result: None,
    };
    Ok((sql, row, PyTaskInfo(info)))
}

/// The Postgres schema as one SQL script.
#[pyfunction]
fn pg_schema() -> String {
    crate::rdb::pg::migration_sql()
}

/// Drops the in-process store behind a `memory://` URL, or every store.
#[pyfunction]
#[pyo3(signature = (url = None))]
fn memory_reset(url: Option<&str>) {
    crate::rdb::memory::reset(url);
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyClient>()?;
    m.add_class::<PyTaskInfo>()?;
    m.add_function(wrap_pyfunction!(pg_enqueue_statement, m)?)?;
    m.add_function(wrap_pyfunction!(pg_schema, m)?)?;
    m.add_function(wrap_pyfunction!(memory_reset, m)?)?;
    Ok(())
}
