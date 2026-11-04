use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::aio::future_into_py;
use crate::base::{pb, TaskState};
use crate::py_client::PyTaskInfo;
use crate::py_errors::rdb_err;
use crate::rdb::inspect::{DailyStats, QueueStats, ServerDetail};
use crate::rdb::Rdb;
use crate::runtime;

fn ts(t: &Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    t.as_ref()
        .filter(|t| t.seconds > 0)
        .and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos.max(0) as u32).single())
}

fn state(s: &str) -> PyResult<TaskState> {
    TaskState::parse(s).ok_or_else(|| PyValueError::new_err(format!("unknown task state {s:?}")))
}

/// Pages are 1-indexed, like asynq's `Page(n)`.
fn page_index(page: usize) -> PyResult<usize> {
    page.checked_sub(1)
        .ok_or_else(|| PyValueError::new_err("page starts at 1"))
}

#[pyclass(name = "QueueInfo", module = "pyasynq._core", frozen, get_all)]
pub struct PyQueueInfo {
    queue: String,
    memory_usage: i64,
    latency: Duration,
    size: i64,
    groups: i64,
    pending: i64,
    active: i64,
    scheduled: i64,
    retry: i64,
    archived: i64,
    completed: i64,
    aggregating: i64,
    processed: i64,
    failed: i64,
    processed_total: i64,
    failed_total: i64,
    paused: bool,
    timestamp: DateTime<Utc>,
}

impl From<QueueStats> for PyQueueInfo {
    fn from(s: QueueStats) -> Self {
        Self {
            queue: s.queue,
            memory_usage: s.memory_usage,
            latency: s.latency,
            size: s.size,
            groups: s.groups,
            pending: s.pending,
            active: s.active,
            scheduled: s.scheduled,
            retry: s.retry,
            archived: s.archived,
            completed: s.completed,
            aggregating: s.aggregating,
            processed: s.processed,
            failed: s.failed,
            processed_total: s.processed_total,
            failed_total: s.failed_total,
            paused: s.paused,
            timestamp: s.timestamp,
        }
    }
}

#[pymethods]
impl PyQueueInfo {
    fn __repr__(&self) -> String {
        format!(
            "QueueInfo(queue={:?}, size={}, pending={}, active={}, scheduled={}, retry={}, archived={}, completed={}, aggregating={}, paused={})",
            self.queue, self.size, self.pending, self.active, self.scheduled, self.retry,
            self.archived, self.completed, self.aggregating, self.paused
        )
    }
}

#[pyclass(name = "DailyStats", module = "pyasynq._core", frozen, get_all)]
pub struct PyDailyStats {
    queue: String,
    processed: i64,
    failed: i64,
    date: DateTime<Utc>,
}

impl From<DailyStats> for PyDailyStats {
    fn from(d: DailyStats) -> Self {
        Self {
            queue: d.queue,
            processed: d.processed,
            failed: d.failed,
            date: d.date,
        }
    }
}

#[pyclass(name = "WorkerInfo", module = "pyasynq._core", frozen, get_all)]
pub struct PyWorkerInfo {
    task_id: String,
    task_type: String,
    task_payload: Vec<u8>,
    queue: String,
    started: Option<DateTime<Utc>>,
    deadline: Option<DateTime<Utc>>,
}

#[pyclass(name = "ServerInfo", module = "pyasynq._core", frozen, get_all)]
pub struct PyServerInfo {
    id: String,
    host: String,
    pid: i32,
    concurrency: i32,
    queues: HashMap<String, i32>,
    strict_priority: bool,
    status: String,
    started: Option<DateTime<Utc>>,
    active_workers: Vec<Py<PyWorkerInfo>>,
}

#[pymethods]
impl PyServerInfo {
    fn __repr__(&self) -> String {
        format!(
            "ServerInfo(id={:?}, host={:?}, pid={}, status={:?}, active_workers={})",
            self.id,
            self.host,
            self.pid,
            self.status,
            self.active_workers.len()
        )
    }
}

fn server_info(py: Python<'_>, s: ServerDetail) -> PyResult<PyServerInfo> {
    let workers = s
        .workers
        .into_iter()
        .map(|w| {
            Py::new(
                py,
                PyWorkerInfo {
                    started: ts(&w.start_time),
                    deadline: ts(&w.deadline),
                    task_id: w.task_id,
                    task_type: w.task_type,
                    task_payload: w.task_payload,
                    queue: w.queue,
                },
            )
        })
        .collect::<PyResult<_>>()?;
    let i = s.info;
    Ok(PyServerInfo {
        started: ts(&i.start_time),
        id: i.server_id,
        host: i.host,
        pid: i.pid,
        concurrency: i.concurrency,
        queues: i.queues,
        strict_priority: i.strict_priority,
        status: i.status,
        active_workers: workers,
    })
}

#[pyclass(name = "SchedulerEntry", module = "pyasynq._core", frozen, get_all)]
pub struct PySchedulerEntry {
    id: String,
    spec: String,
    task_type: String,
    task_payload: Vec<u8>,
    options: Vec<String>,
    next_enqueue_at: Option<DateTime<Utc>>,
    prev_enqueue_at: Option<DateTime<Utc>>,
}

impl From<pb::SchedulerEntry> for PySchedulerEntry {
    fn from(e: pb::SchedulerEntry) -> Self {
        Self {
            next_enqueue_at: ts(&e.next_enqueue_time),
            prev_enqueue_at: ts(&e.prev_enqueue_time),
            id: e.id,
            spec: e.spec,
            task_type: e.task_type,
            task_payload: e.task_payload,
            options: e.enqueue_options,
        }
    }
}

#[pyclass(
    name = "SchedulerEnqueueEvent",
    module = "pyasynq._core",
    frozen,
    get_all
)]
pub struct PySchedulerEnqueueEvent {
    task_id: String,
    enqueued_at: Option<DateTime<Utc>>,
}

/// Read and manage queues and tasks; the same view asynq's Inspector and
/// asynqmon have, so Go and Python tools can be mixed.
#[pyclass(name = "Inspector", module = "pyasynq._core", frozen, subclass)]
pub struct PyInspector {
    rdb: Arc<Rdb>,
}

#[pymethods]
impl PyInspector {
    #[new]
    #[pyo3(signature = (url = "redis://127.0.0.1:6379/0"))]
    fn new(py: Python<'_>, url: &str) -> PyResult<Self> {
        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        Ok(Self { rdb: Arc::new(rdb) })
    }

    fn queues(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        runtime::block_on(py, self.rdb.all_queues()).map_err(rdb_err)
    }

    fn get_queue_info(&self, py: Python<'_>, queue: &str) -> PyResult<PyQueueInfo> {
        Ok(runtime::block_on(py, self.rdb.current_stats(queue))
            .map_err(rdb_err)?
            .into())
    }

    #[pyo3(signature = (queue, days = 7))]
    fn history(&self, py: Python<'_>, queue: &str, days: usize) -> PyResult<Vec<PyDailyStats>> {
        let stats =
            runtime::block_on(py, self.rdb.historical_stats(queue, days)).map_err(rdb_err)?;
        Ok(stats.into_iter().map(Into::into).collect())
    }

    /// `{group: size}` for the queue's groups.
    fn groups(&self, py: Python<'_>, queue: &str) -> PyResult<HashMap<String, i64>> {
        Ok(runtime::block_on(py, self.rdb.group_stats(queue))
            .map_err(rdb_err)?
            .into_iter()
            .collect())
    }

    fn get_task_info(&self, py: Python<'_>, queue: &str, task_id: &str) -> PyResult<PyTaskInfo> {
        runtime::block_on(py, self.rdb.get_task_info(queue, task_id))
            .map(PyTaskInfo)
            .map_err(rdb_err)
    }

    fn _aget_task_info<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        task_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rdb = self.rdb.clone();
        future_into_py(py, async move {
            rdb.get_task_info(&queue, &task_id)
                .await
                .map(PyTaskInfo)
                .map_err(rdb_err)
        })
    }

    /// Tasks in `state` ("pending", "active", "scheduled", "retry",
    /// "archived", "completed", or "aggregating" with `group`).
    #[pyo3(signature = (queue, state, *, group = None, page = 1, size = 30))]
    fn list_tasks(
        &self,
        py: Python<'_>,
        queue: &str,
        state: &str,
        group: Option<&str>,
        page: usize,
        size: usize,
    ) -> PyResult<Vec<PyTaskInfo>> {
        let (st, page) = (self::state(state)?, page_index(page)?);
        let tasks = runtime::block_on(py, self.rdb.list_tasks(queue, st, group, page, size))
            .map_err(rdb_err)?;
        Ok(tasks.into_iter().map(PyTaskInfo).collect())
    }

    fn delete_task(&self, py: Python<'_>, queue: &str, task_id: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.delete_task(queue, task_id)).map_err(rdb_err)
    }

    fn run_task(&self, py: Python<'_>, queue: &str, task_id: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.run_task(queue, task_id)).map_err(rdb_err)
    }

    fn archive_task(&self, py: Python<'_>, queue: &str, task_id: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.archive_task(queue, task_id)).map_err(rdb_err)
    }

    fn update_task_payload(
        &self,
        py: Python<'_>,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> PyResult<()> {
        runtime::block_on(py, self.rdb.update_task_payload(queue, task_id, &payload))
            .map_err(rdb_err)
    }

    #[pyo3(signature = (queue, state, *, group = None))]
    fn delete_all_tasks(
        &self,
        py: Python<'_>,
        queue: &str,
        state: &str,
        group: Option<&str>,
    ) -> PyResult<i64> {
        let st = self::state(state)?;
        runtime::block_on(py, self.rdb.delete_all(queue, st, group)).map_err(rdb_err)
    }

    #[pyo3(signature = (queue, state, *, group = None))]
    fn run_all_tasks(
        &self,
        py: Python<'_>,
        queue: &str,
        state: &str,
        group: Option<&str>,
    ) -> PyResult<i64> {
        let st = self::state(state)?;
        runtime::block_on(py, self.rdb.run_all(queue, st, group)).map_err(rdb_err)
    }

    #[pyo3(signature = (queue, state, *, group = None))]
    fn archive_all_tasks(
        &self,
        py: Python<'_>,
        queue: &str,
        state: &str,
        group: Option<&str>,
    ) -> PyResult<i64> {
        let st = self::state(state)?;
        runtime::block_on(py, self.rdb.archive_all(queue, st, group)).map_err(rdb_err)
    }

    /// Asks whichever worker runs the task to cancel it (best effort).
    fn cancel_processing(&self, py: Python<'_>, task_id: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.publish_cancelation(task_id)).map_err(rdb_err)
    }

    #[pyo3(signature = (queue, *, force = false))]
    fn delete_queue(&self, py: Python<'_>, queue: &str, force: bool) -> PyResult<()> {
        runtime::block_on(py, self.rdb.remove_queue(queue, force)).map_err(rdb_err)
    }

    fn pause_queue(&self, py: Python<'_>, queue: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.pause(queue)).map_err(rdb_err)
    }

    fn unpause_queue(&self, py: Python<'_>, queue: &str) -> PyResult<()> {
        runtime::block_on(py, self.rdb.unpause(queue)).map_err(rdb_err)
    }

    fn servers(&self, py: Python<'_>) -> PyResult<Vec<PyServerInfo>> {
        let servers = runtime::block_on(py, self.rdb.list_servers()).map_err(rdb_err)?;
        servers.into_iter().map(|s| server_info(py, s)).collect()
    }

    fn scheduler_entries(&self, py: Python<'_>) -> PyResult<Vec<PySchedulerEntry>> {
        let entries = runtime::block_on(py, self.rdb.list_scheduler_entries()).map_err(rdb_err)?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    #[pyo3(signature = (entry_id, *, page = 1, size = 30))]
    fn list_scheduler_enqueue_events(
        &self,
        py: Python<'_>,
        entry_id: &str,
        page: usize,
        size: usize,
    ) -> PyResult<Vec<PySchedulerEnqueueEvent>> {
        let page = page_index(page)?;
        let events = runtime::block_on(
            py,
            self.rdb.list_scheduler_enqueue_events(entry_id, page, size),
        )
        .map_err(rdb_err)?;
        Ok(events
            .into_iter()
            .map(|e| PySchedulerEnqueueEvent {
                enqueued_at: ts(&e.enqueue_time),
                task_id: e.task_id,
            })
            .collect())
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyInspector>()?;
    m.add_class::<PyQueueInfo>()?;
    m.add_class::<PyDailyStats>()?;
    m.add_class::<PyServerInfo>()?;
    m.add_class::<PyWorkerInfo>()?;
    m.add_class::<PySchedulerEntry>()?;
    m.add_class::<PySchedulerEnqueueEvent>()
}
