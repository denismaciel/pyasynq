use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::aio::future_into_py;
use crate::py_base::Secs;
use crate::py_client::{enqueue_request, PyTaskInfo};
use crate::py_errors::rdb_err;
use crate::rdb::Rdb;
use crate::runtime;
use crate::scheduler::{PostEnqueue, Scheduler};

/// Enqueues tasks periodically. Entries are visible to asynq's Inspector
/// and asynqmon; run one scheduler per set of entries.
#[pyclass(name = "Scheduler", module = "pyasynq._core", frozen, subclass)]
pub struct PyScheduler {
    inner: Arc<Scheduler>,
}

#[pymethods]
impl PyScheduler {
    #[new]
    #[pyo3(signature = (url = "redis://127.0.0.1:6379/0", *, location = "UTC", heartbeat_interval = 10.0, post_enqueue = None))]
    fn new(
        py: Python<'_>,
        url: &str,
        location: &str,
        heartbeat_interval: f64,
        post_enqueue: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let tz = Tz::from_str(location).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let heartbeat = Duration::try_from_secs_f64(heartbeat_interval)
            .ok()
            .filter(|d| !d.is_zero())
            .ok_or_else(|| PyValueError::new_err("heartbeat_interval must be > 0"))?;
        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        let cb: Option<PostEnqueue> = post_enqueue.map(|f| {
            Arc::new(
                move |entry_id: &str, res: Result<&crate::client::TaskInfo, String>| {
                    Python::attach(|py| {
                        let (info, err) = match res {
                            Ok(i) => (Some(PyTaskInfo(i.clone())), None),
                            Err(e) => (None, Some(e)),
                        };
                        if let Err(e) = f.call1(py, (entry_id, info, err)) {
                            e.print(py);
                        }
                    })
                },
            ) as PostEnqueue
        });
        Ok(Self {
            inner: Arc::new(Scheduler::new(Arc::new(rdb), tz, heartbeat, cb)),
        })
    }

    #[getter]
    fn id(&self) -> String {
        self.inner.id.clone()
    }

    /// Registers a periodic task; returns the entry id. Options are the
    /// same as `Client.enqueue` (except process_at/process_in).
    #[pyo3(signature = (
        cronspec, task_type, payload = None, *, queue = None, max_retry = None,
        task_id = None, timeout = None, deadline = None, unique = None,
        retention = None, group = None, headers = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn register(
        &self,
        cronspec: &str,
        task_type: String,
        payload: Option<Vec<u8>>,
        queue: Option<String>,
        max_retry: Option<i32>,
        task_id: Option<String>,
        timeout: Option<Secs>,
        deadline: Option<DateTime<Utc>>,
        unique: Option<Secs>,
        retention: Option<Secs>,
        group: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let req = enqueue_request(
            task_type.clone(),
            payload.clone(),
            queue,
            max_retry,
            task_id,
            timeout,
            deadline,
            unique,
            None,
            None,
            retention,
            group,
            headers,
        )?;
        self.inner
            .register(cronspec, &task_type, payload.unwrap_or_default(), req.opts)
            .map_err(PyValueError::new_err)
    }

    fn unregister(&self, entry_id: &str) -> bool {
        self.inner.unregister(entry_id)
    }

    fn _arun<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { inner.run().await.map_err(rdb_err) })
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyScheduler>()
}
