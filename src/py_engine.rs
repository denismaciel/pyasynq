use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::IntoPyObjectExt;

use crate::aio::future_into_py;
use crate::base::TaskMessage;
use crate::engine::{
    default_retry_delay, Config, DefaultPolicy, Engine, Event, FailurePolicy, Outcome,
};
use crate::py_base::PyTaskMessage;
use crate::py_errors::rdb_err;
use crate::rdb::Rdb;
use crate::runtime;

/// Calls `policy(retried, error, msg) -> (delay_seconds, is_failure)` for
/// failures the engine records without Python's help (lease recovery,
/// cancel fallback).
struct PyPolicy(Py<PyAny>);

impl FailurePolicy for PyPolicy {
    fn decide(&self, msg: &TaskMessage, err: &str) -> (Duration, bool) {
        let res = Python::attach(|py| -> PyResult<(f64, bool)> {
            let m = PyTaskMessage::from(msg.clone());
            self.0.call1(py, (msg.retried, err, m))?.extract(py)
        });
        match res {
            Ok((secs, is_failure)) => (Duration::from_secs_f64(secs.max(0.0)), is_failure),
            Err(e) => {
                log::error!(target: "pyasynq.worker", "failure policy raised: {e}; using default");
                (default_retry_delay(msg.retried), true)
            }
        }
    }
}

struct Events(Vec<Event>);

impl<'py> IntoPyObject<'py> for Events {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let out = PyList::empty(py);
        for ev in self.0 {
            let item = match ev {
                Event::Job { msg, deadline } => {
                    ("job", PyTaskMessage::from(msg), deadline).into_bound_py_any(py)?
                }
                Event::Cancel { id, reason } => {
                    ("cancel", id, reason.as_str()).into_bound_py_any(py)?
                }
                Event::Aggregate {
                    queue,
                    group,
                    set_id,
                    msgs,
                } => {
                    let msgs: Vec<PyTaskMessage> = msgs.into_iter().map(Into::into).collect();
                    ("aggregate", queue, group, set_id, msgs).into_bound_py_any(py)?
                }
                Event::Health(err) => ("health", err).into_bound_py_any(py)?,
                Event::Closed => ("closed",).into_bound_py_any(py)?,
            };
            out.append(item)?;
        }
        Ok(out.into_any())
    }
}

fn secs(v: f64, name: &str) -> PyResult<Duration> {
    Duration::try_from_secs_f64(v)
        .map_err(|_| PyValueError::new_err(format!("{name} must be >= 0")))
}

/// The Rust worker engine. Python drives it: start it, pull events, ack
/// jobs, shut it down. See `pyasynq.worker` for the user-facing API.
#[pyclass(name = "Engine", module = "pyasynq._core", frozen, subclass)]
pub struct PyEngine {
    engine: Arc<Engine>,
}

#[pymethods]
impl PyEngine {
    #[new]
    #[pyo3(signature = (
        url = "redis://127.0.0.1:6379/0",
        *,
        concurrency = None,
        queues = None,
        strict_priority = false,
        task_check_interval = 1.0,
        delayed_task_check_interval = 5.0,
        shutdown_timeout = 8.0,
        health_check_interval = 15.0,
        heartbeat_interval = 5.0,
        lease_duration = 30.0,
        recoverer_interval = 60.0,
        recoverer_skew = 30.0,
        janitor_interval = 8.0,
        janitor_batch_size = 100,
        sync_interval = 5.0,
        cancel_grace = 5.0,
        group_aggregation = false,
        group_grace_period = 60.0,
        group_max_delay = 0.0,
        group_max_size = 0,
        failure_policy = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        url: &str,
        concurrency: Option<usize>,
        queues: Option<HashMap<String, u32>>,
        strict_priority: bool,
        task_check_interval: f64,
        delayed_task_check_interval: f64,
        shutdown_timeout: f64,
        health_check_interval: f64,
        heartbeat_interval: f64,
        lease_duration: f64,
        recoverer_interval: f64,
        recoverer_skew: f64,
        janitor_interval: f64,
        janitor_batch_size: usize,
        sync_interval: f64,
        cancel_grace: f64,
        group_aggregation: bool,
        group_grace_period: f64,
        group_max_delay: f64,
        group_max_size: usize,
        failure_policy: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut cfg = Config::default();
        if let Some(c) = concurrency {
            if c == 0 {
                return Err(PyValueError::new_err("concurrency must be > 0"));
            }
            cfg.concurrency = c;
        }
        if let Some(q) = queues {
            let mut q: Vec<(String, u32)> = q.into_iter().filter(|(_, w)| *w > 0).collect();
            for (name, _) in &q {
                crate::base::validate_queue_name(name).map_err(PyValueError::new_err)?;
            }
            if q.is_empty() {
                q = Config::default().queues;
            }
            q.sort();
            cfg.queues = q;
        }
        cfg.strict_priority = strict_priority;
        cfg.task_check_interval = secs(task_check_interval, "task_check_interval")?;
        cfg.delayed_task_check_interval =
            secs(delayed_task_check_interval, "delayed_task_check_interval")?;
        cfg.shutdown_timeout = secs(shutdown_timeout, "shutdown_timeout")?;
        cfg.health_check_interval = secs(health_check_interval, "health_check_interval")?;
        cfg.heartbeat_interval = secs(heartbeat_interval, "heartbeat_interval")?;
        cfg.lease_duration = secs(lease_duration, "lease_duration")?;
        cfg.recoverer_interval = secs(recoverer_interval, "recoverer_interval")?;
        cfg.recoverer_skew = secs(recoverer_skew, "recoverer_skew")?;
        cfg.janitor_interval = secs(janitor_interval, "janitor_interval")?;
        cfg.janitor_batch_size = janitor_batch_size.max(1);
        cfg.sync_interval = secs(sync_interval, "sync_interval")?;
        cfg.cancel_grace = secs(cancel_grace, "cancel_grace")?;
        cfg.group.enabled = group_aggregation;
        cfg.group.grace_period =
            secs(group_grace_period, "group_grace_period")?.max(Duration::from_secs(1));
        cfg.group.max_delay = secs(group_max_delay, "group_max_delay")?;
        cfg.group.max_size = group_max_size;

        let rdb = runtime::block_on(py, Rdb::connect(url)).map_err(rdb_err)?;
        let policy: Arc<dyn FailurePolicy> = match failure_policy {
            Some(p) => Arc::new(PyPolicy(p)),
            None => Arc::new(DefaultPolicy),
        };
        Ok(Self {
            engine: Arc::new(Engine::new(Arc::new(rdb), cfg, policy)),
        })
    }

    fn _astart<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        future_into_py(py, async move { engine.start().await.map_err(rdb_err) })
    }

    /// Resolves to a list of event tuples, or None once the engine closed.
    #[pyo3(signature = (max = 256))]
    fn _anext_events<'py>(&self, py: Python<'py>, max: usize) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        future_into_py(py, async move {
            Ok(engine.next_events(max.max(1)).await.map(Events))
        })
    }

    /// Records a task outcome; never blocks. `kind` is one of "success",
    /// "retry", "archive", "revoke". Returns False if the task is no longer
    /// owned by this worker (e.g. its deadline already failed it).
    #[pyo3(signature = (task_id, kind, *, error = "", retry_in = 0.0, is_failure = true, result = None))]
    fn ack(
        &self,
        task_id: &str,
        kind: &str,
        error: &str,
        retry_in: f64,
        is_failure: bool,
        result: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        let outcome = match kind {
            "success" => Outcome::Success { result },
            "revoke" => Outcome::Revoke,
            "archive" => Outcome::Archive {
                err: error.to_string(),
            },
            "retry" => Outcome::Retry {
                err: error.to_string(),
                retry_at: Utc::now() + secs(retry_in, "retry_in")?,
                is_failure,
            },
            other => return Err(PyValueError::new_err(format!("unknown ack kind {other:?}"))),
        };
        Ok(self.engine.shared.ack(task_id, outcome))
    }

    fn _awrite_result<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        task_id: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rdb = self.engine.shared.rdb.clone();
        future_into_py(py, async move {
            rdb.write_result(&queue, &task_id, &data)
                .await
                .map_err(rdb_err)?;
            Ok(data.len())
        })
    }

    /// Deletes an aggregation set after its aggregated task was enqueued.
    fn _acomplete_aggregation<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        group: String,
        set_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rdb = self.engine.shared.rdb.clone();
        future_into_py(py, async move {
            rdb.delete_aggregation_set(&queue, &group, &set_id)
                .await
                .map_err(rdb_err)
        })
    }

    fn stop(&self) {
        self.engine.stop();
    }

    fn _ashutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        future_into_py(py, async move {
            engine.shutdown().await;
            Ok(())
        })
    }

    #[getter]
    fn status(&self) -> &'static str {
        self.engine.shared.status().as_str()
    }

    #[getter]
    fn server_id(&self) -> String {
        self.engine.shared.cfg.server_id.clone()
    }

    #[getter]
    fn host(&self) -> String {
        self.engine.shared.cfg.host.clone()
    }

    #[getter]
    fn pid(&self) -> u32 {
        self.engine.shared.cfg.pid
    }

    #[getter]
    fn concurrency(&self) -> usize {
        self.engine.shared.cfg.concurrency
    }

    #[getter]
    fn queues(&self) -> HashMap<String, u32> {
        self.engine.shared.cfg.queues.iter().cloned().collect()
    }

    #[getter]
    fn active_count(&self) -> usize {
        self.engine.shared.active_count()
    }
}

/// Default asynq retry delay in seconds for `retried` previous attempts.
#[pyfunction]
fn default_retry_delay_seconds(retried: i32) -> f64 {
    default_retry_delay(retried).as_secs_f64()
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEngine>()?;
    m.add_function(wrap_pyfunction!(default_retry_delay_seconds, m)?)
}
