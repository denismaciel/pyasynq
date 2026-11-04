use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::base;

/// A duration argument: `datetime.timedelta` or a number of seconds.
pub struct Secs(pub std::time::Duration);

impl<'a, 'py> FromPyObject<'a, 'py> for Secs {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(d) = obj.extract::<std::time::Duration>() {
            return Ok(Secs(d));
        }
        let secs: f64 = obj.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("expected a timedelta or a number of seconds")
        })?;
        std::time::Duration::try_from_secs_f64(secs)
            .map(Secs)
            .map_err(|_| PyValueError::new_err("duration must be a non-negative number of seconds"))
    }
}

/// Python view of `base::TaskMessage`, the protobuf stored under the `msg`
/// field of every task hash.
#[pyclass(name = "TaskMessage", module = "pyasynq._core", get_all, set_all, eq)]
#[derive(Clone, PartialEq)]
pub struct PyTaskMessage {
    #[pyo3(name = "type")]
    pub task_type: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub id: String,
    pub queue: String,
    pub retry: i32,
    pub retried: i32,
    pub error_msg: String,
    pub last_failed_at: i64,
    pub timeout: i64,
    pub deadline: i64,
    pub unique_key: String,
    pub group_key: String,
    pub retention: i64,
    pub completed_at: i64,
}

#[pymethods]
impl PyTaskMessage {
    #[new]
    #[pyo3(signature = (
        task_type, payload = Vec::new(), *, headers = HashMap::new(), id = String::new(),
        queue = String::new(), retry = 0, retried = 0, error_msg = String::new(),
        last_failed_at = 0, timeout = 0, deadline = 0, unique_key = String::new(),
        group_key = String::new(), retention = 0, completed_at = 0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_type: String,
        payload: Vec<u8>,
        headers: HashMap<String, String>,
        id: String,
        queue: String,
        retry: i32,
        retried: i32,
        error_msg: String,
        last_failed_at: i64,
        timeout: i64,
        deadline: i64,
        unique_key: String,
        group_key: String,
        retention: i64,
        completed_at: i64,
    ) -> Self {
        Self {
            task_type,
            payload,
            headers,
            id,
            queue,
            retry,
            retried,
            error_msg,
            last_failed_at,
            timeout,
            deadline,
            unique_key,
            group_key,
            retention,
            completed_at,
        }
    }

    fn encode(&self) -> Vec<u8> {
        base::encode_message(&self.clone().into())
    }

    #[staticmethod]
    fn decode(data: &[u8]) -> PyResult<Self> {
        base::decode_message(data)
            .map(Into::into)
            .map_err(|e| PyValueError::new_err(format!("invalid TaskMessage: {e}")))
    }

    fn __repr__(&self) -> String {
        format!(
            "TaskMessage(id={:?}, type={:?}, queue={:?}, retried={}/{})",
            self.id, self.task_type, self.queue, self.retried, self.retry
        )
    }
}

impl From<base::TaskMessage> for PyTaskMessage {
    fn from(m: base::TaskMessage) -> Self {
        Self {
            task_type: m.r#type,
            payload: m.payload,
            headers: m.headers,
            id: m.id,
            queue: m.queue,
            retry: m.retry,
            retried: m.retried,
            error_msg: m.error_msg,
            last_failed_at: m.last_failed_at,
            timeout: m.timeout,
            deadline: m.deadline,
            unique_key: m.unique_key,
            group_key: m.group_key,
            retention: m.retention,
            completed_at: m.completed_at,
        }
    }
}

impl From<PyTaskMessage> for base::TaskMessage {
    fn from(m: PyTaskMessage) -> Self {
        Self {
            r#type: m.task_type,
            payload: m.payload,
            headers: m.headers,
            id: m.id,
            queue: m.queue,
            retry: m.retry,
            retried: m.retried,
            error_msg: m.error_msg,
            last_failed_at: m.last_failed_at,
            timeout: m.timeout,
            deadline: m.deadline,
            unique_key: m.unique_key,
            group_key: m.group_key,
            retention: m.retention,
            completed_at: m.completed_at,
        }
    }
}

#[pyfunction]
fn queue_key_prefix(qname: &str) -> String {
    base::queue_key_prefix(qname)
}

#[pyfunction]
fn task_key(qname: &str, id: &str) -> String {
    base::task_key(qname, id)
}

#[pyfunction]
fn pending_key(qname: &str) -> String {
    base::pending_key(qname)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTaskMessage>()?;
    m.add_function(wrap_pyfunction!(queue_key_prefix, m)?)?;
    m.add_function(wrap_pyfunction!(task_key, m)?)?;
    m.add_function(wrap_pyfunction!(pending_key, m)?)?;
    m.add("ALL_QUEUES", base::ALL_QUEUES)?;
    m.add("ASYNQ_VERSION", base::ASYNQ_VERSION)?;
    Ok(())
}
