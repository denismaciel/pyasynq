use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

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
///
/// Frozen with a lock inside rather than a mutable pyclass: on free-threaded
/// Python a mutable pyclass raises "Already borrowed" when one thread reads
/// an attribute while another sets one.
#[pyclass(name = "TaskMessage", module = "pyasynq._core", frozen, eq)]
pub struct PyTaskMessage(Mutex<base::TaskMessage>);

impl PyTaskMessage {
    fn lock(&self) -> MutexGuard<'_, base::TaskMessage> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn message(&self) -> base::TaskMessage {
        self.lock().clone()
    }
}

impl From<base::TaskMessage> for PyTaskMessage {
    fn from(m: base::TaskMessage) -> Self {
        Self(Mutex::new(m))
    }
}

impl PartialEq for PyTaskMessage {
    fn eq(&self, other: &Self) -> bool {
        // Locking both at once would deadlock on `m == m`.
        std::ptr::eq(self, other) || self.message() == *other.lock()
    }
}

/// The `#[pymethods]` block, with a getter and setter per field.
macro_rules! task_message_methods {
    ({ $($methods:tt)* } $($name:literal $get:ident $set:ident $field:ident: $ty:ty),* $(,)?) => {
        #[pymethods]
        impl PyTaskMessage {
            $($methods)*

            $(
                #[getter]
                #[pyo3(name = $name)]
                fn $get(&self) -> $ty {
                    self.lock().$field.clone()
                }

                #[setter]
                #[pyo3(name = $name)]
                fn $set(&self, value: $ty) {
                    self.lock().$field = value;
                }
            )*
        }
    };
}

task_message_methods! {
    {
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
            base::TaskMessage {
                r#type: task_type,
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
            .into()
        }

        fn encode(&self) -> Vec<u8> {
            base::encode_message(&self.lock())
        }

        #[staticmethod]
        fn decode(data: &[u8]) -> PyResult<Self> {
            base::decode_message(data)
                .map(Into::into)
                .map_err(|e| PyValueError::new_err(format!("invalid TaskMessage: {e}")))
        }

        fn __repr__(&self) -> String {
            let m = self.lock();
            format!(
                "TaskMessage(id={:?}, type={:?}, queue={:?}, retried={}/{})",
                m.id, m.r#type, m.queue, m.retried, m.retry
            )
        }
    }
    "type" get_type set_type r#type: String,
    "payload" get_payload set_payload payload: Vec<u8>,
    "headers" get_headers set_headers headers: HashMap<String, String>,
    "id" get_id set_id id: String,
    "queue" get_queue set_queue queue: String,
    "retry" get_retry set_retry retry: i32,
    "retried" get_retried set_retried retried: i32,
    "error_msg" get_error_msg set_error_msg error_msg: String,
    "last_failed_at" get_last_failed_at set_last_failed_at last_failed_at: i64,
    "timeout" get_timeout set_timeout timeout: i64,
    "deadline" get_deadline set_deadline deadline: i64,
    "unique_key" get_unique_key set_unique_key unique_key: String,
    "group_key" get_group_key set_group_key group_key: String,
    "retention" get_retention set_retention retention: i64,
    "completed_at" get_completed_at set_completed_at completed_at: i64,
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
