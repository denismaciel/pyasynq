use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use crate::rdb;

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
create_exception!(
    _core,
    TaskNotFoundError,
    AsynqError,
    "The task is not where the operation expected it to be."
);
create_exception!(
    _core,
    BrokerError,
    AsynqError,
    "The task storage (Redis or Postgres) failed."
);
create_exception!(_core, RedisError, BrokerError, "Redis command failed.");
create_exception!(_core, PostgresError, BrokerError, "Postgres query failed.");
create_exception!(
    _core,
    QueueNotFoundError,
    AsynqError,
    "The queue does not exist."
);
create_exception!(
    _core,
    FailedPreconditionError,
    AsynqError,
    "The task is in a state that does not allow the operation."
);

pub fn rdb_err(e: rdb::Error) -> PyErr {
    match e {
        rdb::Error::DuplicateTask => DuplicateTaskError::new_err(e.to_string()),
        rdb::Error::TaskIdConflict => TaskIDConflictError::new_err(e.to_string()),
        rdb::Error::NotFound => TaskNotFoundError::new_err(e.to_string()),
        rdb::Error::FailedPrecondition(_) => FailedPreconditionError::new_err(e.to_string()),
        rdb::Error::QueueNotFound(_) => QueueNotFoundError::new_err(e.to_string()),
        rdb::Error::Redis(_) | rdb::Error::Internal(_) => RedisError::new_err(e.to_string()),
        rdb::Error::Postgres(_) => PostgresError::new_err(e.to_string()),
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("AsynqError", py.get_type::<AsynqError>())?;
    m.add("DuplicateTaskError", py.get_type::<DuplicateTaskError>())?;
    m.add("TaskIDConflictError", py.get_type::<TaskIDConflictError>())?;
    m.add("TaskNotFoundError", py.get_type::<TaskNotFoundError>())?;
    m.add("BrokerError", py.get_type::<BrokerError>())?;
    m.add("RedisError", py.get_type::<RedisError>())?;
    m.add("PostgresError", py.get_type::<PostgresError>())?;
    m.add(
        "FailedPreconditionError",
        py.get_type::<FailedPreconditionError>(),
    )?;
    m.add("QueueNotFoundError", py.get_type::<QueueNotFoundError>())?;
    Ok(())
}
