//! Port of asynq's `client.go`: turns a task plus options into a
//! `TaskMessage` and picks the right Redis transition for it.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::base::{self, TaskMessage, TaskState};
use crate::rdb::{self, Rdb};

pub const DEFAULT_MAX_RETRY: i32 = 25;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Rdb(#[from] rdb::Error),
}

#[derive(Debug, Default, Clone)]
pub struct EnqueueOptions {
    pub queue: Option<String>,
    pub max_retry: Option<i32>,
    pub task_id: Option<String>,
    pub timeout: Option<Duration>,
    pub deadline: Option<DateTime<Utc>>,
    pub unique_ttl: Option<Duration>,
    pub process_at: Option<DateTime<Utc>>,
    pub retention: Option<Duration>,
    pub group: Option<String>,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub msg: TaskMessage,
    pub state: TaskState,
    pub next_process_at: Option<DateTime<Utc>>,
    pub result: Option<Vec<u8>>,
}

fn invalid(msg: &str) -> Error {
    Error::InvalidArgument(msg.to_string())
}

/// asynq stores durations as whole seconds, where 0 means unset. Zero stays
/// zero, other values round up, and values under a second are rejected
/// rather than silently truncated to "unset".
fn whole_secs(d: Option<Duration>, name: &str) -> Result<i64, Error> {
    match d {
        None => Ok(0),
        Some(d) if d.is_zero() => Ok(0),
        Some(d) if d < Duration::from_secs(1) => Err(Error::InvalidArgument(format!(
            "{name} cannot be less than 1s (asynq stores whole seconds)"
        ))),
        Some(d) => Ok(d.as_secs_f64().ceil() as i64),
    }
}

/// Builds the message the way Go's `EnqueueContext` does, including its
/// defaults: 25 retries, and a 30 minute timeout if neither timeout nor
/// deadline is set.
pub fn build_message(
    task_type: &str,
    payload: Option<&[u8]>,
    opts: &EnqueueOptions,
) -> Result<TaskMessage, Error> {
    if task_type.trim().is_empty() {
        return Err(invalid("task typename cannot be empty"));
    }
    let queue = opts
        .queue
        .clone()
        .unwrap_or_else(|| base::DEFAULT_QUEUE_NAME.to_string());
    base::validate_queue_name(&queue).map_err(invalid)?;
    let id = match &opts.task_id {
        Some(id) if id.trim().is_empty() => return Err(invalid("task ID cannot be empty")),
        Some(id) => id.clone(),
        None => uuid::Uuid::new_v4().to_string(),
    };
    if matches!(&opts.group, Some(g) if g.trim().is_empty()) {
        return Err(invalid("group key cannot be empty"));
    }
    if matches!(opts.unique_ttl, Some(ttl) if ttl < Duration::from_secs(1)) {
        return Err(invalid("unique TTL cannot be less than 1s"));
    }

    let mut timeout = opts.timeout.filter(|d| !d.is_zero());
    let deadline = opts.deadline;
    if timeout.is_none() && deadline.is_none() {
        timeout = Some(DEFAULT_TIMEOUT);
    }
    let unique_key = match opts.unique_ttl {
        Some(_) => base::unique_key(&queue, task_type, payload),
        None => String::new(),
    };

    Ok(TaskMessage {
        r#type: task_type.to_string(),
        payload: payload.map(<[u8]>::to_vec).unwrap_or_default(),
        headers: opts.headers.clone(),
        id,
        queue,
        retry: opts.max_retry.unwrap_or(DEFAULT_MAX_RETRY).max(0),
        timeout: whole_secs(timeout, "timeout")?,
        deadline: deadline.map_or(0, |d| d.timestamp()),
        unique_key,
        group_key: opts.group.clone().unwrap_or_default(),
        retention: whole_secs(opts.retention, "retention")?,
        ..Default::default()
    })
}

pub async fn enqueue(
    rdb: &Rdb,
    task_type: &str,
    payload: Option<&[u8]>,
    opts: &EnqueueOptions,
) -> Result<TaskInfo, Error> {
    let msg = build_message(task_type, payload, opts)?;
    let now = Utc::now();
    let unique_ttl = opts.unique_ttl.map(|d| d.as_secs_f64().ceil() as i64);

    match opts.process_at.filter(|t| *t > now) {
        Some(process_at) => {
            match unique_ttl {
                // The lock must outlive the wait until process_at.
                Some(ttl) => {
                    let ttl = (process_at - now).num_seconds() + ttl;
                    rdb.schedule_unique(&msg, process_at.timestamp(), ttl)
                        .await?
                }
                None => rdb.schedule(&msg, process_at.timestamp()).await?,
            }
            Ok(TaskInfo {
                msg,
                state: TaskState::Scheduled,
                next_process_at: Some(process_at),
                result: None,
            })
        }
        None if opts.group.is_some() => {
            let group = opts.group.as_deref().unwrap_or_default();
            rdb.add_to_group(&msg, group, unique_ttl).await?;
            Ok(TaskInfo {
                msg,
                state: TaskState::Aggregating,
                next_process_at: None,
                result: None,
            })
        }
        None => {
            let now_nanos = now.timestamp_nanos_opt().unwrap_or_default();
            match unique_ttl {
                Some(ttl) => rdb.enqueue_unique(&msg, ttl, now_nanos).await?,
                None => rdb.enqueue(&msg, now_nanos).await?,
            }
            Ok(TaskInfo {
                msg,
                state: TaskState::Pending,
                next_process_at: Some(now),
                result: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_go() {
        let msg = build_message("t", None, &EnqueueOptions::default()).unwrap();
        assert_eq!(msg.queue, "default");
        assert_eq!(msg.retry, 25);
        assert_eq!(msg.timeout, 1800);
        assert_eq!(msg.deadline, 0);
        assert_eq!(msg.id.len(), 36);
    }

    #[test]
    fn deadline_alone_disables_default_timeout() {
        let opts = EnqueueOptions {
            deadline: Some(DateTime::from_timestamp(2_000_000_000, 0).unwrap()),
            ..Default::default()
        };
        let msg = build_message("t", None, &opts).unwrap();
        assert_eq!(msg.timeout, 0);
        assert_eq!(msg.deadline, 2_000_000_000);
    }

    #[test]
    fn rejects_invalid_options() {
        let blank_queue = EnqueueOptions {
            queue: Some(" ".into()),
            ..Default::default()
        };
        let short_unique = EnqueueOptions {
            unique_ttl: Some(Duration::from_millis(500)),
            ..Default::default()
        };
        let short_timeout = EnqueueOptions {
            timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        assert!(build_message("", None, &EnqueueOptions::default()).is_err());
        assert!(build_message("t", None, &blank_queue).is_err());
        assert!(build_message("t", None, &short_unique).is_err());
        assert!(build_message("t", None, &short_timeout).is_err());
        let fractional = EnqueueOptions {
            timeout: Some(Duration::from_millis(1500)),
            ..Default::default()
        };
        assert_eq!(build_message("t", None, &fractional).unwrap().timeout, 2);
    }
}
