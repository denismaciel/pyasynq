use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use tokio::sync::{broadcast, Notify};

use super::inspect::{DailyStats, QueueStats, ServerDetail};
use super::memory::{is_memory_url, MemoryRdb};
use super::pg::PgRdb;
use super::{RedisRdb, Result, LEASE_DURATION};
use crate::base::{pb, TaskMessage, TaskState};
use crate::client::TaskInfo;

enum Backend {
    Redis(RedisRdb),
    Postgres(PgRdb),
    Memory(MemoryRdb),
}

/// Task storage, backed by Redis (`redis://`, `rediss://`, `unix://`),
/// Postgres (`postgres://`, `postgresql://`) or process memory (`memory://`).
pub struct Rdb {
    backend: Backend,
}

/// Task IDs published by `Inspector.cancel_processing`.
pub enum Cancellations {
    Redis(Pin<Box<dyn Stream<Item = redis::Msg> + Send>>),
    Channel(broadcast::Receiver<String>),
}

impl Cancellations {
    /// The next canceled task ID, or `None` once the subscription is lost.
    pub async fn next(&mut self) -> Option<String> {
        match self {
            Cancellations::Redis(s) => loop {
                if let Ok(id) = s.next().await?.get_payload::<String>() {
                    return Some(id);
                }
            },
            Cancellations::Channel(rx) => loop {
                match rx.recv().await {
                    Ok(id) => return Some(id),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            },
        }
    }
}

pub fn is_postgres_url(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

impl Rdb {
    pub async fn connect(url: &str) -> Result<Self> {
        let backend = if is_postgres_url(url) {
            Backend::Postgres(PgRdb::connect(url).await?)
        } else if is_memory_url(url) {
            Backend::Memory(MemoryRdb::connect(url).await?)
        } else {
            Backend::Redis(RedisRdb::connect(url).await?)
        };
        Ok(Self { backend })
    }

    /// Pops the oldest pending task from the first queue (in the given
    /// order) that is neither paused nor empty, moving it to active and
    /// granting it a lease. Returns the task and its lease expiration.
    pub async fn dequeue(&self, qnames: &[String]) -> Result<Option<(TaskMessage, DateTime<Utc>)>> {
        self.dequeue_with_lease(qnames, LEASE_DURATION).await
    }

    /// How many tasks one dequeue may claim. Batching only pays off on
    /// Postgres, where each claim costs a commit.
    pub fn max_dequeue_batch(&self) -> usize {
        match &self.backend {
            Backend::Redis(_) => 1,
            Backend::Postgres(_) | Backend::Memory(_) => 32,
        }
    }

    /// Up to `max` tasks, as from repeated [`Rdb::dequeue_with_lease`].
    pub async fn dequeue_batch(
        &self,
        qnames: &[String],
        lease: Duration,
        max: usize,
    ) -> Result<Vec<(TaskMessage, DateTime<Utc>)>> {
        match &self.backend {
            Backend::Redis(b) => Ok(b
                .dequeue_with_lease(qnames, lease)
                .await?
                .into_iter()
                .collect()),
            Backend::Postgres(b) => b.dequeue_batch(qnames, lease, max).await,
            Backend::Memory(b) => b.dequeue_batch(qnames, lease, max).await,
        }
    }

    /// Signaled when a task may have become pending, so the processor can
    /// skip the rest of its poll interval. Redis has no such signal.
    pub async fn pending_notifier(&self) -> Option<Arc<Notify>> {
        match &self.backend {
            Backend::Redis(_) => None,
            Backend::Postgres(b) => Some(b.pending_notifier().await),
            Backend::Memory(b) => Some(b.pending_notifier()),
        }
    }
}

macro_rules! delegate {
    ($( $(#[$m:meta])* fn $name:ident(&self $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty; )*) => {
        impl Rdb {
            $(
                $(#[$m])*
                pub async fn $name(&self $(, $arg: $ty)*) -> $ret {
                    match &self.backend {
                        Backend::Redis(b) => b.$name($($arg),*).await,
                        Backend::Postgres(b) => b.$name($($arg),*).await,
                        Backend::Memory(b) => b.$name($($arg),*).await,
                    }
                }
            )*
        }
    };
}

delegate! {
    fn ping(&self) -> Result<()>;
    fn enqueue(&self, msg: &TaskMessage, now_nanos: i64) -> Result<()>;
    fn enqueue_unique(&self, msg: &TaskMessage, ttl_secs: i64, now_nanos: i64) -> Result<()>;
    fn schedule(&self, msg: &TaskMessage, process_at_unix: i64) -> Result<()>;
    fn schedule_unique(&self, msg: &TaskMessage, process_at_unix: i64, ttl_secs: i64) -> Result<()>;
    fn dequeue_with_lease(&self, qnames: &[String], lease: Duration)
        -> Result<Option<(TaskMessage, DateTime<Utc>)>>;
    fn done(&self, msg: &TaskMessage) -> Result<()>;
    fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()>;

    fn semaphore_acquire(&self, scope: &str, max_tokens: u32, deadline: DateTime<Utc>, task_id: &str)
        -> Result<bool>;
    fn semaphore_release(&self, scope: &str, task_id: &str) -> Result<bool>;

    fn subscribe_cancellations(&self) -> Result<Cancellations>;
    fn requeue(&self, msg: &TaskMessage) -> Result<()>;
    /// With `lease_cutoff`, only applies while the task's lease is still
    /// expired at the cutoff, so a stale recoverer can't touch a newer attempt.
    fn retry(&self, msg: &TaskMessage, retry_at: DateTime<Utc>, err_msg: &str, is_failure: bool,
        lease_cutoff: Option<DateTime<Utc>>) -> Result<()>;
    fn archive(&self, msg: &TaskMessage, err_msg: &str, lease_cutoff: Option<DateTime<Utc>>)
        -> Result<()>;
    fn forward_if_ready(&self, qname: &str) -> Result<u64>;
    fn extend_lease(&self, qname: &str, ids: &[String], lease: Duration) -> Result<DateTime<Utc>>;
    fn list_lease_expired(&self, cutoff: DateTime<Utc>, qnames: &[String])
        -> Result<Vec<TaskMessage>>;
    fn write_server_state(&self, info: &pb::ServerInfo, workers: &[pb::WorkerInfo], ttl: Duration)
        -> Result<()>;
    fn clear_server_state(&self, host: &str, pid: u32, server_id: &str) -> Result<()>;
    fn delete_expired_completed_tasks(&self, qname: &str, batch_size: usize) -> Result<u64>;
    fn write_result(&self, qname: &str, id: &str, data: &[u8]) -> Result<()>;
    fn publish_cancelation(&self, id: &str) -> Result<()>;

    fn add_to_group(&self, msg: &TaskMessage, group: &str, unique_ttl_secs: Option<i64>)
        -> Result<()>;
    fn list_groups(&self, qname: &str) -> Result<Vec<String>>;
    #[allow(clippy::too_many_arguments)]
    fn aggregation_check(
        &self,
        qname: &str,
        group: &str,
        now: DateTime<Utc>,
        set_id: &str,
        grace_period: Duration,
        max_delay: Duration,
        max_size: usize,
    ) -> Result<bool>;
    fn read_aggregation_set(&self, qname: &str, group: &str, set_id: &str)
        -> Result<Vec<TaskMessage>>;
    fn delete_aggregation_set(&self, qname: &str, group: &str, set_id: &str) -> Result<()>;
    fn reclaim_stale_aggregation_sets(&self, qname: &str) -> Result<()>;

    fn write_scheduler_entries(&self, scheduler_id: &str, entries: &[pb::SchedulerEntry], ttl: Duration)
        -> Result<()>;
    fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()>;
    fn record_scheduler_enqueue_event(&self, entry_id: &str, task_id: &str, enqueued_at: DateTime<Utc>)
        -> Result<()>;
    fn clear_scheduler_history(&self, entry_id: &str) -> Result<()>;

    fn all_queues(&self) -> Result<Vec<String>>;
    fn current_stats(&self, qname: &str) -> Result<QueueStats>;
    fn historical_stats(&self, qname: &str, days: usize) -> Result<Vec<DailyStats>>;
    fn get_task_info(&self, qname: &str, id: &str) -> Result<TaskInfo>;
    fn group_stats(&self, qname: &str) -> Result<Vec<(String, i64)>>;
    fn list_tasks(&self, qname: &str, state: TaskState, group: Option<&str>, page: usize, size: usize)
        -> Result<Vec<TaskInfo>>;
    fn run_task(&self, qname: &str, id: &str) -> Result<()>;
    fn archive_task(&self, qname: &str, id: &str) -> Result<()>;
    fn delete_task(&self, qname: &str, id: &str) -> Result<()>;
    fn update_task_payload(&self, qname: &str, id: &str, payload: &[u8]) -> Result<()>;
    fn run_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64>;
    fn archive_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64>;
    fn delete_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64>;
    fn remove_queue(&self, qname: &str, force: bool) -> Result<()>;
    fn pause(&self, qname: &str) -> Result<()>;
    fn unpause(&self, qname: &str) -> Result<()>;
    fn list_servers(&self) -> Result<Vec<ServerDetail>>;
    fn list_scheduler_entries(&self) -> Result<Vec<pb::SchedulerEntry>>;
    fn list_scheduler_enqueue_events(&self, entry_id: &str, page: usize, size: usize)
        -> Result<Vec<pb::SchedulerEnqueueEvent>>;
}
