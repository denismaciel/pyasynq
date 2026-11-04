//! Task storage. [`Rdb`] dispatches to a backend chosen by URL: the Redis
//! port of asynq's `internal/rdb` (every state transition is one Lua
//! script, wire-compatible with Go asynq), [`pg::PgRdb`] (one table,
//! `FOR UPDATE SKIP LOCKED` dequeues and LISTEN/NOTIFY wakeups), or
//! [`memory::MemoryRdb`] for tests.

use std::sync::LazyLock;
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};

use crate::base::{self, TaskMessage};

macro_rules! lua {
    ($name:ident, $file:literal) => {
        static $name: std::sync::LazyLock<redis::Script> =
            std::sync::LazyLock::new(|| redis::Script::new(include_str!(concat!("lua/", $file))));
    };
}

mod facade;
mod groups;
pub mod inspect;
pub mod memory;
pub mod pg;
pub mod rate;
mod scheduler;
mod worker;

pub use facade::{Cancellations, Rdb};
pub use groups::AGGREGATION_TIMEOUT;

static ENQUEUE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/enqueue.lua")));
static ENQUEUE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/enqueue_unique.lua")));
static SCHEDULE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/schedule.lua")));
static SCHEDULE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/schedule_unique.lua")));
static DEQUEUE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/dequeue.lua")));
static DONE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/done.lua")));
static DONE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/done_unique.lua")));
static MARK_AS_COMPLETE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/mark_as_complete.lua")));
static MARK_AS_COMPLETE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/mark_as_complete_unique.lua")));

/// How long a dequeued task belongs to its worker before it must be
/// extended by a heartbeat; after that the recoverer may take it back.
pub const LEASE_DURATION: Duration = Duration::from_secs(30);

/// Daily processed/failed counters are kept for 90 days.
pub(crate) const STATS_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("task already exists")]
    DuplicateTask,
    #[error("task ID conflicts with another task")]
    TaskIdConflict,
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    FailedPrecondition(String),
    #[error("queue {0:?} does not exist")]
    QueueNotFound(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("redis error: {0}")]
    Redis(redis::RedisError),
    #[error("postgres error: {0}")]
    Postgres(String),
}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Postgres(e.to_string())
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Error::Postgres(e.to_string())
    }
}

impl From<redis::RedisError> for Error {
    fn from(e: redis::RedisError) -> Self {
        // Scripts signal a missing task with `redis.error_reply("NOT FOUND")`.
        if e.code() == Some("NOT") {
            Error::NotFound
        } else {
            Error::Redis(e)
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct RedisRdb {
    client: redis::Client,
    conn: ConnectionManager,
}

/// redis-rs gives up on a response after 500ms by default, which fails
/// bursts of enqueues on a loaded Redis. These are go-redis's dial timeout
/// and a more forgiving response timeout.
fn connection_config() -> redis::aio::ConnectionManagerConfig {
    redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(10)))
}

impl RedisRdb {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new_with_config(client.clone(), connection_config()).await?;
        Ok(Self { client, conn })
    }

    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }

    /// Registers the queue on every enqueue (not cached like Go's client),
    /// so a queue deleted with the Inspector reappears when used again.
    async fn publish_queue(&self, qname: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i64 = conn.sadd(base::ALL_QUEUES, qname).await?;
        Ok(())
    }

    pub async fn enqueue(&self, msg: &TaskMessage, now_nanos: i64) -> Result<()> {
        self.publish_queue(&msg.queue).await?;
        let mut conn = self.conn.clone();
        let n: i64 = ENQUEUE
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::pending_key(&msg.queue))
            .arg(base::encode_message(msg))
            .arg(&msg.id)
            .arg(now_nanos)
            .invoke_async(&mut conn)
            .await?;
        status_to_result(n)
    }

    pub async fn enqueue_unique(
        &self,
        msg: &TaskMessage,
        ttl_secs: i64,
        now_nanos: i64,
    ) -> Result<()> {
        self.publish_queue(&msg.queue).await?;
        let mut conn = self.conn.clone();
        let n: i64 = ENQUEUE_UNIQUE
            .key(&msg.unique_key)
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::pending_key(&msg.queue))
            .arg(&msg.id)
            .arg(ttl_secs)
            .arg(base::encode_message(msg))
            .arg(now_nanos)
            .invoke_async(&mut conn)
            .await?;
        status_to_result(n)
    }

    pub async fn schedule(&self, msg: &TaskMessage, process_at_unix: i64) -> Result<()> {
        self.publish_queue(&msg.queue).await?;
        let mut conn = self.conn.clone();
        let n: i64 = SCHEDULE
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::scheduled_key(&msg.queue))
            .arg(base::encode_message(msg))
            .arg(process_at_unix)
            .arg(&msg.id)
            .arg(&msg.group_key)
            .invoke_async(&mut conn)
            .await?;
        status_to_result(n)
    }

    pub async fn schedule_unique(
        &self,
        msg: &TaskMessage,
        process_at_unix: i64,
        ttl_secs: i64,
    ) -> Result<()> {
        self.publish_queue(&msg.queue).await?;
        let mut conn = self.conn.clone();
        let n: i64 = SCHEDULE_UNIQUE
            .key(&msg.unique_key)
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::scheduled_key(&msg.queue))
            .arg(&msg.id)
            .arg(ttl_secs)
            .arg(process_at_unix)
            .arg(base::encode_message(msg))
            .arg(&msg.group_key)
            .invoke_async(&mut conn)
            .await?;
        status_to_result(n)
    }
}

impl RedisRdb {
    pub async fn dequeue_with_lease(
        &self,
        qnames: &[String],
        lease: Duration,
    ) -> Result<Option<(TaskMessage, DateTime<Utc>)>> {
        let mut conn = self.conn.clone();
        for qname in qnames {
            let lease_expires_at = Utc::now() + lease;
            let encoded: Option<Vec<u8>> = DEQUEUE
                .key(base::pending_key(qname))
                .key(base::paused_key(qname))
                .key(base::active_key(qname))
                .key(base::lease_key(qname))
                .arg(lease_expires_at.timestamp())
                .arg(base::task_key_prefix(qname))
                .invoke_async(&mut conn)
                .await?;
            if let Some(encoded) = encoded {
                return Ok(Some((decode(&encoded)?, lease_expires_at)));
            }
        }
        Ok(None)
    }

    /// Deletes a successfully processed task (no retention) and releases
    /// its uniqueness lock.
    pub async fn done(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let script = if msg.unique_key.is_empty() {
            &DONE
        } else {
            &DONE_UNIQUE
        };
        let mut inv = script.prepare_invoke();
        inv.key(base::active_key(&msg.queue))
            .key(base::lease_key(&msg.queue))
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::processed_key(&msg.queue, &utc_date(now)))
            .key(base::processed_total_key(&msg.queue));
        // Redis Cluster rejects empty key names, hence two scripts.
        if !msg.unique_key.is_empty() {
            inv.key(&msg.unique_key);
        }
        inv.arg(&msg.id).arg(stats_expire_at(now)).arg(i64::MAX);
        let mut conn = self.conn.clone();
        let _: () = inv.invoke_async(&mut conn).await?;
        Ok(())
    }

    /// Moves a successfully processed task with retention to the completed
    /// set, where the janitor deletes it once `retention` has passed.
    pub async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let mut msg = msg.clone();
        msg.completed_at = now.timestamp();
        let script = if msg.unique_key.is_empty() {
            &MARK_AS_COMPLETE
        } else {
            &MARK_AS_COMPLETE_UNIQUE
        };
        let mut inv = script.prepare_invoke();
        inv.key(base::active_key(&msg.queue))
            .key(base::lease_key(&msg.queue))
            .key(base::completed_key(&msg.queue))
            .key(base::task_key(&msg.queue, &msg.id))
            .key(base::processed_key(&msg.queue, &utc_date(now)))
            .key(base::processed_total_key(&msg.queue));
        if !msg.unique_key.is_empty() {
            inv.key(&msg.unique_key);
        }
        inv.arg(&msg.id)
            .arg(stats_expire_at(now))
            .arg(now.timestamp() + msg.retention)
            .arg(base::encode_message(&msg))
            .arg(i64::MAX);
        let mut conn = self.conn.clone();
        let _: () = inv.invoke_async(&mut conn).await?;
        Ok(())
    }
}

pub(crate) fn decode(data: &[u8]) -> Result<TaskMessage> {
    base::decode_message(data)
        .map_err(|e| Error::Internal(format!("cannot decode task message: {e}")))
}

pub(crate) fn utc_date(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

pub(crate) fn stats_expire_at(now: DateTime<Utc>) -> i64 {
    (now + STATS_TTL).timestamp()
}

fn status_to_result(n: i64) -> Result<()> {
    match n {
        1 => Ok(()),
        0 => Err(Error::TaskIdConflict),
        _ => Err(Error::DuplicateTask),
    }
}
