//! Port of asynq's `internal/rdb`: every state transition of a task is one
//! Lua script, so it is atomic on the Redis server.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};

use crate::base::{self, TaskMessage};

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
const STATS_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("task already exists")]
    DuplicateTask,
    #[error("task ID conflicts with another task")]
    TaskIdConflict,
    #[error("task not found")]
    NotFound,
    #[error("redis error: {0}")]
    Redis(redis::RedisError),
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

pub struct Rdb {
    conn: ConnectionManager,
    /// Queues already added to `asynq:queues` by this process, to skip a
    /// round-trip on every enqueue.
    queues_published: Mutex<HashSet<String>>,
}

impl Rdb {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            queues_published: Mutex::new(HashSet::new()),
        })
    }

    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }

    async fn publish_queue(&self, qname: &str) -> Result<()> {
        if self.queues_published.lock().unwrap().contains(qname) {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        let _: i64 = conn.sadd(base::ALL_QUEUES, qname).await?;
        self.queues_published
            .lock()
            .unwrap()
            .insert(qname.to_string());
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
            .invoke_async(&mut conn)
            .await?;
        status_to_result(n)
    }
}

impl Rdb {
    /// Pops the oldest pending task from the first queue (in the given
    /// order) that is neither paused nor empty, moving it to active and
    /// granting it a lease. Returns the task and its lease expiration.
    pub async fn dequeue(&self, qnames: &[String]) -> Result<Option<(TaskMessage, DateTime<Utc>)>> {
        let mut conn = self.conn.clone();
        for qname in qnames {
            let lease_expires_at = Utc::now() + LEASE_DURATION;
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
                let msg = base::decode_message(&encoded).map_err(|e| {
                    Error::Redis(redis::RedisError::from((
                        redis::ErrorKind::Parse,
                        "cannot decode task message",
                        e.to_string(),
                    )))
                })?;
                return Ok(Some((msg, lease_expires_at)));
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

fn utc_date(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

fn stats_expire_at(now: DateTime<Utc>) -> i64 {
    (now + STATS_TTL).timestamp()
}

fn status_to_result(n: i64) -> Result<()> {
    match n {
        1 => Ok(()),
        0 => Err(Error::TaskIdConflict),
        _ => Err(Error::DuplicateTask),
    }
}
