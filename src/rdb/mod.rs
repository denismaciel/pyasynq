//! Port of asynq's `internal/rdb`: every state transition of a task is one
//! Lua script, so it is atomic on the Redis server.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};

use crate::base::{self, TaskMessage};

static ENQUEUE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/enqueue.lua")));
static ENQUEUE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/enqueue_unique.lua")));
static SCHEDULE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/schedule.lua")));
static SCHEDULE_UNIQUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("lua/schedule_unique.lua")));

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("task already exists")]
    DuplicateTask,
    #[error("task ID conflicts with another task")]
    TaskIdConflict,
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
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

fn status_to_result(n: i64) -> Result<()> {
    match n {
        1 => Ok(()),
        0 => Err(Error::TaskIdConflict),
        _ => Err(Error::DuplicateTask),
    }
}
