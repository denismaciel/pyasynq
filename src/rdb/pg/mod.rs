//! Postgres backend, modeled on procrastinate: every task is a row in
//! `pyasynq_tasks`, workers claim pending rows with `FOR UPDATE SKIP LOCKED`,
//! and triggers `NOTIFY` LISTENing workers so they don't wait for the next
//! poll. Each state transition is one statement (data-modifying CTEs), so it
//! is atomic and costs one round trip, like the Lua scripts on Redis.
//!
//! Tasks are still stored as encoded asynq `TaskMessage`s, so everything
//! above this layer is shared with the Redis backend.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures_util::StreamExt;
use prost::Message;
use tokio::sync::{broadcast, mpsc, Notify, OnceCell};
use tokio_postgres::{AsyncMessage, Config, Row};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;

use super::inspect::{DailyStats, QueueStats, ServerDetail};
use super::scheduler::MAX_EVENTS;
use super::worker::{ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE};
use super::{decode, Cancellations, Error, Result, AGGREGATION_TIMEOUT};
use crate::base::{self, pb, TaskMessage, TaskState};
use crate::client::TaskInfo;

/// Schema migrations, applied on connect; migration N brings the database
/// to version N.
const MIGRATIONS: &[&str] = &[include_str!("schema.sql"), include_str!("schema_2.sql")];
const SCHEMA_VERSION: i32 = MIGRATIONS.len() as i32;
const VERSION_TABLE: &str = "CREATE TABLE IF NOT EXISTS pyasynq_schema_version (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);";

/// The full migration as one script, for applying it with an external tool
/// instead of letting pyasynq migrate on connect.
pub fn migration_sql() -> String {
    let mut sql = format!("{VERSION_TABLE}\n");
    for (i, m) in MIGRATIONS.iter().enumerate() {
        let version = i + 1;
        sql.push_str(&format!(
            "\n{m}\nINSERT INTO pyasynq_schema_version (version) VALUES ({version});\n"
        ));
    }
    sql
}

const PENDING_CHANNEL: &str = "pyasynq_pending";
const CANCEL_CHANNEL: &str = "pyasynq_cancel";

pub struct PgRdb {
    pool: Pool,
    config: Config,
    tls: MakeRustlsConnect,
    listener: OnceCell<Listener>,
    stop: CancellationToken,
}

/// Fan-out of the notifications received on the LISTEN connection.
struct Listener {
    pending: Arc<Notify>,
    cancels: broadcast::Sender<String>,
}

impl Drop for PgRdb {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// TLS with the Mozilla root store. With the default `sslmode=prefer` this
/// falls back to plaintext for servers without TLS.
fn tls() -> MakeRustlsConnect {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports the default TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    MakeRustlsConnect::new(config)
}

fn ts(unix: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(unix, 0).single().unwrap_or_default()
}

fn nonempty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

fn today(now: DateTime<Utc>) -> NaiveDate {
    now.date_naive()
}

fn msgs(rows: Vec<Row>) -> Result<Vec<TaskMessage>> {
    rows.iter().map(|r| decode(r.get(0))).collect()
}

fn group_required(group: Option<&str>) -> Result<&str> {
    group.ok_or_else(|| Error::FailedPrecondition("group required".into()))
}

/// A pooled connection for a query whose effect must not land unseen. If
/// the future is dropped mid-query (shutdown), the query is canceled and the
/// connection discarded instead of returned to the pool.
struct CancelOnDrop {
    client: Option<deadpool_postgres::Object>,
    tls: MakeRustlsConnect,
}

impl CancelOnDrop {
    /// Returns the connection to the pool.
    fn release(mut self) {
        self.client.take();
    }
}

impl std::ops::Deref for CancelOnDrop {
    type Target = deadpool_postgres::Object;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("connection released")
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let Some(obj) = self.client.take() else {
            return;
        };
        let token = obj.cancel_token();
        let client = deadpool_postgres::Object::take(obj);
        let tls = self.tls.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let _ = token.cancel_query(tls).await;
                drop(client);
            });
        }
    }
}

/// A new row for `pyasynq_tasks`.
struct NewTask<'a> {
    msg: &'a TaskMessage,
    state: &'static str,
    pending_since: Option<DateTime<Utc>>,
    process_at: Option<DateTime<Utc>>,
    group: Option<&'a str>,
    grouped_at: Option<DateTime<Utc>>,
}

const INSERT_TASK: &str = "
    INSERT INTO pyasynq_tasks
        (queue, id, state, msg, pending_since, process_at, group_key, grouped_at, unique_key)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT DO NOTHING";

impl PgRdb {
    pub async fn connect(url: &str) -> Result<Self> {
        let config: Config = url.parse()?;
        let tls = tls();
        let manager = Manager::from_config(
            config.clone(),
            tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(16)
            .runtime(Runtime::Tokio1)
            .create_timeout(Some(Duration::from_secs(5)))
            .wait_timeout(Some(Duration::from_secs(10)))
            .build()
            .map_err(|e| Error::Postgres(e.to_string()))?;
        let rdb = Self {
            pool,
            config,
            tls,
            listener: OnceCell::new(),
            stop: CancellationToken::new(),
        };
        rdb.migrate().await?;
        Ok(rdb)
    }

    async fn migrate(&self) -> Result<()> {
        let mut client = self.pool.get().await?;
        let current = |row: Row| row.get::<_, Option<i32>>(0).unwrap_or(0);
        let installed: bool = client
            .query_one(
                "SELECT to_regclass('pyasynq_schema_version') IS NOT NULL",
                &[],
            )
            .await?
            .get(0);
        if installed {
            let row = client
                .query_one("SELECT max(version) FROM pyasynq_schema_version", &[])
                .await?;
            if current(row) >= SCHEMA_VERSION {
                return Ok(());
            }
        }
        let tx = client.transaction().await?;
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtext('pyasynq_schema'))",
            &[],
        )
        .await?;
        tx.batch_execute(VERSION_TABLE).await?;
        let row = tx
            .query_one("SELECT max(version) FROM pyasynq_schema_version", &[])
            .await?;
        for version in current(row) + 1..=SCHEMA_VERSION {
            tx.batch_execute(MIGRATIONS[version as usize - 1]).await?;
            tx.execute(
                "INSERT INTO pyasynq_schema_version (version) VALUES ($1)",
                &[&version],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<()> {
        self.pool.get().await?.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn insert(&self, t: NewTask<'_>, unique_ttl_secs: Option<i64>) -> Result<()> {
        let msg = t.msg;
        let unique = unique_ttl_secs.and(nonempty(&msg.unique_key));
        let encoded = base::encode_message(msg);
        let params: [&(dyn tokio_postgres::types::ToSql + Sync); 9] = [
            &msg.queue,
            &msg.id,
            &t.state,
            &encoded,
            &t.pending_since,
            &t.process_at,
            &t.group,
            &t.grouped_at,
            &unique,
        ];
        let mut client = self.pool.get().await?;
        let (Some(key), Some(ttl)) = (unique, unique_ttl_secs) else {
            return match client.execute(INSERT_TASK, &params).await? {
                0 => Err(Error::TaskIdConflict),
                _ => Ok(()),
            };
        };
        let now = Utc::now();
        let tx = client.transaction().await?;
        // Like asynq, an ID conflict wins over a held uniqueness lock.
        let exists = tx
            .query_opt(
                "SELECT 1 FROM pyasynq_tasks WHERE queue = $1 AND id = $2",
                &[&msg.queue, &msg.id],
            )
            .await?;
        if exists.is_some() {
            return Err(Error::TaskIdConflict);
        }
        let locked = tx
            .execute(
                "INSERT INTO pyasynq_unique_locks (key, task_id, expires_at)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (key) DO UPDATE
                 SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
                 WHERE pyasynq_unique_locks.expires_at <= $4",
                &[&key, &msg.id, &(now + chrono::Duration::seconds(ttl)), &now],
            )
            .await?;
        if locked == 0 {
            return Err(Error::DuplicateTask);
        }
        if tx.execute(INSERT_TASK, &params).await? == 0 {
            return Err(Error::TaskIdConflict);
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn enqueue(&self, msg: &TaskMessage, now_nanos: i64) -> Result<()> {
        self.enqueue_with(msg, now_nanos, None).await
    }

    pub async fn enqueue_unique(
        &self,
        msg: &TaskMessage,
        ttl_secs: i64,
        now_nanos: i64,
    ) -> Result<()> {
        self.enqueue_with(msg, now_nanos, Some(ttl_secs)).await
    }

    async fn enqueue_with(
        &self,
        msg: &TaskMessage,
        now_nanos: i64,
        ttl: Option<i64>,
    ) -> Result<()> {
        let task = NewTask {
            msg,
            state: "pending",
            pending_since: Some(Utc.timestamp_nanos(now_nanos)),
            process_at: None,
            group: None,
            grouped_at: None,
        };
        self.insert(task, ttl).await
    }

    pub async fn schedule(&self, msg: &TaskMessage, process_at_unix: i64) -> Result<()> {
        self.schedule_with(msg, process_at_unix, None).await
    }

    pub async fn schedule_unique(
        &self,
        msg: &TaskMessage,
        process_at_unix: i64,
        ttl_secs: i64,
    ) -> Result<()> {
        self.schedule_with(msg, process_at_unix, Some(ttl_secs))
            .await
    }

    async fn schedule_with(&self, msg: &TaskMessage, at: i64, ttl: Option<i64>) -> Result<()> {
        let task = NewTask {
            msg,
            state: "scheduled",
            pending_since: None,
            process_at: Some(ts(at)),
            group: nonempty(&msg.group_key),
            grouped_at: None,
        };
        self.insert(task, ttl).await
    }

    pub async fn add_to_group(
        &self,
        msg: &TaskMessage,
        group: &str,
        unique_ttl_secs: Option<i64>,
    ) -> Result<()> {
        let task = NewTask {
            msg,
            state: "aggregating",
            pending_since: None,
            process_at: None,
            group: Some(group),
            grouped_at: Some(Utc::now()),
        };
        self.insert(task, unique_ttl_secs).await
    }

    pub async fn dequeue_with_lease(
        &self,
        qnames: &[String],
        lease: Duration,
    ) -> Result<Option<(TaskMessage, DateTime<Utc>)>> {
        Ok(self.dequeue_batch(qnames, lease, 1).await?.pop())
    }

    /// Claims up to `max` of the oldest pending tasks, taking from the
    /// queues in order. One statement per queue, so a batch costs a single
    /// commit (and its WAL flush) instead of one per task.
    pub async fn dequeue_batch(
        &self,
        qnames: &[String],
        lease: Duration,
        max: usize,
    ) -> Result<Vec<(TaskMessage, DateTime<Utc>)>> {
        // Dropped mid-claim, the claim must roll back: nobody would run the
        // tasks until their leases expired.
        let client = CancelOnDrop {
            client: Some(self.pool.get().await?),
            tls: self.tls.clone(),
        };
        // RETURNING has no order, hence returning the sort key.
        let stmt = client
            .prepare_cached(
                "WITH c AS (
                     SELECT queue, id, pending_since FROM pyasynq_tasks
                     WHERE queue = $1 AND state = 'pending'
                       AND NOT EXISTS (SELECT 1 FROM pyasynq_queues WHERE name = $1 AND paused)
                     ORDER BY pending_since, id
                     LIMIT $3
                     FOR UPDATE SKIP LOCKED
                 )
                 UPDATE pyasynq_tasks t
                 SET state = 'active', lease_expires_at = $2, pending_since = NULL
                 FROM c
                 WHERE t.queue = c.queue AND t.id = c.id
                 RETURNING t.msg, c.pending_since, t.id",
            )
            .await?;
        let mut out = Vec::new();
        for qname in qnames {
            let want = (max - out.len()) as i64;
            if want == 0 {
                break;
            }
            let lease_expires_at = Utc::now() + lease;
            let mut rows = client
                .query(&stmt, &[qname, &lease_expires_at, &want])
                .await?;
            rows.sort_by(|a, b| {
                let key = |r: &Row| (r.get::<_, Option<DateTime<Utc>>>(1), r.get::<_, String>(2));
                key(a).cmp(&key(b))
            });
            for row in rows {
                out.push((decode(row.get(0))?, lease_expires_at));
            }
        }
        client.release();
        Ok(out)
    }

    /// Runs a transition of an active task; no returned row means the task
    /// was no longer active.
    async fn finish(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<()> {
        let client = self.pool.get().await?;
        let stmt = client.prepare_cached(sql).await?;
        match client.query(&stmt, params).await?.len() {
            0 => Err(Error::NotFound),
            _ => Ok(()),
        }
    }

    pub async fn done(&self, msg: &TaskMessage) -> Result<()> {
        self.finish(
            "WITH t AS (
                 DELETE FROM pyasynq_tasks
                 WHERE queue = $1 AND id = $2 AND state = 'active'
                 RETURNING unique_key
             ), u AS (
                 DELETE FROM pyasynq_unique_locks l USING t
                 WHERE l.key = t.unique_key AND l.task_id = $2
             )
             SELECT pyasynq_count($1, $3, 1, 0) FROM t",
            &[&msg.queue, &msg.id, &today(Utc::now())],
        )
        .await
    }

    pub async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let mut msg = msg.clone();
        msg.completed_at = now.timestamp();
        let expires_at = ts(now.timestamp() + msg.retention);
        self.finish(
            "WITH t AS (
                 UPDATE pyasynq_tasks
                 SET state = 'completed', msg = $3, lease_expires_at = NULL, expires_at = $4
                 WHERE queue = $1 AND id = $2 AND state = 'active'
                 RETURNING unique_key
             ), u AS (
                 DELETE FROM pyasynq_unique_locks l USING t
                 WHERE l.key = t.unique_key AND l.task_id = $2
             )
             SELECT pyasynq_count($1, $5, 1, 0) FROM t",
            &[
                &msg.queue,
                &msg.id,
                &base::encode_message(&msg),
                &expires_at,
                &today(now),
            ],
        )
        .await
    }

    /// Puts an active task back at the head of its queue.
    pub async fn requeue(&self, msg: &TaskMessage) -> Result<()> {
        let client = self.pool.get().await?;
        let n = client
            .execute(
                "UPDATE pyasynq_tasks
                 SET state = 'pending', lease_expires_at = NULL,
                     pending_since = coalesce(
                         (SELECT min(pending_since) FROM pyasynq_tasks
                          WHERE queue = $1 AND state = 'pending'),
                         $3
                     ) - interval '1 microsecond'
                 WHERE queue = $1 AND id = $2 AND state = 'active'",
                &[&msg.queue, &msg.id, &Utc::now()],
            )
            .await?;
        match n {
            0 => Err(Error::NotFound),
            _ => Ok(()),
        }
    }

    pub async fn retry(
        &self,
        msg: &TaskMessage,
        retry_at: DateTime<Utc>,
        err_msg: &str,
        is_failure: bool,
        lease_cutoff: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let now = Utc::now();
        let mut modified = msg.clone();
        if is_failure {
            modified.retried += 1;
        }
        modified.error_msg = err_msg.to_string();
        modified.last_failed_at = now.timestamp();
        self.finish(
            "WITH t AS (
                 UPDATE pyasynq_tasks
                 SET state = 'retry', msg = $3, process_at = $4, lease_expires_at = NULL
                 WHERE queue = $1 AND id = $2 AND state = 'active'
                     AND ($7::timestamptz IS NULL OR lease_expires_at <= $7)
                 RETURNING 1
             )
             SELECT CASE WHEN $6 THEN pyasynq_count($1, $5, 1, 1) END FROM t",
            &[
                &msg.queue,
                &msg.id,
                &base::encode_message(&modified),
                &ts(retry_at.timestamp()),
                &today(now),
                &is_failure,
                &lease_cutoff,
            ],
        )
        .await
    }

    /// Moves an active task to the archive. The janitor trims the archive.
    pub async fn archive(
        &self,
        msg: &TaskMessage,
        err_msg: &str,
        lease_cutoff: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let now = Utc::now();
        let mut modified = msg.clone();
        modified.error_msg = err_msg.to_string();
        modified.last_failed_at = now.timestamp();
        self.finish(
            "WITH t AS (
                 UPDATE pyasynq_tasks
                 SET state = 'archived', msg = $3, archived_at = $4, lease_expires_at = NULL
                 WHERE queue = $1 AND id = $2 AND state = 'active'
                     AND ($6::timestamptz IS NULL OR lease_expires_at <= $6)
                 RETURNING 1
             )
             SELECT pyasynq_count($1, $5, 1, 1) FROM t",
            &[
                &msg.queue,
                &msg.id,
                &base::encode_message(&modified),
                &now,
                &today(now),
                &lease_cutoff,
            ],
        )
        .await
    }

    pub async fn forward_if_ready(&self, qname: &str) -> Result<u64> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                "UPDATE pyasynq_tasks t
                 SET state = CASE WHEN t.group_key IS NULL THEN 'pending' ELSE 'aggregating' END,
                     pending_since = CASE WHEN t.group_key IS NULL THEN $2::timestamptz END,
                     grouped_at = CASE WHEN t.group_key IS NOT NULL THEN $2::timestamptz END,
                     process_at = NULL
                 FROM (
                     SELECT queue, id FROM pyasynq_tasks
                     WHERE queue = $1 AND state IN ('scheduled', 'retry') AND process_at <= $2
                     ORDER BY process_at
                     LIMIT 100
                     FOR UPDATE SKIP LOCKED
                 ) due
                 WHERE t.queue = due.queue AND t.id = due.id",
            )
            .await?;
        let mut total = 0;
        loop {
            let n = client.execute(&stmt, &[&qname, &Utc::now()]).await?;
            total += n;
            if n == 0 {
                return Ok(total);
            }
        }
    }

    pub async fn extend_lease(
        &self,
        qname: &str,
        ids: &[String],
        lease: Duration,
    ) -> Result<DateTime<Utc>> {
        let expires_at = Utc::now() + lease;
        self.pool
            .get()
            .await?
            .execute(
                "UPDATE pyasynq_tasks SET lease_expires_at = $3
                 WHERE queue = $1 AND id = ANY($2) AND state = 'active'",
                &[&qname, &ids, &expires_at],
            )
            .await?;
        Ok(expires_at)
    }

    pub async fn list_lease_expired(
        &self,
        cutoff: DateTime<Utc>,
        qnames: &[String],
    ) -> Result<Vec<TaskMessage>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT msg FROM pyasynq_tasks
                 WHERE queue = ANY($1) AND state = 'active' AND lease_expires_at <= $2",
                &[&qnames, &cutoff],
            )
            .await?;
        msgs(rows)
    }

    pub async fn write_server_state(
        &self,
        info: &pb::ServerInfo,
        workers: &[pb::WorkerInfo],
        ttl: Duration,
    ) -> Result<()> {
        let key = server_key(&info.host, info.pid as u32, &info.server_id);
        let workers: Vec<Vec<u8>> = workers.iter().map(|w| w.encode_to_vec()).collect();
        let now = Utc::now();
        self.pool
            .get()
            .await?
            .execute(
                "WITH gone AS (
                     DELETE FROM pyasynq_servers WHERE expires_at < $5 AND key <> $1
                 )
                 INSERT INTO pyasynq_servers (key, info, workers, expires_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (key) DO UPDATE
                 SET info = EXCLUDED.info, workers = EXCLUDED.workers,
                     expires_at = EXCLUDED.expires_at",
                &[&key, &info.encode_to_vec(), &workers, &(now + ttl), &now],
            )
            .await?;
        Ok(())
    }

    pub async fn clear_server_state(&self, host: &str, pid: u32, server_id: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "DELETE FROM pyasynq_servers WHERE key = $1",
                &[&server_key(host, pid, server_id)],
            )
            .await?;
        Ok(())
    }

    /// The janitor: deletes completed tasks past their retention, trims the
    /// archive by age and size, and drops expired uniqueness locks.
    pub async fn delete_expired_completed_tasks(
        &self,
        qname: &str,
        batch_size: usize,
    ) -> Result<u64> {
        let now = Utc::now();
        let client = self.pool.get().await?;
        client
            .execute(
                "DELETE FROM pyasynq_tasks
                 WHERE queue = $1 AND state = 'archived' AND (
                     archived_at < $2 OR id IN (
                         SELECT id FROM pyasynq_tasks
                         WHERE queue = $1 AND state = 'archived'
                         ORDER BY archived_at DESC
                         OFFSET $3
                     )
                 )",
                &[&qname, &(now - ARCHIVED_EXPIRATION), &MAX_ARCHIVE_SIZE],
            )
            .await?;
        client
            .execute(
                "DELETE FROM pyasynq_unique_locks WHERE expires_at <= $1",
                &[&now],
            )
            .await?;
        let stmt = client
            .prepare_cached(
                "DELETE FROM pyasynq_tasks t
                 USING (
                     SELECT queue, id FROM pyasynq_tasks
                     WHERE queue = $1 AND state = 'completed' AND expires_at <= $2
                     LIMIT $3
                 ) d
                 WHERE t.queue = d.queue AND t.id = d.id
                     -- Rechecked against the latest row version if it was updated
                     -- while we waited for its lock (e.g. requeued by run_task).
                     AND t.state = 'completed' AND t.expires_at <= $2",
            )
            .await?;
        let mut total = 0;
        loop {
            let n = client
                .execute(&stmt, &[&qname, &now, &(batch_size as i64)])
                .await?;
            total += n;
            if n == 0 {
                return Ok(total);
            }
        }
    }

    pub async fn write_result(&self, qname: &str, id: &str, data: &[u8]) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "UPDATE pyasynq_tasks SET result = $3 WHERE queue = $1 AND id = $2",
                &[&qname, &id, &data],
            )
            .await?;
        Ok(())
    }

    pub async fn publish_cancelation(&self, id: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute("SELECT pg_notify($1, $2)", &[&CANCEL_CHANNEL, &id])
            .await?;
        Ok(())
    }

    async fn listener(&self) -> &Listener {
        self.listener
            .get_or_init(|| async {
                let pending = Arc::new(Notify::new());
                let (cancels, _) = broadcast::channel(256);
                tokio::spawn(listen(
                    self.config.clone(),
                    self.tls.clone(),
                    pending.clone(),
                    cancels.clone(),
                    self.stop.clone(),
                ));
                Listener { pending, cancels }
            })
            .await
    }

    pub async fn pending_notifier(&self) -> Arc<Notify> {
        self.listener().await.pending.clone()
    }

    pub async fn subscribe_cancellations(&self) -> Result<Cancellations> {
        Ok(Cancellations::Postgres(
            self.listener().await.cancels.subscribe(),
        ))
    }
}

fn server_key(host: &str, pid: u32, server_id: &str) -> String {
    format!("{host}:{pid}:{server_id}")
}

/// Holds one connection in LISTEN, reconnecting until `stop`. After every
/// (re)connect it signals `pending`, since notifications sent while
/// disconnected are lost.
async fn listen(
    config: Config,
    tls: MakeRustlsConnect,
    pending: Arc<Notify>,
    cancels: broadcast::Sender<String>,
    stop: CancellationToken,
) {
    loop {
        let session = async {
            let (client, mut conn) = config.connect(tls.clone()).await?;
            let (tx, mut rx) = mpsc::unbounded_channel();
            let driver = tokio::spawn(async move {
                let mut messages = futures_util::stream::poll_fn(move |cx| conn.poll_message(cx));
                while let Some(m) = messages.next().await {
                    match m {
                        Ok(AsyncMessage::Notification(n)) => {
                            if tx.send(n).is_err() {
                                return;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            log::warn!("postgres LISTEN connection failed: {e}");
                            return;
                        }
                    }
                }
            });
            let _abort = AbortOnDrop(driver);
            client
                .batch_execute(&format!(
                    "LISTEN {PENDING_CHANNEL}; LISTEN {CANCEL_CHANNEL}"
                ))
                .await?;
            pending.notify_one();
            while let Some(n) = rx.recv().await {
                if n.channel() == CANCEL_CHANNEL {
                    let _ = cancels.send(n.payload().to_string());
                } else {
                    pending.notify_one();
                }
            }
            Ok::<_, tokio_postgres::Error>(())
        };
        tokio::select! {
            _ = stop.cancelled() => return,
            r = session => if let Err(e) = r {
                log::warn!("postgres LISTEN failed: {e}");
            },
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl PgRdb {
    pub async fn list_groups(&self, qname: &str) -> Result<Vec<String>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT DISTINCT group_key FROM pyasynq_tasks
                 WHERE queue = $1 AND state = 'aggregating' AND aggregation_set IS NULL",
                &[&qname],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Same rules as asynq's `aggregation_check.lua`: a set is created when
    /// the group is full, its oldest task waited `max_delay`, or nothing
    /// joined it for `grace_period`.
    #[allow(clippy::too_many_arguments)]
    pub async fn aggregation_check(
        &self,
        qname: &str,
        group: &str,
        now: DateTime<Utc>,
        set_id: &str,
        grace_period: Duration,
        max_delay: Duration,
        max_size: usize,
    ) -> Result<bool> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1 || chr(1) || $2, 0))",
            &[&qname, &group],
        )
        .await?;
        let row = tx
            .query_one(
                "SELECT count(*), min(grouped_at), max(grouped_at) FROM pyasynq_tasks
                 WHERE queue = $1 AND group_key = $2
                   AND state = 'aggregating' AND aggregation_set IS NULL",
                &[&qname, &group],
            )
            .await?;
        let size = row.get::<_, i64>(0) as usize;
        let (Some(oldest), Some(latest)) = (
            row.get::<_, Option<DateTime<Utc>>>(1),
            row.get::<_, Option<DateTime<Utc>>>(2),
        ) else {
            return Ok(false);
        };
        let ready = (max_size != 0 && size >= max_size)
            || (!max_delay.is_zero() && oldest <= now - max_delay)
            || latest <= now - grace_period;
        if !ready {
            return Ok(false);
        }
        let limit = (max_size != 0).then_some(max_size as i64);
        tx.execute(
            "UPDATE pyasynq_tasks t SET aggregation_set = $3
             FROM (
                 SELECT queue, id FROM pyasynq_tasks
                 WHERE queue = $1 AND group_key = $2
                   AND state = 'aggregating' AND aggregation_set IS NULL
                 ORDER BY grouped_at, id
                 LIMIT $4
             ) c
             WHERE t.queue = c.queue AND t.id = c.id",
            &[&qname, &group, &set_id, &limit],
        )
        .await?;
        tx.execute(
            "INSERT INTO pyasynq_aggregation_sets (queue, group_key, set_id, deadline)
             VALUES ($1, $2, $3, $4)",
            &[&qname, &group, &set_id, &(Utc::now() + AGGREGATION_TIMEOUT)],
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn read_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<Vec<TaskMessage>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT msg FROM pyasynq_tasks
                 WHERE queue = $1 AND group_key = $2 AND aggregation_set = $3
                 ORDER BY grouped_at, id",
                &[&qname, &group, &set_id],
            )
            .await?;
        msgs(rows)
    }

    pub async fn delete_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "WITH d AS (
                     DELETE FROM pyasynq_tasks
                     WHERE queue = $1 AND group_key = $2 AND aggregation_set = $3
                 )
                 DELETE FROM pyasynq_aggregation_sets
                 WHERE queue = $1 AND group_key = $2 AND set_id = $3",
                &[&qname, &group, &set_id],
            )
            .await?;
        Ok(())
    }

    pub async fn reclaim_stale_aggregation_sets(&self, qname: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "WITH stale AS (
                     DELETE FROM pyasynq_aggregation_sets
                     WHERE queue = $1 AND deadline <= $2
                     RETURNING group_key, set_id
                 )
                 UPDATE pyasynq_tasks t SET aggregation_set = NULL
                 FROM stale
                 WHERE t.queue = $1 AND t.group_key = stale.group_key
                   AND t.aggregation_set = stale.set_id",
                &[&qname, &Utc::now()],
            )
            .await?;
        Ok(())
    }

    pub async fn semaphore_acquire(
        &self,
        scope: &str,
        max_tokens: u32,
        deadline: DateTime<Utc>,
        task_id: &str,
    ) -> Result<bool> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('pyasynq_sema:' || $1, 0))",
            &[&scope],
        )
        .await?;
        tx.execute(
            "DELETE FROM pyasynq_semaphores WHERE scope = $1 AND expires_at < $2",
            &[&scope, &ts(Utc::now().timestamp())],
        )
        .await?;
        let held: i64 = tx
            .query_one(
                "SELECT count(*) FROM pyasynq_semaphores WHERE scope = $1",
                &[&scope],
            )
            .await?
            .get(0);
        let acquired = held < i64::from(max_tokens);
        if acquired {
            tx.execute(
                "INSERT INTO pyasynq_semaphores (scope, task_id, expires_at) VALUES ($1, $2, $3)
                 ON CONFLICT (scope, task_id) DO UPDATE SET expires_at = EXCLUDED.expires_at",
                &[&scope, &task_id, &ts(deadline.timestamp())],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(acquired)
    }

    pub async fn semaphore_release(&self, scope: &str, task_id: &str) -> Result<bool> {
        let n = self
            .pool
            .get()
            .await?
            .execute(
                "DELETE FROM pyasynq_semaphores WHERE scope = $1 AND task_id = $2",
                &[&scope, &task_id],
            )
            .await?;
        Ok(n == 1)
    }

    pub async fn write_scheduler_entries(
        &self,
        scheduler_id: &str,
        entries: &[pb::SchedulerEntry],
        ttl: Duration,
    ) -> Result<()> {
        let now = Utc::now();
        let entries: Vec<Vec<u8>> = entries.iter().map(|e| e.encode_to_vec()).collect();
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "DELETE FROM pyasynq_scheduler_entries WHERE scheduler_id = $1 OR expires_at < $2",
            &[&scheduler_id, &now],
        )
        .await?;
        tx.execute(
            "INSERT INTO pyasynq_scheduler_entries (scheduler_id, position, entry, expires_at)
             SELECT $1, e.pos::integer, e.entry, $3
             FROM unnest($2::bytea[]) WITH ORDINALITY AS e(entry, pos)",
            &[&scheduler_id, &entries, &(now + ttl)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "DELETE FROM pyasynq_scheduler_entries WHERE scheduler_id = $1",
                &[&scheduler_id],
            )
            .await?;
        Ok(())
    }

    pub async fn record_scheduler_enqueue_event(
        &self,
        entry_id: &str,
        task_id: &str,
        enqueued_at: DateTime<Utc>,
    ) -> Result<()> {
        let event = pb::SchedulerEnqueueEvent {
            task_id: task_id.to_string(),
            enqueue_time: Some(prost_types::Timestamp {
                seconds: enqueued_at.timestamp(),
                nanos: enqueued_at.timestamp_subsec_nanos() as i32,
            }),
        };
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO pyasynq_scheduler_events (entry_id, enqueued_at, event)
                 VALUES ($1, $2, $3)",
                &[&entry_id, &enqueued_at, &event.encode_to_vec()],
            )
            .await?;
        client
            .execute(
                "DELETE FROM pyasynq_scheduler_events
                 WHERE entry_id = $1 AND enqueued_at < (
                     SELECT enqueued_at FROM pyasynq_scheduler_events
                     WHERE entry_id = $1
                     ORDER BY enqueued_at DESC
                     OFFSET $2 LIMIT 1
                 )",
                &[&entry_id, &(MAX_EVENTS - 1)],
            )
            .await?;
        Ok(())
    }

    pub async fn clear_scheduler_history(&self, entry_id: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "DELETE FROM pyasynq_scheduler_events WHERE entry_id = $1",
                &[&entry_id],
            )
            .await?;
        Ok(())
    }
}

const UNSUPPORTED: &str = "unsupported state";

/// Columns cleared when a task leaves its state for pending.
const TO_PENDING: &str = "state = 'pending', pending_since = $3, process_at = NULL,
    archived_at = NULL, expires_at = NULL, aggregation_set = NULL";
const TO_ARCHIVED: &str = "state = 'archived', archived_at = $3, pending_since = NULL,
    process_at = NULL, aggregation_set = NULL";

impl PgRdb {
    pub async fn all_queues(&self) -> Result<Vec<String>> {
        let rows = self
            .pool
            .get()
            .await?
            .query("SELECT name FROM pyasynq_queues ORDER BY name", &[])
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    async fn check_queue(&self, qname: &str) -> Result<()> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt("SELECT 1 FROM pyasynq_queues WHERE name = $1", &[&qname])
            .await?;
        row.map(|_| ())
            .ok_or_else(|| Error::QueueNotFound(qname.to_string()))
    }

    pub async fn current_stats(&self, qname: &str) -> Result<QueueStats> {
        self.check_queue(qname).await?;
        let now = Utc::now();
        let client = self.pool.get().await?;
        let t = client
            .query_one(
                "SELECT
                     count(*) FILTER (WHERE state = 'pending'),
                     count(*) FILTER (WHERE state = 'active'),
                     count(*) FILTER (WHERE state = 'scheduled'),
                     count(*) FILTER (WHERE state = 'retry'),
                     count(*) FILTER (WHERE state = 'archived'),
                     count(*) FILTER (WHERE state = 'completed'),
                     count(*) FILTER (WHERE state = 'aggregating' AND aggregation_set IS NULL),
                     count(DISTINCT group_key)
                         FILTER (WHERE state = 'aggregating' AND aggregation_set IS NULL),
                     min(pending_since) FILTER (WHERE state = 'pending'),
                     coalesce(sum(octet_length(msg) + coalesce(octet_length(result), 0)), 0)::bigint
                 FROM pyasynq_tasks WHERE queue = $1",
                &[&qname],
            )
            .await?;
        let s = client
            .query_one(
                "SELECT
                     coalesce(sum(processed) FILTER (WHERE day = $2), 0)::bigint,
                     coalesce(sum(failed) FILTER (WHERE day = $2), 0)::bigint,
                     coalesce(sum(processed), 0)::bigint,
                     coalesce(sum(failed), 0)::bigint,
                     coalesce((SELECT paused FROM pyasynq_queues WHERE name = $1), false)
                 FROM pyasynq_stats WHERE queue = $1",
                &[&qname, &today(now)],
            )
            .await?;
        let mut stats = QueueStats {
            queue: qname.to_string(),
            memory_usage: t.get(9),
            latency: t
                .get::<_, Option<DateTime<Utc>>>(8)
                .and_then(|since| (now - since).to_std().ok())
                .unwrap_or_default(),
            groups: t.get(7),
            pending: t.get(0),
            active: t.get(1),
            scheduled: t.get(2),
            retry: t.get(3),
            archived: t.get(4),
            completed: t.get(5),
            aggregating: t.get(6),
            processed: s.get(0),
            failed: s.get(1),
            processed_total: s.get(2),
            failed_total: s.get(3),
            paused: s.get(4),
            timestamp: now,
            ..Default::default()
        };
        stats.size = stats.pending
            + stats.active
            + stats.scheduled
            + stats.retry
            + stats.archived
            + stats.completed
            + stats.aggregating;
        Ok(stats)
    }

    pub async fn historical_stats(&self, qname: &str, days: usize) -> Result<Vec<DailyStats>> {
        if days < 1 {
            return Err(Error::FailedPrecondition(
                "the number of days must be positive".into(),
            ));
        }
        self.check_queue(qname).await?;
        let now = Utc::now();
        let dates: Vec<DateTime<Utc>> = (0..days)
            .map(|i| now - chrono::Duration::days(i as i64))
            .collect();
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT day, sum(processed)::bigint, sum(failed)::bigint FROM pyasynq_stats
                 WHERE queue = $1 AND day >= $2 GROUP BY day",
                &[&qname, &today(dates[days - 1])],
            )
            .await?;
        let by_day: std::collections::HashMap<NaiveDate, (i64, i64)> = rows
            .iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2))))
            .collect();
        Ok(dates
            .into_iter()
            .map(|date| {
                let (processed, failed) = by_day.get(&today(date)).copied().unwrap_or_default();
                DailyStats {
                    queue: qname.to_string(),
                    processed,
                    failed,
                    date,
                }
            })
            .collect())
    }

    pub async fn get_task_info(&self, qname: &str, id: &str) -> Result<TaskInfo> {
        self.check_queue(qname).await?;
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT msg, state, process_at, result FROM pyasynq_tasks
                 WHERE queue = $1 AND id = $2",
                &[&qname, &id],
            )
            .await?
            .ok_or(Error::NotFound)?;
        task_info(&row)
    }

    pub async fn group_stats(&self, qname: &str) -> Result<Vec<(String, i64)>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT group_key, count(*) FROM pyasynq_tasks
                 WHERE queue = $1 AND state = 'aggregating' AND aggregation_set IS NULL
                 GROUP BY group_key ORDER BY group_key",
                &[&qname],
            )
            .await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    /// Tasks in a given state, oldest first, `size` per page.
    pub async fn list_tasks(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
        page: usize,
        size: usize,
    ) -> Result<Vec<TaskInfo>> {
        self.check_queue(qname).await?;
        let order = match state {
            TaskState::Pending => "pending_since, id",
            TaskState::Active => "lease_expires_at, id",
            TaskState::Scheduled | TaskState::Retry => "process_at, id",
            TaskState::Archived => "archived_at, id",
            TaskState::Completed => "expires_at, id",
            TaskState::Aggregating => "grouped_at, id",
        };
        let group_filter = if state == TaskState::Aggregating {
            group.ok_or_else(|| {
                Error::FailedPrecondition("listing aggregating tasks needs a group".into())
            })?;
            "AND group_key = $3 AND aggregation_set IS NULL"
        } else {
            "AND $3::text IS NULL"
        };
        let sql = format!(
            "SELECT msg, state, process_at, result FROM pyasynq_tasks
             WHERE queue = $1 AND state = $2 {group_filter}
             ORDER BY {order} OFFSET $4 LIMIT $5"
        );
        let group = if state == TaskState::Aggregating {
            group
        } else {
            None
        };
        let rows = self
            .pool
            .get()
            .await?
            .query(
                &sql,
                &[
                    &qname,
                    &state.as_str(),
                    &group,
                    &((page * size) as i64),
                    &(size as i64),
                ],
            )
            .await?;
        rows.iter().map(task_info).collect()
    }

    /// Locks a task for an Inspector action, returning its state.
    async fn lock_state(
        tx: &deadpool_postgres::Transaction<'_>,
        qname: &str,
        id: &str,
    ) -> Result<String> {
        tx.query_opt(
            "SELECT state FROM pyasynq_tasks WHERE queue = $1 AND id = $2 FOR UPDATE",
            &[&qname, &id],
        )
        .await?
        .map(|r| r.get(0))
        .ok_or(Error::NotFound)
    }

    pub async fn run_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        match Self::lock_state(&tx, qname, id).await?.as_str() {
            "active" => return Err(Error::FailedPrecondition("task is already running".into())),
            "pending" => {
                return Err(Error::FailedPrecondition(
                    "task is already in pending state".into(),
                ))
            }
            _ => {}
        }
        tx.execute(
            &format!("UPDATE pyasynq_tasks SET {TO_PENDING} WHERE queue = $1 AND id = $2"),
            &[&qname, &id, &Utc::now()],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn archive_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        match Self::lock_state(&tx, qname, id).await?.as_str() {
            "active" => {
                return Err(Error::FailedPrecondition(
                    "cannot archive task in active state. use cancel_processing instead.".into(),
                ))
            }
            "archived" => return Err(Error::FailedPrecondition("task is already archived".into())),
            _ => {}
        }
        tx.execute(
            &format!("UPDATE pyasynq_tasks SET {TO_ARCHIVED} WHERE queue = $1 AND id = $2"),
            &[&qname, &id, &Utc::now()],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        if Self::lock_state(&tx, qname, id).await? == "active" {
            return Err(Error::FailedPrecondition(
                "cannot delete task in active state. use cancel_processing instead.".into(),
            ));
        }
        tx.execute(
            "WITH d AS (
                 DELETE FROM pyasynq_tasks WHERE queue = $1 AND id = $2 RETURNING id, unique_key
             )
             DELETE FROM pyasynq_unique_locks l USING d
             WHERE l.key = d.unique_key AND l.task_id = d.id",
            &[&qname, &id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update_task_payload(&self, qname: &str, id: &str, payload: &[u8]) -> Result<()> {
        self.check_queue(qname).await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                "SELECT msg, state FROM pyasynq_tasks WHERE queue = $1 AND id = $2 FOR UPDATE",
                &[&qname, &id],
            )
            .await?
            .ok_or(Error::NotFound)?;
        if row.get::<_, &str>(1) != "scheduled" {
            return Err(Error::FailedPrecondition(
                "cannot update task that is not in scheduled state.".into(),
            ));
        }
        let mut msg = decode(row.get(0))?;
        msg.payload = payload.to_vec();
        tx.execute(
            "UPDATE pyasynq_tasks SET msg = $3 WHERE queue = $1 AND id = $2",
            &[&qname, &id, &base::encode_message(&msg)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Runs `UPDATE pyasynq_tasks SET {set}` on every task in `state` (or in
    /// `group` for aggregating tasks); `$3` in `set` is the current time.
    async fn update_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
        set: &str,
    ) -> Result<i64> {
        let (filter, group) = match state {
            TaskState::Aggregating => (
                "AND group_key = $4 AND aggregation_set IS NULL",
                Some(group_required(group)?),
            ),
            _ => ("AND $4::text IS NULL", None),
        };
        let n = self
            .pool
            .get()
            .await?
            .execute(
                &format!("UPDATE pyasynq_tasks SET {set} WHERE queue = $1 AND state = $2 {filter}"),
                &[&qname, &state.as_str(), &Utc::now(), &group],
            )
            .await?;
        Ok(n as i64)
    }

    /// Moves every task in `state` (or in `group`) to pending.
    pub async fn run_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64> {
        self.check_queue(qname).await?;
        match state {
            TaskState::Pending | TaskState::Active => Err(Error::FailedPrecondition(format!(
                "{UNSUPPORTED} {}",
                state.as_str()
            ))),
            _ => self.update_all(qname, state, group, TO_PENDING).await,
        }
    }

    pub async fn archive_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
    ) -> Result<i64> {
        self.check_queue(qname).await?;
        match state {
            TaskState::Pending
            | TaskState::Aggregating
            | TaskState::Scheduled
            | TaskState::Retry => self.update_all(qname, state, group, TO_ARCHIVED).await,
            s => Err(Error::FailedPrecondition(format!(
                "cannot archive {} tasks",
                s.as_str()
            ))),
        }
    }

    pub async fn delete_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
    ) -> Result<i64> {
        self.check_queue(qname).await?;
        let (filter, group) = match state {
            TaskState::Active => {
                return Err(Error::FailedPrecondition(format!(
                    "{UNSUPPORTED} {}",
                    state.as_str()
                )))
            }
            TaskState::Aggregating => (
                "AND group_key = $3 AND aggregation_set IS NULL",
                Some(group_required(group)?),
            ),
            _ => ("AND $3::text IS NULL", None),
        };
        let row = self
            .pool
            .get()
            .await?
            .query_one(
                &format!(
                    "WITH d AS (
                         DELETE FROM pyasynq_tasks WHERE queue = $1 AND state = $2 {filter}
                         RETURNING id, unique_key
                     ), u AS (
                         DELETE FROM pyasynq_unique_locks l USING d
                         WHERE l.key = d.unique_key AND l.task_id = d.id
                     )
                     SELECT count(*) FROM d"
                ),
                &[&qname, &state.as_str(), &group],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Removes a queue. Without `force` the queue must be empty; active
    /// tasks always block removal.
    pub async fn remove_queue(&self, qname: &str, force: bool) -> Result<()> {
        self.check_queue(qname).await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "SELECT 1 FROM pyasynq_queues WHERE name = $1 FOR UPDATE",
            &[&qname],
        )
        .await?;
        let row = tx
            .query_one(
                "SELECT count(*), count(*) FILTER (WHERE state = 'active')
                 FROM pyasynq_tasks WHERE queue = $1",
                &[&qname],
            )
            .await?;
        let (total, active): (i64, i64) = (row.get(0), row.get(1));
        if !force && total > 0 {
            return Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not empty"
            )));
        }
        if active > 0 {
            return Err(Error::FailedPrecondition(
                "cannot remove queue with active tasks".into(),
            ));
        }
        // A claim committing while we wait for its row lock turns the row
        // active; the DELETE then skips it and the check below aborts.
        tx.execute(
            "WITH d AS (
                 DELETE FROM pyasynq_tasks WHERE queue = $1 AND state <> 'active'
                 RETURNING id, unique_key
             )
             DELETE FROM pyasynq_unique_locks l USING d
             WHERE l.key = d.unique_key AND l.task_id = d.id",
            &[&qname],
        )
        .await?;
        let claimed: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pyasynq_tasks WHERE queue = $1)",
                &[&qname],
            )
            .await?
            .get(0);
        if claimed {
            return Err(Error::FailedPrecondition(
                "cannot remove queue with active tasks".into(),
            ));
        }
        tx.execute(
            "DELETE FROM pyasynq_aggregation_sets WHERE queue = $1",
            &[&qname],
        )
        .await?;
        tx.execute("DELETE FROM pyasynq_queues WHERE name = $1", &[&qname])
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn pause(&self, qname: &str) -> Result<()> {
        let n = self
            .pool
            .get()
            .await?
            .execute(
                "INSERT INTO pyasynq_queues (name, paused) VALUES ($1, true)
                 ON CONFLICT (name) DO UPDATE SET paused = true
                 WHERE NOT pyasynq_queues.paused",
                &[&qname],
            )
            .await?;
        match n {
            0 => Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is already paused"
            ))),
            _ => Ok(()),
        }
    }

    pub async fn unpause(&self, qname: &str) -> Result<()> {
        let n = self
            .pool
            .get()
            .await?
            .execute(
                "UPDATE pyasynq_queues SET paused = false WHERE name = $1 AND paused",
                &[&qname],
            )
            .await?;
        match n {
            0 => Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not paused"
            ))),
            _ => Ok(()),
        }
    }

    pub async fn list_servers(&self) -> Result<Vec<ServerDetail>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT info, workers FROM pyasynq_servers WHERE expires_at > $1 ORDER BY key",
                &[&Utc::now()],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let info = pb::ServerInfo::decode(r.get::<_, &[u8]>(0)).ok()?;
                let workers = r
                    .get::<_, Vec<&[u8]>>(1)
                    .into_iter()
                    .filter_map(|w| pb::WorkerInfo::decode(w).ok())
                    .collect();
                Some(ServerDetail { info, workers })
            })
            .collect())
    }

    pub async fn list_scheduler_entries(&self) -> Result<Vec<pb::SchedulerEntry>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT entry FROM pyasynq_scheduler_entries WHERE expires_at > $1
                 ORDER BY scheduler_id, position",
                &[&Utc::now()],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| pb::SchedulerEntry::decode(r.get::<_, &[u8]>(0)).ok())
            .collect())
    }

    pub async fn list_scheduler_enqueue_events(
        &self,
        entry_id: &str,
        page: usize,
        size: usize,
    ) -> Result<Vec<pb::SchedulerEnqueueEvent>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT event FROM pyasynq_scheduler_events WHERE entry_id = $1
                 ORDER BY enqueued_at DESC OFFSET $2 LIMIT $3",
                &[&entry_id, &((page * size) as i64), &(size as i64)],
            )
            .await?;
        rows.iter()
            .map(|r| pb::SchedulerEnqueueEvent::decode(r.get::<_, &[u8]>(0)).map_err(Error::from))
            .collect()
    }
}

fn task_info(row: &Row) -> Result<TaskInfo> {
    let state: &str = row.get(1);
    let state =
        TaskState::parse(state).ok_or_else(|| Error::Internal(format!("unknown state {state}")))?;
    let next_process_at = match state {
        TaskState::Pending => Some(Utc::now()),
        TaskState::Scheduled | TaskState::Retry => row.get(2),
        _ => None,
    };
    Ok(TaskInfo {
        msg: decode(row.get(0))?,
        state,
        next_process_at,
        result: row.get(3),
    })
}
