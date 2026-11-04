//! State transitions used by the worker engine (processor, heartbeater,
//! forwarder, recoverer, janitor).

use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use super::{decode, stats_expire_at, utc_date, Error, RedisRdb, Result};
use crate::base::{self, pb, TaskMessage};

lua!(REQUEUE, "requeue.lua");
lua!(RETRY, "retry.lua");
lua!(ARCHIVE, "archive.lua");
lua!(FORWARD, "forward.lua");
lua!(WRITE_SERVER_STATE, "write_server_state.lua");
lua!(CLEAR_SERVER_STATE, "clear_server_state.lua");
lua!(DELETE_EXPIRED_COMPLETED, "delete_expired_completed.lua");
lua!(LIST_LEASE_EXPIRED, "list_lease_expired.lua");

pub const MAX_ARCHIVE_SIZE: i64 = 10_000;
pub const ARCHIVED_EXPIRATION: Duration = Duration::from_secs(90 * 24 * 60 * 60);

impl RedisRdb {
    pub async fn subscribe_cancellations(&self) -> Result<super::Cancellations> {
        let mut ps = self.client.get_async_pubsub().await?;
        ps.subscribe(base::CANCEL_CHANNEL).await?;
        Ok(super::Cancellations::Redis(Box::pin(ps.into_on_message())))
    }

    /// Puts an active task back at the head of its pending list.
    pub async fn requeue(&self, msg: &TaskMessage) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: () = REQUEUE
            .key(base::active_key(&msg.queue))
            .key(base::lease_key(&msg.queue))
            .key(base::pending_key(&msg.queue))
            .key(base::task_key(&msg.queue, &msg.id))
            .arg(&msg.id)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Moves an active task to the retry set, to run again at `retry_at`.
    /// Only failures (`is_failure`) count against the retry budget and the
    /// failed stats.
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
        let q = &msg.queue;
        let date = utc_date(now);
        let mut conn = self.conn.clone();
        let _: () = RETRY
            .key(base::task_key(q, &msg.id))
            .key(base::active_key(q))
            .key(base::lease_key(q))
            .key(base::retry_key(q))
            .key(base::processed_key(q, &date))
            .key(base::failed_key(q, &date))
            .key(base::processed_total_key(q))
            .key(base::failed_total_key(q))
            .arg(&msg.id)
            .arg(base::encode_message(&modified))
            .arg(retry_at.timestamp())
            .arg(stats_expire_at(now))
            .arg(is_failure as i64)
            .arg(i64::MAX)
            .arg(lease_cutoff.map_or(-1, |c| c.timestamp()))
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Moves an active task to the archive (dead letters), trimming the
    /// archive by age and size.
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
        let q = &msg.queue;
        let date = utc_date(now);
        let mut conn = self.conn.clone();
        let _: () = ARCHIVE
            .key(base::task_key(q, &msg.id))
            .key(base::active_key(q))
            .key(base::lease_key(q))
            .key(base::archived_key(q))
            .key(base::processed_key(q, &date))
            .key(base::failed_key(q, &date))
            .key(base::processed_total_key(q))
            .key(base::failed_total_key(q))
            .key(base::task_key_prefix(q))
            .arg(&msg.id)
            .arg(base::encode_message(&modified))
            .arg(now.timestamp())
            .arg((now - ARCHIVED_EXPIRATION).timestamp())
            .arg(MAX_ARCHIVE_SIZE)
            .arg(stats_expire_at(now))
            .arg(i64::MAX)
            .arg(lease_cutoff.map_or(-1, |c| c.timestamp()))
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Moves due scheduled/retry tasks of `qname` to pending (or to their
    /// group). Returns how many moved.
    pub async fn forward_if_ready(&self, qname: &str) -> Result<u64> {
        let mut total = 0;
        for delayed in [base::scheduled_key(qname), base::retry_key(qname)] {
            loop {
                let now = Utc::now();
                let mut conn = self.conn.clone();
                let n: u64 = FORWARD
                    .key(&delayed)
                    .key(base::pending_key(qname))
                    .key(base::all_groups(qname))
                    .arg(now.timestamp())
                    .arg(base::task_key_prefix(qname))
                    .arg(now.timestamp_nanos_opt().unwrap_or_default())
                    .arg(base::group_key_prefix(qname))
                    .invoke_async(&mut conn)
                    .await?;
                total += n;
                if n == 0 {
                    break;
                }
            }
        }
        Ok(total)
    }

    /// Extends the leases of the given active tasks. `ZADD XX` only updates
    /// members that still exist, so a lease the recoverer already reclaimed
    /// is not resurrected.
    pub async fn extend_lease(
        &self,
        qname: &str,
        ids: &[String],
        lease: Duration,
    ) -> Result<DateTime<Utc>> {
        let expires_at = Utc::now() + lease;
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(base::lease_key(qname)).arg("XX");
        for id in ids {
            cmd.arg(expires_at.timestamp()).arg(id);
        }
        let mut conn = self.conn.clone();
        let _: i64 = cmd.query_async(&mut conn).await?;
        Ok(expires_at)
    }

    pub async fn list_lease_expired(
        &self,
        cutoff: DateTime<Utc>,
        qnames: &[String],
    ) -> Result<Vec<TaskMessage>> {
        let mut msgs = Vec::new();
        for qname in qnames {
            let mut conn = self.conn.clone();
            let data: Vec<Vec<u8>> = LIST_LEASE_EXPIRED
                .key(base::lease_key(qname))
                .arg(cutoff.timestamp())
                .arg(base::task_key_prefix(qname))
                .invoke_async(&mut conn)
                .await?;
            for d in data {
                msgs.push(decode(&d)?);
            }
        }
        Ok(msgs)
    }

    pub async fn write_server_state(
        &self,
        info: &pb::ServerInfo,
        workers: &[pb::WorkerInfo],
        ttl: Duration,
    ) -> Result<()> {
        use prost::Message;
        let skey = base::server_info_key(&info.host, info.pid as u32, &info.server_id);
        let wkey = base::workers_key(&info.host, info.pid as u32, &info.server_id);
        let exp = (Utc::now() + ttl).timestamp();
        let mut conn = self.conn.clone();
        let _: i64 = conn.zadd(base::ALL_SERVERS, &skey, exp).await?;
        let _: i64 = conn.zadd(base::ALL_WORKERS, &wkey, exp).await?;
        let mut inv = WRITE_SERVER_STATE.prepare_invoke();
        inv.key(&skey)
            .key(&wkey)
            .arg(ttl.as_secs().max(1))
            .arg(info.encode_to_vec());
        for w in workers {
            inv.arg(&w.task_id).arg(w.encode_to_vec());
        }
        let _: () = inv.invoke_async(&mut conn).await?;
        Ok(())
    }

    pub async fn clear_server_state(&self, host: &str, pid: u32, server_id: &str) -> Result<()> {
        let skey = base::server_info_key(host, pid, server_id);
        let wkey = base::workers_key(host, pid, server_id);
        let mut conn = self.conn.clone();
        let _: i64 = conn.zrem(base::ALL_SERVERS, &skey).await?;
        let _: i64 = conn.zrem(base::ALL_WORKERS, &wkey).await?;
        let _: () = CLEAR_SERVER_STATE
            .key(&skey)
            .key(&wkey)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn delete_expired_completed_tasks(
        &self,
        qname: &str,
        batch_size: usize,
    ) -> Result<u64> {
        let mut total = 0;
        loop {
            let mut conn = self.conn.clone();
            let n: u64 = DELETE_EXPIRED_COMPLETED
                .key(base::completed_key(qname))
                .arg(Utc::now().timestamp())
                .arg(base::task_key_prefix(qname))
                .arg(batch_size)
                .invoke_async(&mut conn)
                .await?;
            total += n;
            if n == 0 {
                return Ok(total);
            }
        }
    }

    pub async fn write_result(&self, qname: &str, id: &str, data: &[u8]) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i64 = conn.hset(base::task_key(qname, id), "result", data).await?;
        Ok(())
    }

    pub async fn publish_cancelation(&self, id: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i64 = conn.publish(base::CANCEL_CHANNEL, id).await?;
        Ok(())
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::Internal(e.to_string())
    }
}
