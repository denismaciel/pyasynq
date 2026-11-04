//! Scheduler bookkeeping read by the Inspector: registered entries and the
//! history of what each entry enqueued.

use std::time::Duration;

use chrono::{DateTime, Utc};
use prost::Message;
use redis::AsyncCommands;

use super::{RedisRdb, Result};
use crate::base::{self, pb};

lua!(WRITE_SCHEDULER_ENTRIES, "write_scheduler_entries.lua");
lua!(
    RECORD_SCHEDULER_ENQUEUE_EVENT,
    "record_scheduler_enqueue_event.lua"
);

/// Enqueue events kept per entry.
pub const MAX_EVENTS: i64 = 1000;

impl RedisRdb {
    pub async fn write_scheduler_entries(
        &self,
        scheduler_id: &str,
        entries: &[pb::SchedulerEntry],
        ttl: Duration,
    ) -> Result<()> {
        let key = base::scheduler_entries_key(scheduler_id);
        let mut conn = self.conn.clone();
        let exp = (Utc::now() + ttl).timestamp();
        let _: i64 = conn.zadd(base::ALL_SCHEDULERS, &key, exp).await?;
        let mut inv = WRITE_SCHEDULER_ENTRIES.prepare_invoke();
        inv.key(&key).arg(ttl.as_secs().max(1));
        for e in entries {
            inv.arg(e.encode_to_vec());
        }
        let _: () = inv.invoke_async(&mut conn).await?;
        Ok(())
    }

    pub async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()> {
        let key = base::scheduler_entries_key(scheduler_id);
        let mut conn = self.conn.clone();
        let _: i64 = conn.zrem(base::ALL_SCHEDULERS, &key).await?;
        let _: i64 = conn.del(&key).await?;
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
        let mut conn = self.conn.clone();
        let _: () = RECORD_SCHEDULER_ENQUEUE_EVENT
            .key(base::scheduler_history_key(entry_id))
            .arg(enqueued_at.timestamp())
            .arg(event.encode_to_vec())
            .arg(MAX_EVENTS)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn clear_scheduler_history(&self, entry_id: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i64 = conn.del(base::scheduler_history_key(entry_id)).await?;
        Ok(())
    }
}
