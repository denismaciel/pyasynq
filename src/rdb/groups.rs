//! Task groups: tasks sharing a group key wait in a sorted set until the
//! aggregator turns them into one "aggregation set" for a single handler call.

use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use super::{decode, status_to_result, Rdb, Result};
use crate::base::{self, TaskMessage};

lua!(ADD_TO_GROUP, "add_to_group.lua");
lua!(ADD_TO_GROUP_UNIQUE, "add_to_group_unique.lua");
lua!(AGGREGATION_CHECK, "aggregation_check.lua");
lua!(READ_AGGREGATION_SET, "read_aggregation_set.lua");
lua!(DELETE_AGGREGATION_SET, "delete_aggregation_set.lua");
lua!(
    RECLAIM_STALE_AGGREGATION_SETS,
    "reclaim_stale_aggregation_sets.lua"
);

/// An aggregation set not deleted within this time is put back into its
/// group by the recoverer.
pub const AGGREGATION_TIMEOUT: Duration = Duration::from_secs(120);

impl Rdb {
    pub async fn add_to_group(
        &self,
        msg: &TaskMessage,
        group: &str,
        unique_ttl_secs: Option<i64>,
    ) -> Result<()> {
        self.publish_queue(&msg.queue).await?;
        let q = &msg.queue;
        let mut conn = self.conn.clone();
        let mut inv = match unique_ttl_secs {
            Some(_) => ADD_TO_GROUP_UNIQUE.prepare_invoke(),
            None => ADD_TO_GROUP.prepare_invoke(),
        };
        inv.key(base::task_key(q, &msg.id))
            .key(base::group_key(q, group))
            .key(base::all_groups(q));
        if unique_ttl_secs.is_some() {
            inv.key(&msg.unique_key);
        }
        inv.arg(base::encode_message(msg))
            .arg(&msg.id)
            .arg(Utc::now().timestamp())
            .arg(group);
        if let Some(ttl) = unique_ttl_secs {
            inv.arg(ttl);
        }
        let n: i64 = inv.invoke_async(&mut conn).await?;
        status_to_result(n)
    }

    pub async fn list_groups(&self, qname: &str) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        Ok(conn.smembers(base::all_groups(qname)).await?)
    }

    /// Moves the group's tasks into aggregation set `set_id` if the group is
    /// full, its oldest task waited `max_delay`, or nothing was added for
    /// `grace_period`. Returns whether a set was created.
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
        let mut conn = self.conn.clone();
        let n: i64 = AGGREGATION_CHECK
            .key(base::group_key(qname, group))
            .key(base::aggregation_set_key(qname, group, set_id))
            .key(base::all_aggregation_sets(qname))
            .key(base::all_groups(qname))
            .arg(max_size)
            .arg(max_delay.as_secs())
            .arg(grace_period.as_secs())
            .arg((Utc::now() + AGGREGATION_TIMEOUT).timestamp())
            .arg(now.timestamp())
            .arg(group)
            .invoke_async(&mut conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn read_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<Vec<TaskMessage>> {
        let mut conn = self.conn.clone();
        let data: Vec<Vec<u8>> = READ_AGGREGATION_SET
            .key(base::aggregation_set_key(qname, group, set_id))
            .arg(base::task_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        data.iter().map(|d| decode(d)).collect()
    }

    pub async fn delete_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: () = DELETE_AGGREGATION_SET
            .key(base::aggregation_set_key(qname, group, set_id))
            .key(base::all_aggregation_sets(qname))
            .arg(base::task_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn reclaim_stale_aggregation_sets(&self, qname: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: () = RECLAIM_STALE_AGGREGATION_SETS
            .key(base::all_aggregation_sets(qname))
            .arg(Utc::now().timestamp())
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }
}
