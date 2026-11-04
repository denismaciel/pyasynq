//! Read and admin operations behind the Inspector (port of asynq's
//! `internal/rdb/inspect.go`).

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use prost::Message;
use redis::{AsyncCommands, Value};

use super::worker::{ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE};
use super::{decode, utc_date, Error, RedisRdb, Result};
use crate::base::{self, pb, TaskState};
use crate::client::TaskInfo;

lua!(CURRENT_STATS, "inspect/current_stats.lua");
lua!(MEMORY_USAGE, "inspect/memory_usage.lua");
lua!(HISTORICAL_STATS, "inspect/historical_stats.lua");
lua!(GET_TASK_INFO, "inspect/get_task_info.lua");
lua!(GROUP_STATS, "inspect/group_stats.lua");
lua!(LIST_MESSAGES, "inspect/list_messages.lua");
lua!(LIST_ZSET_ENTRIES, "inspect/list_z_set_entries.lua");
lua!(RUN_TASK, "inspect/run_task.lua");
lua!(RUN_ALL, "inspect/run_all.lua");
lua!(RUN_ALL_AGGREGATING, "inspect/run_all_aggregating.lua");
lua!(ARCHIVE_TASK, "inspect/archive_task.lua");
lua!(ARCHIVE_ALL, "inspect/archive_all.lua");
lua!(ARCHIVE_ALL_PENDING, "inspect/archive_all_pending.lua");
lua!(
    ARCHIVE_ALL_AGGREGATING,
    "inspect/archive_all_aggregating.lua"
);
lua!(UPDATE_TASK_PAYLOAD, "inspect/update_task_payload.lua");
lua!(DELETE_TASK, "inspect/delete_task.lua");
lua!(DELETE_ALL, "inspect/delete_all.lua");
lua!(DELETE_ALL_PENDING, "inspect/delete_all_pending.lua");
lua!(DELETE_ALL_AGGREGATING, "inspect/delete_all_aggregating.lua");
lua!(REMOVE_QUEUE, "inspect/remove_queue.lua");
lua!(REMOVE_QUEUE_FORCE, "inspect/remove_queue_force.lua");
lua!(LIST_SERVER_KEYS, "inspect/list_server_keys.lua");
lua!(LIST_WORKERS, "inspect/list_workers.lua");
lua!(LIST_SCHEDULER_KEYS, "inspect/list_scheduler_keys.lua");

#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub queue: String,
    pub memory_usage: i64,
    pub latency: Duration,
    pub size: i64,
    pub groups: i64,
    pub pending: i64,
    pub active: i64,
    pub scheduled: i64,
    pub retry: i64,
    pub archived: i64,
    pub completed: i64,
    pub aggregating: i64,
    pub processed: i64,
    pub failed: i64,
    pub processed_total: i64,
    pub failed_total: i64,
    pub paused: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DailyStats {
    pub queue: String,
    pub processed: i64,
    pub failed: i64,
    pub date: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ServerDetail {
    pub info: pb::ServerInfo,
    pub workers: Vec<pb::WorkerInfo>,
}

fn parse<T: redis::FromRedisValue>(v: Value) -> Result<T> {
    redis::from_redis_value(v).map_err(|e| Error::Internal(e.to_string()))
}

fn opt_bytes(v: Value) -> Result<Option<Vec<u8>>> {
    parse(v)
}

/// Redis list/zset ranges for page `page` of `size` items.
fn range(page: usize, size: usize) -> (i64, i64) {
    let start = (page * size) as i64;
    (start, start + size as i64 - 1)
}

impl RedisRdb {
    pub async fn all_queues(&self) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let mut qs: Vec<String> = conn.smembers(base::ALL_QUEUES).await?;
        qs.sort();
        Ok(qs)
    }

    async fn check_queue(&self, qname: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let exists: bool = conn.sismember(base::ALL_QUEUES, qname).await?;
        if exists {
            Ok(())
        } else {
            Err(Error::QueueNotFound(qname.to_string()))
        }
    }

    pub async fn current_stats(&self, qname: &str) -> Result<QueueStats> {
        self.check_queue(qname).await?;
        let now = Utc::now();
        let date = utc_date(now);
        let keys = [
            base::pending_key(qname),
            base::active_key(qname),
            base::scheduled_key(qname),
            base::retry_key(qname),
            base::archived_key(qname),
            base::completed_key(qname),
            base::processed_key(qname, &date),
            base::failed_key(qname, &date),
            base::processed_total_key(qname),
            base::failed_total_key(qname),
            base::paused_key(qname),
            base::all_groups(qname),
        ];
        let mut inv = CURRENT_STATS.prepare_invoke();
        for k in &keys {
            inv.key(k);
        }
        inv.arg(base::task_key_prefix(qname))
            .arg(base::group_key_prefix(qname));
        let mut conn = self.conn.clone();
        let data: Vec<Value> = inv.invoke_async(&mut conn).await?;
        let mut s = QueueStats {
            queue: qname.to_string(),
            timestamp: now,
            ..Default::default()
        };
        let mut it = data.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            let key: String = parse(k)?;
            let val: i64 = parse::<Option<i64>>(v)?.unwrap_or(0);
            match key.as_str() {
                "oldest_pending_since" if val > 0 => {
                    let since = Utc.timestamp_nanos(val);
                    s.latency = (now - since).to_std().unwrap_or_default();
                }
                "group_size" => s.groups = val,
                "aggregating_count" => s.aggregating = val,
                k if k == keys[0] => s.pending = val,
                k if k == keys[1] => s.active = val,
                k if k == keys[2] => s.scheduled = val,
                k if k == keys[3] => s.retry = val,
                k if k == keys[4] => s.archived = val,
                k if k == keys[5] => s.completed = val,
                k if k == keys[6] => s.processed = val,
                k if k == keys[7] => s.failed = val,
                k if k == keys[8] => s.processed_total = val,
                k if k == keys[9] => s.failed_total = val,
                k if k == keys[10] => s.paused = val != 0,
                _ => {}
            }
        }
        s.size =
            s.pending + s.active + s.scheduled + s.retry + s.archived + s.completed + s.aggregating;
        s.memory_usage = self.memory_usage(qname).await?;
        Ok(s)
    }

    /// Sampled estimate, like asynq (20 tasks per state, 5 groups).
    async fn memory_usage(&self, qname: &str) -> Result<i64> {
        let mut conn = self.conn.clone();
        Ok(MEMORY_USAGE
            .key(base::active_key(qname))
            .key(base::pending_key(qname))
            .key(base::scheduled_key(qname))
            .key(base::retry_key(qname))
            .key(base::archived_key(qname))
            .key(base::completed_key(qname))
            .key(base::all_groups(qname))
            .arg(base::task_key_prefix(qname))
            .arg(20)
            .arg(5)
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?)
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
        let mut inv = HISTORICAL_STATS.prepare_invoke();
        for d in &dates {
            let day = utc_date(*d);
            inv.key(base::processed_key(qname, &day))
                .key(base::failed_key(qname, &day));
        }
        let mut conn = self.conn.clone();
        let data: Vec<i64> = inv.invoke_async(&mut conn).await?;
        Ok(data
            .chunks(2)
            .zip(dates)
            .map(|(pair, date)| DailyStats {
                queue: qname.to_string(),
                processed: pair[0],
                failed: pair[1],
                date,
            })
            .collect())
    }

    pub async fn get_task_info(&self, qname: &str, id: &str) -> Result<TaskInfo> {
        self.check_queue(qname).await?;
        let mut conn = self.conn.clone();
        let res: std::result::Result<Vec<Value>, redis::RedisError> = GET_TASK_INFO
            .key(base::task_key(qname, id))
            .arg(id)
            .arg(Utc::now().timestamp())
            .arg(base::queue_key_prefix(qname))
            .invoke_async(&mut conn)
            .await;
        let vals = match res {
            Ok(v) => v,
            Err(e) if e.to_string().contains("NOT FOUND") => return Err(Error::NotFound),
            Err(e) => return Err(e.into()),
        };
        let [msg, state, process_at, result]: [Value; 4] = vals
            .try_into()
            .map_err(|_| Error::Internal("unexpected reply from get_task_info".into()))?;
        let msg = decode(&parse::<Vec<u8>>(msg)?)?;
        let state: String = parse(state)?;
        let state = TaskState::parse(&state)
            .ok_or_else(|| Error::Internal(format!("unknown state {state}")))?;
        let process_at: i64 = parse(process_at)?;
        Ok(TaskInfo {
            msg,
            state,
            next_process_at: (process_at != 0).then(|| Utc.timestamp_opt(process_at, 0).unwrap()),
            result: opt_bytes(result)?,
        })
    }

    pub async fn group_stats(&self, qname: &str) -> Result<Vec<(String, i64)>> {
        let mut conn = self.conn.clone();
        let data: Vec<Value> = GROUP_STATS
            .key(base::all_groups(qname))
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        let mut out = Vec::new();
        let mut it = data.into_iter();
        while let (Some(g), Some(n)) = (it.next(), it.next()) {
            out.push((parse(g)?, parse(n)?));
        }
        out.sort();
        Ok(out)
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
        match state {
            TaskState::Pending | TaskState::Active => {
                let key = if state == TaskState::Pending {
                    base::pending_key(qname)
                } else {
                    base::active_key(qname)
                };
                // Lists are pushed on the left, so the oldest are at the end.
                let (start, stop) = range(page, size);
                let mut conn = self.conn.clone();
                let data: Vec<Value> = LIST_MESSAGES
                    .key(key)
                    .arg(-stop - 1)
                    .arg(-start - 1)
                    .arg(base::task_key_prefix(qname))
                    .invoke_async(&mut conn)
                    .await?;
                let mut out = Vec::new();
                let mut it = data.into_iter();
                while let (Some(m), Some(r)) = (it.next(), it.next()) {
                    let Some(raw) = parse::<Option<Vec<u8>>>(m)? else {
                        continue;
                    };
                    let Ok(msg) = decode(&raw) else { continue };
                    out.push(TaskInfo {
                        msg,
                        state,
                        next_process_at: (state == TaskState::Pending).then(Utc::now),
                        result: opt_bytes(r)?,
                    });
                }
                out.reverse();
                Ok(out)
            }
            _ => {
                let key = match state {
                    TaskState::Scheduled => base::scheduled_key(qname),
                    TaskState::Retry => base::retry_key(qname),
                    TaskState::Archived => base::archived_key(qname),
                    TaskState::Completed => base::completed_key(qname),
                    TaskState::Aggregating => {
                        let g = group.ok_or_else(|| {
                            Error::FailedPrecondition(
                                "listing aggregating tasks needs a group".into(),
                            )
                        })?;
                        base::group_key(qname, g)
                    }
                    _ => unreachable!(),
                };
                let (start, stop) = range(page, size);
                let mut conn = self.conn.clone();
                let data: Vec<Value> = LIST_ZSET_ENTRIES
                    .key(key)
                    .arg(start)
                    .arg(stop)
                    .arg(base::task_key_prefix(qname))
                    .invoke_async(&mut conn)
                    .await?;
                let mut out = Vec::new();
                let mut it = data.into_iter();
                while let (Some(m), Some(score), Some(r)) = (it.next(), it.next(), it.next()) {
                    let Some(raw) = parse::<Option<Vec<u8>>>(m)? else {
                        continue;
                    };
                    let Ok(msg) = decode(&raw) else { continue };
                    let score: f64 = parse(score)?;
                    let next = matches!(state, TaskState::Scheduled | TaskState::Retry)
                        .then(|| Utc.timestamp_opt(score as i64, 0).unwrap());
                    out.push(TaskInfo {
                        msg,
                        state,
                        next_process_at: next,
                        result: opt_bytes(r)?,
                    });
                }
                Ok(out)
            }
        }
    }

    pub async fn run_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let mut conn = self.conn.clone();
        let n: i64 = RUN_TASK
            .key(base::task_key(qname, id))
            .key(base::pending_key(qname))
            .key(base::all_groups(qname))
            .arg(id)
            .arg(base::queue_key_prefix(qname))
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        match n {
            1 => Ok(()),
            0 => Err(Error::NotFound),
            -1 => Err(Error::FailedPrecondition("task is already running".into())),
            -2 => Err(Error::FailedPrecondition(
                "task is already in pending state".into(),
            )),
            n => Err(Error::Internal(format!("unexpected return value {n}"))),
        }
    }

    pub async fn archive_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let now = Utc::now();
        let mut conn = self.conn.clone();
        let n: i64 = ARCHIVE_TASK
            .key(base::task_key(qname, id))
            .key(base::archived_key(qname))
            .key(base::all_groups(qname))
            .arg(id)
            .arg(now.timestamp())
            .arg((now - ARCHIVED_EXPIRATION).timestamp())
            .arg(MAX_ARCHIVE_SIZE)
            .arg(base::queue_key_prefix(qname))
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        match n {
            1 => Ok(()),
            0 => Err(Error::NotFound),
            -1 => Err(Error::FailedPrecondition("task is already archived".into())),
            -2 => Err(Error::FailedPrecondition(
                "cannot archive task in active state. use cancel_processing instead.".into(),
            )),
            n => Err(Error::Internal(format!("unexpected return value {n}"))),
        }
    }

    pub async fn delete_task(&self, qname: &str, id: &str) -> Result<()> {
        self.check_queue(qname).await?;
        let mut conn = self.conn.clone();
        let n: i64 = DELETE_TASK
            .key(base::task_key(qname, id))
            .key(base::all_groups(qname))
            .arg(id)
            .arg(base::queue_key_prefix(qname))
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        match n {
            1 => Ok(()),
            0 => Err(Error::NotFound),
            -1 => Err(Error::FailedPrecondition(
                "cannot delete task in active state. use cancel_processing instead.".into(),
            )),
            n => Err(Error::Internal(format!("unexpected return value {n}"))),
        }
    }

    pub async fn update_task_payload(&self, qname: &str, id: &str, payload: &[u8]) -> Result<()> {
        let mut info = self.get_task_info(qname, id).await?;
        info.msg.payload = payload.to_vec();
        let mut conn = self.conn.clone();
        let n: i64 = UPDATE_TASK_PAYLOAD
            .key(base::task_key(qname, id))
            .arg(base::encode_message(&info.msg))
            .invoke_async(&mut conn)
            .await?;
        match n {
            1 => Ok(()),
            0 => Err(Error::NotFound),
            _ => Err(Error::FailedPrecondition(
                "cannot update task that is not in scheduled state.".into(),
            )),
        }
    }

    fn state_zset(qname: &str, state: TaskState) -> Result<String> {
        Ok(match state {
            TaskState::Scheduled => base::scheduled_key(qname),
            TaskState::Retry => base::retry_key(qname),
            TaskState::Archived => base::archived_key(qname),
            TaskState::Completed => base::completed_key(qname),
            s => {
                return Err(Error::FailedPrecondition(format!(
                    "unsupported state {}",
                    s.as_str()
                )))
            }
        })
    }

    /// Moves every task in `state` (or in `group`) to pending.
    pub async fn run_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64> {
        self.check_queue(qname).await?;
        let mut conn = self.conn.clone();
        if state == TaskState::Aggregating {
            let g = group.ok_or_else(|| Error::FailedPrecondition("group required".into()))?;
            return Ok(RUN_ALL_AGGREGATING
                .key(base::group_key(qname, g))
                .key(base::pending_key(qname))
                .key(base::all_groups(qname))
                .arg(base::task_key_prefix(qname))
                .arg(g)
                .invoke_async(&mut conn)
                .await?);
        }
        Ok(RUN_ALL
            .key(Self::state_zset(qname, state)?)
            .key(base::pending_key(qname))
            .arg(base::task_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?)
    }

    pub async fn archive_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
    ) -> Result<i64> {
        self.check_queue(qname).await?;
        let now = Utc::now();
        let cutoff = (now - ARCHIVED_EXPIRATION).timestamp();
        let mut conn = self.conn.clone();
        let (mut inv, extra) = match state {
            TaskState::Pending => {
                let mut inv = ARCHIVE_ALL_PENDING.prepare_invoke();
                inv.key(base::pending_key(qname))
                    .key(base::archived_key(qname));
                (inv, None)
            }
            TaskState::Aggregating => {
                let g = group.ok_or_else(|| Error::FailedPrecondition("group required".into()))?;
                let mut inv = ARCHIVE_ALL_AGGREGATING.prepare_invoke();
                inv.key(base::group_key(qname, g))
                    .key(base::archived_key(qname))
                    .key(base::all_groups(qname));
                (inv, Some(g.to_string()))
            }
            TaskState::Scheduled | TaskState::Retry => {
                let mut inv = ARCHIVE_ALL.prepare_invoke();
                inv.key(Self::state_zset(qname, state)?)
                    .key(base::archived_key(qname));
                (inv, Some(qname.to_string()))
            }
            s => {
                return Err(Error::FailedPrecondition(format!(
                    "cannot archive {} tasks",
                    s.as_str()
                )))
            }
        };
        inv.arg(now.timestamp())
            .arg(cutoff)
            .arg(MAX_ARCHIVE_SIZE)
            .arg(base::task_key_prefix(qname));
        if let Some(e) = extra {
            inv.arg(e);
        }
        Ok(inv.invoke_async(&mut conn).await?)
    }

    pub async fn delete_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
    ) -> Result<i64> {
        self.check_queue(qname).await?;
        let mut conn = self.conn.clone();
        match state {
            TaskState::Pending => Ok(DELETE_ALL_PENDING
                .key(base::pending_key(qname))
                .arg(base::task_key_prefix(qname))
                .invoke_async(&mut conn)
                .await?),
            TaskState::Aggregating => {
                let g = group.ok_or_else(|| Error::FailedPrecondition("group required".into()))?;
                Ok(DELETE_ALL_AGGREGATING
                    .key(base::group_key(qname, g))
                    .key(base::all_groups(qname))
                    .arg(base::task_key_prefix(qname))
                    .arg(g)
                    .invoke_async(&mut conn)
                    .await?)
            }
            s => Ok(DELETE_ALL
                .key(Self::state_zset(qname, s)?)
                .arg(base::task_key_prefix(qname))
                .arg(qname)
                .invoke_async(&mut conn)
                .await?),
        }
    }

    /// Removes a queue. Without `force` the queue must be empty; active
    /// tasks always block removal.
    pub async fn remove_queue(&self, qname: &str, force: bool) -> Result<()> {
        self.check_queue(qname).await?;
        let script = if force {
            &*REMOVE_QUEUE_FORCE
        } else {
            &*REMOVE_QUEUE
        };
        let mut conn = self.conn.clone();
        let n: i64 = script
            .key(base::pending_key(qname))
            .key(base::active_key(qname))
            .key(base::scheduled_key(qname))
            .key(base::retry_key(qname))
            .key(base::archived_key(qname))
            .key(base::lease_key(qname))
            .key(base::completed_key(qname))
            .key(base::all_groups(qname))
            .key(base::all_aggregation_sets(qname))
            .arg(base::task_key_prefix(qname))
            .arg(base::group_key_prefix(qname))
            .invoke_async(&mut conn)
            .await?;
        match n {
            1 => {
                let _: i64 = conn.srem(base::ALL_QUEUES, qname).await?;
                Ok(())
            }
            -1 => Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not empty"
            ))),
            -2 => Err(Error::FailedPrecondition(
                "cannot remove queue with active tasks".into(),
            )),
            n => Err(Error::Internal(format!("unexpected return value {n}"))),
        }
    }

    pub async fn pause(&self, qname: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let ok: bool = conn
            .set_nx(base::paused_key(qname), Utc::now().timestamp())
            .await?;
        if ok {
            Ok(())
        } else {
            Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is already paused"
            )))
        }
    }

    pub async fn unpause(&self, qname: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let n: i64 = conn.del(base::paused_key(qname)).await?;
        if n == 1 {
            Ok(())
        } else {
            Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not paused"
            )))
        }
    }

    /// Live servers with their active workers.
    pub async fn list_servers(&self) -> Result<Vec<ServerDetail>> {
        let now = Utc::now().timestamp();
        let mut conn = self.conn.clone();
        let keys: Vec<String> = LIST_SERVER_KEYS
            .key(base::ALL_SERVERS)
            .arg(now)
            .invoke_async(&mut conn)
            .await?;
        let mut servers = Vec::new();
        for key in keys {
            let data: Option<Vec<u8>> = conn.get(&key).await?;
            let Some(Ok(info)) = data.map(|d| pb::ServerInfo::decode(d.as_slice())) else {
                continue;
            };
            let wkey = base::workers_key(&info.host, info.pid as u32, &info.server_id);
            let raw: Vec<Vec<u8>> = conn.hvals(&wkey).await?;
            let workers = raw
                .iter()
                .filter_map(|w| pb::WorkerInfo::decode(w.as_slice()).ok())
                .collect();
            servers.push(ServerDetail { info, workers });
        }
        // Prune dead worker keys the same way asynq does.
        let _: Vec<String> = LIST_WORKERS
            .key(base::ALL_WORKERS)
            .arg(now)
            .invoke_async(&mut conn)
            .await?;
        Ok(servers)
    }

    pub async fn list_scheduler_entries(&self) -> Result<Vec<pb::SchedulerEntry>> {
        let mut conn = self.conn.clone();
        let keys: Vec<String> = LIST_SCHEDULER_KEYS
            .key(base::ALL_SCHEDULERS)
            .arg(Utc::now().timestamp())
            .invoke_async(&mut conn)
            .await?;
        let mut entries = Vec::new();
        for key in keys {
            let raw: Vec<Vec<u8>> = conn.lrange(&key, 0, -1).await?;
            entries.extend(
                raw.iter()
                    .filter_map(|e| pb::SchedulerEntry::decode(e.as_slice()).ok()),
            );
        }
        Ok(entries)
    }

    pub async fn list_scheduler_enqueue_events(
        &self,
        entry_id: &str,
        page: usize,
        size: usize,
    ) -> Result<Vec<pb::SchedulerEnqueueEvent>> {
        let (start, stop) = range(page, size);
        let mut conn = self.conn.clone();
        let raw: Vec<Vec<u8>> = conn
            .zrevrange(
                base::scheduler_history_key(entry_id),
                start as isize,
                stop as isize,
            )
            .await?;
        raw.iter()
            .map(|e| pb::SchedulerEnqueueEvent::decode(e.as_slice()).map_err(Error::from))
            .collect()
    }
}
