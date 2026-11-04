//! In-process backend (`memory://<name>`) for tests: the Postgres backend's
//! data model held in a mutex. Every client, worker and inspector in the
//! process that opens the same URL shares one store; nothing is persisted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use prost::Message;
use tokio::sync::{broadcast, Notify};

use super::inspect::{DailyStats, QueueStats, ServerDetail};
use super::scheduler::MAX_EVENTS;
use super::worker::{ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE};
use super::{Cancellations, Error, Result, AGGREGATION_TIMEOUT};
use crate::base::{pb, TaskMessage, TaskState};
use crate::client::TaskInfo;

pub fn is_memory_url(url: &str) -> bool {
    url.starts_with("memory://")
}

static STORES: LazyLock<Mutex<HashMap<String, Arc<Store>>>> = LazyLock::new(Default::default);

/// Drops the store behind `url`, or every store; open handles keep theirs.
pub fn reset(url: Option<&str>) {
    let mut stores = STORES.lock().unwrap();
    match url {
        Some(url) => {
            stores.remove(url);
        }
        None => stores.clear(),
    }
}

struct Store {
    data: Mutex<Data>,
    pending: Arc<Notify>,
    cancels: broadcast::Sender<String>,
}

#[derive(Default)]
struct Data {
    queues: BTreeMap<String, Queue>,
    unique_locks: HashMap<String, UniqueLock>,
    servers: BTreeMap<String, Server>,
    scheduler_entries: BTreeMap<String, Expiring<Vec<pb::SchedulerEntry>>>,
    scheduler_events: HashMap<String, Vec<(DateTime<Utc>, pb::SchedulerEnqueueEvent)>>,
    semaphores: HashMap<String, HashMap<String, DateTime<Utc>>>,
}

#[derive(Default)]
struct Queue {
    paused: bool,
    tasks: HashMap<String, Task>,
    /// (group, set ID) -> deadline.
    aggregation_sets: HashMap<(String, String), DateTime<Utc>>,
    /// Day -> (processed, failed).
    stats: BTreeMap<NaiveDate, (i64, i64)>,
}

struct Task {
    msg: TaskMessage,
    state: TaskState,
    pending_since: Option<DateTime<Utc>>,
    process_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    group_key: Option<String>,
    grouped_at: Option<DateTime<Utc>>,
    aggregation_set: Option<String>,
    unique_key: Option<String>,
    result: Option<Vec<u8>>,
    archived_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
}

impl Task {
    fn new(msg: &TaskMessage, state: TaskState) -> Self {
        Self {
            msg: msg.clone(),
            state,
            pending_since: None,
            process_at: None,
            lease_expires_at: None,
            group_key: None,
            grouped_at: None,
            aggregation_set: None,
            unique_key: None,
            result: None,
            archived_at: None,
            expires_at: None,
        }
    }

    fn info(&self) -> TaskInfo {
        TaskInfo {
            msg: self.msg.clone(),
            state: self.state,
            next_process_at: match self.state {
                TaskState::Pending => Some(Utc::now()),
                TaskState::Scheduled | TaskState::Retry => self.process_at,
                _ => None,
            },
            result: self.result.clone(),
        }
    }

    fn make_pending(&mut self, now: DateTime<Utc>) {
        self.state = TaskState::Pending;
        self.pending_since = Some(now);
        self.process_at = None;
        self.archived_at = None;
        self.expires_at = None;
        self.aggregation_set = None;
    }

    fn make_archived(&mut self, now: DateTime<Utc>) {
        self.state = TaskState::Archived;
        self.archived_at = Some(now);
        self.pending_since = None;
        self.process_at = None;
        self.aggregation_set = None;
    }

    /// Aggregating and not yet claimed by an aggregation set.
    fn in_group(&self, group: &str) -> bool {
        self.state == TaskState::Aggregating
            && self.aggregation_set.is_none()
            && self.group_key.as_deref() == Some(group)
    }

    fn sort_key(&self) -> Option<DateTime<Utc>> {
        match self.state {
            TaskState::Pending => self.pending_since,
            TaskState::Active => self.lease_expires_at,
            TaskState::Scheduled | TaskState::Retry => self.process_at,
            TaskState::Archived => self.archived_at,
            TaskState::Completed => self.expires_at,
            TaskState::Aggregating => self.grouped_at,
        }
    }
}

struct UniqueLock {
    task_id: String,
    expires_at: DateTime<Utc>,
}

struct Server {
    info: pb::ServerInfo,
    workers: Vec<pb::WorkerInfo>,
    expires_at: DateTime<Utc>,
}

struct Expiring<T> {
    value: T,
    expires_at: DateTime<Utc>,
}

fn ts(unix: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(unix, 0).single().unwrap_or_default()
}

fn nonempty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

fn group_required(group: Option<&str>) -> Result<&str> {
    group.ok_or_else(|| Error::FailedPrecondition("group required".into()))
}

fn unsupported(state: TaskState) -> Error {
    Error::FailedPrecondition(format!("unsupported state {}", state.as_str()))
}

impl Data {
    fn queue(&self, qname: &str) -> Result<&Queue> {
        self.queues
            .get(qname)
            .ok_or_else(|| Error::QueueNotFound(qname.to_string()))
    }

    fn queue_mut(&mut self, qname: &str) -> Result<&mut Queue> {
        self.queues
            .get_mut(qname)
            .ok_or_else(|| Error::QueueNotFound(qname.to_string()))
    }

    fn task_mut(&mut self, qname: &str, id: &str) -> Result<&mut Task> {
        self.queues
            .get_mut(qname)
            .and_then(|q| q.tasks.get_mut(id))
            .ok_or(Error::NotFound)
    }

    /// The task if it is active (and, with `lease_cutoff`, its lease
    /// expired by then).
    fn active_mut(
        &mut self,
        msg: &TaskMessage,
        lease_cutoff: Option<DateTime<Utc>>,
    ) -> Result<&mut Task> {
        let t = self.task_mut(&msg.queue, &msg.id)?;
        let expired = match (lease_cutoff, t.lease_expires_at) {
            (None, _) => true,
            (Some(cutoff), Some(lease)) => lease <= cutoff,
            (Some(_), None) => false,
        };
        if t.state != TaskState::Active || !expired {
            return Err(Error::NotFound);
        }
        Ok(t)
    }

    fn count(&mut self, qname: &str, now: DateTime<Utc>, processed: i64, failed: i64) {
        if let Some(q) = self.queues.get_mut(qname) {
            let day = q.stats.entry(now.date_naive()).or_default();
            day.0 += processed;
            day.1 += failed;
        }
    }

    fn release_unique_lock(&mut self, key: Option<&str>, id: &str) {
        if let Some(key) = key {
            if self.unique_locks.get(key).is_some_and(|l| l.task_id == id) {
                self.unique_locks.remove(key);
            }
        }
    }

    /// Removes tasks of `qname` matching `pred`, with their uniqueness locks.
    fn delete_where(&mut self, qname: &str, pred: impl Fn(&Task) -> bool) -> i64 {
        let Some(q) = self.queues.get_mut(qname) else {
            return 0;
        };
        let ids: Vec<String> = q
            .tasks
            .iter()
            .filter(|(_, t)| pred(t))
            .map(|(id, _)| id.clone())
            .collect();
        let removed: Vec<Task> = ids.iter().filter_map(|id| q.tasks.remove(id)).collect();
        for t in &removed {
            self.release_unique_lock(t.unique_key.as_deref(), &t.msg.id);
        }
        removed.len() as i64
    }
}

pub struct MemoryRdb {
    store: Arc<Store>,
}

impl MemoryRdb {
    pub async fn connect(url: &str) -> Result<Self> {
        let store = STORES
            .lock()
            .unwrap()
            .entry(url.to_string())
            .or_insert_with(|| {
                Arc::new(Store {
                    data: Mutex::default(),
                    pending: Arc::new(Notify::new()),
                    cancels: broadcast::channel(256).0,
                })
            })
            .clone();
        Ok(Self { store })
    }

    fn data(&self) -> MutexGuard<'_, Data> {
        self.store.data.lock().unwrap()
    }

    fn wake(&self) {
        self.store.pending.notify_waiters();
        self.store.pending.notify_one();
    }

    pub async fn ping(&self) -> Result<()> {
        Ok(())
    }

    pub fn pending_notifier(&self) -> Arc<Notify> {
        self.store.pending.clone()
    }

    fn insert(&self, mut task: Task, unique_ttl_secs: Option<i64>) -> Result<()> {
        let now = Utc::now();
        let mut d = self.data();
        let (qname, id) = (task.msg.queue.clone(), task.msg.id.clone());
        if d.queues
            .get(&qname)
            .is_some_and(|q| q.tasks.contains_key(&id))
        {
            return Err(Error::TaskIdConflict);
        }
        let unique = unique_ttl_secs.and(nonempty(&task.msg.unique_key));
        if let (Some(key), Some(ttl)) = (unique, unique_ttl_secs) {
            if d.unique_locks.get(key).is_some_and(|l| l.expires_at > now) {
                return Err(Error::DuplicateTask);
            }
            d.unique_locks.insert(
                key.to_string(),
                UniqueLock {
                    task_id: id.clone(),
                    expires_at: now + chrono::Duration::seconds(ttl),
                },
            );
            task.unique_key = Some(key.to_string());
        }
        let pending = task.state == TaskState::Pending;
        d.queues.entry(qname).or_default().tasks.insert(id, task);
        drop(d);
        if pending {
            self.wake();
        }
        Ok(())
    }

    pub async fn enqueue(&self, msg: &TaskMessage, now_nanos: i64) -> Result<()> {
        self.enqueue_with(msg, now_nanos, None)
    }

    pub async fn enqueue_unique(
        &self,
        msg: &TaskMessage,
        ttl_secs: i64,
        now_nanos: i64,
    ) -> Result<()> {
        self.enqueue_with(msg, now_nanos, Some(ttl_secs))
    }

    fn enqueue_with(&self, msg: &TaskMessage, now_nanos: i64, ttl: Option<i64>) -> Result<()> {
        let mut task = Task::new(msg, TaskState::Pending);
        task.pending_since = Some(Utc.timestamp_nanos(now_nanos));
        self.insert(task, ttl)
    }

    pub async fn schedule(&self, msg: &TaskMessage, process_at_unix: i64) -> Result<()> {
        self.schedule_with(msg, process_at_unix, None)
    }

    pub async fn schedule_unique(
        &self,
        msg: &TaskMessage,
        process_at_unix: i64,
        ttl_secs: i64,
    ) -> Result<()> {
        self.schedule_with(msg, process_at_unix, Some(ttl_secs))
    }

    fn schedule_with(&self, msg: &TaskMessage, at: i64, ttl: Option<i64>) -> Result<()> {
        let mut task = Task::new(msg, TaskState::Scheduled);
        task.process_at = Some(ts(at));
        task.group_key = nonempty(&msg.group_key).map(str::to_string);
        self.insert(task, ttl)
    }

    pub async fn add_to_group(
        &self,
        msg: &TaskMessage,
        group: &str,
        unique_ttl_secs: Option<i64>,
    ) -> Result<()> {
        let mut task = Task::new(msg, TaskState::Aggregating);
        task.group_key = Some(group.to_string());
        task.grouped_at = Some(Utc::now());
        self.insert(task, unique_ttl_secs)
    }

    pub async fn dequeue_with_lease(
        &self,
        qnames: &[String],
        lease: Duration,
    ) -> Result<Option<(TaskMessage, DateTime<Utc>)>> {
        Ok(self.dequeue_batch(qnames, lease, 1).await?.pop())
    }

    /// Claims up to `max` of the oldest pending tasks, taking from the
    /// queues in order.
    pub async fn dequeue_batch(
        &self,
        qnames: &[String],
        lease: Duration,
        max: usize,
    ) -> Result<Vec<(TaskMessage, DateTime<Utc>)>> {
        let lease_expires_at = Utc::now() + lease;
        let mut d = self.data();
        let mut out = Vec::new();
        for qname in qnames {
            let Some(q) = d.queues.get_mut(qname).filter(|q| !q.paused) else {
                continue;
            };
            let mut pending: Vec<(DateTime<Utc>, &String)> = q
                .tasks
                .iter()
                .filter(|(_, t)| t.state == TaskState::Pending)
                .map(|(id, t)| (t.pending_since.unwrap_or_default(), id))
                .collect();
            pending.sort();
            let ids: Vec<String> = pending
                .into_iter()
                .take(max - out.len())
                .map(|(_, id)| id.clone())
                .collect();
            for id in ids {
                let t = q.tasks.get_mut(&id).expect("listed above");
                t.state = TaskState::Active;
                t.lease_expires_at = Some(lease_expires_at);
                t.pending_since = None;
                out.push((t.msg.clone(), lease_expires_at));
            }
            if out.len() == max {
                break;
            }
        }
        Ok(out)
    }

    pub async fn done(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let mut d = self.data();
        d.active_mut(msg, None)?;
        let t = d
            .queue_mut(&msg.queue)?
            .tasks
            .remove(&msg.id)
            .expect("checked above");
        d.release_unique_lock(t.unique_key.as_deref(), &msg.id);
        d.count(&msg.queue, now, 1, 0);
        Ok(())
    }

    pub async fn mark_as_complete(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let mut msg = msg.clone();
        msg.completed_at = now.timestamp();
        let mut d = self.data();
        let t = d.active_mut(&msg, None)?;
        t.state = TaskState::Completed;
        t.lease_expires_at = None;
        t.expires_at = Some(ts(now.timestamp() + msg.retention));
        let unique_key = t.unique_key.clone();
        t.msg = msg.clone();
        d.release_unique_lock(unique_key.as_deref(), &msg.id);
        d.count(&msg.queue, now, 1, 0);
        Ok(())
    }

    /// Puts an active task back at the head of its queue.
    pub async fn requeue(&self, msg: &TaskMessage) -> Result<()> {
        let now = Utc::now();
        let mut d = self.data();
        let head = d
            .queue(&msg.queue)?
            .tasks
            .values()
            .filter(|t| t.state == TaskState::Pending)
            .filter_map(|t| t.pending_since)
            .min()
            .unwrap_or(now);
        let t = d.active_mut(msg, None)?;
        t.state = TaskState::Pending;
        t.lease_expires_at = None;
        t.pending_since = Some(head - chrono::Duration::microseconds(1));
        drop(d);
        self.wake();
        Ok(())
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
        let mut d = self.data();
        let t = d.active_mut(msg, lease_cutoff)?;
        t.state = TaskState::Retry;
        t.msg = modified;
        t.process_at = Some(ts(retry_at.timestamp()));
        t.lease_expires_at = None;
        if is_failure {
            d.count(&msg.queue, now, 1, 1);
        }
        Ok(())
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
        let mut d = self.data();
        let t = d.active_mut(msg, lease_cutoff)?;
        t.state = TaskState::Archived;
        t.msg = modified;
        t.archived_at = Some(now);
        t.lease_expires_at = None;
        d.count(&msg.queue, now, 1, 1);
        Ok(())
    }

    pub async fn forward_if_ready(&self, qname: &str) -> Result<u64> {
        let now = Utc::now();
        let mut d = self.data();
        let Some(q) = d.queues.get_mut(qname) else {
            return Ok(0);
        };
        let mut moved = 0;
        let mut pending = false;
        for t in q.tasks.values_mut() {
            let due = matches!(t.state, TaskState::Scheduled | TaskState::Retry)
                && t.process_at.is_some_and(|at| at <= now);
            if !due {
                continue;
            }
            if t.group_key.is_none() {
                t.state = TaskState::Pending;
                t.pending_since = Some(now);
                pending = true;
            } else {
                t.state = TaskState::Aggregating;
                t.grouped_at = Some(now);
            }
            t.process_at = None;
            moved += 1;
        }
        drop(d);
        if pending {
            self.wake();
        }
        Ok(moved)
    }

    pub async fn extend_lease(
        &self,
        qname: &str,
        ids: &[String],
        lease: Duration,
    ) -> Result<DateTime<Utc>> {
        let expires_at = Utc::now() + lease;
        let mut d = self.data();
        if let Some(q) = d.queues.get_mut(qname) {
            for id in ids {
                if let Some(t) = q.tasks.get_mut(id).filter(|t| t.state == TaskState::Active) {
                    t.lease_expires_at = Some(expires_at);
                }
            }
        }
        Ok(expires_at)
    }

    pub async fn list_lease_expired(
        &self,
        cutoff: DateTime<Utc>,
        qnames: &[String],
    ) -> Result<Vec<TaskMessage>> {
        let d = self.data();
        Ok(qnames
            .iter()
            .filter_map(|q| d.queues.get(q))
            .flat_map(|q| q.tasks.values())
            .filter(|t| {
                t.state == TaskState::Active && t.lease_expires_at.is_some_and(|l| l <= cutoff)
            })
            .map(|t| t.msg.clone())
            .collect())
    }

    pub async fn write_server_state(
        &self,
        info: &pb::ServerInfo,
        workers: &[pb::WorkerInfo],
        ttl: Duration,
    ) -> Result<()> {
        let key = server_key(&info.host, info.pid as u32, &info.server_id);
        let now = Utc::now();
        let mut d = self.data();
        d.servers.retain(|k, s| s.expires_at >= now || *k == key);
        d.servers.insert(
            key,
            Server {
                info: info.clone(),
                workers: workers.to_vec(),
                expires_at: now + ttl,
            },
        );
        Ok(())
    }

    pub async fn clear_server_state(&self, host: &str, pid: u32, server_id: &str) -> Result<()> {
        self.data()
            .servers
            .remove(&server_key(host, pid, server_id));
        Ok(())
    }

    /// The janitor: deletes completed tasks past their retention, trims the
    /// archive by age and size, and drops expired uniqueness locks.
    pub async fn delete_expired_completed_tasks(
        &self,
        qname: &str,
        _batch_size: usize,
    ) -> Result<u64> {
        let now = Utc::now();
        let mut d = self.data();
        if let Some(q) = d.queues.get(qname) {
            let mut archived: Vec<(DateTime<Utc>, &String)> = q
                .tasks
                .iter()
                .filter(|(_, t)| t.state == TaskState::Archived)
                .map(|(id, t)| (t.archived_at.unwrap_or_default(), id))
                .collect();
            archived.sort_by(|a, b| b.cmp(a));
            let too_old = now - ARCHIVED_EXPIRATION;
            let trim: HashSet<String> = archived
                .iter()
                .enumerate()
                .filter(|(i, (at, _))| *i as i64 >= MAX_ARCHIVE_SIZE || *at < too_old)
                .map(|(_, (_, id))| (*id).clone())
                .collect();
            d.queue_mut(qname)?.tasks.retain(|id, _| !trim.contains(id));
        }
        d.unique_locks.retain(|_, l| l.expires_at > now);
        let n = d.delete_where(qname, |t| {
            t.state == TaskState::Completed && t.expires_at.is_some_and(|e| e <= now)
        });
        Ok(n as u64)
    }

    pub async fn write_result(&self, qname: &str, id: &str, data: &[u8]) -> Result<()> {
        if let Ok(t) = self.data().task_mut(qname, id) {
            t.result = Some(data.to_vec());
        }
        Ok(())
    }

    pub async fn publish_cancelation(&self, id: &str) -> Result<()> {
        let _ = self.store.cancels.send(id.to_string());
        Ok(())
    }

    pub async fn subscribe_cancellations(&self) -> Result<Cancellations> {
        Ok(Cancellations::Channel(self.store.cancels.subscribe()))
    }

    pub async fn list_groups(&self, qname: &str) -> Result<Vec<String>> {
        let d = self.data();
        let mut groups: Vec<String> = d
            .queues
            .get(qname)
            .into_iter()
            .flat_map(|q| q.tasks.values())
            .filter(|t| t.state == TaskState::Aggregating && t.aggregation_set.is_none())
            .filter_map(|t| t.group_key.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        groups.sort();
        Ok(groups)
    }

    /// A set is created when the group is full, its oldest task waited
    /// `max_delay`, or nothing joined it for `grace_period`.
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
        let mut d = self.data();
        let Some(q) = d.queues.get_mut(qname) else {
            return Ok(false);
        };
        let mut members: Vec<(DateTime<Utc>, String)> = q
            .tasks
            .iter()
            .filter(|(_, t)| t.in_group(group))
            .map(|(id, t)| (t.grouped_at.unwrap_or_default(), id.clone()))
            .collect();
        members.sort();
        let (Some(oldest), Some(latest)) = (members.first(), members.iter().map(|m| m.0).max())
        else {
            return Ok(false);
        };
        let ready = (max_size != 0 && members.len() >= max_size)
            || (!max_delay.is_zero() && oldest.0 <= now - max_delay)
            || latest <= now - grace_period;
        if !ready {
            return Ok(false);
        }
        let take = if max_size == 0 {
            members.len()
        } else {
            max_size
        };
        for (_, id) in members.into_iter().take(take) {
            q.tasks.get_mut(&id).expect("listed above").aggregation_set = Some(set_id.to_string());
        }
        q.aggregation_sets.insert(
            (group.to_string(), set_id.to_string()),
            Utc::now() + AGGREGATION_TIMEOUT,
        );
        Ok(true)
    }

    pub async fn read_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<Vec<TaskMessage>> {
        let d = self.data();
        let mut members: Vec<&Task> = d
            .queues
            .get(qname)
            .into_iter()
            .flat_map(|q| q.tasks.values())
            .filter(|t| {
                t.group_key.as_deref() == Some(group)
                    && t.aggregation_set.as_deref() == Some(set_id)
            })
            .collect();
        members.sort_by(|a, b| (a.grouped_at, &a.msg.id).cmp(&(b.grouped_at, &b.msg.id)));
        Ok(members.into_iter().map(|t| t.msg.clone()).collect())
    }

    pub async fn delete_aggregation_set(
        &self,
        qname: &str,
        group: &str,
        set_id: &str,
    ) -> Result<()> {
        let mut d = self.data();
        if let Some(q) = d.queues.get_mut(qname) {
            q.tasks.retain(|_, t| {
                !(t.group_key.as_deref() == Some(group)
                    && t.aggregation_set.as_deref() == Some(set_id))
            });
            q.aggregation_sets
                .remove(&(group.to_string(), set_id.to_string()));
        }
        Ok(())
    }

    pub async fn reclaim_stale_aggregation_sets(&self, qname: &str) -> Result<()> {
        let now = Utc::now();
        let mut d = self.data();
        if let Some(q) = d.queues.get_mut(qname) {
            let stale: Vec<(String, String)> = q
                .aggregation_sets
                .iter()
                .filter(|(_, deadline)| **deadline <= now)
                .map(|(k, _)| k.clone())
                .collect();
            for (group, set_id) in stale {
                q.aggregation_sets.remove(&(group.clone(), set_id.clone()));
                for t in q.tasks.values_mut() {
                    if t.group_key.as_deref() == Some(&group)
                        && t.aggregation_set.as_deref() == Some(&set_id)
                    {
                        t.aggregation_set = None;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn semaphore_acquire(
        &self,
        scope: &str,
        max_tokens: u32,
        deadline: DateTime<Utc>,
        task_id: &str,
    ) -> Result<bool> {
        let now = ts(Utc::now().timestamp());
        let mut d = self.data();
        let holders = d.semaphores.entry(scope.to_string()).or_default();
        holders.retain(|_, expires_at| *expires_at >= now);
        let acquired = holders.len() < max_tokens as usize;
        if acquired {
            holders.insert(task_id.to_string(), ts(deadline.timestamp()));
        }
        Ok(acquired)
    }

    pub async fn semaphore_release(&self, scope: &str, task_id: &str) -> Result<bool> {
        let mut d = self.data();
        Ok(d.semaphores
            .get_mut(scope)
            .is_some_and(|h| h.remove(task_id).is_some()))
    }

    pub async fn write_scheduler_entries(
        &self,
        scheduler_id: &str,
        entries: &[pb::SchedulerEntry],
        ttl: Duration,
    ) -> Result<()> {
        let now = Utc::now();
        let mut d = self.data();
        d.scheduler_entries.retain(|_, e| e.expires_at >= now);
        d.scheduler_entries.insert(
            scheduler_id.to_string(),
            Expiring {
                value: entries.to_vec(),
                expires_at: now + ttl,
            },
        );
        Ok(())
    }

    pub async fn clear_scheduler_entries(&self, scheduler_id: &str) -> Result<()> {
        self.data().scheduler_entries.remove(scheduler_id);
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
        let mut d = self.data();
        let events = d.scheduler_events.entry(entry_id.to_string()).or_default();
        events.push((enqueued_at, event));
        events.sort_by(|a, b| b.0.cmp(&a.0));
        events.truncate(MAX_EVENTS as usize);
        Ok(())
    }

    pub async fn clear_scheduler_history(&self, entry_id: &str) -> Result<()> {
        self.data().scheduler_events.remove(entry_id);
        Ok(())
    }

    pub async fn all_queues(&self) -> Result<Vec<String>> {
        Ok(self.data().queues.keys().cloned().collect())
    }

    pub async fn current_stats(&self, qname: &str) -> Result<QueueStats> {
        let now = Utc::now();
        let d = self.data();
        let q = d.queue(qname)?;
        let mut s = QueueStats {
            queue: qname.to_string(),
            paused: q.paused,
            timestamp: now,
            ..Default::default()
        };
        let mut groups = HashSet::new();
        let mut oldest_pending: Option<DateTime<Utc>> = None;
        for t in q.tasks.values() {
            s.memory_usage += (t.msg.encoded_len() + t.result.as_ref().map_or(0, Vec::len)) as i64;
            match t.state {
                TaskState::Pending => {
                    s.pending += 1;
                    if let Some(since) = t.pending_since {
                        oldest_pending = Some(oldest_pending.map_or(since, |o| o.min(since)));
                    }
                }
                TaskState::Active => s.active += 1,
                TaskState::Scheduled => s.scheduled += 1,
                TaskState::Retry => s.retry += 1,
                TaskState::Archived => s.archived += 1,
                TaskState::Completed => s.completed += 1,
                TaskState::Aggregating if t.aggregation_set.is_none() => {
                    s.aggregating += 1;
                    groups.extend(t.group_key.as_deref());
                }
                TaskState::Aggregating => {}
            }
        }
        s.groups = groups.len() as i64;
        s.latency = oldest_pending
            .and_then(|since| (now - since).to_std().ok())
            .unwrap_or_default();
        let (processed, failed) = q.stats.get(&now.date_naive()).copied().unwrap_or_default();
        s.processed = processed;
        s.failed = failed;
        s.processed_total = q.stats.values().map(|d| d.0).sum();
        s.failed_total = q.stats.values().map(|d| d.1).sum();
        s.size =
            s.pending + s.active + s.scheduled + s.retry + s.archived + s.completed + s.aggregating;
        Ok(s)
    }

    pub async fn historical_stats(&self, qname: &str, days: usize) -> Result<Vec<DailyStats>> {
        if days < 1 {
            return Err(Error::FailedPrecondition(
                "the number of days must be positive".into(),
            ));
        }
        let now = Utc::now();
        let d = self.data();
        let q = d.queue(qname)?;
        Ok((0..days)
            .map(|i| {
                let date = now - chrono::Duration::days(i as i64);
                let (processed, failed) =
                    q.stats.get(&date.date_naive()).copied().unwrap_or_default();
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
        let d = self.data();
        d.queue(qname)?
            .tasks
            .get(id)
            .map(Task::info)
            .ok_or(Error::NotFound)
    }

    pub async fn group_stats(&self, qname: &str) -> Result<Vec<(String, i64)>> {
        let d = self.data();
        let mut counts: BTreeMap<String, i64> = BTreeMap::new();
        for t in d
            .queues
            .get(qname)
            .into_iter()
            .flat_map(|q| q.tasks.values())
        {
            if t.state == TaskState::Aggregating && t.aggregation_set.is_none() {
                if let Some(g) = &t.group_key {
                    *counts.entry(g.clone()).or_default() += 1;
                }
            }
        }
        Ok(counts.into_iter().collect())
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
        let d = self.data();
        let q = d.queue(qname)?;
        let group = match state {
            TaskState::Aggregating => Some(group.ok_or_else(|| {
                Error::FailedPrecondition("listing aggregating tasks needs a group".into())
            })?),
            _ => None,
        };
        let mut tasks: Vec<&Task> = q
            .tasks
            .values()
            .filter(|t| match group {
                Some(g) => t.in_group(g),
                None => t.state == state,
            })
            .collect();
        tasks.sort_by(|a, b| (a.sort_key(), &a.msg.id).cmp(&(b.sort_key(), &b.msg.id)));
        Ok(tasks
            .into_iter()
            .skip(page * size)
            .take(size)
            .map(Task::info)
            .collect())
    }

    pub async fn run_task(&self, qname: &str, id: &str) -> Result<()> {
        let mut d = self.data();
        d.queue(qname)?;
        let t = d.task_mut(qname, id)?;
        match t.state {
            TaskState::Active => {
                return Err(Error::FailedPrecondition("task is already running".into()))
            }
            TaskState::Pending => {
                return Err(Error::FailedPrecondition(
                    "task is already in pending state".into(),
                ))
            }
            _ => t.make_pending(Utc::now()),
        }
        drop(d);
        self.wake();
        Ok(())
    }

    pub async fn archive_task(&self, qname: &str, id: &str) -> Result<()> {
        let mut d = self.data();
        d.queue(qname)?;
        let t = d.task_mut(qname, id)?;
        match t.state {
            TaskState::Active => Err(Error::FailedPrecondition(
                "cannot archive task in active state. use cancel_processing instead.".into(),
            )),
            TaskState::Archived => {
                Err(Error::FailedPrecondition("task is already archived".into()))
            }
            _ => {
                t.make_archived(Utc::now());
                Ok(())
            }
        }
    }

    pub async fn delete_task(&self, qname: &str, id: &str) -> Result<()> {
        let mut d = self.data();
        d.queue(qname)?;
        if d.task_mut(qname, id)?.state == TaskState::Active {
            return Err(Error::FailedPrecondition(
                "cannot delete task in active state. use cancel_processing instead.".into(),
            ));
        }
        d.delete_where(qname, |t| t.msg.id == id);
        Ok(())
    }

    pub async fn update_task_payload(&self, qname: &str, id: &str, payload: &[u8]) -> Result<()> {
        let mut d = self.data();
        d.queue(qname)?;
        let t = d.task_mut(qname, id)?;
        if t.state != TaskState::Scheduled {
            return Err(Error::FailedPrecondition(
                "cannot update task that is not in scheduled state.".into(),
            ));
        }
        t.msg.payload = payload.to_vec();
        Ok(())
    }

    /// Applies `f` to every task in `state` (or in `group` for aggregating
    /// tasks), returning how many there were.
    fn update_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
        f: impl Fn(&mut Task),
    ) -> Result<i64> {
        let group = match state {
            TaskState::Aggregating => Some(group_required(group)?),
            _ => None,
        };
        let mut d = self.data();
        let mut n = 0;
        for t in d.queue_mut(qname)?.tasks.values_mut() {
            let matches = match group {
                Some(g) => t.in_group(g),
                None => t.state == state,
            };
            if matches {
                f(t);
                n += 1;
            }
        }
        Ok(n)
    }

    /// Moves every task in `state` (or in `group`) to pending.
    pub async fn run_all(&self, qname: &str, state: TaskState, group: Option<&str>) -> Result<i64> {
        self.data().queue(qname)?;
        match state {
            TaskState::Pending | TaskState::Active => Err(unsupported(state)),
            _ => {
                let now = Utc::now();
                let n = self.update_all(qname, state, group, |t| t.make_pending(now))?;
                self.wake();
                Ok(n)
            }
        }
    }

    pub async fn archive_all(
        &self,
        qname: &str,
        state: TaskState,
        group: Option<&str>,
    ) -> Result<i64> {
        self.data().queue(qname)?;
        match state {
            TaskState::Pending
            | TaskState::Aggregating
            | TaskState::Scheduled
            | TaskState::Retry => {
                let now = Utc::now();
                self.update_all(qname, state, group, |t| t.make_archived(now))
            }
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
        let mut d = self.data();
        d.queue(qname)?;
        match state {
            TaskState::Active => Err(unsupported(state)),
            TaskState::Aggregating => {
                let group = group_required(group)?;
                Ok(d.delete_where(qname, |t| t.in_group(group)))
            }
            _ => Ok(d.delete_where(qname, |t| t.state == state)),
        }
    }

    /// Removes a queue. Without `force` the queue must be empty; active
    /// tasks always block removal.
    pub async fn remove_queue(&self, qname: &str, force: bool) -> Result<()> {
        let mut d = self.data();
        let q = d.queue(qname)?;
        if !force && !q.tasks.is_empty() {
            return Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not empty"
            )));
        }
        if q.tasks.values().any(|t| t.state == TaskState::Active) {
            return Err(Error::FailedPrecondition(
                "cannot remove queue with active tasks".into(),
            ));
        }
        d.delete_where(qname, |_| true);
        d.queues.remove(qname);
        Ok(())
    }

    pub async fn pause(&self, qname: &str) -> Result<()> {
        let mut d = self.data();
        let q = d.queues.entry(qname.to_string()).or_default();
        if q.paused {
            return Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is already paused"
            )));
        }
        q.paused = true;
        Ok(())
    }

    pub async fn unpause(&self, qname: &str) -> Result<()> {
        let mut d = self.data();
        match d.queues.get_mut(qname).filter(|q| q.paused) {
            Some(q) => {
                q.paused = false;
                drop(d);
                self.wake();
                Ok(())
            }
            None => Err(Error::FailedPrecondition(format!(
                "queue {qname:?} is not paused"
            ))),
        }
    }

    pub async fn list_servers(&self) -> Result<Vec<ServerDetail>> {
        let now = Utc::now();
        Ok(self
            .data()
            .servers
            .values()
            .filter(|s| s.expires_at > now)
            .map(|s| ServerDetail {
                info: s.info.clone(),
                workers: s.workers.clone(),
            })
            .collect())
    }

    pub async fn list_scheduler_entries(&self) -> Result<Vec<pb::SchedulerEntry>> {
        let now = Utc::now();
        Ok(self
            .data()
            .scheduler_entries
            .values()
            .filter(|e| e.expires_at > now)
            .flat_map(|e| e.value.iter().cloned())
            .collect())
    }

    pub async fn list_scheduler_enqueue_events(
        &self,
        entry_id: &str,
        page: usize,
        size: usize,
    ) -> Result<Vec<pb::SchedulerEnqueueEvent>> {
        Ok(self
            .data()
            .scheduler_events
            .get(entry_id)
            .into_iter()
            .flatten()
            .skip(page * size)
            .take(size)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

fn server_key(host: &str, pid: u32, server_id: &str) -> String {
    format!("{host}:{pid}:{server_id}")
}
