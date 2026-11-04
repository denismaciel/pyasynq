//! Periodic task scheduler (port of asynq's `Scheduler`): enqueues tasks
//! on cron specs, publishes its entries for the Inspector and records an
//! enqueue history per entry.

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::Tz;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::base::pb;
use crate::client::{self, EnqueueOptions, TaskInfo};
use crate::rdb::{self, Rdb};

macro_rules! slog {
    ($lvl:ident, $($arg:tt)+) => { ::log::$lvl!(target: "pyasynq.scheduler", $($arg)+) };
}

#[derive(Debug, Clone)]
pub enum Schedule {
    Cron { cron: Box<croner::Cron>, tz: Tz },
    Every(Duration),
}

impl Schedule {
    /// Accepts what asynq (robfig/cron) accepts: 5-field cron,
    /// descriptors like `@daily`, `@every 1h30m`, and a `CRON_TZ=Zone`
    /// prefix to override the scheduler's time zone.
    pub fn parse(spec: &str, default_tz: Tz) -> Result<Self, String> {
        let spec = spec.trim();
        if let Some(rest) = spec.strip_prefix("@every") {
            let d = parse_go_duration(rest.trim())?;
            if d < Duration::from_secs(1) {
                return Err("@every interval must be at least 1s".into());
            }
            return Ok(Self::Every(d));
        }
        let (tz, pattern) = match spec
            .strip_prefix("CRON_TZ=")
            .or_else(|| spec.strip_prefix("TZ="))
        {
            Some(rest) => {
                let (name, pattern) = rest
                    .split_once(char::is_whitespace)
                    .ok_or("missing cron spec after time zone")?;
                (
                    Tz::from_str(name).map_err(|e| e.to_string())?,
                    pattern.trim(),
                )
            }
            None => (default_tz, spec),
        };
        let cron = croner::Cron::from_str(pattern)
            .map_err(|e| format!("invalid cron spec {pattern:?}: {e}"))?;
        Ok(Self::Cron {
            cron: Box::new(cron),
            tz,
        })
    }

    pub fn next_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            // Like robfig's ConstantDelaySchedule: whole seconds.
            Self::Every(d) => Some(t.with_nanosecond(0)? + *d),
            Self::Cron { cron, tz } => cron
                .find_next_occurrence(&t.with_timezone(tz), false)
                .ok()
                .map(|n| n.with_timezone(&Utc)),
        }
    }
}

/// Go's `time.ParseDuration` subset: "1h30m", "90s", "1.5h", "500ms".
pub fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let err = || format!("invalid duration {s:?}");
    if s.is_empty() {
        return Err(err());
    }
    let mut total = 0f64;
    let mut rest = s;
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(err)?;
        let value: f64 = rest[..num_len].parse().map_err(|_| err())?;
        rest = &rest[num_len..];
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..unit_len] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return Err(err()),
        };
        total += value * scale;
        rest = &rest[unit_len..];
    }
    Ok(Duration::from_secs_f64(total))
}

fn go_duration(d: Duration) -> String {
    format!("{}s", d.as_secs())
}

/// The option strings asynq stores with each entry (parsed back by Go's
/// Inspector, shown by asynqmon).
pub fn option_strings(o: &EnqueueOptions) -> Vec<String> {
    let mut v = Vec::new();
    if let Some(q) = &o.queue {
        v.push(format!("Queue({q:?})"));
    }
    if let Some(n) = o.max_retry {
        v.push(format!("MaxRetry({n})"));
    }
    if let Some(id) = &o.task_id {
        v.push(format!("TaskID({id:?})"));
    }
    if let Some(d) = o.timeout {
        v.push(format!("Timeout({})", go_duration(d)));
    }
    if let Some(t) = o.deadline {
        v.push(format!(
            "Deadline({})",
            t.format("%a %b %e %H:%M:%S UTC %Y")
        ));
    }
    if let Some(d) = o.unique_ttl {
        v.push(format!("Unique({})", go_duration(d)));
    }
    if let Some(d) = o.retention {
        v.push(format!("Retention({})", go_duration(d)));
    }
    if let Some(g) = &o.group {
        v.push(format!("Group({g:?})"));
    }
    v
}

pub type PostEnqueue = Arc<dyn Fn(&str, Result<&TaskInfo, String>) + Send + Sync>;

struct Entry {
    id: String,
    spec: String,
    schedule: Schedule,
    task_type: String,
    payload: Vec<u8>,
    opts: EnqueueOptions,
    next: Option<DateTime<Utc>>,
    prev: Option<DateTime<Utc>>,
}

pub struct Scheduler {
    rdb: Arc<Rdb>,
    pub id: String,
    tz: Tz,
    heartbeat: Duration,
    entries: Mutex<Vec<Entry>>,
    wake: Notify,
    stop: CancellationToken,
    post_enqueue: Option<PostEnqueue>,
}

impl Scheduler {
    pub fn new(
        rdb: Arc<Rdb>,
        tz: Tz,
        heartbeat: Duration,
        post_enqueue: Option<PostEnqueue>,
    ) -> Self {
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        Self {
            rdb,
            id: format!("{host}:{}:{}", std::process::id(), uuid::Uuid::new_v4()),
            tz,
            heartbeat,
            entries: Mutex::new(Vec::new()),
            wake: Notify::new(),
            stop: CancellationToken::new(),
            post_enqueue,
        }
    }

    pub fn register(
        &self,
        spec: &str,
        task_type: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<String, String> {
        let schedule = Schedule::parse(spec, self.tz)?;
        client::build_message(task_type, Some(&payload), &opts).map_err(|e| e.to_string())?;
        let id = uuid::Uuid::new_v4().to_string();
        self.entries.lock().unwrap().push(Entry {
            id: id.clone(),
            spec: spec.to_string(),
            next: schedule.next_after(Utc::now()),
            schedule,
            task_type: task_type.to_string(),
            payload,
            opts,
            prev: None,
        });
        self.wake.notify_one();
        Ok(id)
    }

    pub fn unregister(&self, id: &str) -> bool {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|e| e.id != id);
        let removed = entries.len() != before;
        drop(entries);
        self.wake.notify_one();
        removed
    }

    pub fn shutdown(&self) {
        self.stop.cancel();
    }

    fn snapshot(&self) -> Vec<pb::SchedulerEntry> {
        let ts = |t: DateTime<Utc>| prost_types::Timestamp {
            seconds: t.timestamp(),
            nanos: t.timestamp_subsec_nanos() as i32,
        };
        self.entries
            .lock()
            .unwrap()
            .iter()
            .map(|e| pb::SchedulerEntry {
                id: e.id.clone(),
                spec: e.spec.clone(),
                task_type: e.task_type.clone(),
                task_payload: e.payload.clone(),
                enqueue_options: option_strings(&e.opts),
                next_enqueue_time: e.next.map(ts),
                prev_enqueue_time: e.prev.map(ts),
            })
            .collect()
    }

    async fn beat(&self) {
        let entries = self.snapshot();
        if let Err(e) = self
            .rdb
            .write_scheduler_entries(&self.id, &entries, self.heartbeat * 2)
            .await
        {
            slog!(warn, "could not write scheduler entries: {e}");
        }
    }

    /// Runs until `shutdown()`, then removes its entries and their history.
    pub async fn run(&self) -> rdb::Result<()> {
        self.rdb.ping().await?;
        slog!(info, "scheduler starting: id={}", self.id);
        self.beat().await;
        let mut heartbeat = tokio::time::interval(self.heartbeat);
        heartbeat.tick().await;
        loop {
            let next_due = self
                .entries
                .lock()
                .unwrap()
                .iter()
                .filter_map(|e| e.next)
                .min();
            let sleep = next_due
                .map(|t| (t - Utc::now()).to_std().unwrap_or_default())
                .unwrap_or(Duration::from_secs(3600));
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = heartbeat.tick() => self.beat().await,
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(sleep) => self.fire_due().await,
            }
        }
        let ids: Vec<String> = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.id.clone())
            .collect();
        for id in ids {
            if let Err(e) = self.rdb.clear_scheduler_history(&id).await {
                slog!(warn, "could not clear history of entry {id}: {e}");
            }
        }
        if let Err(e) = self.rdb.clear_scheduler_entries(&self.id).await {
            slog!(warn, "could not clear scheduler entries: {e}");
        }
        slog!(info, "scheduler stopped");
        Ok(())
    }

    async fn fire_due(&self) {
        let now = Utc::now();
        let due: Vec<(String, String, Vec<u8>, EnqueueOptions)> = {
            let mut entries = self.entries.lock().unwrap();
            entries
                .iter_mut()
                .filter(|e| e.next.is_some_and(|n| n <= now))
                .map(|e| {
                    e.prev = Some(now);
                    e.next = e.schedule.next_after(now);
                    (
                        e.id.clone(),
                        e.task_type.clone(),
                        e.payload.clone(),
                        e.opts.clone(),
                    )
                })
                .collect()
        };
        for (id, task_type, payload, opts) in due {
            match client::enqueue(&self.rdb, &task_type, Some(&payload), &opts).await {
                Ok(info) => {
                    slog!(
                        debug,
                        "scheduler enqueued task id={} type={task_type}",
                        info.msg.id
                    );
                    if let Err(e) = self
                        .rdb
                        .record_scheduler_enqueue_event(&id, &info.msg.id, now)
                        .await
                    {
                        slog!(warn, "could not record enqueue event: {e}");
                    }
                    self.notify(&id, Ok(&info)).await;
                }
                Err(e) => {
                    slog!(
                        error,
                        "scheduler could not enqueue task type={task_type}: {e}"
                    );
                    self.notify(&id, Err(e.to_string())).await;
                }
            }
        }
    }

    async fn notify(&self, entry_id: &str, res: Result<&TaskInfo, String>) {
        if let Some(cb) = self.post_enqueue.clone() {
            let id = entry_id.to_string();
            let res = res.cloned();
            let _ =
                tokio::task::spawn_blocking(move || cb(&id, res.as_ref().map_err(|e| e.clone())))
                    .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parses_go_durations() {
        assert_eq!(
            parse_go_duration("1h30m").unwrap(),
            Duration::from_secs(5400)
        );
        assert_eq!(
            parse_go_duration("1.5s").unwrap(),
            Duration::from_millis(1500)
        );
        assert!(parse_go_duration("5 minutes").is_err());
    }

    #[test]
    fn every_and_cron_schedules() {
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 30).unwrap();
        let every = Schedule::parse("@every 1m", Tz::UTC).unwrap();
        assert_eq!(every.next_after(t), Some(t + Duration::from_secs(60)));
        let cron = Schedule::parse("*/15 * * * *", Tz::UTC).unwrap();
        assert_eq!(
            cron.next_after(t),
            Some(Utc.with_ymd_and_hms(2026, 1, 1, 10, 15, 0).unwrap())
        );
        let tokyo = Schedule::parse("CRON_TZ=Asia/Tokyo 0 9 * * *", Tz::UTC).unwrap();
        assert_eq!(
            tokyo.next_after(t),
            Some(Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap())
        );
        assert!(Schedule::parse("not a spec", Tz::UTC).is_err());
    }
}
