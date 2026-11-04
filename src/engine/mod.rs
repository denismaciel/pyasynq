//! The worker engine: everything a Go `asynq.Server` does except calling
//! the handler. Rust owns Redis and every background loop; Python only
//! receives events (`Job`, `Cancel`, ...) and answers with acks.
//!
//! Lock rule: `Shared::inflight` is never held across an `.await` or while
//! logging/calling into Python (both may need the GIL, and Python threads
//! holding the GIL call `ack`, which takes this lock).

macro_rules! wlog {
    ($lvl:ident, $($arg:tt)+) => { ::log::$lvl!(target: "pyasynq.worker", $($arg)+) };
}

pub mod background;
pub mod processor;
pub mod queues;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rand::RngExt;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::base::TaskMessage;
use crate::client::DEFAULT_TIMEOUT;
use crate::rdb::{self, Rdb};
use crate::runtime;
use queues::QueueOrder;

pub const ERR_LEASE_EXPIRED: &str = "asynq: task lease expired";
pub const ERR_DEADLINE_EXCEEDED: &str = "context deadline exceeded";
pub const ERR_CANCELED: &str = "context canceled";

#[derive(Debug, Clone)]
pub struct Config {
    pub concurrency: usize,
    pub queues: Vec<(String, u32)>,
    pub strict_priority: bool,
    pub task_check_interval: Duration,
    pub delayed_task_check_interval: Duration,
    pub shutdown_timeout: Duration,
    pub health_check_interval: Duration,
    pub heartbeat_interval: Duration,
    pub lease_duration: Duration,
    pub recoverer_interval: Duration,
    /// Only lease-expired tasks older than this are recovered (clock skew).
    pub recoverer_skew: Duration,
    pub janitor_interval: Duration,
    pub janitor_batch_size: usize,
    pub sync_interval: Duration,
    /// How long Python gets to ack a cancelled task before Rust records the
    /// failure itself.
    pub cancel_grace: Duration,
    pub group: GroupConfig,
    pub host: String,
    pub pid: u32,
    pub server_id: String,
}

#[derive(Debug, Clone)]
pub struct GroupConfig {
    pub enabled: bool,
    pub grace_period: Duration,
    pub max_delay: Duration,
    pub max_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: std::thread::available_parallelism().map_or(4, |n| n.get()),
            queues: vec![(crate::base::DEFAULT_QUEUE_NAME.to_string(), 1)],
            strict_priority: false,
            task_check_interval: Duration::from_secs(1),
            delayed_task_check_interval: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(8),
            health_check_interval: Duration::from_secs(15),
            heartbeat_interval: Duration::from_secs(5),
            lease_duration: rdb::LEASE_DURATION,
            recoverer_interval: Duration::from_secs(60),
            recoverer_skew: Duration::from_secs(30),
            janitor_interval: Duration::from_secs(8),
            janitor_batch_size: 100,
            sync_interval: Duration::from_secs(5),
            cancel_grace: Duration::from_secs(5),
            group: GroupConfig {
                enabled: false,
                grace_period: Duration::from_secs(60),
                max_delay: Duration::ZERO,
                max_size: 0,
            },
            host: gethostname::gethostname().to_string_lossy().into_owned(),
            pid: std::process::id(),
            server_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    DeadlineExceeded,
    Canceled,
    LeaseExpired,
    Shutdown,
}

impl CancelReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Canceled => "canceled",
            Self::LeaseExpired => "lease_expired",
            Self::Shutdown => "shutdown",
        }
    }

    fn error_message(self) -> &'static str {
        match self {
            Self::DeadlineExceeded => ERR_DEADLINE_EXCEEDED,
            Self::Canceled | Self::Shutdown => ERR_CANCELED,
            Self::LeaseExpired => ERR_LEASE_EXPIRED,
        }
    }
}

#[derive(Debug)]
pub enum Event {
    Job {
        msg: TaskMessage,
        deadline: DateTime<Utc>,
    },
    /// Stop running the handler. For `DeadlineExceeded`/`Canceled` the
    /// engine still expects a failure ack; for the others the task has
    /// already been taken care of.
    Cancel {
        id: String,
        reason: CancelReason,
    },
    Aggregate {
        queue: String,
        group: String,
        set_id: String,
        msgs: Vec<TaskMessage>,
    },
    Health(Option<String>),
    Closed,
}

/// What happens to a finished task.
#[derive(Debug, Clone)]
pub enum Outcome {
    Success {
        result: Option<Vec<u8>>,
    },
    Retry {
        err: String,
        retry_at: DateTime<Utc>,
        is_failure: bool,
    },
    Archive {
        err: String,
    },
    /// Delete without retrying or archiving (asynq's `RevokeTask`).
    Revoke,
}

/// Decides retry delay and whether an error counts as a failure, for the
/// failures the engine records on its own (lease recovery, cancel fallback).
pub trait FailurePolicy: Send + Sync {
    fn decide(&self, msg: &TaskMessage, err: &str) -> (Duration, bool);
}

pub struct DefaultPolicy;

impl FailurePolicy for DefaultPolicy {
    fn decide(&self, msg: &TaskMessage, _err: &str) -> (Duration, bool) {
        (default_retry_delay(msg.retried), true)
    }
}

/// Sidekiq's formula, as used by asynq: n^4 + 15 + rand(30) * (n + 1) seconds.
pub fn default_retry_delay(retried: i32) -> Duration {
    let n = retried.max(0) as u64;
    let jitter = rand::rng().random_range(0..30u64);
    Duration::from_secs(n.saturating_pow(4) + 15 + jitter * (n + 1))
}

/// Archive if the retry budget is spent, else retry with the policy's delay.
pub fn failure_outcome(policy: &dyn FailurePolicy, msg: &TaskMessage, err: &str) -> Outcome {
    if msg.retried >= msg.retry {
        return Outcome::Archive {
            err: err.to_string(),
        };
    }
    let (delay, is_failure) = policy.decide(msg, err);
    Outcome::Retry {
        err: err.to_string(),
        retry_at: Utc::now() + delay,
        is_failure,
    }
}

/// Lease expiry in unix milliseconds, extended by the heartbeater.
pub struct Lease(AtomicI64);

impl Lease {
    fn new(expires_at: DateTime<Utc>) -> Self {
        Self(AtomicI64::new(expires_at.timestamp_millis()))
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(self.0.load(Ordering::Acquire))
            .unwrap()
    }

    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at()
    }

    /// Like Go's `Lease.Reset`: an expired lease stays expired.
    pub fn extend(&self, to: DateTime<Utc>) -> bool {
        if !self.is_valid() {
            return false;
        }
        self.0.fetch_max(to.timestamp_millis(), Ordering::AcqRel);
        true
    }
}

pub struct InFlight {
    pub msg: TaskMessage,
    pub started: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub lease: Arc<Lease>,
    _permit: OwnedSemaphorePermit,
    watchdog: Option<AbortHandle>,
    cancel_sent: bool,
}

/// Snapshot used by the heartbeater.
pub struct WorkerSnapshot {
    pub msg: TaskMessage,
    pub started: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub lease: Arc<Lease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    New,
    Active,
    Stopped,
    Closed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Active => "active",
            Self::Stopped => "stopped",
            Self::Closed => "closed",
        }
    }
}

pub struct SyncRequest {
    msg: TaskMessage,
    outcome: Outcome,
    deadline: DateTime<Utc>,
}

pub struct Shared {
    pub rdb: Arc<Rdb>,
    pub cfg: Config,
    pub queues: QueueOrder,
    pub policy: Arc<dyn FailurePolicy>,
    pub started: DateTime<Utc>,
    inflight: Mutex<HashMap<String, InFlight>>,
    events: mpsc::UnboundedSender<Event>,
    sema: Arc<Semaphore>,
    status: Mutex<Status>,
    sync_tx: mpsc::UnboundedSender<SyncRequest>,
}

impl Shared {
    pub fn status(&self) -> Status {
        *self.status.lock().unwrap()
    }

    fn set_status(&self, s: Status) {
        *self.status.lock().unwrap() = s;
    }

    pub fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    pub fn active_count(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    pub fn snapshot(&self) -> Vec<WorkerSnapshot> {
        self.inflight
            .lock()
            .unwrap()
            .values()
            .map(|w| WorkerSnapshot {
                msg: w.msg.clone(),
                started: w.started,
                deadline: w.deadline,
                lease: w.lease.clone(),
            })
            .collect()
    }

    pub fn is_inflight(&self, id: &str) -> bool {
        self.inflight.lock().unwrap().contains_key(id)
    }

    /// Registers a dequeued task and hands it to Python.
    fn start_task(
        self: &Arc<Self>,
        msg: TaskMessage,
        lease_expires_at: DateTime<Utc>,
        permit: OwnedSemaphorePermit,
    ) {
        let deadline = compute_deadline(&msg);
        let lease = Arc::new(Lease::new(lease_expires_at));
        let id = msg.id.clone();
        let mut inflight = self.inflight.lock().unwrap();
        let watchdog = runtime::spawn(processor::watchdog(
            Arc::downgrade(self),
            id.clone(),
            deadline,
            lease.clone(),
        ));
        inflight.insert(
            id,
            InFlight {
                msg: msg.clone(),
                started: Utc::now(),
                deadline,
                lease,
                _permit: permit,
                watchdog: Some(watchdog.abort_handle()),
                cancel_sent: false,
            },
        );
        drop(inflight);
        self.emit(Event::Job { msg, deadline });
    }

    /// Removes a task from the in-flight map. Whoever takes it first
    /// (Python ack, cancel fallback, lease loss, shutdown) decides its fate.
    fn take(&self, id: &str) -> Option<InFlight> {
        let mut w = self.inflight.lock().unwrap().remove(id)?;
        if let Some(h) = w.watchdog.take() {
            h.abort();
        }
        Some(w)
    }

    /// Records the outcome of a task. Returns false if the task is no
    /// longer ours (already finalized, lease lost, requeued on shutdown).
    pub fn ack(self: &Arc<Self>, id: &str, outcome: Outcome) -> bool {
        let Some(w) = self.take(id) else {
            return false;
        };
        let this = self.clone();
        runtime::spawn(async move { this.finish(w, outcome).await });
        true
    }

    async fn finish(&self, w: InFlight, outcome: Outcome) {
        if !w.lease.is_valid() {
            wlog!(
                warn,
                "lease expired for task id={}; leaving it to the recoverer",
                w.msg.id
            );
            return;
        }
        if let Err(e) = apply(&self.rdb, &w.msg, &outcome).await {
            wlog!(
                warn,
                "could not finalize task id={}: {e}; will retry syncing",
                w.msg.id
            );
            let _ = self.sync_tx.send(SyncRequest {
                msg: w.msg.clone(),
                outcome,
                deadline: w.lease.expires_at(),
            });
        }
        // The permit in `w` is released here, after the first write attempt.
    }

    /// Asks Python to stop the handler and ack a failure. If it does not
    /// within `cancel_grace`, the engine records the failure itself.
    pub fn request_cancel(self: &Arc<Self>, id: &str, reason: CancelReason) {
        let first = match self.inflight.lock().unwrap().get_mut(id) {
            Some(w) if !w.cancel_sent => {
                w.cancel_sent = true;
                true
            }
            _ => false,
        };
        if !first {
            return;
        }
        self.emit(Event::Cancel {
            id: id.to_string(),
            reason,
        });
        let this = self.clone();
        let id = id.to_string();
        runtime::spawn(async move {
            tokio::time::sleep(this.cfg.cancel_grace).await;
            let Some(w) = this.take(&id) else { return };
            wlog!(
                warn,
                "task id={id} was not acked {:?} after cancel; recording failure",
                this.cfg.cancel_grace
            );
            let outcome = this.decide_failure(&w.msg, reason.error_message()).await;
            this.finish(w, outcome).await;
        });
    }

    fn lease_lost(&self, id: &str) {
        if self.take(id).is_some() {
            wlog!(warn, "lease expired for task id={id}");
            self.emit(Event::Cancel {
                id: id.to_string(),
                reason: CancelReason::LeaseExpired,
            });
        }
    }

    /// Runs the (possibly Python-backed, GIL-taking) policy off the async
    /// worker threads.
    pub async fn decide_failure(&self, msg: &TaskMessage, err: &str) -> Outcome {
        let policy = self.policy.clone();
        let (msg, err) = (msg.clone(), err.to_string());
        tokio::task::spawn_blocking(move || failure_outcome(policy.as_ref(), &msg, &err))
            .await
            .unwrap_or_else(|_| Outcome::Archive {
                err: "failure policy panicked".into(),
            })
    }

    /// Shutdown timeout hit: push every unfinished task back to its queue.
    async fn abort_all(&self) {
        let ids: Vec<String> = self.inflight.lock().unwrap().keys().cloned().collect();
        for id in ids {
            let Some(w) = self.take(&id) else { continue };
            wlog!(warn, "quitting worker; task id={id}");
            if w.lease.is_valid() {
                match self.rdb.requeue(&w.msg).await {
                    Ok(()) => wlog!(info, "pushed task id={id} back to queue"),
                    Err(e) => wlog!(error, "could not push task id={id} back to queue: {e}"),
                }
            }
            self.emit(Event::Cancel {
                id,
                reason: CancelReason::Shutdown,
            });
        }
    }
}

pub async fn apply(rdb: &Rdb, msg: &TaskMessage, outcome: &Outcome) -> rdb::Result<()> {
    match outcome {
        Outcome::Success { result } => {
            if msg.retention > 0 {
                if let Some(data) = result {
                    rdb.write_result(&msg.queue, &msg.id, data).await?;
                }
                rdb.mark_as_complete(msg).await
            } else {
                rdb.done(msg).await
            }
        }
        Outcome::Revoke => rdb.done(msg).await,
        Outcome::Retry {
            err,
            retry_at,
            is_failure,
        } => rdb.retry(msg, *retry_at, err, *is_failure).await,
        Outcome::Archive { err } => rdb.archive(msg, err).await,
    }
}

fn compute_deadline(msg: &TaskMessage) -> DateTime<Utc> {
    let now = Utc::now();
    let from_timeout = (msg.timeout > 0).then(|| now + chrono::Duration::seconds(msg.timeout));
    let from_deadline = (msg.deadline > 0).then(|| Utc.timestamp_opt(msg.deadline, 0).unwrap());
    match (from_timeout, from_deadline) {
        (Some(t), Some(d)) => t.min(d),
        (Some(t), None) => t,
        (None, Some(d)) => d,
        (None, None) => {
            wlog!(
                error,
                "both timeout and deadline are unset for task id={}",
                msg.id
            );
            now + DEFAULT_TIMEOUT
        }
    }
}

struct Loops {
    processor: JoinHandle<()>,
    forwarder: JoinHandle<()>,
    background: Vec<JoinHandle<()>>,
    heartbeater: JoinHandle<()>,
}

pub struct Engine {
    pub shared: Arc<Shared>,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>,
    closed: AtomicBool,
    sync_rx: Mutex<Option<mpsc::UnboundedReceiver<SyncRequest>>>,
    processing: CancellationToken,
    background: CancellationToken,
    heartbeat: CancellationToken,
    loops: tokio::sync::Mutex<Option<Loops>>,
}

impl Engine {
    pub fn new(rdb: Arc<Rdb>, cfg: Config, policy: Arc<dyn FailurePolicy>) -> Self {
        let (events, events_rx) = mpsc::unbounded_channel();
        let (sync_tx, sync_rx) = mpsc::unbounded_channel();
        let queues = QueueOrder::new(&cfg.queues, cfg.strict_priority);
        let shared = Arc::new(Shared {
            rdb,
            queues,
            policy,
            started: Utc::now(),
            inflight: Mutex::new(HashMap::new()),
            events,
            sema: Arc::new(Semaphore::new(cfg.concurrency)),
            status: Mutex::new(Status::New),
            sync_tx,
            cfg,
        });
        Self {
            shared,
            events_rx: tokio::sync::Mutex::new(events_rx),
            closed: AtomicBool::new(false),
            sync_rx: Mutex::new(Some(sync_rx)),
            processing: CancellationToken::new(),
            background: CancellationToken::new(),
            heartbeat: CancellationToken::new(),
            loops: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn start(&self) -> rdb::Result<()> {
        let mut loops = self.loops.lock().await;
        if loops.is_some() || self.shared.status() != Status::New {
            return Err(rdb::Error::FailedPrecondition(
                "engine already started".into(),
            ));
        }
        self.shared.rdb.ping().await?;
        self.shared.set_status(Status::Active);
        let s = &self.shared;
        let bg = &self.background;
        let sync_rx = self.sync_rx.lock().unwrap().take().expect("sync receiver");
        let mut background = vec![
            runtime::spawn(background::recoverer(s.clone(), bg.clone())),
            runtime::spawn(background::syncer(s.clone(), sync_rx, bg.clone())),
            runtime::spawn(background::subscriber(s.clone(), bg.clone())),
            runtime::spawn(background::janitor(s.clone(), bg.clone())),
        ];
        if s.cfg.group.enabled {
            background.push(runtime::spawn(background::aggregator(
                s.clone(),
                bg.clone(),
            )));
        }
        background.push(runtime::spawn(background::healthcheck(
            s.clone(),
            bg.clone(),
        )));
        *loops = Some(Loops {
            heartbeater: runtime::spawn(background::heartbeater(s.clone(), self.heartbeat.clone())),
            forwarder: runtime::spawn(background::forwarder(s.clone(), self.processing.clone())),
            processor: runtime::spawn(processor::run(s.clone(), self.processing.clone())),
            background,
        });
        wlog!(
            info,
            "worker started: server_id={} queues={:?} concurrency={}",
            s.cfg.server_id,
            s.queues.weights(),
            s.cfg.concurrency
        );
        Ok(())
    }

    /// Stops pulling new tasks; in-flight tasks keep running.
    pub fn stop(&self) {
        if self.shared.status() == Status::Active {
            wlog!(info, "stopping processor: no new tasks will be processed");
            self.processing.cancel();
            self.shared.set_status(Status::Stopped);
        }
    }

    /// Graceful shutdown in asynq's order. Tasks still running after
    /// `shutdown_timeout` are requeued.
    pub async fn shutdown(&self) {
        let Some(loops) = self.loops.lock().await.take() else {
            if self.shared.status() != Status::Closed {
                self.shared.set_status(Status::Closed);
                self.shared.emit(Event::Closed);
            }
            return;
        };
        wlog!(info, "starting graceful shutdown");
        self.processing.cancel();
        if self.shared.status() == Status::Active {
            self.shared.set_status(Status::Stopped);
        }
        let _ = loops.forwarder.await;
        let _ = loops.processor.await;

        let s = &self.shared;
        let all = s.cfg.concurrency as u32;
        wlog!(info, "waiting for all workers to finish...");
        let drained = tokio::time::timeout(s.cfg.shutdown_timeout, s.sema.acquire_many(all)).await;
        if drained.is_err() {
            s.abort_all().await;
            let _ = tokio::time::timeout(Duration::from_secs(5), s.sema.acquire_many(all)).await;
        }
        wlog!(info, "all workers have finished");

        self.background.cancel();
        for h in loops.background {
            let _ = h.await;
        }
        self.heartbeat.cancel();
        let _ = loops.heartbeater.await;
        s.set_status(Status::Closed);
        s.emit(Event::Closed);
        wlog!(info, "exiting");
    }

    /// Waits for at least one event and returns up to `max`. Returns None
    /// once the engine has closed.
    pub async fn next_events(&self, max: usize) -> Option<Vec<Event>> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let mut rx = self.events_rx.lock().await;
        let first = rx.recv().await?;
        let mut out = vec![first];
        while out.len() < max {
            match rx.try_recv() {
                Ok(ev) => out.push(ev),
                Err(_) => break,
            }
        }
        if out.iter().any(|e| matches!(e, Event::Closed)) {
            self.closed.store(true, Ordering::Release);
        }
        Some(out)
    }
}
