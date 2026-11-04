//! Pulls tasks from the broker whenever a concurrency slot is free.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rand::RngExt;
use tokio_util::sync::CancellationToken;

use super::{CancelReason, ExecId, Lease, Shared};

pub async fn run(shared: Arc<Shared>, stop: CancellationToken) {
    let mut last_err_log: Option<Instant> = None;
    let pending = shared.rdb.pending_notifier().await;
    let woken = || async {
        match &pending {
            Some(n) => n.notified().await,
            None => std::future::pending().await,
        }
    };
    loop {
        let permit = tokio::select! {
            _ = stop.cancelled() => return,
            p = shared.sema.clone().acquire_owned() => p.expect("semaphore closed"),
        };
        let mut permits = vec![permit];
        while permits.len() < shared.rdb.max_dequeue_batch() {
            match shared.sema.clone().try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => break,
            }
        }
        let qnames = shared.queues.order();
        let dequeued = tokio::select! {
            _ = stop.cancelled() => return,
            r = shared.rdb.dequeue_batch(&qnames, shared.cfg.lease_duration, permits.len()) => r,
        };
        match dequeued {
            Ok(tasks) if !tasks.is_empty() => {
                for ((msg, lease_expires_at), permit) in tasks.into_iter().zip(permits) {
                    shared.start_task(msg, lease_expires_at, permit);
                }
            }
            Ok(_) => {
                drop(permits);
                // Polling, like asynq: blocking pops across many queues would
                // put more load on Redis than an occasional empty check.
                // Postgres also wakes us via NOTIFY; the poll stays as a
                // fallback for notifications lost while reconnecting.
                let interval = shared.cfg.task_check_interval;
                let sleep = interval / 2 + jitter(interval);
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(sleep) => {}
                    _ = woken() => {}
                }
            }
            Err(e) => {
                drop(permits);
                if last_err_log.is_none_or(|t| t.elapsed() > Duration::from_secs(3)) {
                    wlog!(error, "dequeue error: {e}");
                    last_err_log = Some(Instant::now());
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(shared.cfg.task_check_interval) => {}
                }
            }
        }
    }
}

fn jitter(d: Duration) -> Duration {
    let ms = d.as_millis().max(1) as u64;
    Duration::from_millis(rand::rng().random_range(0..ms))
}

fn until(t: DateTime<Utc>) -> Duration {
    (t - Utc::now()).to_std().unwrap_or(Duration::ZERO)
}

/// One per in-flight task: fires when the task deadline passes or its
/// lease runs out (i.e. heartbeats could not extend it). Aborted when the
/// task is finalized.
pub async fn watchdog(
    shared: Weak<Shared>,
    exec: ExecId,
    deadline: DateTime<Utc>,
    lease: Arc<Lease>,
) {
    loop {
        let wake = deadline.min(lease.expires_at());
        tokio::time::sleep(until(wake)).await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if Utc::now() >= deadline {
            shared.request_cancel(exec, CancelReason::DeadlineExceeded);
            // Keep watching the lease while Python handles the cancel.
            loop {
                tokio::time::sleep(until(lease.expires_at())).await;
                if !lease.is_valid() {
                    shared.lease_lost(exec);
                    return;
                }
            }
        }
        if !lease.is_valid() {
            shared.lease_lost(exec);
            return;
        }
    }
}
