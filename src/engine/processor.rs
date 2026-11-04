//! Pulls tasks from Redis whenever a concurrency slot is free.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rand::RngExt;
use tokio_util::sync::CancellationToken;

use super::{CancelReason, ExecId, Lease, Shared};

pub async fn run(shared: Arc<Shared>, stop: CancellationToken) {
    let mut last_err_log: Option<Instant> = None;
    loop {
        let permit = tokio::select! {
            _ = stop.cancelled() => return,
            p = shared.sema.clone().acquire_owned() => p.expect("semaphore closed"),
        };
        let qnames = shared.queues.order();
        match shared
            .rdb
            .dequeue_with_lease(&qnames, shared.cfg.lease_duration)
            .await
        {
            Ok(Some((msg, lease_expires_at))) => shared.start_task(msg, lease_expires_at, permit),
            Ok(None) => {
                drop(permit);
                // Polling, like asynq: blocking pops across many queues would
                // put more load on Redis than an occasional empty check.
                let interval = shared.cfg.task_check_interval;
                let sleep = interval / 2 + jitter(interval);
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(sleep) => {}
                }
            }
            Err(e) => {
                drop(permit);
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
