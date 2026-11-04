//! Periodic loops that keep the server's Redis state healthy.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{apply, CancelReason, Event, Shared, SyncRequest, ERR_LEASE_EXPIRED};
use crate::base::{self, pb};

fn ts(t: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}

/// Runs `tick` immediately and then every `every` until cancelled.
async fn every<F, Fut>(stop: &CancellationToken, every: Duration, mut tick: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        tick().await;
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(every) => {}
        }
    }
}

/// Publishes server + worker info (read by the Inspector / asynqmon) and
/// extends the leases of in-flight tasks. Clears the server state on exit.
pub async fn heartbeater(shared: Arc<Shared>, stop: CancellationToken) {
    let interval = shared.cfg.heartbeat_interval;
    every(&stop, interval, || beat(&shared)).await;
    let c = &shared.cfg;
    if let Err(e) = shared
        .rdb
        .clear_server_state(&c.host, c.pid, &c.server_id)
        .await
    {
        wlog!(error, "failed to clear server state: {e}");
    }
}

async fn beat(shared: &Shared) {
    let c = &shared.cfg;
    let workers = shared.snapshot();
    let info = pb::ServerInfo {
        host: c.host.clone(),
        pid: c.pid as i32,
        server_id: c.server_id.clone(),
        concurrency: c.concurrency as i32,
        queues: c
            .queues
            .iter()
            .map(|(q, w)| (q.clone(), *w as i32))
            .collect(),
        strict_priority: c.strict_priority,
        status: shared.status().as_str().to_string(),
        start_time: Some(ts(shared.started)),
        active_worker_count: workers.len() as i32,
    };
    let mut infos = Vec::with_capacity(workers.len());
    let mut ids_by_queue: HashMap<&str, Vec<String>> = HashMap::new();
    for w in &workers {
        infos.push(pb::WorkerInfo {
            host: c.host.clone(),
            pid: c.pid as i32,
            server_id: c.server_id.clone(),
            task_id: w.msg.id.clone(),
            task_type: w.msg.r#type.clone(),
            task_payload: w.msg.payload.clone(),
            queue: w.msg.queue.clone(),
            start_time: Some(ts(w.started)),
            deadline: Some(ts(w.deadline)),
        });
        if w.lease.is_valid() {
            ids_by_queue
                .entry(&w.msg.queue)
                .or_default()
                .push(w.msg.id.clone());
        }
    }
    // TTL of two intervals: survives one missed beat, expires soon after a crash.
    if let Err(e) = shared
        .rdb
        .write_server_state(&info, &infos, shared.cfg.heartbeat_interval * 2)
        .await
    {
        wlog!(error, "failed to write server state: {e}");
    }
    for (qname, ids) in ids_by_queue {
        match shared
            .rdb
            .extend_lease(qname, &ids, shared.cfg.lease_duration)
            .await
        {
            Ok(expires_at) => {
                for w in workers.iter().filter(|w| w.msg.queue == qname) {
                    if !w.lease.extend(expires_at) {
                        wlog!(warn, "lease reset failed for task id={}", w.msg.id);
                    }
                }
            }
            Err(e) => wlog!(error, "failed to extend lease for tasks {ids:?}: {e}"),
        }
    }
}

/// Moves scheduled and retry tasks whose time has come into pending.
pub async fn forwarder(shared: Arc<Shared>, stop: CancellationToken) {
    let queues = shared.queues.names();
    every(&stop, shared.cfg.delayed_task_check_interval, || async {
        for q in &queues {
            if let Err(e) = shared.rdb.forward_if_ready(q).await {
                wlog!(
                    error,
                    "forwarder: could not enqueue scheduled tasks of {q}: {e}"
                );
            }
        }
    })
    .await;
}

/// Retries (or archives) tasks whose lease expired: their worker crashed
/// or lost Redis. This is what makes delivery at-least-once.
pub async fn recoverer(shared: Arc<Shared>, stop: CancellationToken) {
    let queues = shared.queues.names();
    every(&stop, shared.cfg.recoverer_interval, || async {
        let cutoff = Utc::now() - shared.cfg.recoverer_skew;
        match shared.rdb.list_lease_expired(cutoff, &queues).await {
            Ok(msgs) => {
                for msg in msgs {
                    wlog!(warn, "recovering task id={} with expired lease", msg.id);
                    let outcome = shared.decide_failure(&msg, ERR_LEASE_EXPIRED).await;
                    if let Err(e) = apply(&shared.rdb, &msg, &outcome).await {
                        wlog!(warn, "recoverer: could not recover task id={}: {e}", msg.id);
                    }
                }
            }
            Err(e) => wlog!(warn, "recoverer: could not list lease expired tasks: {e}"),
        }
        for q in &queues {
            if let Err(e) = shared.rdb.reclaim_stale_aggregation_sets(q).await {
                wlog!(
                    warn,
                    "recoverer: could not reclaim stale aggregation sets in {q}: {e}"
                );
            }
        }
    })
    .await;
}

/// Retries Redis writes that failed when a task finished, until the
/// task's lease runs out. Makes one last attempt on shutdown.
pub async fn syncer(
    shared: Arc<Shared>,
    mut rx: mpsc::UnboundedReceiver<SyncRequest>,
    stop: CancellationToken,
) {
    let mut pending: Vec<SyncRequest> = Vec::new();
    let mut tick = tokio::time::interval(shared.cfg.sync_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.cancelled() => {
                while let Ok(r) = rx.try_recv() {
                    pending.push(r);
                }
                for r in pending {
                    if let Err(e) = apply(&shared.rdb, &r.msg, &r.outcome).await {
                        wlog!(error, "could not sync task id={} on shutdown: {e}", r.msg.id);
                    }
                }
                return;
            }
            Some(r) = rx.recv() => pending.push(r),
            _ = tick.tick() => {
                let mut still = Vec::new();
                for r in pending.drain(..) {
                    if Utc::now() > r.deadline {
                        wlog!(warn, "giving up syncing task id={}: lease expired", r.msg.id);
                        continue;
                    }
                    if apply(&shared.rdb, &r.msg, &r.outcome).await.is_err() {
                        still.push(r);
                    }
                }
                pending = still;
            }
        }
    }
}

/// Listens on `asynq:cancel` for `Inspector.cancel_processing`.
pub async fn subscriber(shared: Arc<Shared>, stop: CancellationToken) {
    loop {
        let pubsub = async {
            let mut ps = shared.rdb.redis_client().get_async_pubsub().await?;
            ps.subscribe(base::CANCEL_CHANNEL).await?;
            Ok::<_, redis::RedisError>(ps)
        };
        let mut ps = tokio::select! {
            _ = stop.cancelled() => return,
            r = pubsub => match r {
                Ok(ps) => ps,
                Err(e) => {
                    wlog!(error, "cannot subscribe to cancelation channel: {e}");
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    }
                }
            }
        };
        let mut messages = ps.on_message();
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                m = messages.next() => match m {
                    Some(m) => {
                        if let Ok(id) = m.get_payload::<String>() {
                            shared.request_cancel(&id, CancelReason::Canceled);
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

/// Deletes completed tasks whose retention has passed.
pub async fn janitor(shared: Arc<Shared>, stop: CancellationToken) {
    let queues = shared.queues.names();
    every(&stop, shared.cfg.janitor_interval, || async {
        for q in &queues {
            if let Err(e) = shared
                .rdb
                .delete_expired_completed_tasks(q, shared.cfg.janitor_batch_size)
                .await
            {
                wlog!(
                    error,
                    "janitor: could not delete expired completed tasks of {q}: {e}"
                );
            }
        }
    })
    .await;
}

/// Pings Redis and reports the result to Python's health check hook.
pub async fn healthcheck(shared: Arc<Shared>, stop: CancellationToken) {
    every(&stop, shared.cfg.health_check_interval, || async {
        let err = shared.rdb.ping().await.err().map(|e| e.to_string());
        shared.emit(Event::Health(err));
    })
    .await;
}

/// Checks groups and turns ready ones into aggregation sets for Python.
pub async fn aggregator(shared: Arc<Shared>, stop: CancellationToken) {
    let g = &shared.cfg.group;
    let interval = g.grace_period.min(Duration::from_secs(7));
    let queues = shared.queues.names();
    every(&stop, interval, || async {
        for q in &queues {
            if let Err(e) = aggregate_queue(&shared, q).await {
                wlog!(error, "aggregator: failed to aggregate groups of {q}: {e}");
            }
        }
    })
    .await;
}

async fn aggregate_queue(shared: &Shared, qname: &str) -> crate::rdb::Result<()> {
    let g = &shared.cfg.group;
    for group in shared.rdb.list_groups(qname).await? {
        let set_id = uuid::Uuid::new_v4().to_string();
        let ok = shared
            .rdb
            .aggregation_check(
                qname,
                &group,
                Utc::now(),
                &set_id,
                g.grace_period,
                g.max_delay,
                g.max_size,
            )
            .await?;
        if !ok {
            continue;
        }
        let msgs = shared
            .rdb
            .read_aggregation_set(qname, &group, &set_id)
            .await?;
        shared.emit(Event::Aggregate {
            queue: qname.to_string(),
            group,
            set_id,
            msgs,
        });
    }
    Ok(())
}
