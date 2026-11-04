//! Distributed counting semaphore (port of asynq's `x/rate`): at most N
//! tasks across all workers hold a token for a scope at once. Tokens
//! expire at the holder's deadline, so crashed workers don't leak them.

use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use super::{Rdb, Result};

lua!(SEMAPHORE_ACQUIRE, "semaphore_acquire.lua");

pub fn semaphore_key(scope: &str) -> String {
    format!("asynq:sema:{scope}")
}

impl Rdb {
    pub async fn semaphore_acquire(
        &self,
        scope: &str,
        max_tokens: u32,
        deadline: DateTime<Utc>,
        task_id: &str,
    ) -> Result<bool> {
        let mut conn = self.conn.clone();
        let res: String = SEMAPHORE_ACQUIRE
            .key(semaphore_key(scope))
            .arg(max_tokens)
            .arg(Utc::now().timestamp())
            .arg(deadline.timestamp())
            .arg(task_id)
            .invoke_async(&mut conn)
            .await?;
        Ok(res == "true")
    }

    /// Returns false if the task held no token.
    pub async fn semaphore_release(&self, scope: &str, task_id: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let n: i64 = conn.zrem(semaphore_key(scope), task_id).await?;
        Ok(n == 1)
    }
}
