//! Port of asynq's `internal/base`: Redis key layout, task states, and
//! message encoding. Everything here must stay byte-for-byte compatible
//! with Go asynq.

use md5::{Digest, Md5};
use prost::Message;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/asynq.rs"));
}

pub use pb::TaskMessage;

/// Version of Go asynq whose wire format we mirror.
pub const ASYNQ_VERSION: &str = "0.26.0";

pub const DEFAULT_QUEUE_NAME: &str = "default";

pub const ALL_SERVERS: &str = "asynq:servers";
pub const ALL_WORKERS: &str = "asynq:workers";
pub const ALL_SCHEDULERS: &str = "asynq:schedulers";
pub const ALL_QUEUES: &str = "asynq:queues";
pub const CANCEL_CHANNEL: &str = "asynq:cancel";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Active = 1,
    Pending,
    Scheduled,
    Retry,
    Archived,
    Completed,
    Aggregating,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Active => "active",
            TaskState::Pending => "pending",
            TaskState::Scheduled => "scheduled",
            TaskState::Retry => "retry",
            TaskState::Archived => "archived",
            TaskState::Completed => "completed",
            TaskState::Aggregating => "aggregating",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => TaskState::Active,
            "pending" => TaskState::Pending,
            "scheduled" => TaskState::Scheduled,
            "retry" => TaskState::Retry,
            "archived" => TaskState::Archived,
            "completed" => TaskState::Completed,
            "aggregating" => TaskState::Aggregating,
            _ => return None,
        })
    }
}

pub fn validate_queue_name(qname: &str) -> Result<(), &'static str> {
    if qname.trim().is_empty() {
        return Err("queue name must contain one or more characters");
    }
    Ok(())
}

// The `{qname}` braces are a Redis Cluster hash tag: every key of a queue
// hashes to the same slot, which lets Lua scripts touch several of them.
pub fn queue_key_prefix(qname: &str) -> String {
    format!("asynq:{{{qname}}}:")
}

pub fn task_key_prefix(qname: &str) -> String {
    queue_key_prefix(qname) + "t:"
}

pub fn task_key(qname: &str, id: &str) -> String {
    task_key_prefix(qname) + id
}

pub fn pending_key(qname: &str) -> String {
    queue_key_prefix(qname) + "pending"
}

pub fn active_key(qname: &str) -> String {
    queue_key_prefix(qname) + "active"
}

pub fn scheduled_key(qname: &str) -> String {
    queue_key_prefix(qname) + "scheduled"
}

pub fn retry_key(qname: &str) -> String {
    queue_key_prefix(qname) + "retry"
}

pub fn archived_key(qname: &str) -> String {
    queue_key_prefix(qname) + "archived"
}

pub fn lease_key(qname: &str) -> String {
    queue_key_prefix(qname) + "lease"
}

pub fn completed_key(qname: &str) -> String {
    queue_key_prefix(qname) + "completed"
}

pub fn paused_key(qname: &str) -> String {
    queue_key_prefix(qname) + "paused"
}

pub fn processed_total_key(qname: &str) -> String {
    queue_key_prefix(qname) + "processed"
}

pub fn failed_total_key(qname: &str) -> String {
    queue_key_prefix(qname) + "failed"
}

/// `date` must be a UTC date formatted as `YYYY-MM-DD`.
pub fn processed_key(qname: &str, date: &str) -> String {
    format!("{}processed:{date}", queue_key_prefix(qname))
}

/// `date` must be a UTC date formatted as `YYYY-MM-DD`.
pub fn failed_key(qname: &str, date: &str) -> String {
    format!("{}failed:{date}", queue_key_prefix(qname))
}

pub fn server_info_key(hostname: &str, pid: u32, server_id: &str) -> String {
    format!("asynq:servers:{{{hostname}:{pid}:{server_id}}}")
}

pub fn workers_key(hostname: &str, pid: u32, server_id: &str) -> String {
    format!("asynq:workers:{{{hostname}:{pid}:{server_id}}}")
}

pub fn scheduler_entries_key(scheduler_id: &str) -> String {
    format!("asynq:schedulers:{{{scheduler_id}}}")
}

pub fn scheduler_history_key(entry_id: &str) -> String {
    format!("asynq:scheduler_history:{entry_id}")
}

/// Go distinguishes a nil payload (no checksum suffix) from an empty one;
/// `None` here corresponds to nil.
pub fn unique_key(qname: &str, task_type: &str, payload: Option<&[u8]>) -> String {
    let prefix = format!("{}unique:{task_type}:", queue_key_prefix(qname));
    match payload {
        None => prefix,
        Some(p) => prefix + &hex::encode(Md5::digest(p)),
    }
}

pub fn group_key_prefix(qname: &str) -> String {
    queue_key_prefix(qname) + "g:"
}

pub fn group_key(qname: &str, gkey: &str) -> String {
    group_key_prefix(qname) + gkey
}

pub fn aggregation_set_key(qname: &str, gname: &str, set_id: &str) -> String {
    format!("{}:{set_id}", group_key(qname, gname))
}

pub fn all_groups(qname: &str) -> String {
    queue_key_prefix(qname) + "groups"
}

pub fn all_aggregation_sets(qname: &str) -> String {
    queue_key_prefix(qname) + "aggregation_sets"
}

pub fn encode_message(msg: &TaskMessage) -> Vec<u8> {
    msg.encode_to_vec()
}

pub fn decode_message(data: &[u8]) -> Result<TaskMessage, prost::DecodeError> {
    TaskMessage::decode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_go() {
        assert_eq!(task_key("default", "abc"), "asynq:{default}:t:abc");
        assert_eq!(pending_key("critical"), "asynq:{critical}:pending");
        assert_eq!(
            processed_key("default", "2026-11-04"),
            "asynq:{default}:processed:2026-11-04"
        );
        assert_eq!(
            server_info_key("host", 42, "sid"),
            "asynq:servers:{host:42:sid}"
        );
        assert_eq!(aggregation_set_key("q", "g1", "s1"), "asynq:{q}:g:g1:s1");
    }

    #[test]
    fn unique_key_uses_md5_of_payload() {
        assert_eq!(
            unique_key("default", "email:send", Some(b"hello")),
            "asynq:{default}:unique:email:send:5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(
            unique_key("default", "email:send", None),
            "asynq:{default}:unique:email:send:"
        );
    }

    #[test]
    fn task_state_roundtrip() {
        for s in [
            TaskState::Active,
            TaskState::Pending,
            TaskState::Scheduled,
            TaskState::Retry,
            TaskState::Archived,
            TaskState::Completed,
            TaskState::Aggregating,
        ] {
            assert_eq!(TaskState::parse(s.as_str()), Some(s));
        }
    }

    #[test]
    fn message_roundtrip() {
        let msg = TaskMessage {
            r#type: "email:send".into(),
            payload: b"{\"to\":\"x\"}".to_vec(),
            id: "id-1".into(),
            queue: "default".into(),
            retry: 25,
            timeout: 1800,
            headers: [("k".to_string(), "v".to_string())].into(),
            ..Default::default()
        };
        assert_eq!(decode_message(&encode_message(&msg)).unwrap(), msg);
    }
}
