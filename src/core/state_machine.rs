//! Pure state machine logic — no IO, no async, no side effects.
//!
//! This module owns the canonical in-memory state. The shell (sled adapter)
//! persists it for durability but all mutations and queries go through here.

use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use crate::core::types::{AppRequest, AppResponse};

/// Pure, serializable state of the binlog state machine.
/// Sled is the persistence layer; this struct is the source of truth at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogState {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub entries: Vec<String>,
}

impl Default for BinlogState {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            entries: Vec::new(),
        }
    }
}

impl BinlogState {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Entry application (pure mutations) ───────────────────────────

    /// Apply an Append request. Pushes the message and returns the response.
    pub fn apply_request(&mut self, log_index: u64, req: AppRequest) -> AppResponse {
        match req {
            AppRequest::Append { message } => {
                self.entries.push(message.clone());
                AppResponse {
                    index: log_index,
                    message,
                }
            }
        }
    }

    /// Record a blank entry (leader commit marker, no-op).
    pub fn apply_blank(log_index: u64) -> AppResponse {
        AppResponse {
            index: log_index,
            message: String::new(),
        }
    }

    /// Record a membership change.
    pub fn apply_membership(
        &mut self,
        log_id: LogId<u64>,
        membership: StoredMembership<u64, BasicNode>,
    ) -> AppResponse {
        self.last_membership = membership;
        AppResponse {
            index: log_id.index,
            message: String::new(),
        }
    }

    /// Advance the last-applied pointer.
    pub fn set_last_applied(&mut self, log_id: LogId<u64>) {
        self.last_applied_log = Some(log_id);
    }

    // ── Queries (pure reads) ─────────────────────────────────────────

    /// All committed entries, in order.
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    // ── Snapshot support (pure ser/de) ───────────────────────────────

    /// Serialize the full state to JSON bytes (for snapshot transport).
    pub fn to_snapshot_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Restore state from snapshot bytes.
    pub fn from_snapshot_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }

    /// Deterministic snapshot ID derived from last-applied log position.
    pub fn snapshot_id(&self) -> String {
        format!(
            "{}-{}",
            self.last_applied_log
                .map(|l| l.leader_id.term)
                .unwrap_or(0),
            self.last_applied_log.map(|l| l.index).unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state_is_empty() {
        let state = BinlogState::new();
        assert!(state.entries().is_empty());
        assert!(state.last_applied_log.is_none());
    }

    #[test]
    fn test_apply_append() {
        let mut state = BinlogState::new();
        let resp = state.apply_request(
            1,
            AppRequest::Append {
                message: "hello".into(),
            },
        );
        assert_eq!(resp.index, 1);
        assert_eq!(resp.message, "hello");
        assert_eq!(state.entries(), &["hello"]);
    }

    #[test]
    fn test_apply_multiple() {
        let mut state = BinlogState::new();
        state.apply_request(1, AppRequest::Append { message: "a".into() });
        state.apply_request(2, AppRequest::Append { message: "b".into() });
        state.apply_request(3, AppRequest::Append { message: "c".into() });
        assert_eq!(state.entries(), &["a", "b", "c"]);
    }

    #[test]
    fn test_blank_response() {
        let resp = BinlogState::apply_blank(42);
        assert_eq!(resp.index, 42);
        assert!(resp.message.is_empty());
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let mut state = BinlogState::new();
        state.apply_request(1, AppRequest::Append { message: "x".into() });
        state.apply_request(2, AppRequest::Append { message: "y".into() });

        let bytes = state.to_snapshot_bytes().unwrap();
        let restored = BinlogState::from_snapshot_bytes(&bytes).unwrap();

        assert_eq!(restored.entries(), state.entries());
        assert_eq!(restored.last_applied_log, state.last_applied_log);
    }

    #[test]
    fn test_snapshot_id_default() {
        let state = BinlogState::new();
        assert_eq!(state.snapshot_id(), "0-0");
    }

    #[test]
    fn test_set_last_applied() {
        let mut state = BinlogState::new();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 5);
        state.set_last_applied(log_id);
        assert_eq!(state.last_applied_log, Some(log_id));
        assert_eq!(state.snapshot_id(), "1-5");
    }

    #[test]
    fn test_membership_change() {
        let mut state = BinlogState::new();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 3);
        let membership = StoredMembership::default();
        let resp = state.apply_membership(log_id, membership);
        assert_eq!(resp.index, 3);
        assert!(resp.message.is_empty());
    }
}
