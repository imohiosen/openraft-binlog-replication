//! Pure state machine logic — no IO, no async, no side effects.
//!
//! This module owns the canonical in-memory state. The shell (sled adapter)
//! persists it for durability but all mutations and queries go through here.

use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use crate::core::sql::types::{SqlCommand, SqlResult, SqlState};
use crate::core::types::{AppRequest, AppResponse};

/// Pure, serializable state of the binlog state machine.
/// Sled is the persistence layer; this struct is the source of truth at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogState {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub entries: Vec<String>,
    #[serde(default)]
    pub sql: SqlState,
}

impl Default for BinlogState {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            entries: Vec::new(),
            sql: SqlState::default(),
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
                AppResponse::Append {
                    index: log_index,
                    message,
                }
            }
            AppRequest::Sql(cmd) => self.apply_sql(log_index, cmd),
        }
    }

    /// Apply a SQL command through the state machine.
    pub fn apply_sql(&mut self, log_index: u64, cmd: SqlCommand) -> AppResponse {
        let result = match self.sql.execute(cmd) {
            Ok(r) => r,
            Err(e) => SqlResult::Error(e.to_string()),
        };
        AppResponse::Sql {
            index: log_index,
            result,
        }
    }

    /// Record a blank entry (leader commit marker, no-op).
    pub fn apply_blank(log_index: u64) -> AppResponse {
        AppResponse::Append {
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
        AppResponse::Append {
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

    /// Magic header for LZ4-compressed snapshots ("SLZ4").
    pub const LZ4_MAGIC: &'static [u8; 4] = b"SLZ4";

    /// Serialize with optional LZ4 compression.
    /// Compressed format: [SLZ4][4-byte LE uncompressed len][lz4 data]
    pub fn to_snapshot_bytes_compressed(
        &self,
        compression: crate::core::types::SnapshotCompression,
    ) -> Result<Vec<u8>, String> {
        let json = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        match compression {
            crate::core::types::SnapshotCompression::None => Ok(json),
            crate::core::types::SnapshotCompression::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(&json);
                let mut out = Vec::with_capacity(4 + compressed.len());
                out.extend_from_slice(Self::LZ4_MAGIC);
                out.extend_from_slice(&compressed);
                Ok(out)
            }
        }
    }

    /// Deserialize, auto-detecting LZ4 compression from magic header.
    pub fn from_snapshot_bytes_auto(data: &[u8]) -> Result<Self, String> {
        if data.len() >= 4 && &data[..4] == Self::LZ4_MAGIC {
            let decompressed = lz4_flex::decompress_size_prepended(&data[4..])
                .map_err(|e| format!("lz4 decompress error: {}", e))?;
            serde_json::from_slice(&decompressed).map_err(|e| e.to_string())
        } else {
            serde_json::from_slice(data).map_err(|e| e.to_string())
        }
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
        match resp {
            AppResponse::Append { index, message } => {
                assert_eq!(index, 1);
                assert_eq!(message, "hello");
            }
            _ => panic!("expected Append response"),
        }
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
        match resp {
            AppResponse::Append { index, message } => {
                assert_eq!(index, 42);
                assert!(message.is_empty());
            }
            _ => panic!("expected Append response"),
        }
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
