use serde::{Deserialize, Serialize};

use crate::core::sql::types::{SqlCommand, SqlResult};

/// Snapshot compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCompression {
    None,
    Lz4,
}

impl std::fmt::Display for SnapshotCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
        }
    }
}

/// Parsed and validated node configuration — pure data, no IO.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: u64,
    pub http_addr: String,
    pub grpc_addr: String,
    pub advertise_addr: String,
    pub storage_path: String,
    pub peers: Vec<PeerAddr>,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub snapshot_compression: SnapshotCompression,
    pub snapshot_logs_since_last: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub purge_batch_size: u64,
    pub snapshot_max_chunk_size: u64,
    pub max_payload_entries: u64,
    pub replication_lag_threshold: u64,
    pub log_compression: SnapshotCompression,
}

#[derive(Debug, Clone)]
pub struct PeerAddr {
    pub node_id: u64,
    pub addr: String,
}

/// Application request — the payload replicated through Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppRequest {
    Append { message: String },
    Sql(SqlCommand),
}

impl std::fmt::Display for AppRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppRequest::Append { message } => write!(f, "Append({})", message),
            AppRequest::Sql(cmd) => write!(f, "Sql({})", cmd),
        }
    }
}

/// Application response returned after a log entry is applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppResponse {
    Append { index: u64, message: String },
    Sql { index: u64, result: SqlResult },
}
