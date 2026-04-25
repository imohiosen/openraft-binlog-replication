use serde::{Deserialize, Serialize};

/// Parsed and validated node configuration — pure data, no IO.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: u64,
    pub http_addr: String,
    pub advertise_addr: String,
    pub peers: Vec<PeerAddr>,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
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
}

impl std::fmt::Display for AppRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppRequest::Append { message } => write!(f, "Append({})", message),
        }
    }
}

/// Application response returned after a log entry is applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppResponse {
    pub index: u64,
    pub message: String,
}
