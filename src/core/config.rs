use std::collections::HashMap;

use crate::core::types::{NodeConfig, PeerAddr, SnapshotCompression};

#[derive(Debug)]
pub enum ConfigError {
    Missing(String),
    Invalid(String, String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Missing(key) => write!(f, "missing required env var: {}", key),
            ConfigError::Invalid(key, reason) => write!(f, "invalid {}: {}", key, reason),
        }
    }
}

/// Pure function: parse a HashMap of env vars into a validated NodeConfig.
/// No IO — takes raw strings, returns config or error.
pub fn parse_config(vars: &HashMap<String, String>) -> Result<NodeConfig, ConfigError> {
    let node_id = get_required(vars, "NODE_ID")?
        .parse::<u64>()
        .map_err(|e| ConfigError::Invalid("NODE_ID".into(), e.to_string()))?;

    let http_addr = get_required(vars, "HTTP_ADDR")?;
    let grpc_addr = vars.get("GRPC_ADDR")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| "0.0.0.0:9090".to_string());
    let advertise_addr = get_required(vars, "ADVERTISE_ADDR")?;

    let storage_path = vars.get("STORAGE_PATH")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| format!("/data/node-{}", node_id));

    let peers = parse_peers(vars.get("PEER_ADDRS").map(|s| s.as_str()).unwrap_or(""))?;

    let heartbeat_interval_ms = get_or_default(vars, "HEARTBEAT_INTERVAL_MS", 500)?;
    let election_timeout_min_ms = get_or_default(vars, "ELECTION_TIMEOUT_MIN_MS", 1500)?;
    let election_timeout_max_ms = get_or_default(vars, "ELECTION_TIMEOUT_MAX_MS", 3000)?;

    if election_timeout_min_ms >= election_timeout_max_ms {
        return Err(ConfigError::Invalid(
            "ELECTION_TIMEOUT".into(),
            "min must be less than max".into(),
        ));
    }

    let snapshot_compression = parse_compression(vars, "SNAPSHOT_COMPRESSION", SnapshotCompression::Lz4)?;
    let log_compression = parse_compression(vars, "LOG_COMPRESSION", SnapshotCompression::Lz4)?;

    let snapshot_logs_since_last = get_or_default(vars, "SNAPSHOT_LOGS_SINCE_LAST", 5000)?;
    let max_in_snapshot_log_to_keep = get_or_default(vars, "MAX_IN_SNAPSHOT_LOG_TO_KEEP", 500)?;
    let purge_batch_size = get_or_default(vars, "PURGE_BATCH_SIZE", 256)?;
    let snapshot_max_chunk_size = get_or_default(vars, "SNAPSHOT_MAX_CHUNK_SIZE", 4_194_304)?; // 4 MiB
    let max_payload_entries = get_or_default(vars, "MAX_PAYLOAD_ENTRIES", 300)?;
    let replication_lag_threshold = get_or_default(vars, "REPLICATION_LAG_THRESHOLD", 10000)?;

    Ok(NodeConfig {
        node_id,
        http_addr,
        grpc_addr,
        advertise_addr,
        storage_path,
        peers,
        heartbeat_interval_ms,
        election_timeout_min_ms,
        election_timeout_max_ms,
        snapshot_compression,
        snapshot_logs_since_last,
        max_in_snapshot_log_to_keep,
        purge_batch_size,
        snapshot_max_chunk_size,
        max_payload_entries,
        replication_lag_threshold,
        log_compression,
    })
}

fn parse_compression(
    vars: &HashMap<String, String>,
    key: &str,
    default: SnapshotCompression,
) -> Result<SnapshotCompression, ConfigError> {
    match vars.get(key).map(|s| s.as_str()) {
        Some("none") | Some("off") | Some("false") => Ok(SnapshotCompression::None),
        Some("lz4") => Ok(SnapshotCompression::Lz4),
        None => Ok(default),
        Some(other) => Err(ConfigError::Invalid(
            key.into(),
            format!("expected 'none' or 'lz4', got '{}'", other),
        )),
    }
}

fn get_required(vars: &HashMap<String, String>, key: &str) -> Result<String, ConfigError> {
    vars.get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| ConfigError::Missing(key.into()))
}

fn get_or_default(vars: &HashMap<String, String>, key: &str, default: u64) -> Result<u64, ConfigError> {
    match vars.get(key) {
        Some(v) if !v.is_empty() => v
            .parse::<u64>()
            .map_err(|e| ConfigError::Invalid(key.into(), e.to_string())),
        _ => Ok(default),
    }
}

/// Parse "2=node2:8080,3=node3:8080" into Vec<PeerAddr>.
fn parse_peers(raw: &str) -> Result<Vec<PeerAddr>, ConfigError> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|entry| {
            let entry = entry.trim();
            let (id_str, addr) = entry
                .split_once('=')
                .ok_or_else(|| ConfigError::Invalid("PEER_ADDRS".into(), format!("expected id=host:port, got '{}'", entry)))?;
            let node_id = id_str
                .parse::<u64>()
                .map_err(|e| ConfigError::Invalid("PEER_ADDRS".into(), format!("bad node id '{}': {}", id_str, e)))?;
            Ok(PeerAddr {
                node_id,
                addr: addr.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_parse_minimal_config() {
        let vars = make_vars(&[("NODE_ID", "1"), ("HTTP_ADDR", "0.0.0.0:8080"), ("ADVERTISE_ADDR", "node1:9090")]);
        let cfg = parse_config(&vars).unwrap();
        assert_eq!(cfg.node_id, 1);
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
        assert!(cfg.peers.is_empty());
        assert_eq!(cfg.heartbeat_interval_ms, 500);
    }

    #[test]
    fn test_parse_peers() {
        let vars = make_vars(&[
            ("NODE_ID", "1"),
            ("HTTP_ADDR", "0.0.0.0:8080"),
            ("ADVERTISE_ADDR", "node1:9090"),
            ("PEER_ADDRS", "2=node2:8080,3=node3:8080"),
        ]);
        let cfg = parse_config(&vars).unwrap();
        assert_eq!(cfg.peers.len(), 2);
        assert_eq!(cfg.peers[0].node_id, 2);
        assert_eq!(cfg.peers[0].addr, "node2:8080");
    }

    #[test]
    fn test_missing_node_id() {
        let vars = make_vars(&[("HTTP_ADDR", "0.0.0.0:8080")]);
        assert!(parse_config(&vars).is_err());
    }

    #[test]
    fn test_invalid_timeout_range() {
        let vars = make_vars(&[
            ("NODE_ID", "1"),
            ("HTTP_ADDR", "0.0.0.0:8080"),
            ("ADVERTISE_ADDR", "node1:9090"),
            ("ELECTION_TIMEOUT_MIN_MS", "3000"),
            ("ELECTION_TIMEOUT_MAX_MS", "1500"),
        ]);
        assert!(parse_config(&vars).is_err());
    }
}
