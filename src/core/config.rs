use std::collections::HashMap;

use crate::core::types::{NodeConfig, PeerAddr};

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
    let advertise_addr = vars.get("ADVERTISE_ADDR")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| http_addr.clone());

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

    Ok(NodeConfig {
        node_id,
        http_addr,
        advertise_addr,
        storage_path,
        peers,
        heartbeat_interval_ms,
        election_timeout_min_ms,
        election_timeout_max_ms,
    })
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
        let vars = make_vars(&[("NODE_ID", "1"), ("HTTP_ADDR", "0.0.0.0:8080")]);
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
            ("ELECTION_TIMEOUT_MIN_MS", "3000"),
            ("ELECTION_TIMEOUT_MAX_MS", "1500"),
        ]);
        assert!(parse_config(&vars).is_err());
    }
}
