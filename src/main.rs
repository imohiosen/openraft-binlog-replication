use std::collections::HashMap;
use std::sync::Arc;

use actix_web::web::Data;
use openraft::Config;
use tracing_subscriber::EnvFilter;

use openraft_binlog_replication::core::config::parse_config;
use openraft_binlog_replication::shell::app::App;
use openraft_binlog_replication::shell::network::Network;
use openraft_binlog_replication::shell::server;
use openraft_binlog_replication::shell::store::log_store::LogStore;
use openraft_binlog_replication::shell::store::state_machine::StateMachineStore;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // ── Imperative shell: load .env ──
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ── Functional core: parse config (pure) ──
    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let node_config = parse_config(&env_vars).unwrap_or_else(|e| {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    });

    tracing::info!(
        node_id = node_config.node_id,
        addr = %node_config.http_addr,
        peers = ?node_config.peers,
        "Starting binlog replication node"
    );

    // ── Imperative shell: construct Raft + server ──
    let raft_config = Config {
        heartbeat_interval: node_config.heartbeat_interval_ms,
        election_timeout_min: node_config.election_timeout_min_ms,
        election_timeout_max: node_config.election_timeout_max_ms,
        ..Default::default()
    };
    let raft_config = Arc::new(raft_config.validate().unwrap());

    let storage_path = &node_config.storage_path;
    tracing::info!(path = %storage_path, "Opening sled storage");
    std::fs::create_dir_all(storage_path).expect("create storage directory");

    let log_store = LogStore::new(storage_path).expect("open log store");
    let state_machine = StateMachineStore::new(storage_path).expect("open state machine store");
    let network = Network::default();

    let raft = openraft::Raft::new(
        node_config.node_id,
        raft_config.clone(),
        network,
        log_store,
        state_machine.clone(),
    )
    .await
    .unwrap();

    let app = Data::new(App {
        id: node_config.node_id,
        addr: node_config.http_addr.clone(),
        advertise_addr: node_config.advertise_addr.clone(),
        raft,
        state_machine,
        config: raft_config,
    });

    tracing::info!(addr = %node_config.http_addr, "HTTP server starting");
    server::run(app, &node_config.http_addr).await
}
