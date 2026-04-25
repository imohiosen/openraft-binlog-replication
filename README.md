# OpenRaft Binlog Replication

A 3-node replicated append-only log (binlog) built with [OpenRaft](https://github.com/databendlabs/openraft) in Rust, demonstrating active-passive failover via Docker Compose.

## Architecture

**Functional Core / Imperative Shell** — pure logic separated from IO.

```
src/
├── core/                        # Pure, no IO, no async
│   ├── types.rs                 # AppRequest, AppResponse, NodeConfig
│   └── config.rs                # parse_config(HashMap) → Result<NodeConfig>
└── shell/                       # All side effects
    ├── app.rs                   # App state (Raft instance + stores)
    ├── server.rs                # actix-web route wiring
    ├── network.rs               # RaftNetworkFactory (reqwest HTTP)
    ├── handlers/
    │   ├── api.rs               # POST /api/append, GET /api/log, GET /api/leader
    │   ├── raft.rs              # /raft/vote, /raft/append, /raft/snapshot
    │   └── management.rs        # /cluster/init, /add-learner, /change-membership
    └── store/
        ├── log_store.rs         # RaftLogStorage (in-memory)
        └── state_machine.rs     # RaftStateMachine (append-only Vec<String>)
```

**Key choices:**
- `openraft 0.9.24` with `storage-v2` feature
- `actix-web 4` for HTTP, `reqwest` for inter-node RPCs
- `dotenvy` for env-based config (no CLI flags)
- In-memory storage (demo scope)

## Quick Start

```bash
docker compose build
docker compose up -d
./demo.sh
```

## Manual Walkthrough

### 1. Start the cluster

```bash
docker compose up -d
```

Three nodes start on ports `18080`, `28080`, `38080`.

### 2. Initialize and form the cluster

```bash
# Initialize node1 as a single-node cluster
curl -X POST http://localhost:18080/cluster/init

# Add node2 and node3 as learners
curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id":2,"addr":"node2:8080"}'

curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id":3,"addr":"node3:8080"}'

# Promote all to voters
curl -X POST http://localhost:18080/cluster/change-membership \
  -H 'Content-Type: application/json' \
  -d '[1,2,3]'
```

### 3. Write entries

```bash
curl -X POST http://localhost:18080/api/append \
  -H 'Content-Type: application/json' \
  -d '{"message":"event-1"}'
```

### 4. Read from any node

```bash
# Leader
curl http://localhost:18080/api/log

# Followers
curl http://localhost:28080/api/log
curl http://localhost:38080/api/log
```

### 5. Trigger failover

```bash
# Kill the leader
docker compose stop node1

# Check who became leader
curl http://localhost:28080/api/leader
curl http://localhost:38080/api/leader

# Write to the new leader
curl -X POST http://localhost:28080/api/append \
  -H 'Content-Type: application/json' \
  -d '{"message":"event-after-failover"}'
```

### 6. Rejoin the old leader

```bash
docker compose start node1

# Verify it caught up
curl http://localhost:18080/api/log
```

## Validated Test Results

Full end-to-end test run on April 25, 2026:

| Step | Action | Result |
|------|--------|--------|
| 1 | Init cluster on node1 | Node1 became **Leader** (term 1) |
| 2–3 | Add node2 & node3 as learners, promote to voters | Membership: `[1, 2, 3]` |
| 4 | Write `event-1` through `event-5` to leader | All 5 entries committed (log indices 6–10) |
| 5–7 | Read log from all 3 nodes | All show identical `["event-1", "event-2", "event-3", "event-4", "event-5"]` |
| 8 | **Kill leader (node1)** | Container stopped |
| 9 | Check new leader | **Node2 elected leader** (term 2), node3 is Follower |
| 10 | Write `event-6` through `event-8` to new leader (node2) | All 3 committed (log indices 12–14) |
| 11–12 | Read log from surviving nodes | Both show all 8 entries |
| 13 | **Restart node1** | Container restarted |
| 14 | Read log from rejoined node1 | Shows **all 8 entries** — caught up automatically. Node1 now Follower under node2 |

### Raw output (condensed)

```
=== Init ===
{"Ok":null}                                          # node1 initialized
{"leader_id":1,"current_node":1,"state":"Leader"}    # node1 is leader

=== Add learners + promote ===
membership: {"configs":[[1,2,3]], "nodes":{...}}     # 3-node quorum

=== Write 5 entries ===
{"Ok":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":6}, "data":{"message":"event-1"}}}
{"Ok":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":7}, "data":{"message":"event-2"}}}
...
{"Ok":{"log_id":{"leader_id":{"term":1,"node_id":1},"index":10},"data":{"message":"event-5"}}}

=== Read from all nodes ===
node1: {"entries":["event-1","event-2","event-3","event-4","event-5"]}
node2: {"entries":["event-1","event-2","event-3","event-4","event-5"]}
node3: {"entries":["event-1","event-2","event-3","event-4","event-5"]}

=== Kill node1, check new leader ===
node2: {"leader_id":2,"current_node":2,"state":"Leader"}
node3: {"leader_id":2,"current_node":3,"state":"Follower"}

=== Write to new leader (node2) ===
{"Ok":{"log_id":{"leader_id":{"term":2,"node_id":2},"index":12},"data":{"message":"event-6"}}}
{"Ok":{"log_id":{"leader_id":{"term":2,"node_id":2},"index":13},"data":{"message":"event-7"}}}
{"Ok":{"log_id":{"leader_id":{"term":2,"node_id":2},"index":14},"data":{"message":"event-8"}}}

=== Read from surviving nodes ===
node2: {"entries":["event-1","event-2","event-3","event-4","event-5","event-6","event-7","event-8"]}
node3: {"entries":["event-1","event-2","event-3","event-4","event-5","event-6","event-7","event-8"]}

=== Restart node1, verify catch-up ===
node1: {"entries":["event-1","event-2","event-3","event-4","event-5","event-6","event-7","event-8"]}
node1: {"leader_id":2,"current_node":1,"state":"Follower"}
```

## Configuration

All config via environment variables (`.env` or Docker Compose `environment:`):

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | required | Unique node ID (1, 2, 3, …) |
| `HTTP_ADDR` | required | Bind address (`0.0.0.0:8080`) |
| `ADVERTISE_ADDR` | same as `HTTP_ADDR` | Address peers use to reach this node |
| `PEER_ADDRS` | `""` | Comma-separated peers: `2=node2:8080,3=node3:8080` |
| `HEARTBEAT_INTERVAL_MS` | `500` | Leader heartbeat interval |
| `ELECTION_TIMEOUT_MIN_MS` | `1500` | Min election timeout |
| `ELECTION_TIMEOUT_MAX_MS` | `3000` | Max election timeout |
| `RUST_LOG` | `info` | Log level |

## API Reference

### Application

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/append` | Append `{"message":"..."}` to the replicated log |
| `GET` | `/api/log` | Read all committed entries from this node |
| `GET` | `/api/leader` | Current leader ID, node ID, and Raft state |

### Cluster Management

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/cluster/init` | Initialize single-node cluster |
| `POST` | `/cluster/add-learner` | Add `{"node_id":N,"addr":"host:port"}` as learner |
| `POST` | `/cluster/change-membership` | Promote learners: `[1,2,3]` |
| `GET` | `/cluster/metrics` | Full Raft metrics (term, leader, log state) |

### Raft Internal (node-to-node)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/raft/vote` | RequestVote RPC |
| `POST` | `/raft/append` | AppendEntries RPC |
| `POST` | `/raft/snapshot` | InstallSnapshot RPC |

## Cleanup

```bash
docker compose down
```
