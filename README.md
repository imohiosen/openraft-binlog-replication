# OpenRaft Binlog Replication

A 3-node replicated log with a **SQL query engine** built on [OpenRaft](https://github.com/databendlabs/openraft) in Rust. Supports CREATE TABLE, INSERT, SELECT (with JOINs, aggregates, GROUP BY), UPDATE, DELETE, TRUNCATE — all replicated through Raft consensus and persisted to sled. Demonstrated via Docker Compose with active-passive failover.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Client (curl)                       │
│         POST /api/sql  {"sql": "SELECT ..."}            │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP :8080
┌────────────────────────▼────────────────────────────────┐
│                   Leader Node                           │
│  ┌──────────┐   ┌───────────┐   ┌───────────────────┐  │
│  │ sqlparser │──▶│ SqlCommand │──▶│ raft.client_write │  │
│  │ (parse)   │   │ (AST)     │   │ (replicate)       │  │
│  └──────────┘   └───────────┘   └─────────┬─────────┘  │
│                                            │            │
│  SELECT path:                     gRPC :9090│ (Vote,    │
│  raft.ensure_linearizable()                 │ Append,   │
│  → query local sled state                   │ Snapshot) │
└─────────────────────────────────────────────┼───────────┘
                         ┌────────────────────┼──────┐
                         ▼                    ▼      ▼
                   ┌──────────┐        ┌──────────┐ ┌──────────┐
                   │ Follower │        │ Follower │ │   sled   │
                   │  Node 2  │        │  Node 3  │ │ (durable │
                   │  :8080   │        │  :8080   │ │ storage) │
                   │  :9090   │        │  :9090   │ └──────────┘
                   └──────────┘        └──────────┘
```

**Data flow for writes:** Client → HTTP `POST /api/sql` → leader parses SQL with `sqlparser-rs` into a deterministic `SqlCommand` AST → replicated through Raft to all nodes via gRPC → each node applies the AST to its local sled-backed state machine.

**Data flow for reads:** Client → HTTP `POST /api/sql` with `SELECT` → leader calls `raft.ensure_linearizable()` to confirm it still holds leadership → queries the local in-memory SQL state → returns rows. Linearizable reads are leader-only by design.

### Functional Core / Imperative Shell

Pure logic separated from IO. The core is synchronous, zero-dependency, and unit-testable. The shell owns all side effects.

```
src/
├── lib.rs                           # declare_raft_types! TypeConfig
├── main.rs                          # Startup: gRPC server (tokio) + HTTP server (actix thread)
│
├── core/                            # Pure — no IO, no async, no side effects
│   ├── types.rs                     # AppRequest (Append | Sql), AppResponse, NodeConfig
│   ├── config.rs                    # parse_config(HashMap) → Result<NodeConfig>
│   ├── state_machine.rs             # BinlogState: pure apply logic + snapshot ser/de
│   └── sql/
│       ├── types.rs                 # Value, Column, TableSchema, Row, SqlCommand, Expr, SelectPlan
│       ├── expr.rs                  # eval(): 3-valued NULL logic, numeric promotion, aggregates
│       ├── engine.rs                # SqlState.execute(): CREATE/DROP/INSERT/UPDATE/DELETE/TRUNCATE
│       ├── exec.rs                  # SqlState.query_select(): FROM→JOIN→WHERE→GROUP→HAVING→ORDER→LIMIT
│       └── error.rs                 # SqlError enum
│
└── shell/                           # All side effects: HTTP, gRPC, sled, async
    ├── app.rs                       # App { raft, state_machine, config }
    ├── server.rs                    # actix-web route registration
    ├── grpc.rs                      # tonic RaftService (Vote, AppendEntries, InstallSnapshot)
    ├── network.rs                   # RaftNetworkFactory → tonic gRPC client
    ├── sql/
    │   └── parser.rs                # sqlparser AST → SqlCommand / SelectPlan translation
    ├── handlers/
    │   ├── api.rs                   # POST /api/sql, POST /api/append, GET /api/log, GET /api/leader
    │   └── management.rs            # /cluster/init, /add-learner, /change-membership, /metrics
    └── store/
        ├── log_store.rs             # RaftLogStorage (sled: raft-log/)
        └── state_machine.rs         # RaftStateMachine (sled: state-machine/ + sql_state)
```

### Key Technology Choices

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Consensus | `openraft 0.9.24` (storage-v2) | Raft leader election, log replication, snapshots |
| Inter-node RPC | `tonic 0.12` / `prost 0.13` (gRPC) | Vote, AppendEntries, InstallSnapshot — JSON-in-protobuf envelope |
| Client API | `actix-web 4` (HTTP) | SQL endpoint, cluster management, binlog append |
| SQL parsing | `sqlparser 0.53` | Raw SQL text → deterministic AST (parsed on leader, AST replicated) |
| Storage | `sled 0.34` | Durable Raft log, state machine, SQL tables/indexes/schemas |
| Config | `dotenvy 0.15` | All config via environment variables, no CLI flags |

### SQL Engine Design

The SQL engine is **replicated at the AST level** — the leader parses SQL text into a `SqlCommand` enum using `sqlparser-rs`, then replicates that deterministic command through Raft. Followers never parse SQL; they only apply the pre-parsed AST. This eliminates parser-version skew across nodes.

**State model:** All SQL state (schemas, tables, indexes, sequences) lives in a single `SqlState` struct that is serialized to sled as JSON. This struct is also included in Raft snapshots for consistency during node recovery.

**Supported SQL:**

| Category | Statements |
|----------|-----------|
| DDL | `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX` |
| DML | `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` |
| Query | `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, `HAVING` |
| Joins | `INNER JOIN` (nested-loop) |
| Aggregates | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` |
| Types | `INT`, `BIGINT`, `TEXT`, `BOOL`, `REAL` |
| Constraints | `PRIMARY KEY` (uniqueness enforced), `NOT NULL` |

**Limitations:**
- No transactions (each statement = one Raft entry)
- No `LEFT`/`RIGHT`/`OUTER` joins — only `INNER JOIN`
- Index push-down for equality on a single indexed column only; otherwise full scan
- Linearizable reads are leader-only (`ensure_linearizable()`)
- No prepared statements or parameterized queries

## Quick Start

```bash
docker compose build
docker compose up -d

# Initialize cluster
curl -X POST http://localhost:18080/cluster/init
curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' -d '{"node_id":2,"addr":"node2:9090"}'
curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' -d '{"node_id":3,"addr":"node3:9090"}'
curl -X POST http://localhost:18080/cluster/change-membership \
  -H 'Content-Type: application/json' -d '[1,2,3]'

# Create a table and insert data
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)"}'

curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (1, '\''alice'\'', 30), (2, '\''bob'\'', 25)"}'

curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users"}'
# → {"results":[{"columns":["id","name","age"],"rows":[[1,"alice",30],[2,"bob",25]]}]}
```

## Manual Walkthrough

### 1. Start the cluster

```bash
docker compose up -d
```

Three nodes start — HTTP on ports `18080`, `28080`, `38080`; gRPC on `19090`, `29090`, `39090`.

### 2. Initialize and form the cluster

```bash
# Initialize node1 as a single-node cluster
curl -X POST http://localhost:18080/cluster/init

# Add node2 and node3 as learners (gRPC addresses)
curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id":2,"addr":"node2:9090"}'

curl -X POST http://localhost:18080/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{"node_id":3,"addr":"node3:9090"}'

# Promote all to voters
curl -X POST http://localhost:18080/cluster/change-membership \
  -H 'Content-Type: application/json' \
  -d '[1,2,3]'
```

### 3. SQL operations

```bash
# Create a table
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)"}'

# Create an index
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "CREATE INDEX idx_users_age ON users(age)"}'

# Insert rows
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (1, '\''alice'\'', 30), (2, '\''bob'\'', 25), (3, '\''carol'\'', 35)"}'

# Query with WHERE, ORDER BY, LIMIT
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC LIMIT 2"}'
# → {"results":[{"columns":["name","age"],"rows":[["carol",35],["alice",30]]}]}

# Aggregates
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT COUNT(*), AVG(age) FROM users"}'
# → {"results":[{"columns":["COUNT(*)","AVG(age)"],"rows":[[3,30.0]]}]}

# JOIN
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id"}'

# UPDATE
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "UPDATE users SET age = age + 1 WHERE id = 1"}'

# DELETE
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "DELETE FROM users WHERE id = 3"}'

# GROUP BY
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT age, COUNT(*) FROM users GROUP BY age"}'

# TRUNCATE
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "TRUNCATE TABLE users"}'
```

### 4. Legacy binlog append (still supported)

```bash
curl -X POST http://localhost:18080/api/append \
  -H 'Content-Type: application/json' \
  -d '{"message":"event-1"}'

curl http://localhost:18080/api/log
```

### 5. Trigger failover

```bash
# Kill the leader
docker compose kill node1

# Check who became leader (wait ~5s for election)
curl http://localhost:28080/api/leader
curl http://localhost:38080/api/leader

# Write SQL to the new leader
curl -X POST http://localhost:38080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (4, '\''dave'\'', 28)"}'
```

### 6. Rejoin the old leader

```bash
docker compose start node1
sleep 5

# Verify it caught up — all SQL state replicated
curl -X POST http://localhost:18080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users"}'
```

## Validated Test Results

Full end-to-end test run on April 25, 2026:

### SQL E2E Tests (all on leader)

| # | SQL | Result |
|---|-----|--------|
| 1 | `CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)` | `{"status":"created"}` |
| 2 | `CREATE INDEX idx_users_age ON users(age)` | `{"status":"created"}` |
| 3 | `INSERT INTO users VALUES (1,'alice',30), (2,'bob',25), (3,'carol',35)` | `{"rows_affected":3}` |
| 4 | `SELECT * FROM users` | 3 rows: alice/30, bob/25, carol/35 |
| 5 | `SELECT name, age ... WHERE age > 25 ORDER BY age DESC LIMIT 2` | carol/35, eve/32 |
| 6 | `SELECT COUNT(*), AVG(age) FROM users` | 5, 30.0 |
| 7 | `UPDATE users SET age = age + 1 WHERE id = 1` | `{"rows_affected":1}` — alice now 31 |
| 8 | `DELETE FROM users WHERE id = 5` | `{"rows_affected":1}` |
| 9 | `SELECT age, COUNT(*) FROM users GROUP BY age` | 4 distinct groups |
| 10 | `CREATE TABLE orders` + `INSERT` + `INNER JOIN` | alice/99.99, alice/25.0, bob/49.5 |
| 11 | `TRUNCATE TABLE users` | `{"status":"truncated"}`, then `SELECT *` returns empty |

### Durability Test

| Step | Action | Result |
|------|--------|--------|
| 1 | Insert users + orders, create index | All committed |
| 2 | `docker compose restart` (all 3 nodes) | All restarted |
| 3 | `SELECT * FROM users` after restart | All 3 rows intact |
| 4 | `SELECT * FROM orders` after restart | All 3 rows intact |
| 5 | `JOIN` query after restart | Identical results — schemas, indexes, sequences all survived |

### Failover Test

| Step | Action | Result |
|------|--------|--------|
| 1 | `docker compose kill node1` (leader) | Container killed |
| 2 | Check leaders after ~5s | **Node3 elected leader** (term 2), node2 is Follower |
| 3 | `INSERT INTO users VALUES (4,'dave',28)` on node3 | `{"rows_affected":1}` |
| 4 | `SELECT * FROM users` on node3 | 4 rows — all original data + dave |

## Configuration

All config via environment variables (`.env` or Docker Compose `environment:`):

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | required | Unique node ID (1, 2, 3, …) |
| `HTTP_ADDR` | required | HTTP bind address for client API (`0.0.0.0:8080`) |
| `GRPC_ADDR` | `0.0.0.0:9090` | gRPC bind address for Raft inter-node RPCs |
| `ADVERTISE_ADDR` | same as `GRPC_ADDR` | gRPC address peers use to reach this node |
| `STORAGE_PATH` | `/data/node-{NODE_ID}` | Sled database directory for durable log + state |
| `PEER_ADDRS` | `""` | Comma-separated peers: `2=node2:9090,3=node3:9090` |
| `HEARTBEAT_INTERVAL_MS` | `500` | Leader heartbeat interval |
| `ELECTION_TIMEOUT_MIN_MS` | `1500` | Min election timeout |
| `ELECTION_TIMEOUT_MAX_MS` | `3000` | Max election timeout |
| `RUST_LOG` | `info` | Log level |

## API Reference

### SQL (primary API)

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/api/sql` | `{"sql": "<statement>"}` | Execute one or more SQL statements. Writes go through Raft; `SELECT` uses linearizable reads (leader-only). |

**Response format:**
```json
{
  "results": [
    {"columns": ["id","name","age"], "rows": [[1,"alice",30]]},
    {"rows_affected": 3},
    {"status": "created"},
    {"error": "table 'x' not found"}
  ]
}
```

### Legacy Binlog (backward-compatible)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/append` | Append `{"message":"..."}` to the replicated binlog |
| `GET` | `/api/log` | Read all committed binlog entries from this node |
| `GET` | `/api/leader` | Current leader ID, node ID, and Raft state |

### Cluster Management (HTTP)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/cluster/init` | Initialize single-node cluster |
| `POST` | `/cluster/add-learner` | Add `{"node_id":N,"addr":"host:9090"}` as learner (gRPC address) |
| `POST` | `/cluster/change-membership` | Promote learners: `[1,2,3]` |
| `GET` | `/cluster/metrics` | Full Raft metrics (term, leader, log state) |

### Raft Internal (gRPC, node-to-node, port 9090)

| Service | RPC | Description |
|---------|-----|-------------|
| `RaftService` | `Vote` | RequestVote — leader election |
| `RaftService` | `AppendEntries` | Log replication |
| `RaftService` | `InstallSnapshot` | Snapshot transfer to lagging nodes |

Uses a JSON-in-protobuf envelope: `message RaftRequest { bytes data; }` carries serde_json-encoded openraft types inside a protobuf wrapper to avoid hand-translating every openraft type into proto messages.

## Storage

Each node persists to [sled](https://github.com/spacejam/sled) on disk at `STORAGE_PATH`:

| Sled Database | Contents |
|--------------|----------|
| `{STORAGE_PATH}/raft-log/` | Raft log entries, vote, last purged index |
| `{STORAGE_PATH}/state-machine/` | Binlog entries, membership, snapshots, SQL state |

The SQL state (schemas, tables, indexes, sequences) is persisted as a single JSON blob in the `sql_state` sled tree, updated after every write operation. Snapshots include the full SQL state for consistency during node recovery.

Data survives container restarts. To wipe all state:

```bash
docker compose down -v
```

## Cleanup

```bash
# Stop containers (data preserved in volumes)
docker compose down

# Stop and delete all data
docker compose down -v
```
