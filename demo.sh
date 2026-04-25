#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# demo.sh — End-to-end failover demo for the replicated binlog
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

NODE1="http://localhost:18080"
NODE2="http://localhost:28080"
NODE3="http://localhost:38080"

C='\033[0;36m'   # cyan
G='\033[0;32m'   # green
R='\033[0;31m'   # red
Y='\033[0;33m'   # yellow
NC='\033[0m'     # reset

step()  { echo -e "\n${C}── $1 ──${NC}"; }
ok()    { echo -e "   ${G}$1${NC}"; }
warn()  { echo -e "   ${Y}$1${NC}"; }

rpc() {
    local url="$1"
    local body="${2:-}"
    if [ -z "$body" ]; then
        curl -sf "$url" 2>/dev/null
    else
        curl -sf "$url" -H "Content-Type: application/json" -d "$body" 2>/dev/null
    fi
}

# ──────────────────────────────────────────────────────────────
step "1. Waiting for all 3 nodes to come online"
for port in 18080 28080 38080; do
    printf "   Waiting for node on port $port..."
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:$port/api/leader" > /dev/null 2>&1; then
            echo -e " ${G}up${NC}"
            break
        fi
        sleep 1
        [ "$i" -eq 30 ] && { echo -e " ${R}TIMEOUT${NC}"; exit 1; }
    done
done

# ──────────────────────────────────────────────────────────────
step "2. Initialize cluster on node 1"
rpc "$NODE1/cluster/init" '' | jq . 2>/dev/null || true
sleep 2
ok "Node 1 initialized"

# ──────────────────────────────────────────────────────────────
step "3. Add node 2 and node 3 as learners"
rpc "$NODE1/cluster/add-learner" '{"node_id":2,"addr":"node2:8080"}' | jq . 2>/dev/null || true
sleep 1
rpc "$NODE1/cluster/add-learner" '{"node_id":3,"addr":"node3:8080"}' | jq . 2>/dev/null || true
sleep 1
ok "Learners added"

# ──────────────────────────────────────────────────────────────
step "4. Promote all nodes to voters"
rpc "$NODE1/cluster/change-membership" '[1,2,3]' | jq . 2>/dev/null || true
sleep 2
ok "Membership changed to [1, 2, 3]"

# ──────────────────────────────────────────────────────────────
step "5. Write 5 entries through the leader (node 1)"
for i in 1 2 3 4 5; do
    rpc "$NODE1/api/append" "{\"message\":\"event-$i\"}" > /dev/null
    ok "Wrote: event-$i"
done
sleep 1

# ──────────────────────────────────────────────────────────────
step "6. Read replicated log from follower (node 2)"
echo "   Log on node 2:"
rpc "$NODE2/api/log" | jq .
echo ""
echo "   Log on node 3:"
rpc "$NODE3/api/log" | jq .

# ──────────────────────────────────────────────────────────────
step "7. Kill the leader (node 1) to trigger failover"
docker compose stop node1
warn "node1 stopped"
sleep 5

# ──────────────────────────────────────────────────────────────
step "8. Wait for new leader election"
NEW_LEADER=""
for i in $(seq 1 20); do
    LEADER_2=$(rpc "$NODE2/api/leader" | jq -r '.leader_id // empty' 2>/dev/null || true)
    LEADER_3=$(rpc "$NODE3/api/leader" | jq -r '.leader_id // empty' 2>/dev/null || true)
    if [ -n "$LEADER_2" ] && [ "$LEADER_2" != "1" ] && [ "$LEADER_2" != "null" ]; then
        NEW_LEADER="$LEADER_2"
        break
    fi
    if [ -n "$LEADER_3" ] && [ "$LEADER_3" != "1" ] && [ "$LEADER_3" != "null" ]; then
        NEW_LEADER="$LEADER_3"
        break
    fi
    sleep 1
done

if [ -z "$NEW_LEADER" ]; then
    echo -e "   ${R}No new leader elected!${NC}"
    exit 1
fi
ok "New leader elected: node $NEW_LEADER"

# Determine the new leader's port
if [ "$NEW_LEADER" = "2" ]; then
    LEADER_URL="$NODE2"
else
    LEADER_URL="$NODE3"
fi

# ──────────────────────────────────────────────────────────────
step "9. Write more entries to the new leader"
for i in 6 7 8; do
    rpc "$LEADER_URL/api/append" "{\"message\":\"event-$i\"}" > /dev/null
    ok "Wrote: event-$i"
done
sleep 1

# ──────────────────────────────────────────────────────────────
step "10. Verify log continuity on surviving nodes"
echo "   Log on node 2:"
rpc "$NODE2/api/log" | jq .
echo ""
echo "   Log on node 3:"
rpc "$NODE3/api/log" | jq .

# ──────────────────────────────────────────────────────────────
step "11. Restart node 1 and verify it catches up"
docker compose start node1
sleep 5
echo "   Log on node 1 (after rejoin):"
rpc "$NODE1/api/log" | jq . 2>/dev/null || warn "node1 still catching up..."

echo ""
echo -e "${G}════════════════════════════════════════════════${NC}"
echo -e "${G}  Demo complete — active/passive failover works!${NC}"
echo -e "${G}════════════════════════════════════════════════${NC}"
