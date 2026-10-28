#!/usr/bin/env bash
# Synchronized multi-container load test through NGINX, with exact drop accounting.
#
# One Python load generator tops out around 2-3k req/s, so a bigger test needs several, started together.
# Each is its own container (its own source IP), all sending distinct customers at the demo checkout
# (no per-IP limit). Needs the stack running with DEMO_MODE=1.
#
#   scripts/distributed_load.sh [generators=5] [requests_per_generator=20000] [concurrency=400]
#
# Reports total requests, wall time, aggregate req/s and the DROPPED count (connection errors, timeouts,
# 5xx). For a known launch, pre-scale first:  MIN_REPLICAS=8 DEMO_MODE=1 docker compose up -d
set -euo pipefail
N=${1:-5}; PER=${2:-20000}; CONC=${3:-400}
cd "$(dirname "$0")/../backend"
NET="${COMPOSE_PROJECT:-flash-sale-system}_flash_net"
WIN_PWD=$(pwd -W 2>/dev/null || pwd)

docker image inspect flash-loadgen >/dev/null 2>&1 || docker build -q -t flash-loadgen -f tests/Dockerfile.loadgen tests >/dev/null
for i in $(seq 1 "$N"); do docker rm -f "bigload$i" >/dev/null 2>&1 || true; done
curl -s -X POST localhost/api/v1/admin/reset >/dev/null || true

START=$(( $(date +%s) + 10 ))
for i in $(seq 1 "$N"); do
  MSYS_NO_PATHCONV=1 docker run -d --name "bigload$i" --network "$NET" -v "$WIN_PWD/tests:/t:ro" \
    -e TARGET_URL=http://nginx -e TOTAL="$PER" -e CONCURRENCY="$CONC" -e START_AT="$START" -e TIMEOUT=30 \
    flash-loadgen python /t/sustained_load.py >/dev/null
done
for i in $(seq 1 "$N"); do until docker logs "bigload$i" 2>&1 | grep -q DROPPED; do sleep 2; done; done

python - "$N" <<'PYEOF'
import re, subprocess, sys
n = int(sys.argv[1]); tot = dropped = served = throttled = recovered = 0; starts = []; ends = []
for i in range(1, n + 1):
    out = subprocess.run(["docker", "logs", f"bigload{i}"], capture_output=True, text=True).stdout
    w = re.search(r"window ([\d.]+) -> ([\d.]+)", out); starts.append(float(w[1])); ends.append(float(w[2]))
    tot += int(re.search(r"requests (\d+)", out)[1])
    dropped += int(re.search(r"DROPPED\s+(\d+)", out)[1])
    served += int(re.search(r"served\s+(\d+)", out)[1])
    m = re.search(r"throttled\s+(\d+)", out); throttled += int(m[1]) if m else 0
    m = re.search(r"recovered by retry[^:]*: (\d+)", out); recovered += int(m[1]) if m else 0
wall = max(ends) - min(starts)
print(f"{n} generators: {tot:,} requests in {wall:.1f}s = {tot / wall:,.0f} req/s aggregate")
print(f"served {served:,}  throttled {throttled:,}  DROPPED {dropped:,} ({100 * dropped / tot:.3f}%)  recovered-by-retry {recovered:,}")
PYEOF
for i in $(seq 1 "$N"); do docker rm -f "bigload$i" >/dev/null 2>&1 || true; done
