#!/usr/bin/env bash

#   infra/scripts/run_metrics.sh                 # última ejecución encontrada
#   infra/scripts/run_metrics.sh runs/<run_id>   # una ejecución concreta

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ $# -ge 1 ]; then
  RUN_DIR="${1%/}"
else
  CANDIDATES=()
  for d in "${RUNS_DIR:-}" "$PWD/runs" "$REPO_ROOT/runs" "$(dirname "$REPO_ROOT")/runs"; do
    [ -n "$d" ] && [ -d "$d" ] && CANDIDATES+=("$d")
  done
  RUN_DIR=""
  if [ ${#CANDIDATES[@]} -gt 0 ]; then
    # most recent finished run (run_id starts with the UTC timestamp) across all candidate folders
    RUN_DIR="$(for d in "${CANDIDATES[@]}"; do
                 for r in "$d"/*/; do [ -f "$r/summary.json" ] && echo "$(basename "$r") ${r%/}"; done
               done | sort | tail -n 1 | cut -d' ' -f2-)"
  fi
fi

if [ -z "$RUN_DIR" ] || [ ! -f "$RUN_DIR/summary.json" ]; then
  echo "No se encuentra ninguna ejecución terminada (carpeta runs/ con summary.json)." >&2
  echo "Busqué en: ./runs, $REPO_ROOT/runs y $(dirname "$REPO_ROOT")/runs. Pasa la ruta como argumento o usa RUNS_DIR." >&2
  exit 1
fi
echo "usando: $RUN_DIR"

python3 - "$RUN_DIR" <<'EOF'
import boto3, json, os, sys
from datetime import datetime, timezone

run_dir = sys.argv[1]
run = os.path.basename(run_dir)
start = datetime.strptime(run[:15], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
summary = json.load(open(f"{run_dir}/summary.json"))
config = json.load(open(f"{run_dir}/config.json"))
dups = sum(1 for _ in open(f"{run_dir}/faults.jsonl"))
ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))

ddb = boto3.client("dynamodb", region_name="us-east-1")
lat, stored = [], 0
for page in ddb.get_paginator("scan").paginate(
        TableName="v2x-events", ProjectionExpression="event_time, #t", ExpressionAttributeNames={"#t": "trace"}):
    for it in page["Items"]:
        e = ts(it["event_time"]["S"])
        if e < start:
            continue
        stored += 1
        c = it.get("trace", {}).get("M", {}).get("cloud_received_time", {}).get("S")
        if c:
            lat.append((ts(c) - e).total_seconds() * 1000)

alerts = []
for page in ddb.get_paginator("scan").paginate(TableName="v2x-alerts"):
    for it in page["Items"]:
        if ts(it["created_time"]["S"]) >= start:
            alerts.append(f'{it["alert_type"]["S"]}  {it["message"]["S"]}')

lat.sort()
pct = lambda p: round(lat[min(len(lat) - 1, int(len(lat) * p))]) if lat else None
print("ejecución:", run, "| semilla:", config["seed"])
print("generados:", summary["generated"], "| enviados a SQS:", summary.get("sqs", {}).get("sent"))
print("duplicados inyectados:", dups)
print("eventos guardados de esta ejecución:", stored)
print("latencia p50 / p95 ms:", pct(0.50), "/", pct(0.95))
print("alertas de esta ejecución:", len(alerts))
for a in alerts:
    print("  -", a)
if stored == 0:
    print("\nAviso: 0 eventos guardados. Si repetiste la misma semilla sin vaciar las tablas,"
          " todos se descartaron como duplicados. Usa --seed $RANDOM.")
EOF
# Optional D2 edge metrics (keeps the original D1 AWS/DynamoDB behavior unchanged).
# Example: RSU1_METRICS_URL=http://127.0.0.1:8090 RSU2_METRICS_URL=http://127.0.0.1:8091 \
#          METRICS_TOKEN=... infra/scripts/run_metrics.sh runs/<run_id>
if [[ -n "${RSU1_METRICS_URL:-}" || -n "${RSU2_METRICS_URL:-}" ]]; then
  CMD=(python3 "$SCRIPT_DIR/edge_metrics.py" "$RUN_DIR")
  [[ -n "${RSU1_METRICS_URL:-}" ]] && CMD+=(--rsu1 "$RSU1_METRICS_URL")
  [[ -n "${RSU2_METRICS_URL:-}" ]] && CMD+=(--rsu2 "$RSU2_METRICS_URL")
  [[ -n "${METRICS_TOKEN:-}" ]] && CMD+=(--token "$METRICS_TOKEN")
  "${CMD[@]}"
fi
