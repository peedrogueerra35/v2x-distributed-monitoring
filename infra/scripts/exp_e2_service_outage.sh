#!/usr/bin/env bash
# Experiment E2 (component failure): stop V2X-SERVICE for a fixed time while the simulator runs,
# then restart it and measure queue growth, recovery time, duplicates and final state.
# Usage (repository root, lab running, simulator dependencies installed):
#   infra/scripts/exp_e2_service_outage.sh [outage_s=60] [stop_at_s=90] [seed=random]
#   SIM_ARGS="--transport sqs" infra/scripts/exp_e2_service_outage.sh 60    # without RSUs
# Results: results/e2/<timestamp>-out<outage>-s<seed>/ (queue.csv, simulator.log, metrics.txt, summary.json)
set -euo pipefail
OUTAGE="${1:-60}"; STOP_AT="${2:-90}"; SEED="${3:-$RANDOM}"
SIM_ARGS="${SIM_ARGS:---transport mqtt}"
export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
cd "$(dirname "$0")/../.."
OUT="results/e2/$(date -u +%Y%m%d-%H%M%S)-out${OUTAGE}-s${SEED}"; mkdir -p "$OUT"
QUEUE_URL=$(aws sqs get-queue-url --queue-name v2x-events --query QueueUrl --output text)
DLQ_URL=$(aws sqs get-queue-url --queue-name v2x-events-dlq --query QueueUrl --output text)
read -r IID IP < <(aws ec2 describe-instances --filters Name=tag:Role,Values=v2x-service Name=instance-state-name,Values=running \
  --query 'Reservations[0].Instances[0].[InstanceId,PublicIpAddress]' --output text)
now() { date -u +%FT%T.%3NZ; }
depth() { aws sqs get-queue-attributes --queue-url "$1" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' --output text; }
ssm_run() {   # run one shell command on the V2X-SERVICE instance and wait for it
  local cmd; cmd=$(aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$IID" \
    --parameters "{\"commands\":[\"$1\"]}" --query Command.CommandId --output text)
  aws ssm wait command-executed --command-id "$cmd" --instance-id "$IID" || true
}

echo "E2: outage ${OUTAGE}s at t=${STOP_AT}s, seed ${SEED}, simulator ${SIM_ARGS} -> $OUT"
echo "time,visible,in_flight" > "$OUT/queue.csv"
( while true; do read -r v f < <(depth "$QUEUE_URL"); echo "$(now),$v,$f" >> "$OUT/queue.csv"; sleep 5; done ) &
SAMPLER=$!
SIM=""; STOPPED=0
cleanup() {
  kill "$SAMPLER" 2>/dev/null || true
  if [ -n "$SIM" ] && kill -0 "$SIM" 2>/dev/null; then kill "$SIM" 2>/dev/null || true; echo "  simulator stopped"; fi
  if [ "$STOPPED" = "1" ]; then
    echo "  interrupted during the outage: restarting v2x-service"; ssm_run "docker start v2x-service"
  fi
}
trap cleanup EXIT
trap 'echo; echo "  interrupted"; exit 130' INT TERM

# shellcheck disable=SC2086
python -m simulator --scenario simulator/scenarios/accident.yaml --seed "$SEED" $SIM_ARGS > "$OUT/simulator.log" 2>&1 &
SIM=$!

echo "  simulator running; stopping v2x-service in ${STOP_AT}s"
sleep "$STOP_AT"
T_STOP=$(now); STOPPED=1; ssm_run "docker stop v2x-service"; echo "  stopped   $T_STOP  (outage of ${OUTAGE}s, the queue grows meanwhile)"
sleep "$OUTAGE"
T_START=$(now); ssm_run "docker start v2x-service"; STOPPED=0; echo "  restarted $T_START"
START_EPOCH=$(date +%s)

echo "  waiting for the simulator to finish (5 min scenario in total)"
wait "$SIM" || echo "  simulator exited with an error (see simulator.log)"
SIM=""
echo "  simulator finished; waiting for the queue to drain"
T_DRAINED=""; zeros=0
for _ in $(seq 1 120); do
  read -r v f < <(depth "$QUEUE_URL")
  if [ "$v" = "0" ] && [ "$f" = "0" ]; then zeros=$((zeros+1)); else zeros=0; fi
  if [ $zeros -ge 2 ]; then T_DRAINED=$(now); break; fi
  echo "    queue: $v visible, $f in flight"
  sleep 5
done
kill $SAMPLER 2>/dev/null || true

curl -sf "http://$IP:8080/report" > "$OUT/report_after_restart.json" || echo '{}' > "$OUT/report_after_restart.json"
read -r DLQ_V DLQ_F < <(depth "$DLQ_URL")
infra/scripts/run_metrics.sh > "$OUT/metrics.txt" 2>&1 || true

OUT="$OUT" T_STOP="$T_STOP" T_START="$T_START" T_DRAINED="$T_DRAINED" OUTAGE="$OUTAGE" SEED="$SEED" \
DLQ="$DLQ_V" python3 - <<'PY'
import csv, json, os
from datetime import datetime
ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None
out = os.environ["OUT"]
rows = list(csv.DictReader(open(f"{out}/queue.csv")))
depths = [int(r["visible"]) + int(r["in_flight"]) for r in rows]
restart, drained = ts(os.environ["T_START"]), ts(os.environ["T_DRAINED"])
summary = {
    "seed": int(os.environ["SEED"]), "outage_s": int(os.environ["OUTAGE"]),
    "stopped_at": os.environ["T_STOP"], "restarted_at": os.environ["T_START"],
    "queue_drained_at": os.environ["T_DRAINED"] or None,
    "recovery_s": round((drained - restart).total_seconds(), 1) if drained else None,
    "max_queue_depth": max(depths) if depths else None,
    "dlq_depth_after": int(os.environ["DLQ"]),
    "service_counters_after_restart": json.load(open(f"{out}/report_after_restart.json")),
}
json.dump(summary, open(f"{out}/summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
PY
echo "--- run_metrics.sh"; cat "$OUT/metrics.txt"
