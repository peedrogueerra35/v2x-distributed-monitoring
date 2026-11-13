#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT/services/rsu/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "First: cp services/rsu/.env.example services/rsu/.env and set strong passwords" >&2
  exit 1
fi
if grep -Eq '^(MQTT_VEHICLE_PASSWORD|MQTT_APP_PASSWORD)=CHANGE_ME' "$ENV_FILE"; then
  echo "Replace CHANGE_ME passwords in $ENV_FILE before starting Docker" >&2
  exit 1
fi
cmd="${1:-up}"
case "$cmd" in
  up)
    RSU_ID=rsu-1 MQTT_EXTERNAL_PORT=1883 SNS_EXTERNAL_PORT=8090 docker compose --env-file "$ENV_FILE" -f "$ROOT/services/rsu/compose.yml" -p v2x-rsu1 up -d --build
    RSU_ID=rsu-2 MQTT_EXTERNAL_PORT=1884 SNS_EXTERNAL_PORT=8091 docker compose --env-file "$ENV_FILE" -f "$ROOT/services/rsu/compose.yml" -p v2x-rsu2 up -d --build
    echo "RSUs at MQTT :1883/:1884; HTTP /health at :8090/:8091"
    ;;
  down)
    for n in 1 2; do
      RSU_ID="rsu-$n" docker compose --env-file "$ENV_FILE" -f "$ROOT/services/rsu/compose.yml" -p "v2x-rsu$n" down
    done
    ;;
  logs)
    for n in 1 2; do
      RSU_ID="rsu-$n" docker compose --env-file "$ENV_FILE" -f "$ROOT/services/rsu/compose.yml" -p "v2x-rsu$n" ps
    done
    ;;
  *) echo "Usage: $0 up|down|logs"; exit 2 ;;
esac
