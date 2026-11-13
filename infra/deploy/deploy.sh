#!/bin/bash
# Deploys one image tag on this EC2 instance. Same script for every instance; the role comes from
# /opt/v2x/env (written by the user data): ROLE=service or ROLE=rsu (+ RSU_ID=rsu-1 | rsu-2).
# Run by the pipeline through SSM:  bash /opt/v2x/bundle/infra/deploy/deploy.sh <git sha>
set -euo pipefail
TAG="$1"
HERE="$(cd "$(dirname "$0")" && pwd)"
BUNDLE="$(cd "$HERE/../.." && pwd)"
if [ -f /opt/v2x/env ]; then set -a; . /opt/v2x/env; set +a; fi
ROLE="${ROLE:-service}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
export REG="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com" TAG REGION
aws ecr get-login-password --region "$REGION" | docker login -u AWS --password-stdin "$REG" >/dev/null

param() { aws ssm get-parameter --region "$REGION" --name "$1" --with-decryption --query Parameter.Value --output text; }
wait_health() {
  for _ in $(seq 1 30); do curl -sf "$1" && echo && return 0; sleep 2; done
  echo "health check failed: $1"; return 1
}

case "$ROLE" in
  service)
    docker pull "$REG/v2x-service:$TAG"
    docker rm -f v2x-service 2>/dev/null || true
    docker run -d --name v2x-service --restart unless-stopped -p 8080:8080 \
      -e AWS_DEFAULT_REGION="$REGION" -e ALERTS_TOPIC_ARN="${ALERTS_TOPIC_ARN:-}" \
      "$REG/v2x-service:$TAG"
    wait_health http://localhost:8080/health
    ;;
  rsu)
    : "${RSU_ID:?RSU_ID missing in /opt/v2x/env}"
    SRC="$BUNDLE/services/rsu/mosquitto"
    [ -f "$SRC/mosquitto.conf" ] || { echo "services/rsu/mosquitto/mosquitto.conf missing from the bundle"; exit 1; }
    CONF=/opt/v2x/mosquitto
    mkdir -p "$CONF" && cp "$SRC"/* "$CONF"/
    # Each broker must only accept vehicle topics in its own RSU zone.
    sed -i "s/__RSU_ID__/$RSU_ID/g" "$CONF/acl"
    # MQTT users: 'sim' (vehicles) and 'rsu' (RSU application); passwords live in SSM Parameter Store
    export MQTT_APP_PASSWORD="$(param /v2x/mqtt/rsu-password)"
    SIM_PASSWORD="$(param /v2x/mqtt/sim-password)"
    export MQTT_VEHICLE_PASSWORD="$SIM_PASSWORD"
    # The SNS topic was already created by setup_d2_aws.sh.
    export RSU_IMAGE="$REG/v2x-rsu:$TAG"
    export AWS_DEFAULT_REGION="$REGION" SQS_QUEUE_NAME=v2x-events
    export SNS_TOPIC_ARN="arn:aws:sns:$REGION:$ACCOUNT:v2x-alerts"
    # If missing, /metrics and /audit stay disabled; /health and /sns still work.
    export METRICS_TOKEN="$(param /v2x/metrics/token 2>/dev/null || true)"
    docker run --rm -v "$CONF":/m \
      -e SIM_PASSWORD="$SIM_PASSWORD" -e APP_PASSWORD="$MQTT_APP_PASSWORD" \
      eclipse-mosquitto:2.0.20 sh -c \
      'mosquitto_passwd -c -b /m/passwd sim "$SIM_PASSWORD" && mosquitto_passwd -b /m/passwd rsu "$APP_PASSWORD" && chown 1883:1883 /m/passwd && chmod 0600 /m/passwd' 
    export RSU_ID
    # Compose reads a root-only .env alongside the AWS production compose file.
    # This complements the exports and keeps the contract inspectable on each EC2.
    (
      umask 077
      printf 'RSU_ID=%s\nAWS_DEFAULT_REGION=%s\nSQS_QUEUE_NAME=%s\nSNS_TOPIC_ARN=%s\nRSU_IMAGE=%s\nMQTT_APP_PASSWORD=%s\nMQTT_VEHICLE_PASSWORD=%s\nMETRICS_TOKEN=%s\n' \
        "$RSU_ID" "$AWS_DEFAULT_REGION" "$SQS_QUEUE_NAME" "$SNS_TOPIC_ARN" \
        "$RSU_IMAGE" "$MQTT_APP_PASSWORD" "$MQTT_VEHICLE_PASSWORD" "$METRICS_TOKEN" > "$HERE/.env"
    )
    docker compose -p v2x --env-file "$HERE/.env" -f "$HERE/compose.rsu.yml" up -d --pull always --force-recreate --remove-orphans
    wait_health http://localhost:8090/health
    ;;
  *) echo "unknown ROLE '$ROLE' in /opt/v2x/env"; exit 1 ;;
esac
echo "deployed $ROLE ${RSU_ID:-} with tag $TAG"
