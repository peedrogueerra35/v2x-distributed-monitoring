#!/usr/bin/env bash
# Subscribes each RSU to SNS v2x-alerts over HTTP (http://<Elastic IP>:8090/sns), with a filter on the
# 'zona' message attribute: rsu-1 receives zona = rsu-1 or broadcast; rsu-2 receives zona = rsu-2 or broadcast.
# Run once the RSUs answer on :8090/health (the RSU application confirms the subscription itself).
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
TOPIC="arn:aws:sns:us-east-1:$ACCOUNT:v2x-alerts"
for RSU in rsu-1 rsu-2; do
  IP=$(aws ec2 describe-addresses --filters Name=tag:Name,Values=$RSU --query 'Addresses[0].PublicIp' --output text)
  URL="http://$IP:8090/sns"
  echo "== $RSU  $URL"
  curl -sf --max-time 5 "http://$IP:8090/health" >/dev/null || { echo "   $RSU does not answer on :8090/health, skipping"; continue; }
  EXISTING=$(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" \
             --query "Subscriptions[?Endpoint=='$URL'].SubscriptionArn" --output text)
  if [ -n "$EXISTING" ] && [ "$EXISTING" != "PendingConfirmation" ]; then echo "   already subscribed"; continue; fi
  ATTRS=$(python3 -c "import json; print(json.dumps({'FilterPolicy': json.dumps({'zona': ['$RSU', 'broadcast']}),
          'FilterPolicyScope': 'MessageAttributes'}))")
  aws sns subscribe --topic-arn "$TOPIC" --protocol http --notification-endpoint "$URL" --attributes "$ATTRS" >/dev/null
done
echo "== Waiting for the RSUs to confirm (up to 60 s)"
for _ in $(seq 1 12); do
  PENDING=$(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" \
            --query "length(Subscriptions[?SubscriptionArn=='PendingConfirmation'])" --output text)
  [ "$PENDING" = "0" ] && break; sleep 5
done
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" --query 'Subscriptions[].[Endpoint,SubscriptionArn]' --output table
