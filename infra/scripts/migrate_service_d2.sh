#!/usr/bin/env bash
# One-time: tells the existing V2X-SERVICE instance its role and the SNS topic, so that
# infra/deploy/deploy.sh starts the container with ALERTS_TOPIC_ARN. Run after setup_d2_aws.sh.
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
TOPIC="arn:aws:sns:us-east-1:$ACCOUNT:v2x-alerts"
IID=$(aws ec2 describe-instances --filters Name=tag:Role,Values=v2x-service Name=instance-state-name,Values=running \
      --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID" != "None" ] || { echo "v2x-service instance is not running"; exit 1; }
PARAMS=$(TOPIC="$TOPIC" python3 -c 'import json, os; t = os.environ["TOPIC"]; print(json.dumps({"commands": [
  "echo ROLE=service > /opt/v2x/env",
  "echo AWS_REGION=us-east-1 >> /opt/v2x/env",
  f"echo ALERTS_TOPIC_ARN={t} >> /opt/v2x/env",
  "cat /opt/v2x/env"]}))')
CMD=$(aws ssm send-command --document-name AWS-RunShellScript --instance-ids "$IID" --parameters "$PARAMS" \
      --query Command.CommandId --output text)
aws ssm wait command-executed --command-id "$CMD" --instance-id "$IID" || true
aws ssm get-command-invocation --command-id "$CMD" --instance-id "$IID" --query StandardOutputContent --output text
