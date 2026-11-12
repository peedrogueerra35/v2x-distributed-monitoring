#!/usr/bin/env bash
# One-time creation of the Deliverable 2 AWS resources. Safe to run again: existing resources are reused.
# Usage (from the repository root, with the lab session credentials):
#   infra/scripts/setup_d2_aws.sh <team-ip-1> [<team-ip-2> ...]
# Creates: SNS topic v2x-alerts, ECR repository v2x-rsu, S3 bucket for deployment bundles,
#          MQTT passwords in SSM Parameter Store, security group v2x-rsu-sg,
#          two RSU instances (rsu-1, rsu-2), each with an Elastic IP.
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: $0 <team-ip> [<team-ip> ...]"; exit 1; }
export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
cd "$(dirname "$0")/../.."
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="v2x-deploy-$ACCOUNT"

echo "== SNS topic"
TOPIC=$(aws sns create-topic --name v2x-alerts --tags Key=project,Value=v2x --query TopicArn --output text)
echo "   $TOPIC"

echo "== ECR repository v2x-rsu"
aws ecr describe-repositories --repository-names v2x-rsu >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name v2x-rsu --tags Key=project,Value=v2x >/dev/null
aws ecr put-lifecycle-policy --repository-name v2x-rsu --lifecycle-policy-text \
  '{"rules":[{"rulePriority":1,"description":"keep 10","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}' >/dev/null

echo "== S3 bucket for deployment bundles: $BUCKET"
aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws s3 mb "s3://$BUCKET" >/dev/null

echo "== MQTT passwords in SSM Parameter Store"
for user in sim rsu; do
  name="/v2x/mqtt/$user-password"
  aws ssm get-parameter --name "$name" >/dev/null 2>&1 || \
    aws ssm put-parameter --name "$name" --type SecureString --value "$(openssl rand -hex 12)" >/dev/null
done

echo "== Security group v2x-rsu-sg"
RSG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=v2x-rsu-sg \
      --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ -z "$RSG" ] || [ "$RSG" = "None" ]; then
  RSG=$(aws ec2 create-security-group --group-name v2x-rsu-sg --description "V2X RSU: MQTT for the team, HTTP for SNS" \
        --query GroupId --output text)
fi
allow() { aws ec2 authorize-security-group-ingress --group-id "$RSG" --protocol tcp --port "$1" --cidr "$2" >/dev/null 2>&1 || true; }
allow 8090 0.0.0.0/0                   # SNS delivers from addresses that cannot be listed
for ip in "$@"; do allow 1883 "$ip/32"; done
echo "   $RSG"

AMI=resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64
for RSU in rsu-1 rsu-2; do
  echo "== Instance $RSU"
  IID=$(aws ec2 describe-instances --filters Name=tag:Role,Values=$RSU \
        Name=instance-state-name,Values=pending,running,stopping,stopped \
        --query 'Reservations[0].Instances[0].InstanceId' --output text)
  if [ -z "$IID" ] || [ "$IID" = "None" ]; then
    sed "s/__RSU_ID__/$RSU/" infra/aws/userdata-rsu.sh > "/tmp/userdata-$RSU.sh"
    IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t3.small \
          --iam-instance-profile Name=LabInstanceProfile --security-group-ids "$RSG" \
          --user-data "file:///tmp/userdata-$RSU.sh" \
          --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2 \
          --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$RSU},{Key=Role,Value=$RSU},{Key=project,Value=v2x}]" \
          --query 'Instances[0].InstanceId' --output text)
  fi
  aws ec2 start-instances --instance-ids "$IID" >/dev/null 2>&1 || true
  aws ec2 wait instance-running --instance-ids "$IID"
  ALLOC=$(aws ec2 describe-addresses --filters Name=tag:Name,Values=$RSU --query 'Addresses[0].AllocationId' --output text)
  if [ -z "$ALLOC" ] || [ "$ALLOC" = "None" ]; then
    ALLOC=$(aws ec2 allocate-address --domain vpc \
            --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=$RSU},{Key=project,Value=v2x}]" \
            --query AllocationId --output text)
  fi
  aws ec2 associate-address --instance-id "$IID" --allocation-id "$ALLOC" >/dev/null
  IP=$(aws ec2 describe-addresses --allocation-ids "$ALLOC" --query 'Addresses[0].PublicIp' --output text)
  echo "   $IID  Elastic IP $IP"
done

cat <<SUMMARY

Done.
  SNS topic ............ $TOPIC
  Deployment bucket .... $BUCKET   -> GitHub variable DEPLOY_BUCKET
  RSU image repository . v2x-rsu   -> GitHub variable ECR_REPOSITORY_RSU
  MQTT 'sim' password .. aws ssm get-parameter --name /v2x/mqtt/sim-password --with-decryption --query Parameter.Value --output text
Next: infra/scripts/migrate_service_d2.sh, then merge to main so the pipeline deploys the three instances.
SUMMARY
