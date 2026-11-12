#!/bin/bash
# User data of the V2X-SERVICE instance (only used when the instance is created).
# __TOPIC_ARN__ is replaced by infra/scripts/setup_d2_aws.sh; deployments come from the pipeline.
dnf install -y docker
systemctl enable --now docker
mkdir -p /opt/v2x
printf 'ROLE=service\nAWS_REGION=us-east-1\nALERTS_TOPIC_ARN=__TOPIC_ARN__\n' > /opt/v2x/env
