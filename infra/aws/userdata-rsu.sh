#!/bin/bash
# User data of an RSU instance. __RSU_ID__ is replaced by infra/scripts/setup_d2_aws.sh (rsu-1 / rsu-2).
dnf install -y docker
systemctl enable --now docker
mkdir -p /usr/local/lib/docker/cli-plugins
curl -sSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
mkdir -p /opt/v2x
printf 'ROLE=rsu\nRSU_ID=__RSU_ID__\nAWS_REGION=us-east-1\n' > /opt/v2x/env
