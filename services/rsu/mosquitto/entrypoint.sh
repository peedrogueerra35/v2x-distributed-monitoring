#!/bin/sh
set -eu
: "${RSU_ID:?Missing RSU_ID}"
: "${MQTT_VEHICLE_PASSWORD:?Missing MQTT_VEHICLE_PASSWORD}"
: "${MQTT_APP_PASSWORD:?Missing MQTT_APP_PASSWORD}"
case "$RSU_ID" in rsu-1|rsu-2) ;; *) echo 'Bad RSU_ID' >&2; exit 1 ;; esac
mkdir -p /mosquitto/config /mosquitto/data
cp /templates/mosquitto.conf /mosquitto/config/mosquitto.conf
sed "s/__RSU_ID__/$RSU_ID/g" /templates/acl > /mosquitto/config/acl
mosquitto_passwd -b -c /mosquitto/config/passwd sim "$MQTT_VEHICLE_PASSWORD"
mosquitto_passwd -b /mosquitto/config/passwd rsu "$MQTT_APP_PASSWORD"
chown -R mosquitto:mosquitto /mosquitto/config /mosquitto/data
chmod 0600 /mosquitto/config/passwd
exec mosquitto -c /mosquitto/config/mosquitto.conf
