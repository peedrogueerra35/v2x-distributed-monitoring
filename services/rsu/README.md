# RSU (roadside unit)

Each RSU runs two containers: Mosquitto, the MQTT broker of its zone, and the RSU application (FastAPI on port
8090), which connects to the broker as user `rsu`. In AWS each RSU has its own EC2 instance and is deployed by
`infra/deploy/deploy.sh` with `infra/deploy/compose.rsu.yml`; locally, `services/rsu/compose.yml` starts two RSUs
on one machine.

## What it does

- Subscribes to `v2x/<rsu>/+/+/+` with QoS 1 and acknowledges each message only after committing it to SQLite.
- Validates every event against `schema/v2x-event.schema.json` and the MQTT topic, checks the zone and discards
  repeated `event_id`s.
- Aggregates positions in 5 s event-time tumbling windows per segment, with 2 s of allowed lateness, and emits
  one `density` event per window (vehicle count, mean speed, occupancy and the latest position of each vehicle).
- Forwards critical events (`brake`, `crash`, `hazard`, `emergency`) at once, with higher priority than `density`.
- Raises local `collision_risk` alerts when two vehicles in the same lane have a time to collision below 2 s and a
  gap below 60 m, with a 15 s cool-down per pair, and records the risk as an event for the cloud.
- Sends everything to SQS through a durable SQLite outbox (WAL), deleting each message only after SQS accepts it
  and retrying with exponential back-off.
- Receives cloud alerts from SNS on `POST /sns`: verifies the AWS signature (topic, timestamp, regional
  certificate URL, RSA signature), validates `schema/v2x-alert.schema.json`, checks the zone, discards repeated
  `alert_id`s and publishes the alert over MQTT.

MQTT users are `sim` (vehicles) and `rsu` (application); the ACL restricts each one to its zone's topics.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | `ok`, MQTT connection, cloud pause flag and pending outbox (503 if MQTT is down) |
| `GET /metrics`, `GET /audit` | Counters and outbox audit; require header `X-Metrics-Token` (403 if the token is missing or wrong) |
| `POST /sns` | SNS subscription confirmation and alert notifications |

## Two local RSUs with Docker

```bash
pip install pytest jsonschema -r simulator/requirements.txt -r services/rsu/requirements.txt
cp services/rsu/.env.example services/rsu/.env     # set the passwords and METRICS_TOKEN
./infra/scripts/local_rsus.sh up
curl -i http://127.0.0.1:8090/health
curl -i http://127.0.0.1:8091/health
set -a; source services/rsu/.env; set +a
python infra/scripts/smoke_edge.py                 # triggers one collision_risk alert on each RSU
```

Both `/health` calls must return HTTP 200 and `smoke_edge.py` must receive one local alert from each RSU.
Without AWS credentials the outbox keeps the events, since `/health` checks MQTT, not SQS.

A full local run, in real time:

```bash
python -m simulator --transport mqtt --duration 120 --scenario simulator/scenarios/accident.yaml \
  --mqtt-rsu-1-host 127.0.0.1 --mqtt-rsu-1-port 1883 --mqtt-rsu-2-host 127.0.0.1 --mqtt-rsu-2-port 1884
python infra/scripts/edge_metrics.py runs/<run_id> --rsu1 http://127.0.0.1:8090 --rsu2 http://127.0.0.1:8091 --token "$METRICS_TOKEN"
```

Experiment E1 also works locally (`--mode local`), pausing the SQS sender of a local RSU container:

```bash
python experiments/experiment_e1.py --mode local --container v2x-rsu1-rsu-1 \
  --rsu-url http://127.0.0.1:8090 --token "$METRICS_TOKEN" --repetitions 5 --outage-s 20
```

E1 pauses the application's SQS sender (file `/data/cloud.paused`); it does not cut the network.

Stop the local environment with `./infra/scripts/local_rsus.sh down`.
