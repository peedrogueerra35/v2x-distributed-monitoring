# V2X Distributed Monitoring

Distributed system that monitors a road with connected vehicles and raises safety alerts. Vehicles publish
events over MQTT to the roadside unit (RSU) of their zone; each RSU processes them at the edge and forwards
summaries and critical events to AWS, where V2X-SERVICE stores them and generates the alerts that need a global
view. Alerts travel back to the RSUs through SNS and on to the vehicles over MQTT.

Sistemas Distribuidos 2026-27 · Máster en Ingeniería de Telecomunicación · Universidade de Vigo.

## Architecture

```text
 simulator (5 vehicles) ──MQTT──▶ rsu-1 / rsu-2   (one EC2 each: Mosquitto + RSU app, SQLite buffer)
        ▲                            │   ▲
        └──── MQTT alerts ───────────┘   │ HTTP /sns (signed, filtered by zone)
                                     │   │
                          SQS v2x-events │
                             (+ DLQ)     │
                                     ▼   │
                     V2X-SERVICE (EC2) ──┴── SNS v2x-alerts
                     consumer + dashboard :8080
                                     │
                                  DynamoDB (v2x-events, v2x-windows, v2x-alerts)
```

| Path | Route | Purpose |
| --- | --- | --- |
| Periodic data | vehicle → MQTT → RSU → SQS → V2X-SERVICE → DynamoDB | positions are aggregated at the RSU; only `density` summaries travel |
| Critical events | same route, no aggregation | `brake`, `crash`, `hazard` and `emergency` are forwarded at once, with priority |
| Local alert | vehicle → MQTT → RSU → MQTT → vehicles of the segment | collision risk, independent of the cloud |
| Cloud alert | V2X-SERVICE → SNS → RSU `/sns` → MQTT → vehicles | sudden braking, crash and emergency vehicle, filtered by zone or sent to both RSUs |

The road is a synthetic 1.2 km ring with twelve 100 m segments (`S00`–`S11`) and two lanes: `S00`–`S05`
belong to `rsu-1` and `S06`–`S11` to `rsu-2`.

## Components

- **Simulator** (`simulator/`): five vehicles following the Intelligent Driver Model, YAML scenarios,
  reproducible seeds and fault injection (loss, duplication, delay, reordering, bursts). Each vehicle is an
  MQTT client of its zone's RSU and switches RSU when it crosses zones. See [`simulator/README.md`](simulator/README.md).
- **RSU** (`services/rsu/`): Mosquitto (users `sim` and `rsu`, ACL per zone) plus a FastAPI application that
  validates and de-duplicates events, aggregates positions in 5 s tumbling windows, detects collision risk
  (time to collision below 2 s), buffers everything for SQS in SQLite and verifies the signature of SNS alerts.
  It exposes `/health`, `/metrics` and `/audit` (these two require the header `X-Metrics-Token`) and `/sns`.
  See [`services/rsu/README.md`](services/rsu/README.md).
- **V2X-SERVICE** (`services/v2x-service/`): SQS consumer that stores each event in a conditional DynamoDB
  transaction, evaluates the alert rules (`rules.py`), publishes alerts to SNS with a `zona` attribute and serves
  a real-time dashboard (`live.py`, `dashboard.html`) and a small REST API.
- **Contracts** (`schema/`): JSON Schemas for events (v1.1, backwards compatible with v1.0) and alerts, shared by
  every component and the tests.
- **Infrastructure** (`infra/`): one-time AWS setup scripts, the per-role deployment script run through SSM,
  the production Compose file of the RSUs and the experiment scripts.

## Design decisions

- **At-least-once delivery, exactly-once effect.** Every hop retries until it gets a confirmation, and every
  receiver discards repeats by `event_id` or `alert_id`: the RSU acknowledges MQTT messages only after saving
  them, V2X-SERVICE deletes an SQS message only after the DynamoDB transaction succeeds, and SNS alerts are
  de-duplicated at the RSU.
- **Standard SQS queue with a dead-letter queue.** Order is rebuilt from `seq` and `event_time`; duplicates are
  removed in DynamoDB rather than relying on FIFO de-duplication.
- **Edge aggregation.** Positions become one `density` summary per occupied segment every 5 s, which cuts the
  messages sent to AWS by about 70 %. Each summary also carries the latest position of every vehicle, which the
  dashboard uses to draw one dot per vehicle.
- **Durable outbox at the RSU.** If SQS is unreachable the RSU keeps aggregating and alerting, and sends the
  backlog (critical events first) when the connection returns.
- **One SNS topic, filtered by attribute.** Each RSU is subscribed over HTTP with a filter policy on `zona`;
  messages are accepted only after verifying their AWS signature.
- **No keys on the instances.** Containers use the instance role; MQTT passwords and the metrics token live in
  SSM Parameter Store; administration and deployment go through SSM, without SSH.

## Repository layout

```text
.github/workflows/  ci.yml (tests, images, deployment), aws-check.yml, ecr-smoke.yml
schema/             v2x-event.schema.json, v2x-alert.schema.json
simulator/          simulator, scenarios (accident.yaml, baseline.yaml)
services/rsu/       RSU application, Mosquitto configuration, local Compose file
services/v2x-service/  consumer, alert rules, live view and dashboard
infra/aws/          EC2 user data for the service and RSU instances
infra/deploy/       deploy.sh (run on each instance through SSM) and compose.rsu.yml
infra/scripts/      AWS setup, SNS subscriptions, metrics, local RSUs, experiment E2
experiments/        experiment E1
tests/              unit, contract and integration tests
```

## Tests

41 tests, no AWS needed:

```bash
pip install pytest jsonschema -r simulator/requirements.txt -r services/rsu/requirements.txt
pytest -q
```

## Deployment on AWS Academy (us-east-1)

Prerequisites: an AWS Academy Learner Lab session with its credentials in `~/.aws/credentials`, the SQS queue
`v2x-events` with its dead-letter queue, the DynamoDB tables `v2x-events`, `v2x-windows` and `v2x-alerts`, and
the V2X-SERVICE instance (tag `Role=v2x-service`, user data `infra/aws/userdata-service.sh`).

```bash
infra/scripts/setup_d2_aws.sh <team-ip> [<team-ip> ...]   # SNS topic, ECR v2x-rsu, S3 bucket, SSM secrets, RSU instances
infra/scripts/migrate_service_d2.sh                      # tells V2X-SERVICE its role and the SNS topic
```

Then configure the GitHub repository and merge to `main`; the pipeline tests, builds and publishes both images
and deploys the three instances. Once the RSUs answer on `:8090/health`, subscribe them to SNS:

```bash
infra/scripts/subscribe_rsus.sh
```

GitHub configuration: secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` (Learner
Lab session credentials, renewed every session); variables `AWS_REGION` (`us-east-1`), `ECR_REPOSITORY`
(`v2x-service`), `ECR_REPOSITORY_RSU` (`v2x-rsu`) and `DEPLOY_BUCKET` (`v2x-deploy-<account>`).

## Running the system

```bash
export MQTT_VEHICLE_PASSWORD=$(aws ssm get-parameter --name /v2x/mqtt/sim-password --with-decryption --query Parameter.Value --output text)
python -m simulator --scenario simulator/scenarios/accident.yaml --transport mqtt --seed $RANDOM \
  --mqtt-rsu-1-host <rsu-1 IP> --mqtt-rsu-1-port 1883 --mqtt-rsu-2-host <rsu-2 IP> --mqtt-rsu-2-port 1883
```

Use a new seed for every run: the same seed produces the same `event_id`s, which the RSUs discard as
duplicates. Run only one simulator at a time, because vehicle client IDs are fixed.

- Dashboard: `http://<V2X-SERVICE IP>:8080` (port open only to the team's IPs).
- RSU status: `curl http://<rsu IP>:8090/health`; counters: `/metrics` with header `X-Metrics-Token`
  (token in SSM `/v2x/metrics/token`).
- Run metrics: `infra/scripts/run_metrics.sh` (stored events, latency and alerts of the last run).

## Experiments

- **E1, RSU–cloud disconnection** (`experiments/experiment_e1.py`): pauses the SQS sender of one RSU through
  SSM while a simulator runs, and measures how the buffer grows and drains.

  ```bash
  python experiments/experiment_e1.py --mode ssm --instance-id <rsu-1 instance id> --rsu-url http://<rsu-1 IP>:8090 \
    --repetitions 3 --outage-s 20 --out results/e1/<run>
  ```

- **E2, V2X-SERVICE failure** (`infra/scripts/exp_e2_service_outage.sh`): starts its own simulator, stops
  V2X-SERVICE for a given time, restarts it and records the SQS queue depth, duplicates and dead-letter queue.

  ```bash
  export SIM_ARGS="--transport mqtt --mqtt-rsu-1-host <rsu-1 IP> --mqtt-rsu-1-port 1883 --mqtt-rsu-2-host <rsu-2 IP> --mqtt-rsu-2-port 1883"
  infra/scripts/exp_e2_service_outage.sh 60 90 $RANDOM     # outage of 60 s, starting at t = 90 s
  ```

Run one experiment at a time and do not merge to `main` while one is running: a deployment restarts the
services.

## Simulator options

| Option | Meaning |
| --- | --- |
| `--scenario` | `simulator/scenarios/accident.yaml` (crash at 60 s, oil on `S07` from 120 s, ambulance from 180 s) or `baseline.yaml`; 5 vehicles, 300 s |
| `--seed` | Random seed; same seed, same events |
| `--transport` | `mqtt` (default), `stdout` (print only) or `sqs` (direct to SQS, without RSUs) |
| `--duration`, `--vehicles` | Override the scenario duration (s) or number of vehicles |
| `--drop`, `--dup`, `--delay-ms A:B`, `--reorder-window N`, `--burst S:E:F` | Fault injection: loss, duplication, delay, reordering and rate bursts |
| `--time-scale F`, `--fast` | Faster than real time (offline use only; MQTT runs must be real time) |
| `--out` | Output folder (default `runs/`): configuration, ground truth, injected faults, alerts received by each vehicle and summary |

## Local development

Two RSUs can run on one machine with Docker (brokers on ports 1883 and 1884, APIs on 8090 and 8091):

```bash
cp services/rsu/.env.example services/rsu/.env     # set the MQTT passwords and METRICS_TOKEN
./infra/scripts/local_rsus.sh up
set -a; source services/rsu/.env; set +a
python infra/scripts/smoke_edge.py                 # one local collision_risk alert on each RSU
python -m simulator --duration 120 --mqtt-rsu-1-host 127.0.0.1 --mqtt-rsu-2-host 127.0.0.1
./infra/scripts/local_rsus.sh down
```

`python infra/scripts/offline_evidence.py` runs the simulator and both RSUs in memory, without Docker or AWS, and
reports how many events each RSU would send to the cloud.
