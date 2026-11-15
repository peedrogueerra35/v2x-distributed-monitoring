# V2X simulator

Synthetic traffic for the V2X system: the On-Board Unit of every vehicle. Vehicles follow the Intelligent
Driver Model (IDM) on a 1.2 km ring road with twelve 100 m segments and two lanes, split into two RSU zones
(`S00`–`S05` → `rsu-1`, `S06`–`S11` → `rsu-2`). All data is synthetic and reproducible from a seed.

## Install

    pip install -r simulator/requirements.txt

## Run (from the repository root)

    # MQTT to the two RSUs (default transport); the 'sim' password comes from SSM or the local .env
    export MQTT_VEHICLE_PASSWORD='<password>'
    python -m simulator --scenario simulator/scenarios/accident.yaml --seed $RANDOM \
      --mqtt-rsu-1-host <rsu-1 IP> --mqtt-rsu-1-port 1883 --mqtt-rsu-2-host <rsu-2 IP> --mqtt-rsu-2-port 1883

    # dry run, events printed to the terminal, no broker or AWS needed
    python -m simulator --scenario simulator/scenarios/accident.yaml --transport stdout --time-scale 10

With MQTT, each vehicle is a client of the broker of its current zone, reconnects to the other RSU when it
crosses `S05 → S06` or `S11 → S00`, publishes positions with QoS 0 and critical events with QoS 1, and
subscribes to `alerts/<segment>` and `alerts/all` of its RSU. Received alerts are written, with their latency,
to `runs/<id>/vehicle_alerts.jsonl`.

Run in real time (no `--fast`) when using MQTT: the RSU windows follow the clock. Use a new seed for every run
(the same seed gives the same `event_id`s, which the RSUs discard as duplicates), and run only one simulator at
a time against the same RSUs (vehicle client IDs are fixed).

## Options

| Option | Meaning |
| --- | --- |
| `--scenario` | YAML file in `simulator/scenarios/` (`accident.yaml`, `baseline.yaml`; 5 vehicles, 300 s) |
| `--seed` | Random seed (default 42); same seed, same events |
| `--vehicles N`, `--duration S` | Override the number of vehicles or the duration in seconds |
| `--transport` | `mqtt` (default), `stdout` or `sqs` (direct to SQS, without RSUs) |
| `--mqtt-rsu-1-host/-port`, `--mqtt-rsu-2-host/-port` | Brokers of the two RSUs (defaults `localhost:1883` and `localhost:1884`) |
| `--mqtt-user`, `--mqtt-password` | MQTT credentials (default user `sim`; password from `MQTT_VEHICLE_PASSWORD`) |
| `--rate-scale F` | Multiply every publish rate |
| `--time-scale F`, `--fast` | Run model time F times faster, or without waiting (offline use only) |
| `--drop P`, `--dup P` | Loss and duplication probability |
| `--delay-ms A:B`, `--reorder-window N` | Extra delay per event (uniform, ms) and shuffling every N events |
| `--burst S:E:F` | Multiply rates by F between second S and E |
| `--out DIR` | Folder for the run outputs (default `runs`) |

## Scenario `accident`

Crash of `veh-0002` at 60 s, oil on `S07` from 120 s to 240 s (reported with probability 0.5), and an ambulance
from 180 s that drives `S04 → S07` across both zones.

## Output

Each run writes `runs/<run_id>/`: `config.json` (resolved settings, seed, git SHA), `groundtruth.jsonl` (what
really happened: crashes, hard brakes, hazards, emergency vehicles), `faults.jsonl` (every injected drop and
duplicate), `vehicle_alerts.jsonl` (alerts received by each vehicle) and `summary.json` (event and MQTT
counters).

## Event types

`position` (1 Hz per vehicle), `brake` (deceleration of at least 4 m/s²), `crash`, `hazard` and `emergency`
(2 Hz from the emergency vehicle). With MQTT, `density` summaries are produced by the RSUs; only the `sqs` and
`stdout` transports generate them in the simulator. All events validate against `schema/v2x-event.schema.json`.
