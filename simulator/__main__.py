"""python -m simulator --scenario simulator/scenarios/accident.yaml --transport sqs"""
import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone

from . import scenario as scenario_mod
from .engine import Simulator
from .groundtruth import JsonlLog
from .transport import MqttTransport, SqsTransport, StdoutTransport


def _range(text):
    lo, hi = text.split(":")
    return float(lo), float(hi)


def _burst(text):
    start, end, factor = (float(x) for x in text.split(":"))
    return start, end, factor


def _git_sha():
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(prog="python -m simulator", description="Synthetic V2X traffic simulator")
    p.add_argument("--scenario", default="simulator/scenarios/accident.yaml")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--vehicles", type=int, help="override the scenario's vehicle count")
    p.add_argument("--duration", type=float, help="override the scenario's duration (s)")
    p.add_argument("--transport", choices=["sqs", "stdout", "mqtt"], default="mqtt")
    p.add_argument("--mqtt-rsu-1-host", default="localhost")
    p.add_argument("--mqtt-rsu-1-port", type=int, default=1883)
    p.add_argument("--mqtt-rsu-2-host", default="localhost")
    p.add_argument("--mqtt-rsu-2-port", type=int, default=1884)
    p.add_argument("--mqtt-user", default="sim")
    p.add_argument("--mqtt-password", default=os.environ.get("MQTT_VEHICLE_PASSWORD", ""),
                   help="prefer MQTT_VEHICLE_PASSWORD env var, not command-line arguments")
    p.add_argument("--mqtt-alert-grace-s", type=float, default=2)
    p.add_argument("--queue", default="v2x-events")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--rate-scale", type=float, default=1.0, help="multiply every publish rate")
    p.add_argument("--time-scale", type=float, default=1.0, help=">1 runs model time faster than wall time")
    p.add_argument("--fast", action="store_true", help="no waiting: run as fast as possible (offline use)")
    p.add_argument("--drop", type=float, default=0.0, help="loss probability")
    p.add_argument("--dup", type=float, default=0.0, help="duplication probability")
    p.add_argument("--delay-ms", type=_range, default=(0, 0), help="extra delay range, e.g. 50:400")
    p.add_argument("--reorder-window", type=int, default=0, help="shuffle every N events")
    p.add_argument("--burst", type=_burst, help="start:end:factor, e.g. 120:180:10")
    p.add_argument("--out", default="runs", help="folder for config, ground truth and fault logs")
    a = p.parse_args()

    cfg = scenario_mod.load(a.scenario)
    if a.vehicles:
        cfg["vehicles"] = a.vehicles
    if a.duration:
        cfg["duration_s"] = a.duration

    run_id = f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{cfg['name']}-s{a.seed}"
    run_dir = os.path.join(a.out, run_id)
    os.makedirs(run_dir, exist_ok=True)
    faults = {"drop": a.drop, "dup": a.dup, "delay_ms": a.delay_ms, "reorder_window": a.reorder_window}
    with open(os.path.join(run_dir, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "seed": a.seed, "scenario": cfg, "faults": faults, "burst": a.burst,
                   "rate_scale": a.rate_scale, "time_scale": a.time_scale, "transport": a.transport,
                   "queue": a.queue, "mqtt_endpoints": {
                       "rsu-1": [a.mqtt_rsu_1_host, a.mqtt_rsu_1_port],
                       "rsu-2": [a.mqtt_rsu_2_host, a.mqtt_rsu_2_port]} if a.transport == "mqtt" else None,
                   "git_sha": _git_sha()}, f, indent=2)

    if a.transport == "sqs":
        transport = SqsTransport(a.queue, a.region)
    elif a.transport == "mqtt":
        transport = MqttTransport(
            broker_hosts={"rsu-1": (a.mqtt_rsu_1_host, a.mqtt_rsu_1_port),
                          "rsu-2": (a.mqtt_rsu_2_host, a.mqtt_rsu_2_port)},
            username=a.mqtt_user, password=a.mqtt_password,
            alerts_path=os.path.join(run_dir, "vehicle_alerts.jsonl"), grace_s=a.mqtt_alert_grace_s)
    else:
        transport = StdoutTransport()

    gt = JsonlLog(os.path.join(run_dir, "groundtruth.jsonl"))
    fl = JsonlLog(os.path.join(run_dir, "faults.jsonl"))
    sim = Simulator(cfg, transport, seed=a.seed, realtime=not a.fast, time_scale=a.time_scale,
                    rate_scale=a.rate_scale, burst=a.burst, faults=faults, groundtruth=gt, fault_log=fl,
                    generate_density=(a.transport != "mqtt"))
    t0 = time.time()
    stats = sim.run()
    gt.close()
    fl.close()
    if isinstance(transport, SqsTransport):
        stats["sqs"] = transport.stats
    if isinstance(transport, MqttTransport):
        stats["mqtt"] = transport.stats
    stats["wall_s"] = round(time.time() - t0, 1)
    with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps({"run_id": run_id, **stats}, indent=2), flush=True)


if __name__ == "__main__":
    main()
