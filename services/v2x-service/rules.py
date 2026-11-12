"""Cloud alert rules of V2X-SERVICE. Pure functions: no AWS calls, so they are unit-tested in CI.

Each rule returns alerts that follow schema/v2x-alert.schema.json. Alert IDs are deterministic,
so the same situation always produces the same ID and DynamoDB stores it only once.
"""
from datetime import datetime, timedelta, timezone

HARD_BRAKE_MPS2 = 4.0
EMERGENCY_BRAKE_MPS2 = 7.0
EMERGENCY_BUCKET_S = 300          # one emergency alert per vehicle and destination every 5 minutes


def _iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _alert(ev, alert_id, alert_type, severity, zona, message, valid_min, now):
    loc = ev["location"]
    return {
        "alert_id": alert_id, "alert_type": alert_type, "origin": "cloud", "severity": severity,
        "created_time": _iso(now), "valid_until": _iso(now + timedelta(minutes=valid_min)),
        "zona": zona, "location": {"segment_id": loc["segment_id"], "rsu_id": loc["rsu_id"]},
        "cause_event_ids": [ev["event_id"]], "message": message, "schema_version": "1.0",
    }


def evaluate(ev, now=None):
    """Return the list of alerts that this event raises (possibly empty)."""
    now = now or datetime.now(timezone.utc)
    p, loc, t = ev["payload"], ev["location"], ev["event_type"]
    seg, rsu = loc["segment_id"], loc["rsu_id"]
    out = []

    # Sudden braking (D1 rule): only the RSU of the zone needs it.
    if t == "brake" and float(p["decel_mps2"]) >= HARD_BRAKE_MPS2:
        sev = 2 if float(p["decel_mps2"]) >= EMERGENCY_BRAKE_MPS2 else 1
        out.append(_alert(ev, f"brake:{ev['event_id']}", "sudden_braking", sev, rsu,
                          f"Sudden braking by {ev['source_id']} on {seg}", 2, now))

    # Crash: maximum severity, RSU of the zone.
    elif t == "crash":
        out.append(_alert(ev, f"crash:{ev['event_id']}", "crash", 3, rsu,
                          f"Crash of {ev['source_id']} on {seg}", 10, now))

    # Emergency-vehicle priority: its route crosses both zones -> broadcast to both RSUs.
    elif t == "emergency":
        route = list(p["route_segments"]) or [seg]
        event_ts = datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00")).timestamp()
        bucket = int(event_ts // EMERGENCY_BUCKET_S)
        out.append(_alert(ev, f"emergency:{ev['source_id']}:{route[-1]}:{bucket}", "emergency_priority", 3,
                          "broadcast",
                          f"{p['vehicle_class']} {ev['source_id']} approaching via {'-'.join(route)}, "
                          f"ETA {p['eta_s']} s: give way", 5, now))
    return out
