"""Tests of the V2X-SERVICE alert rules and of the event/alert schemas (no AWS needed)."""
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import jsonschema

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("rules", ROOT / "services" / "v2x-service" / "rules.py")
rules = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rules)
EVENT_SCHEMA = json.loads((ROOT / "schema" / "v2x-event.schema.json").read_text())
ALERT_SCHEMA = json.loads((ROOT / "schema" / "v2x-alert.schema.json").read_text())
NOW = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)


def event(event_type, payload, rsu="rsu-1", seg="S03", source="veh-0002", source_type="vehicle"):
    ev = {"event_id": f"id-{event_type}", "source_id": source, "source_type": source_type, "seq": 1,
          "event_type": event_type, "event_time": "2026-11-10T12:00:00.000Z",
          "location": {"segment_id": seg, "rsu_id": rsu, "lane": 0, "x": 1.0, "y": 2.0},
          "payload": payload, "schema_version": "1.1"}
    jsonschema.validate(ev, EVENT_SCHEMA)
    return ev


def test_hard_brake_alerts_only_its_rsu():
    [a] = rules.evaluate(event("brake", {"decel_mps2": 5.0, "speed_before_mps": 27.0}, rsu="rsu-2", seg="S07"), NOW)
    assert (a["alert_type"], a["zona"], a["severity"]) == ("sudden_braking", "rsu-2", 1)
    jsonschema.validate(a, ALERT_SCHEMA)


def test_emergency_brake_has_severity_2():
    [a] = rules.evaluate(event("brake", {"decel_mps2": 8.5, "speed_before_mps": 30.0}), NOW)
    assert a["severity"] == 2


def test_soft_brake_and_position_raise_nothing():
    assert rules.evaluate(event("brake", {"decel_mps2": 3.9, "speed_before_mps": 20.0}), NOW) == []
    assert rules.evaluate(event("position", {"speed_mps": 25.0, "accel_mps2": 0.1, "heading_deg": 90.0}), NOW) == []


def test_emergency_vehicle_is_broadcast_to_both_rsus():
    ev = event("emergency", {"vehicle_class": "ambulance", "route_segments": ["S04", "S05", "S06", "S07"], "eta_s": 12.3},
               source="ev-0001", source_type="emergency_vehicle", seg="S04")
    [a] = rules.evaluate(ev, NOW)
    assert (a["alert_type"], a["zona"], a["severity"]) == ("emergency_priority", "broadcast", 3)
    jsonschema.validate(a, ALERT_SCHEMA)


def test_emergency_alert_id_is_stable_while_the_route_shrinks():
    first = event("emergency", {"vehicle_class": "ambulance", "route_segments": ["S04", "S05", "S06", "S07"], "eta_s": 12},
                  source="ev-0001", source_type="emergency_vehicle")
    later = event("emergency", {"vehicle_class": "ambulance", "route_segments": ["S06", "S07"], "eta_s": 4},
                  source="ev-0001", source_type="emergency_vehicle")
    assert rules.evaluate(first, NOW)[0]["alert_id"] == rules.evaluate(later, NOW)[0]["alert_id"]


def test_crash_alerts_its_rsu_with_max_severity():
    [a] = rules.evaluate(event("crash", {"severity": 3}), NOW)
    assert (a["alert_type"], a["zona"], a["severity"]) == ("crash", "rsu-1", 3)


def test_collision_risk_event_from_rsu_is_valid_and_raises_no_cloud_alert():
    ev = event("collision_risk", {"pair": ["veh-0001", "veh-0002"], "ttc_s": 1.4, "gap_m": 18.0},
               source="rsu-1", source_type="rsu")
    assert rules.evaluate(ev, NOW) == []


def test_collision_risk_must_come_from_an_rsu():
    bad = {"event_id": "x", "source_id": "veh-0001", "source_type": "vehicle", "seq": 1, "event_type": "collision_risk",
           "event_time": "2026-11-10T12:00:00Z", "location": {"segment_id": "S01", "rsu_id": "rsu-1"},
           "payload": {"pair": ["veh-0001", "veh-0002"], "ttc_s": 1.0, "gap_m": 5.0}, "schema_version": "1.1"}
    try:
        jsonschema.validate(bad, EVENT_SCHEMA)
        assert False, "collision_risk from a vehicle must be rejected"
    except jsonschema.ValidationError:
        pass
