"""Tests of the dashboard live view (services/v2x-service/live.py); no AWS needed."""
import importlib.util
import json
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("live", ROOT / "services" / "v2x-service" / "live.py")
live = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(live)


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def ev(event_type, payload, seg="S03", rsu="rsu-1", source="veh-0001", x="10.5", y="-3.2"):
    return {"event_id": f"id-{event_type}-{seg}", "source_id": source, "event_type": event_type,
            "event_time": "2026-11-15T10:00:00.000Z",
            "location": {"segment_id": seg, "rsu_id": rsu, "lane": 0, "x": Decimal(x), "y": Decimal(y)},
            "payload": payload}


def test_density_fills_its_segment_and_others_stay_empty():
    v = live.LiveView(Clock())
    v.update(ev("density", {"vehicle_count": 3, "mean_speed_mps": Decimal("24.5"), "occupancy": Decimal("0.07"),
                            "window_s": 5, "window_end": "2026-11-15T10:00:05.000Z"}, source="rsu-1"))
    snap = v.snapshot()
    seg = {s["segment_id"]: s for s in snap["segments"]}
    assert len(snap["segments"]) == 12
    assert seg["S03"]["vehicle_count"] == 3 and seg["S03"]["mean_speed_mps"] == 24.5
    assert seg["S08"]["vehicle_count"] is None and seg["S08"]["rsu_id"] == "rsu-2"


def test_emergency_vehicle_appears_and_expires():
    clock = Clock()
    v = live.LiveView(clock)
    v.update(ev("emergency", {"vehicle_class": "ambulance", "route_segments": ["S04", "S05"], "eta_s": Decimal("12.3")},
                seg="S04", source="ev-0001"))
    assert v.snapshot()["emergency"][0]["eta_s"] == 12.3
    clock.t += live.EMERGENCY_TTL_S + 1
    assert v.snapshot()["emergency"] == []


def test_incidents_become_markers_with_detail_and_expire():
    clock = Clock()
    v = live.LiveView(clock)
    v.update(ev("brake", {"decel_mps2": Decimal("7.6"), "speed_before_mps": Decimal("28")}))
    v.update(ev("collision_risk", {"pair": ["veh-0005", "veh-0004"], "ttc_s": Decimal("0.6"), "gap_m": 9},
                seg="S11", rsu="rsu-2", source="rsu-2"))
    v.update(ev("position", {"speed_mps": 20}))
    markers = v.snapshot()["markers"]
    assert [m["type"] for m in markers] == ["collision_risk", "brake"]
    assert markers[1]["detail"] == 7.6 and markers[0]["pair"] == ["veh-0005", "veh-0004"]
    clock.t += live.MARKER_TTL_S + 1
    assert v.snapshot()["markers"] == []


def test_rsu_activity_and_throughput():
    clock = Clock()
    v = live.LiveView(clock)
    for _ in range(4):
        v.update(ev("position", {"speed_mps": 20}))
    v.update(ev("position", {"speed_mps": 20}, seg="S07", rsu="rsu-2"))
    clock.t += 3
    snap = v.snapshot()
    assert snap["rsus"]["rsu-1"]["events"] == 4 and snap["rsus"]["rsu-1"]["last_event_age_s"] == 3
    assert len(snap["throughput"]) == live.BUCKETS and sum(snap["throughput"]) == 5


def test_snapshot_is_json_serializable():
    v = live.LiveView(Clock())
    v.update(ev("hazard", {"hazard_type": "oil", "severity": 2, "confidence": Decimal("0.8")}, seg="S07", rsu="rsu-2"))
    json.dumps(v.snapshot())
