"""In-memory live view for the dashboard: latest state of the road, built from the events V2X-SERVICE stores.

The cloud does not receive every vehicle position (the RSUs aggregate them into per-segment `density`
summaries every 5 s), so the live view shows exactly what the cloud knows:
  - per segment: vehicles, mean speed and occupancy from the latest RSU summary;
  - exact positions of critical events (brake, crash, hazard) and of emergency vehicles;
  - RSU activity and recent throughput.
Pure Python, no AWS calls, thread-safe; unit-tested without the service.
"""
import threading
import time
from collections import deque
from datetime import datetime

SEGMENTS = [f"S{i:02d}" for i in range(12)]
MARKER_TTL_S = 120          # incidents stay on the map for 2 minutes
EMERGENCY_TTL_S = 10        # an emergency vehicle disappears 10 s after its last report
BUCKET_S = 5                # throughput histogram resolution
BUCKETS = 60                # 60 x 5 s = last 5 minutes
MARKER_TYPES = {"brake", "crash", "hazard", "collision_risk"}


def _epoch(iso_time):
    return datetime.fromisoformat(str(iso_time).replace("Z", "+00:00")).timestamp()


def _num(v):
    return None if v is None else float(v)


class LiveView:
    def __init__(self, clock=time.time):
        self.clock = clock
        self.lock = threading.Lock()
        self.segments = {}          # segment -> latest density summary
        self.emergency = {}         # source_id -> latest emergency report
        self.markers = deque(maxlen=200)
        self.recent = deque(maxlen=40)
        self.rsus = {}              # rsu_id -> {"last": epoch, "events": n}
        self.buckets = {}           # bucket start epoch -> count

    def update(self, ev):
        """Record one stored (non-duplicate) event."""
        now = self.clock()
        loc, p, etype = ev["location"], ev.get("payload", {}), ev["event_type"]
        seg, rsu = loc["segment_id"], loc["rsu_id"]
        with self.lock:
            r = self.rsus.setdefault(rsu, {"last": 0.0, "events": 0})
            r["last"], r["events"] = now, r["events"] + 1
            b = int(now // BUCKET_S * BUCKET_S)
            self.buckets[b] = self.buckets.get(b, 0) + 1
            for old in [k for k in self.buckets if k < b - BUCKET_S * BUCKETS]:
                del self.buckets[old]
            self.recent.appendleft({"time": ev["event_time"], "type": etype, "source": ev["source_id"],
                                    "segment": seg, "rsu": rsu})
            if etype == "density":
                self.segments[seg] = {
                    "segment_id": seg, "rsu_id": rsu,
                    "vehicle_count": int(p.get("vehicle_count", 0)),
                    "mean_speed_mps": _num(p.get("mean_speed_mps")),
                    "occupancy": _num(p.get("occupancy")),
                    "window_end": p.get("window_end", ev["event_time"]), "updated": now}
            elif etype == "emergency":
                self.emergency[ev["source_id"]] = {
                    "source_id": ev["source_id"], "segment_id": seg, "rsu_id": rsu,
                    "x": _num(loc.get("x")), "y": _num(loc.get("y")), "lane": loc.get("lane"),
                    "eta_s": _num(p.get("eta_s")), "route": list(p.get("route_segments", [])),
                    "vehicle_class": p.get("vehicle_class"), "updated": now}
            if etype in MARKER_TYPES:
                detail = {"brake": p.get("decel_mps2"), "crash": p.get("severity"),
                          "hazard": p.get("hazard_type"), "collision_risk": p.get("ttc_s")}[etype]
                self.markers.appendleft({
                    "type": etype, "source_id": ev["source_id"], "segment_id": seg, "rsu_id": rsu,
                    "x": _num(loc.get("x")), "y": _num(loc.get("y")), "lane": loc.get("lane"),
                    "pair": list(p.get("pair", [])),
                    "detail": _num(detail) if isinstance(detail, (int, float)) or hasattr(detail, "as_tuple") else detail,
                    "time": ev["event_time"], "updated": now})

    def snapshot(self):
        now = self.clock()
        with self.lock:
            segments = []
            for s in SEGMENTS:
                d = self.segments.get(s)
                segments.append(dict(d, age_s=round(now - d["updated"], 1)) if d else
                                {"segment_id": s, "rsu_id": "rsu-1" if int(s[1:]) < 6 else "rsu-2",
                                 "vehicle_count": None, "age_s": None})
            emergency = [dict(e, age_s=round(now - e["updated"], 1)) for e in self.emergency.values()
                         if now - e["updated"] <= EMERGENCY_TTL_S]
            markers = [dict(m, age_s=round(now - m["updated"], 1)) for m in self.markers
                       if now - m["updated"] <= MARKER_TTL_S]
            rsus = {k: {"events": v["events"], "last_event_age_s": round(now - v["last"], 1)}
                    for k, v in self.rsus.items()}
            b_now = int(now // BUCKET_S * BUCKET_S)
            throughput = [self.buckets.get(b_now - BUCKET_S * i, 0) for i in range(BUCKETS - 1, -1, -1)]
            return {"server_time": now, "segments": segments, "emergency": emergency, "markers": markers,
                    "rsus": rsus, "recent": list(self.recent), "throughput": throughput,
                    "bucket_s": BUCKET_S}
