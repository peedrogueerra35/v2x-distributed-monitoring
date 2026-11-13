"""Simulation loop: applies scenario injections, advances the IDM model, emits schema-valid events."""
import hashlib
import random
import time
import uuid
from datetime import datetime, timezone

from . import mobility
from .faults import FaultInjector
from .road import N_SEGMENTS, Road

DT = 0.1                 # integration step (s)
HARD_BRAKE = 4.0         # m/s^2: a brake event is emitted above this deceleration
BRAKE_REFRACTORY = 2.0   # s between two brake events of the same vehicle


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Simulator:
    def __init__(self, cfg: dict, transport, seed: int = 42, realtime: bool = True, time_scale: float = 1.0,
                 rate_scale: float = 1.0, burst=None, faults=None, groundtruth=None, fault_log=None,
                 start_wall: float = None, generate_density: bool = True):
        self.cfg = cfg
        self.generate_density = generate_density
        self.transport = transport
        self.seed = seed
        self.realtime = realtime
        self.time_scale = time_scale
        self.rate_scale = rate_scale
        self.burst = burst                        # (start_s, end_s, factor) or None
        self.gt = groundtruth or (lambda r: None)
        self.start_wall = start_wall if start_wall is not None else time.time()
        self.road = Road(cfg["segment_length_m"])
        self.rng = {name: random.Random(self._sub_seed(name))
                    for name in ("mobility", "ids", "hazard", "drop", "dup", "delay", "reorder")}
        f = faults or {}
        self.faults = FaultInjector(self.rng, drop=f.get("drop", 0.0), dup=f.get("dup", 0.0),
                                    delay_ms=f.get("delay_ms", (0, 0)),
                                    reorder_window=f.get("reorder_window", 0), log=fault_log)
        self.vehicles = self._place_vehicles(cfg["vehicles"])
        self.seq = {}
        self.next_emit = {}
        self.braking = {}                         # vid -> (in_episode, last_brake_t, speed_before)
        self.hazards = []                         # active hazard injections
        self.crash_clear = {}                     # vid -> time to clear
        self.pending_injections = list(cfg["injections"])
        self.stats = {"generated": 0, "by_type": {}}
        self.t = 0.0

    # ---------- setup ----------
    def _sub_seed(self, name):
        return int(hashlib.sha256(f"{self.seed}:{name}".encode()).hexdigest()[:16], 16)

    def _place_vehicles(self, n):
        L, rng = self.road.length_m, self.rng["mobility"]
        seg = self.cfg["segment_length_m"]
        if n <= 5:   # fixed layout: veh 1-3 in rsu-1's zone (lane 0), veh 4-5 in rsu-2's zone (lane 1)
            layout = [(0, 0.8), (0, 1.6), (0, 2.4), (1, 7.0), (1, 7.8)][:n]
            spots = [(lane, frac * seg) for lane, frac in layout]
        else:        # spread evenly over both lanes
            spots = [(i % 2, (i // 2) * L / ((n + 1) // 2)) for i in range(n)]
        return [mobility.Vehicle(vid=f"veh-{i + 1:04d}", lane=lane, s=s, v=20.0,
                                 v0=30.0 * rng.uniform(0.9, 1.1)) for i, (lane, s) in enumerate(spots)]

    # ---------- events ----------
    def _now(self):
        return time.time() if self.realtime else self.start_wall + self.t

    def _event(self, source_id, source_type, event_type, s, lane, payload):
        self.seq[source_id] = self.seq.get(source_id, -1) + 1
        x, y = self.road.xy(s)
        ev = {
            "event_id": str(uuid.UUID(int=self.rng["ids"].getrandbits(128), version=4)),
            "source_id": source_id, "source_type": source_type, "seq": self.seq[source_id],
            "event_type": event_type, "event_time": iso(self._now()),
            "location": {"segment_id": self.road.segment_id(s), "rsu_id": self.road.rsu_id(s),
                         "lane": lane, "x": x, "y": y},
            "payload": payload, "schema_version": "1.1", "sim_time_s": round(self.t, 2),
        }
        self.stats["generated"] += 1
        self.stats["by_type"][event_type] = self.stats["by_type"].get(event_type, 0) + 1
        return ev

    def _truth(self, kind, **fields):
        self.gt({"sim_time_s": round(self.t, 2), "event_time": iso(self._now()), "kind": kind, **fields})

    def _rate(self, kind):
        factor = self.rate_scale
        if self.burst and self.burst[0] <= self.t < self.burst[1]:
            factor *= self.burst[2]
        return self.cfg["rate_hz"][kind] * factor

    def _periodic(self, key, kind):
        """True when source `key` must emit a periodic `kind` event now."""
        due = self.next_emit.get(key, 0.0)
        if self.t + 1e-9 >= due:
            self.next_emit[key] = due + 1.0 / self._rate(kind)
            return True
        return False

    # ---------- scenario ----------
    def _apply_injections(self, out):
        while self.pending_injections and self.pending_injections[0]["t"] <= self.t + 1e-9:
            inj = self.pending_injections.pop(0)
            kind = inj["kind"]
            if kind == "crash":
                veh = self._vehicle(inj["vehicle"])
                if veh is None:
                    continue
                veh.stopped, veh.v = True, 0.0
                out.append(self._event(veh.vid, veh.source_type, "crash", veh.s, veh.lane,
                                       {"severity": int(inj.get("severity", 3))}))
                self._truth("crash", source_id=veh.vid, segment_id=self.road.segment_id(veh.s))
                if "until" in inj:
                    self.crash_clear[veh.vid] = inj["until"]
            elif kind == "hazard":
                self.hazards.append({**inj, "reported": set()})
                self._truth("hazard_start", segment_id=inj["segment"], hazard_type=inj["hazard_type"])
            elif kind == "emergency":
                start = self.road.segment_start(inj["start"])
                route = inj["route"]
                end = self.road.segment_start(route[-1]) + self.cfg["segment_length_m"]
                ev = mobility.Vehicle(vid="ev-0001", lane=int(inj.get("lane", 1)), s=start + 1.0, v=20.0, v0=40.0,
                                      source_type="emergency_vehicle",
                                      extra={"route": route, "class": inj.get("vehicle_class", "ambulance"),
                                             "remaining": self.road.wrap(end - start)})
                self.vehicles.append(ev)
                self._truth("emergency_start", source_id=ev.vid, route=route)

    def _vehicle(self, vid):
        return next((v for v in self.vehicles if v.vid == vid and v.active), None)

    def _active_hazard(self, segment_id):
        return next((h for h in self.hazards
                     if h["segment"] == segment_id and h["t"] <= self.t < h.get("until", float("inf"))), None)

    def _v0(self, veh):
        hz = self._active_hazard(self.road.segment_id(veh.s))
        return min(veh.v0, 15.0) if hz else veh.v0

    # ---------- main loop ----------
    def step(self):
        out = []
        self._apply_injections(out)
        for vid, until in list(self.crash_clear.items()):
            if self.t >= until:
                veh = self._vehicle(vid)
                if veh:
                    veh.stopped = False
                self._truth("crash_cleared", source_id=vid)
                del self.crash_clear[vid]

        before = {v.vid: (v.v, self.road.segment_id(v.s)) for v in self.vehicles if v.active}
        mobility.step(self.vehicles, self.road, DT, self._v0)

        for veh in [v for v in self.vehicles if v.active]:
            v_before, seg_before = before.get(veh.vid, (veh.v, self.road.segment_id(veh.s)))
            seg = self.road.segment_id(veh.s)
            # hard braking
            in_ep, last_t, _ = self.braking.get(veh.vid, (False, -1e9, 0.0))
            if veh.accel <= -HARD_BRAKE:
                if not in_ep and self.t - last_t >= BRAKE_REFRACTORY:
                    out.append(self._event(veh.vid, veh.source_type, "brake", veh.s, veh.lane,
                                           {"decel_mps2": round(-veh.accel, 2),
                                            "speed_before_mps": round(v_before, 2)}))
                    self._truth("hard_brake", source_id=veh.vid, segment_id=seg, decel_mps2=round(-veh.accel, 2))
                    self.braking[veh.vid] = (True, self.t, v_before)
                else:
                    self.braking[veh.vid] = (True, last_t, v_before)
            else:
                self.braking[veh.vid] = (False, last_t, 0.0)
            # hazard reports when entering an active hazard segment
            hz = self._active_hazard(seg)
            if hz and seg != seg_before and veh.vid not in hz["reported"]:
                hz["reported"].add(veh.vid)
                if self.rng["hazard"].random() < hz.get("report_p", 0.5):
                    out.append(self._event(veh.vid, veh.source_type, "hazard", veh.s, veh.lane,
                                           {"hazard_type": hz["hazard_type"], "severity": int(hz.get("severity", 2)),
                                            "confidence": round(self.rng["hazard"].uniform(0.6, 0.95), 2)}))
            # emergency vehicle: progress along its route
            if veh.source_type == "emergency_vehicle":
                veh.extra["remaining"] -= veh.v * DT
                if veh.extra["remaining"] <= 0:
                    veh.active = False
                    self._truth("emergency_end", source_id=veh.vid)
                    continue
                if self._periodic(veh.vid + ":emergency", "emergency"):
                    remaining_route = veh.extra["route"][veh.extra["route"].index(seg):] if seg in veh.extra["route"] \
                        else veh.extra["route"]
                    out.append(self._event(veh.vid, veh.source_type, "emergency", veh.s, veh.lane,
                                           {"vehicle_class": veh.extra["class"], "route_segments": remaining_route,
                                            "eta_s": round(veh.extra["remaining"] / max(veh.v, 0.1), 1)}))
            # periodic position
            if self._periodic(veh.vid + ":position", "position"):
                out.append(self._event(veh.vid, veh.source_type, "position", veh.s, veh.lane,
                                       {"speed_mps": round(veh.v, 2), "accel_mps2": round(veh.accel, 2),
                                        "heading_deg": self.road.heading_deg(veh.s),
                                        "s_m": round(veh.s, 3), "length_m": veh.length}))

        # D1 SQS backward compatibility only. In D2 MQTT, density is derived at the edge.
        if self.generate_density:
            for rsu, segs in (("rsu-1", range(0, N_SEGMENTS // 2)), ("rsu-2", range(N_SEGMENTS // 2, N_SEGMENTS))):
                if self._periodic(rsu + ":density", "rsu_density"):
                    for idx in segs:
                        s_mid = (idx + 0.5) * self.cfg["segment_length_m"]
                        here = [v for v in self.vehicles if v.active and self.road.segment_index(v.s) == idx]
                        occ = sum(v.length for v in here) / (self.cfg["segment_length_m"] * 2)
                        out.append(self._event(rsu, "rsu", "density", s_mid, 0,
                                               {"vehicle_count": len(here),
                                                "mean_speed_mps": round(sum(v.v for v in here) / len(here), 2) if here else 0.0,
                                                "occupancy": round(min(occ, 1.0), 3),
                                                "window_s": round(1.0 / self._rate("rsu_density"), 1)}))
        return out

    def run(self):
        steps = int(round(self.cfg["duration_s"] / DT))
        for n in range(steps):
            self.t = n * DT
            if self.realtime:
                target = self.start_wall + self.t / self.time_scale
                delay = target - time.time()
                if delay > 0:
                    time.sleep(delay)
            now = self._now()
            for ev in self.step():
                self.faults.submit(ev, now)
            ready = self.faults.due(now)
            if ready:
                self.transport.send(ready)
        rest = self.faults.flush()
        if rest:
            self.transport.send(rest)
        self.transport.close()
        return {**self.stats, **self.faults.stats}
