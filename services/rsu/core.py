"""RSU edge logic. Pure business code + a durable SQLite outbox.

Inspired by the user's Edge Analytics ventanas.py: fixed, non-overlapping
EVENT-TIME tumbling windows. The sensor's (second, temperature) becomes
(event_time, segment_id, speed_mps); the aggregate is V2X density.

All inbound MQTT QoS1 messages are atomically recorded before ACK by the app.
SQS delivery is AT LEAST ONCE; event_id allows downstream idempotency.
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import jsonschema

CRITICAL = frozenset({"brake", "crash", "hazard", "emergency"})
VEHICLE_EVENTS = CRITICAL | {"position"}
RSU_EVENTS = {"density", "collision_risk"}
ROAD_LENGTH_M = 1200.0
CAR_LENGTH_M = 4.5
SEGMENT_LENGTH_M = 100.0
LANES = 2


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def compact(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def destination_topic(rsu_id: str, alert: dict) -> str:
    if alert.get("zona") == "broadcast" or alert.get("location", {}).get("segment_id") in (None, "all"):
        return f"v2x/{rsu_id}/alerts/all"
    return f"v2x/{rsu_id}/alerts/{alert['location']['segment_id']}"


class EdgeStore:
    """SQLite-serialized state machine; safe to call from MQTT, SNS and SQS threads."""

    def __init__(self, db_path, rsu_id: str, schema_path, window_s=5, lateness_s=2,
                 ttc_threshold_s=2.0, alert_cooldown_s=15, position_age_s=4):
        if rsu_id not in ("rsu-1", "rsu-2"):
            raise ValueError("RSU_ID must be rsu-1 or rsu-2")
        if window_s <= 0 or lateness_s < 0:
            raise ValueError("window_s > 0 and lateness_s >= 0 required")
        self.rsu_id = rsu_id
        self.window_s = int(window_s)
        self.lateness_s = float(lateness_s)
        self.ttc_threshold_s = float(ttc_threshold_s)
        self.alert_cooldown_s = float(alert_cooldown_s)
        self.position_age_s = float(position_age_s)
        self.schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        self.validator = jsonschema.Draft202012Validator(self.schema)
        alert_schema_path = Path(schema_path).with_name("v2x-alert.schema.json")
        self.alert_validator = jsonschema.Draft202012Validator(
            json.loads(alert_schema_path.read_text(encoding="utf-8")))
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level="DEFERRED", timeout=10)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._create_tables()

    def _create_tables(self):
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS seen_events (event_id TEXT PRIMARY KEY, received_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS windows (
            segment_id TEXT NOT NULL, window_start INTEGER NOT NULL,
            sample_count INTEGER NOT NULL, speed_sum REAL NOT NULL, vehicles_json TEXT NOT NULL,
            PRIMARY KEY(segment_id, window_start));
          CREATE TABLE IF NOT EXISTS positions (
            source_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, event_epoch REAL NOT NULL,
            segment_id TEXT NOT NULL, lane INTEGER NOT NULL, s_m REAL NOT NULL,
            speed_mps REAL NOT NULL, arrived REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS outbox (
            event_id TEXT PRIMARY KEY, body TEXT NOT NULL, priority INTEGER NOT NULL,
            queued_at REAL NOT NULL, next_try REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
          CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(next_try, priority, queued_at);
          CREATE TABLE IF NOT EXISTS outbox_audit (
            event_id TEXT PRIMARY KEY, event_time TEXT NOT NULL, event_type TEXT NOT NULL,
            priority INTEGER NOT NULL, queued_at REAL NOT NULL, sent_at REAL);
          CREATE INDEX IF NOT EXISTS idx_audit_queued ON outbox_audit(queued_at);
          CREATE TABLE IF NOT EXISTS mqtt_outbox (
            alert_id TEXT PRIMARY KEY, topic TEXT NOT NULL, body TEXT NOT NULL,
            origin TEXT NOT NULL, queued_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS alert_cooldowns (pair_key TEXT PRIMARY KEY, emitted_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS seen_sns (alert_id TEXT PRIMARY KEY, received_at REAL NOT NULL);
        """)

    def _inc(self, name, amount=1):
        self.db.execute("INSERT INTO counters(name,value) VALUES(?,?) "
                        "ON CONFLICT(name) DO UPDATE SET value=value+excluded.value", (name, int(amount)))

    def _next_seq(self) -> int:
        """Persistent, gap-free (for committed events), per-RSU sequence number.

        Must only be used inside the same SQLite transaction as the generated event.
        """
        row = self.db.execute(
            "INSERT INTO counters(name,value) VALUES('rsu_event_seq',0) "
            "ON CONFLICT(name) DO UPDATE SET value=value+1 RETURNING value"
        ).fetchone()
        return int(row["value"])

    def _enqueue(self, event: dict, now: float, priority: int):
        self.db.execute("INSERT OR IGNORE INTO outbox(event_id,body,priority,queued_at,next_try) "
                        "VALUES(?,?,?,?,?)", (event["event_id"], compact(event), priority, now, now))
        self.db.execute("INSERT OR IGNORE INTO outbox_audit(event_id,event_time,event_type,priority,queued_at) "
                        "VALUES(?,?,?,?,?)", (event["event_id"], event["event_time"],
                                           event["event_type"], priority, now))

    def _mqtt_enqueue(self, alert: dict, now: float):
        self.db.execute("INSERT OR IGNORE INTO mqtt_outbox(alert_id,topic,body,origin,queued_at) "
                        "VALUES(?,?,?,?,?)", (alert["alert_id"], destination_topic(self.rsu_id, alert),
                                               compact(alert), alert["origin"], now))

    def _in_zone(self, segment: str):
        if len(segment) != 3 or segment[0] != 'S' or not segment[1:].isdigit():
            return False
        number = int(segment[1:])
        return (0 <= number <= 5) if self.rsu_id == 'rsu-1' else (6 <= number <= 11)

    def _validate(self, topic, event):
        parts = topic.split("/")
        if len(parts) != 5 or parts[0:2] != ["v2x", self.rsu_id]:
            raise ValueError("unexpected MQTT topic")
        seg, source, kind = parts[2:]
        if not self._in_zone(seg) or event["location"]["rsu_id"] != self.rsu_id:
            raise ValueError("wrong RSU or segment")
        if event["location"]["segment_id"] != seg or event["source_id"] != source:
            raise ValueError("topic does not match event body")
        if event["event_type"] != kind or kind not in VEHICLE_EVENTS:
            raise ValueError("vehicles cannot publish derived events")
        self.validator.validate(event)
        if event["source_type"] not in ("vehicle", "emergency_vehicle"):
            raise ValueError("expected vehicle source")
        if kind == "position" and ("s_m" not in event["payload"] or "speed_mps" not in event["payload"]):
            raise ValueError("position needs s_m and speed_mps to compute TTC")

    def process(self, topic: str, event: dict, *, now=None, wire_bytes=None) -> str:
        """Validate, deduplicate, aggregate or enqueue. Commit before MQTT ACK."""
        now = time.time() if now is None else float(now)
        with self.lock:
            try:
                self._validate(topic, event)
            except (ValueError, KeyError, TypeError, jsonschema.ValidationError):
                with self.db:
                    self._inc("mqtt_invalid")
                    self._inc("mqtt_received")
                return "invalid"
            with self.db:
                self._inc("mqtt_received")
                self._inc("mqtt_rx_bytes", wire_bytes if wire_bytes is not None else len(compact(event).encode()))
                cursor = self.db.execute("INSERT OR IGNORE INTO seen_events VALUES(?,?)",
                                         (event["event_id"], now))
                if not cursor.rowcount:
                    self._inc("mqtt_duplicate")
                    return "duplicate"
                if event["event_type"] == "position":
                    t = epoch(event["event_time"])
                    start = math.floor(t / self.window_s) * self.window_s
                    if start + self.window_s + self.lateness_s < now:
                        self._inc("position_too_late")
                        return "late"
                    if t > now + 120:
                        self._inc("position_future")
                        return "future"
                    self._accept_position(event, start, t, now)
                    self._inc("position_accepted")
                else:
                    forwarded = json.loads(compact(event))
                    forwarded.setdefault("trace", {}).update({"rsu_received_time": iso(now), "aggregated": False, "rsu_id": self.rsu_id})
                    self._enqueue(forwarded, now, priority=0)
                    self._inc("critical_enqueued")
        return "accepted"

    def _accept_position(self, ev, start, event_epoch, now):
        seg = ev["location"]["segment_id"]
        row = self.db.execute("SELECT * FROM windows WHERE segment_id=? AND window_start=?",
                              (seg, start)).fetchone()
        veh_map = json.loads(row["vehicles_json"]) if row else {}
        veh_map[ev["source_id"]] = 1
        speed = float(ev["payload"]["speed_mps"])
        if row:
            self.db.execute("UPDATE windows SET sample_count=sample_count+1, speed_sum=speed_sum+?, "
                            "vehicles_json=? WHERE segment_id=? AND window_start=?",
                            (speed, compact(veh_map), seg, start))
        else:
            self.db.execute("INSERT INTO windows VALUES(?,?,?,?,?)",
                            (seg, start, 1, speed, compact(veh_map)))
        old = self.db.execute("SELECT event_epoch FROM positions WHERE source_id=?", (ev["source_id"],)).fetchone()
        if old and old["event_epoch"] > event_epoch:
            return                       # event-time reordering: keep freshest position for safety
        self.db.execute("INSERT INTO positions VALUES(?,?,?,?,?,?,?,?) "
                        "ON CONFLICT(source_id) DO UPDATE SET event_id=excluded.event_id, "
                        "event_epoch=excluded.event_epoch,segment_id=excluded.segment_id,lane=excluded.lane,"
                        "s_m=excluded.s_m,speed_mps=excluded.speed_mps,arrived=excluded.arrived",
                        (ev["source_id"], ev["event_id"], event_epoch, seg,
                         int(ev["location"].get("lane", 0)), float(ev["payload"]["s_m"]), speed, now))
        self._detect_ttc(ev, now)

    def _detect_ttc(self, ev, now):
        """Same-lane car-following TTC (not crossing-trajectory prediction)."""
        own = ev["source_id"]
        lane = int(ev["location"].get("lane", 0))
        candidates = list(self.db.execute("SELECT * FROM positions WHERE lane=? AND arrived>=?",
                                          (lane, now - self.position_age_s)))
        for i in candidates:
            for j in candidates:
                if i["source_id"] == j["source_id"]:
                    continue
                # Must share the currently served zone, excluding stale position records
                if not self._in_zone(i["segment_id"]) or not self._in_zone(j["segment_id"]):
                    continue
                if own not in (i["source_id"], j["source_id"]):
                    continue
                gap = (j["s_m"] - i["s_m"]) % ROAD_LENGTH_M - CAR_LENGTH_M
                closing = i["speed_mps"] - j["speed_mps"]
                if not (0 < gap < 60 and closing > 0):
                    continue
                ttc = gap / closing
                if ttc >= self.ttc_threshold_s:
                    continue
                key = f"{i['source_id']}:{j['source_id']}"
                prev = self.db.execute("SELECT emitted_at FROM alert_cooldowns WHERE pair_key=?", (key,)).fetchone()
                if prev and now - prev["emitted_at"] < self.alert_cooldown_s:
                    continue
                self.db.execute("INSERT INTO alert_cooldowns(pair_key,emitted_at) VALUES(?,?) "
                                "ON CONFLICT(pair_key) DO UPDATE SET emitted_at=excluded.emitted_at", (key, now))
                seg = i["segment_id"]
                aid = f"collision:{self.rsu_id}:{key}:{int(now * 1000)}"
                alert = {
                    "alert_id": aid, "alert_type": "collision_risk", "origin": self.rsu_id,
                    "severity": 3, "created_time": iso(now),
                    "valid_until": iso(now + 60), "zona": self.rsu_id,
                    "schema_version": "1.0",
                    "location": {"segment_id": seg, "rsu_id": self.rsu_id},
                    "cause_event_ids": [i["event_id"], j["event_id"]],
                    "message": f"Collision risk: {i['source_id']} approaching {j['source_id']}; TTC={ttc:.2f}s",
                }
                self.alert_validator.validate(alert)
                self._mqtt_enqueue(alert, now)
                risk = {
                    "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, aid)),
                    "source_id": self.rsu_id, "source_type": "rsu",
                    "event_type": "collision_risk", "event_time": iso(now),
                    "location": {"segment_id": seg, "rsu_id": self.rsu_id, "lane": lane},
                    "seq": self._next_seq(), "schema_version": "1.1",
                    "payload": {"pair": [i["source_id"], j["source_id"]],
                                "ttc_s": round(ttc, 3), "gap_m": round(gap, 3),
                                "closing_speed_mps": round(closing, 3)},
                    "trace": {"rsu_received_time": iso(now), "aggregated": False,
                              "rsu_id": self.rsu_id},
                }
                self.validator.validate(risk)
                self._enqueue(risk, now, priority=0)
                self._inc("local_alerts_generated")
                return    # a single most urgent alert per received position

    def emit_due_windows(self, *, now=None, force=False):
        now = time.time() if now is None else float(now)
        count = 0
        with self.lock, self.db:
            where = "" if force else "WHERE window_start + ? + ? <= ?"
            args = () if force else (self.window_s, self.lateness_s, now)
            rows = list(self.db.execute(f"SELECT * FROM windows {where} ORDER BY window_start,segment_id", args))
            for row in rows:
                start, seg = row["window_start"], row["segment_id"]
                distinct = len(json.loads(row["vehicles_json"]))
                mean = row["speed_sum"] / row["sample_count"]
                ev = {
                    "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"density:{self.rsu_id}:{seg}:{start}:{self.window_s}")),
                    "source_id": self.rsu_id, "source_type": "rsu", "event_type": "density",
                    "event_time": iso(start + self.window_s), "schema_version": "1.1",
                    "seq": self._next_seq(),
                    "location": {"segment_id": seg, "rsu_id": self.rsu_id},
                    "payload": {"vehicle_count": distinct, "sample_count": row["sample_count"],
                                "mean_speed_mps": round(mean, 3),
                                "occupancy": round(min(1.0, distinct * CAR_LENGTH_M / (SEGMENT_LENGTH_M * LANES)), 3),
                                "window_s": self.window_s,
                                "window_start": iso(start), "window_end": iso(start + self.window_s)},
                    "trace": {"rsu_received_time": iso(now), "aggregated": True,
                              "aggregated_count": row["sample_count"], "rsu_id": self.rsu_id},
                }
                self.validator.validate(ev)
                self._enqueue(ev, now, priority=10)
                self.db.execute("DELETE FROM windows WHERE segment_id=? AND window_start=?", (seg, start))
                self._inc("windows_emitted")
                count += 1
        return count

    def next_outbox(self, *, now=None):
        now = time.time() if now is None else float(now)
        with self.lock:
            r = self.db.execute("SELECT * FROM outbox WHERE next_try <= ? "
                                "ORDER BY priority ASC,queued_at ASC,event_id ASC LIMIT 1", (now,)).fetchone()
            return dict(r) if r else None

    def sent(self, item, *, now=None):
        now = time.time() if now is None else float(now)
        with self.lock, self.db:
            cur = self.db.execute("DELETE FROM outbox WHERE event_id=?", (item["event_id"],))
            if cur.rowcount:
                self.db.execute("UPDATE outbox_audit SET sent_at=? WHERE event_id=?", (now, item["event_id"]))
                self._inc("sqs_sent")
                self._inc("sqs_bytes", len(item["body"].encode()))
                self._inc("sqs_critical_sent" if item["priority"] == 0 else "sqs_density_sent")
                self.db.execute("INSERT OR REPLACE INTO counters VALUES('last_sqs_success_epoch',?)", (int(now),))

    def failed(self, item, *, now=None, backoff_base=1, backoff_max=60):
        now = time.time() if now is None else float(now)
        with self.lock, self.db:
            attempts = item["attempts"] + 1
            delay = min(backoff_max, backoff_base * 2 ** min(attempts - 1, 16))
            self.db.execute("UPDATE outbox SET attempts=?,next_try=? WHERE event_id=?",
                            (attempts, now + delay, item["event_id"]))
            self._inc("sqs_failures")

    def next_mqtt_alert(self):
        with self.lock:
            row = self.db.execute("SELECT * FROM mqtt_outbox ORDER BY queued_at LIMIT 1").fetchone()
            return dict(row) if row else None

    def mqtt_alert_sent(self, alert):
        with self.lock, self.db:
            cur = self.db.execute("DELETE FROM mqtt_outbox WHERE alert_id=?", (alert["alert_id"],))
            if cur.rowcount:
                self._inc("mqtt_local_alert_sent" if alert["origin"] == self.rsu_id else "mqtt_cloud_alert_sent")
                self._inc("mqtt_alert_bytes", len(alert["body"].encode()))

    def receive_sns_alert(self, alert: dict, *, now=None) -> str:
        """Called only AFTER trusted SNS signature validation."""
        now = time.time() if now is None else float(now)
        try:
            self.alert_validator.validate(alert)
        except (jsonschema.ValidationError, TypeError):
            return "invalid"
        if alert.get("location", {}).get("rsu_id") not in (self.rsu_id, "broadcast", None):
            if alert.get("zona") != "broadcast":
                return "wrong_zone"
        if alert.get("zona") not in (self.rsu_id, "broadcast"):
            return "wrong_zone"
        with self.lock, self.db:
            cur = self.db.execute("INSERT OR IGNORE INTO seen_sns VALUES(?,?)", (alert["alert_id"], now))
            if not cur.rowcount:
                self._inc("sns_duplicates")
                return "duplicate"
            self._mqtt_enqueue(alert, now)
            self._inc("sns_alerts_received")
            return "accepted"

    def metrics(self, *, now=None):
        with self.lock:
            counters = {r["name"]: r["value"] for r in self.db.execute("SELECT * FROM counters")}
            counts = {
                "outbox_pending": self.db.execute("SELECT count(*) FROM outbox").fetchone()[0],
                "outbox_critical_pending": self.db.execute("SELECT count(*) FROM outbox WHERE priority=0").fetchone()[0],
                "outbox_density_pending": self.db.execute("SELECT count(*) FROM outbox WHERE priority=10").fetchone()[0],
                "mqtt_alerts_pending": self.db.execute("SELECT count(*) FROM mqtt_outbox").fetchone()[0],
                "windows_pending": self.db.execute("SELECT count(*) FROM windows").fetchone()[0],
                "seen_event_ids": self.db.execute("SELECT count(*) FROM seen_events").fetchone()[0],
            }
            return {"rsu_id": self.rsu_id, "window_s": self.window_s, "allowed_lateness_s": self.lateness_s,
                    **counters, **counts}

    def audit(self, since: float, limit: int = 10000):
        with self.lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM outbox_audit WHERE queued_at>=? ORDER BY queued_at,event_id LIMIT ?",
                (float(since), min(max(int(limit),1),10000)))]

    def close(self):
        with self.lock:
            self.db.close()
