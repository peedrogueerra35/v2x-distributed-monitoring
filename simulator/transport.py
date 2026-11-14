"""V2X event transports: MQTT (D2), legacy SQS (D1), stdout and tests."""
import json
import sys
import time


class ListTransport:
    def __init__(self):
        self.events = []

    def send(self, events):
        self.events.extend(events)

    def close(self):
        pass


class StdoutTransport:
    def send(self, events):
        for ev in events:
            sys.stdout.write(json.dumps(ev, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def close(self):
        pass


class SqsTransport:
    """Sends events to an SQS queue with SendMessageBatch (10 per call), retrying failed entries."""

    def __init__(self, queue_name="v2x-events", region="us-east-1", max_retries=3):
        import boto3  # imported here so tests and dry runs do not need AWS
        self.sqs = boto3.client("sqs", region_name=region)
        self.url = self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        self.max_retries = max_retries
        self.stats = {"sent": 0, "failed": 0, "batches": 0}

    def send(self, events):
        for i in range(0, len(events), 10):
            self._send_batch(events[i:i + 10])

    def _send_batch(self, batch):
        entries = {str(n): json.dumps(ev, separators=(",", ":")) for n, ev in enumerate(batch)}
        for attempt in range(self.max_retries + 1):
            resp = self.sqs.send_message_batch(
                QueueUrl=self.url,
                Entries=[{"Id": k, "MessageBody": body} for k, body in entries.items()])
            self.stats["batches"] += 1
            self.stats["sent"] += len(resp.get("Successful", []))
            failed = {f["Id"] for f in resp.get("Failed", [])}
            entries = {k: v for k, v in entries.items() if k in failed}
            if not entries:
                return
            time.sleep(0.2 * 2 ** attempt)
        self.stats["failed"] += len(entries)

    def close(self):
        pass




def mqtt_event_topic(event):
    """Fixed A/B topic contract: v2x/<rsu_id>/<segment>/<source_id>/<event_type>."""
    loc = event["location"]
    return f"v2x/{loc['rsu_id']}/{loc['segment_id']}/{event['source_id']}/{event['event_type']}"


class MqttTransport:
    """One MQTT OBU session per vehicle; reconnect at RSU zone boundaries.

    MQTT QoS0 position, QoS1 for critical events. Persisted client sessions
    handle broker reconnects; alert callbacks write observed end-to-end latency.
    """

    def __init__(self, broker_hosts=None, username="sim", password="", alerts_path=None,
                 connect_timeout=10, grace_s=2):
        import threading
        from datetime import datetime, timezone
        import paho.mqtt.client as mqtt

        self.mqtt = mqtt
        self.datetime = datetime
        self.timezone = timezone
        self.threading = threading
        self.hosts = broker_hosts or {"rsu-1": ("localhost", 1883), "rsu-2": ("localhost", 1884)}
        self.username = username
        self.password = password
        self.connect_timeout = connect_timeout
        self.grace_s = grace_s
        self.connections = {}
        self.lock = threading.RLock()          # protects self.connections (main thread)
        self.alert_lock = threading.Lock()     # protects stats/alert file from paho network threads
        self.alert_file = open(alerts_path, 'w', encoding='utf-8') if alerts_path else None
        self.stats = {"published": 0, "publish_failures": 0, "alerts_received": 0, "by_rsu": {}}

    def _connect(self, vehicle_id, rsu, segment):
        mqtt = self.mqtt
        ready = self.threading.Event()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=vehicle_id,
                             clean_session=False, protocol=mqtt.MQTTv311)
        client.username_pw_set(self.username, self.password)
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        current = {"rsu": rsu, "segment": segment, "client": client, "ready": ready,
                   "alert_ids": set()}

        def on_connect(c, userdata, flags, reason_code, properties):
            if reason_code == 0:
                c.subscribe([(f"v2x/{rsu}/alerts/all", 1),
                             (f"v2x/{rsu}/alerts/{current['segment']}", 1)])
                c.publish(f"v2x/{rsu}/status/{vehicle_id}", "online", qos=1, retain=True)
            else:
                ready.clear()

        def on_subscribe(c, userdata, mid, reason_codes, properties):
            if all(not reason.is_failure for reason in reason_codes):
                ready.set()

        def on_disconnect(c, userdata, flags, reason_code, properties):
            ready.clear()

        def on_message(c, userdata, msg):
            try:
                alert = json.loads(msg.payload)
                now = self.datetime.now(self.timezone.utc)
                created = self.datetime.fromisoformat(alert["created_time"].replace('Z', '+00:00'))
                valid = self.datetime.fromisoformat(alert["valid_until"].replace('Z', '+00:00'))
                if valid <= now or alert.get("alert_id") in current["alert_ids"]:
                    return
                current["alert_ids"].add(alert["alert_id"])
                record = {"vehicle_id": vehicle_id, "rsu_id": rsu,
                          "received_time": now.isoformat(timespec="milliseconds").replace('+00:00','Z'),
                          "alert_id": alert.get("alert_id"), "origin": alert.get("origin"),
                          "alert_type": alert.get("alert_type"), "segment_id": current["segment"],
                          "latency_ms": round((now-created).total_seconds()*1000, 2)}
                # Never take self.lock here: send() holds it while waiting for this same
                # client's network thread to flush a publish, which would deadlock.
                with self.alert_lock:
                    self.stats["alerts_received"] += 1
                    if self.alert_file:
                        self.alert_file.write(json.dumps(record, separators=(',', ':'))+'\n')
                        self.alert_file.flush()
            except (ValueError, KeyError, TypeError):
                pass

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.will_set(f"v2x/{rsu}/status/{vehicle_id}", payload="offline", qos=1, retain=True)
        self.connections[vehicle_id] = current
        try:
            client.connect(*self.hosts[rsu], keepalive=30)
            client.loop_start()
            if not ready.wait(self.connect_timeout):
                raise TimeoutError(f"{vehicle_id} failed to connect to MQTT {rsu}")
        except Exception:
            self.connections.pop(vehicle_id, None)
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:
                pass
            raise
        return current

    def send(self, events):
        for ev in events:
            rsu, segment, vid = ev["location"]["rsu_id"], ev["location"]["segment_id"], ev["source_id"]
            with self.lock:
                conn = self.connections.get(vid)
                if conn is None or conn["rsu"] != rsu:
                    if conn:
                        conn["client"].disconnect()
                        conn["client"].loop_stop()
                    conn = self._connect(vid, rsu, segment)
                if conn["segment"] != segment:
                    conn["client"].unsubscribe(f"v2x/{rsu}/alerts/{conn['segment']}")
                    conn["segment"] = segment
                    conn["ready"].clear()
                    conn["client"].subscribe(f"v2x/{rsu}/alerts/{segment}", qos=1)
                    if not conn["ready"].wait(self.connect_timeout):
                        raise TimeoutError(f"alert subscription for {vid} on {segment} timed out")
                topic = mqtt_event_topic(ev)
                qos = 0 if ev["event_type"] == "position" else 1
                info = conn["client"].publish(topic, json.dumps(ev, separators=(',', ':')), qos=qos)
                if info.rc != self.mqtt.MQTT_ERR_SUCCESS:
                    self.stats["publish_failures"] += 1
                    raise ConnectionError(f"MQTT publish failed: {info.rc} for {topic}")
                info.wait_for_publish(timeout=5)
                if not info.is_published():
                    # QoS 0 is best effort; a QoS 1 message stays in paho's queue and is
                    # retried on reconnect. Count it instead of aborting the whole run.
                    self.stats["publish_failures"] += 1
                    self.stats["unconfirmed_qos%d" % qos] = self.stats.get("unconfirmed_qos%d" % qos, 0) + 1
                    continue
                self.stats["published"] += 1
                self.stats["by_rsu"][rsu] = self.stats["by_rsu"].get(rsu, 0) + 1

    def close(self):
        if self.grace_s > 0:
            time.sleep(self.grace_s)
        for conn in self.connections.values():
            conn["client"].disconnect()
            conn["client"].loop_stop()
        if self.alert_file:
            self.alert_file.close()
