"""RSU service: MQTT → durable SQLite + edge analytics → SQS, and SNS → MQTT.

Run as uvicorn services.rsu.app:app. MQTT and SQS worker threads run in the
FastAPI lifespan; no AWS calls are required for startup/local window tests.
"""
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import boto3
import jsonschema
import paho.mqtt.client as mqtt
from botocore.exceptions import BotoCoreError, ClientError
from botocore.config import Config
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from urllib.request import HTTPRedirectHandler, build_opener

from services.rsu.core import EdgeStore
from services.rsu.sns_security import SNSVerificationError, verify_sns

log = logging.getLogger("rsu")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
ROOT = Path(__file__).resolve().parents[2]


class RSUApplication:
    def __init__(self):
        self.rsu_id = os.getenv("RSU_ID", "rsu-1")
        self.region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.queue_name = os.getenv("SQS_QUEUE_NAME", "v2x-events")
        self.topic_arn = os.getenv("SNS_TOPIC_ARN", "")
        self.pause_file = Path(os.getenv("CLOUD_PAUSE_FILE", "/data/cloud.paused"))
        self.store = EdgeStore(os.getenv("SQLITE_PATH", "/data/edge.sqlite3"), self.rsu_id,
                               ROOT / "schema/v2x-event.schema.json",
                               window_s=int(os.getenv("WINDOW_S", "5")),
                               lateness_s=float(os.getenv("WINDOW_LATENESS_S", "2")))
        self.alert_validator = jsonschema.Draft202012Validator(
            json.loads((ROOT / "schema/v2x-alert.schema.json").read_text()))
        self.stop_event = threading.Event()
        self.mqtt_connected = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=f"rsu-app-{self.rsu_id}", clean_session=False,
                                  protocol=mqtt.MQTTv311, manual_ack=True)
        self.client.username_pw_set(os.getenv("MQTT_APP_USER", "rsu"),
                                    os.getenv("MQTT_APP_PASSWORD", ""))
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.threads = []

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.mqtt_connected.set()
            client.subscribe(f"v2x/{self.rsu_id}/+/+/+", qos=1)
            log.info("MQTT connected/subscribed: %s", self.rsu_id)
        else:
            self.mqtt_connected.clear()
            log.error("MQTT connection rejected: %s", reason_code)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.mqtt_connected.clear()

    def on_message(self, client, userdata, msg):
        try:
            event = json.loads(msg.payload)
            outcome = self.store.process(msg.topic, event, wire_bytes=len(msg.payload))
            if outcome != "accepted":
                log.debug("Discarded MQTT event: %s", outcome)
        except (json.JSONDecodeError, UnicodeDecodeError):
            with self.store.lock, self.store.db:
                self.store._inc("mqtt_invalid")
                self.store._inc("mqtt_received")
        except Exception:
            # Do NOT ACK a message when local durable storage failed.
            # The broker retries QoS1 after the client reconnects.
            log.exception("Message not persisted; refusing MQTT ACK")
            return
        if msg.qos >= 1:
            client.ack(msg.mid, msg.qos)

    def start(self):
        host = os.getenv("MQTT_BROKER_HOST", "broker")
        port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
        # Connect asynchronously: FastAPI can start even while the broker is booting.
        self.client.connect_async(host, port, keepalive=30)
        self.client.loop_start()
        for target, name in ((self._sqs_worker, "sqs-outbox"),
                             (self._window_worker, "tumbling"),
                             (self._mqtt_alert_worker, "alerts-outbox")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self.threads.append(thread)

    def close(self):
        self.stop_event.set()
        self.client.disconnect()
        self.client.loop_stop()
        for thread in self.threads:
            thread.join(timeout=2)
        self.store.close()

    def _window_worker(self):
        while not self.stop_event.wait(0.25):
            try:
                count = self.store.emit_due_windows()
                if count:
                    log.info("Closed %d event-time windows", count)
            except Exception:
                log.exception("Window aggregation failed")

    def _sqs_worker(self):
        sqs, queue_url = None, None
        while not self.stop_event.wait(0.15):
            if self.pause_file.exists():
                continue
            item = self.store.next_outbox()
            if not item:
                continue
            try:
                if sqs is None:
                    sqs = boto3.client("sqs", region_name=self.region,
                                       config=Config(connect_timeout=3, read_timeout=5, retries={'max_attempts': 1}))
                if queue_url is None:
                    queue_url = sqs.get_queue_url(QueueName=self.queue_name)["QueueUrl"]
                sqs.send_message(QueueUrl=queue_url, MessageBody=item["body"])
                self.store.sent(item)
            except Exception as ex:
                log.warning("SQS unavailable; retained in SQLite: %s", ex)
                self.store.failed(item)
                queue_url = None
                self.stop_event.wait(0.5)

    def _mqtt_alert_worker(self):
        while not self.stop_event.wait(0.10):
            if not self.mqtt_connected.is_set():
                continue
            alert = self.store.next_mqtt_alert()
            if not alert:
                continue
            try:
                info = self.client.publish(alert["topic"], alert["body"], qos=1, retain=False)
                info.wait_for_publish(timeout=2)
                if info.is_published():
                    self.store.mqtt_alert_sent(alert)
            except Exception:
                log.exception("MQTT alert publish failed; alert retained")
                self.stop_event.wait(0.5)

    def confirm_subscription(self, payload):
        """SNS confirms the signed request through its pinned SubscribeURL.

        verify_sns() already checks the URL host, scheme, and notification signature.
        Reject redirects to prevent an SSRF bypass.
        """
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                raise SNSVerificationError("SNS confirmation redirect not allowed")
        with build_opener(NoRedirect()).open(payload["SubscribeURL"], timeout=5) as response:
            if response.status != 200:
                raise RuntimeError(f"SubscribeURL returned HTTP {response.status}")
            response.read(4096)


runtime: RSUApplication | None = None


@asynccontextmanager
async def lifespan(application):
    global runtime
    runtime = RSUApplication()
    runtime.start()
    try:
        yield
    finally:
        runtime.close()
        runtime = None


app = FastAPI(title="V2X RSU edge service", lifespan=lifespan)


def get_runtime() -> RSUApplication:
    if runtime is None:
        raise HTTPException(status_code=503, detail="RSU is starting")
    return runtime


@app.get("/health")
def health():
    r = get_runtime()
    connected = r.mqtt_connected.is_set()
    return JSONResponse({"ok": connected, "rsu_id": r.rsu_id, "mqtt_connected": connected,
                         "cloud_paused": r.pause_file.exists(),
                         "outbox_pending": r.store.metrics()["outbox_pending"]},
                        status_code=200 if connected else 503)


@app.get("/metrics")
def metrics(x_metrics_token: str | None = Header(default=None)):
    expected = os.getenv("METRICS_TOKEN", "")
    if not expected or x_metrics_token != expected:
        raise HTTPException(status_code=403, detail="Metrics disabled or token missing/invalid")
    r = get_runtime()
    return {**r.store.metrics(), "mqtt_connected": r.mqtt_connected.is_set(),
            "cloud_paused": r.pause_file.exists()}


@app.get("/audit")
def audit(since: float, x_metrics_token: str | None = Header(default=None)):
    expected = os.getenv("METRICS_TOKEN", "")
    if not expected or x_metrics_token != expected:
        raise HTTPException(status_code=403, detail="Audit requires a configured METRICS_TOKEN")
    return {"events": get_runtime().store.audit(since), "note": "RSU->SQS SendMessage ACKs, not cloud commits"}


@app.post("/sns")
async def sns_endpoint(request: Request):
    r = get_runtime()
    if not r.topic_arn:
        raise HTTPException(status_code=503, detail="SNS_TOPIC_ARN not configured")
    body = await request.body()
    if len(body) > 256 * 1024:
        raise HTTPException(status_code=413, detail="SNS body too large")
    try:
        data = json.loads(body)
        typ = verify_sns(data, expected_topic_arn=r.topic_arn, region=r.region)
        header = request.headers.get("x-amz-sns-message-type")
        if header and header != typ:
            raise SNSVerificationError("SNS header type doesn't match payload")
    except (SNSVerificationError, KeyError, TypeError, ValueError) as ex:
        raise HTTPException(status_code=403, detail=str(ex)) from ex
    if typ == "SubscriptionConfirmation":
        try:
            r.confirm_subscription(data)
        except Exception as ex:
            log.exception("SNS confirmation failed")
            raise HTTPException(status_code=503, detail="SNS confirmation failed") from ex
        return {"result": "confirmed"}
    if typ == "UnsubscribeConfirmation":
        log.warning("SNS unsubscribed: %s", data.get("TopicArn"))
        return {"result": "unsubscribed"}
    try:
        alert = json.loads(data["Message"])
        r.alert_validator.validate(alert)
    except (json.JSONDecodeError, jsonschema.ValidationError, KeyError, TypeError) as ex:
        raise HTTPException(status_code=400, detail=f"Invalid alert body: {ex}") from ex
    result = r.store.receive_sns_alert(alert)
    return {"result": result}
