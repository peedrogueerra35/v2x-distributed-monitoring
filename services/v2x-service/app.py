import json, os, threading, time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import boto3, jsonschema
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

import rules

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
TOPIC_ARN = os.getenv("ALERTS_TOPIC_ARN")          # unset -> alerts are stored but not published (D1 behaviour)
sqs = boto3.client("sqs", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
QUEUE_URL = sqs.get_queue_url(QueueName="v2x-events")["QueueUrl"]
SCHEMA = json.load(open("schema/v2x-event.schema.json"))
ALERT_SCHEMA = json.load(open("schema/v2x-alert.schema.json"))
ser, des = TypeSerializer(), TypeDeserializer()
stats = {"received": 0, "stored": 0, "duplicate": 0, "invalid": 0, "alerts": 0,
         "alerts_published": 0, "publish_errors": 0}
state = {"last_poll": time.time()}

def item(d): return {k: ser.serialize(v) for k, v in d.items()}
def now_iso(): return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
def plain(o): return float(o) if isinstance(o, Decimal) else str(o)

def publish(alert):
    """Publish an alert to SNS v2x-alerts with the 'zona' attribute used by the RSU subscription filters."""
    if not TOPIC_ARN:
        return
    msg = {k: v for k, v in alert.items() if k not in ("published", "published_time")}
    sns.publish(TopicArn=TOPIC_ARN, Message=json.dumps(msg, default=plain),
                Subject=f"{alert['alert_type']} {alert['zona']}"[:100],
                MessageAttributes={"zona": {"DataType": "String", "StringValue": alert["zona"]},
                                   "alert_type": {"DataType": "String", "StringValue": alert["alert_type"]}})
    ddb.update_item(TableName="v2x-alerts", Key={"alert_id": {"S": alert["alert_id"]}},
                    UpdateExpression="SET published = :t, published_time = :pt",
                    ExpressionAttributeValues={":t": {"BOOL": True}, ":pt": {"S": now_iso()}})
    stats["alerts_published"] += 1

def raise_alert(alert):
    """Store the alert once (deterministic ID) and publish it; a failed publish is retried by republish_loop."""
    jsonschema.validate(alert, ALERT_SCHEMA)
    try:
        ddb.put_item(TableName="v2x-alerts", Item=item(dict(alert, published=False)),
                     ConditionExpression="attribute_not_exists(alert_id)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return                                   # same situation already alerted
        raise
    stats["alerts"] += 1
    try:
        publish(alert)
    except Exception as e:
        stats["publish_errors"] += 1
        print(json.dumps({"publish_error": str(e), "alert_id": alert["alert_id"]}), flush=True)

def process(ev):
    ev.setdefault("trace", {})["cloud_received_time"] = now_iso()
    seg = ev["location"]["segment_id"]
    t = datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00")).timestamp()
    ws, exp = int(t // 10 * 10), int(time.time()) + 86400
    p = ev["payload"]
    try:
        ddb.transact_write_items(TransactItems=[
            {"Put": {"TableName": "v2x-events", "Item": item(dict(ev, expire_at=exp)),
                     "ConditionExpression": "attribute_not_exists(event_id)"}},
            {"Update": {"TableName": "v2x-windows",
                        "Key": {"segment_id": {"S": seg}, "window_start": {"N": str(ws)}},
                        "UpdateExpression": "ADD event_count :one, speed_sum :s, speed_n :n, brake_count :b SET expire_at = :e",
                        "ExpressionAttributeValues": {
                            ":one": {"N": "1"}, ":s": {"N": str(p.get("speed_mps", 0))},
                            ":n": {"N": "1" if "speed_mps" in p else "0"},
                            ":b": {"N": "1" if ev["event_type"] == "brake" else "0"},
                            ":e": {"N": str(exp)}}}}])
    except ClientError as e:
        reasons = e.response.get("CancellationReasons", [{}])
        if e.response["Error"]["Code"] == "TransactionCanceledException" and reasons[0].get("Code") == "ConditionalCheckFailed":
            stats["duplicate"] += 1
            return
        raise
    stats["stored"] += 1
    for alert in rules.evaluate(ev):
        raise_alert(alert)

def consume():
    while True:
        r = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=20)
        state["last_poll"] = time.time()
        for m in r.get("Messages", []):
            stats["received"] += 1
            try:
                ev = json.loads(m["Body"], parse_float=Decimal)
                jsonschema.validate(json.loads(m["Body"]), SCHEMA)
            except Exception:
                stats["invalid"] += 1
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
                continue
            try:
                process(ev)
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
            except Exception as e:  # not deleted: SQS redelivers after the visibility timeout
                print(json.dumps({"error": str(e), "event_id": ev.get("event_id")}), flush=True)

def republish_loop():
    """Every 5 s, publish again the alerts whose SNS publish failed (published = false)."""
    while True:
        time.sleep(5)
        if not TOPIC_ARN:
            continue
        try:
            rows = ddb.scan(TableName="v2x-alerts", FilterExpression="published = :f",
                            ExpressionAttributeValues={":f": {"BOOL": False}})["Items"]
            for r in rows:
                publish({k: des.deserialize(v) for k, v in r.items()})
        except Exception as e:
            stats["publish_errors"] += 1
            print(json.dumps({"republish_error": str(e)}), flush=True)

@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=consume, daemon=True).start()
    threading.Thread(target=republish_loop, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health():
    ok = time.time() - state["last_poll"] < 60
    return JSONResponse({"ok": ok, "sns": bool(TOPIC_ARN)}, status_code=200 if ok else 503)

@app.get("/alerts")
def alerts():
    rows = [{k: des.deserialize(v) for k, v in i.items()} for i in ddb.scan(TableName="v2x-alerts")["Items"]]
    return sorted(rows, key=lambda a: a["created_time"], reverse=True)[:100]

@app.get("/report")
def report():
    depth = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"])
    return {**stats, "queue_depth": int(depth["Attributes"]["ApproximateNumberOfMessages"])}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return open("dashboard.html").read()   # small page that fetches /report and /alerts every 2 s
