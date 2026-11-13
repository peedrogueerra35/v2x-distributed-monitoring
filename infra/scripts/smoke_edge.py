#!/usr/bin/env python3
"""Local MQTT smoke: trigger one deterministic TTC alert on EACH RSU.

Requires the two Docker Compose RSUs running. It deliberately does NOT use AWS.
The script acts as four synthetic OBUs and logs local alerts received on MQTT.
"""
import argparse
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')


def position(rsu,seg,vid,speed,s_m,seq):
    return {'event_id':str(uuid.uuid4()), 'source_id':vid,'source_type':'vehicle',
            'event_type':'position', 'event_time':now_iso(), 'seq':seq,'schema_version':'1.1',
            'location':{'segment_id':seg,'rsu_id':rsu,'lane':0},
            'payload':{'speed_mps':speed,'s_m':s_m,'accel_mps2':0}}


def check_rsu(host,port,rsu,segment,s_m,user,password,timeout):
    import paho.mqtt.client as mqtt
    got=threading.Event()
    ready=threading.Event()
    messages=[]
    client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=f"smoke-{rsu}-{uuid.uuid4().hex[:7]}")
    client.username_pw_set(user,password)

    def on_connect(c,u,flags,rc,properties):
        if rc==0:
            c.subscribe(f'v2x/{rsu}/alerts/{segment}',qos=1)
            c.subscribe(f'v2x/{rsu}/alerts/all',qos=1)

    def on_subscribe(c,u,mid,reasons,properties):
        if all(not code.is_failure for code in reasons):
            ready.set()

    def on_message(c,u,msg):
        try:
            alert=json.loads(msg.payload)
            if alert.get('origin')==rsu and alert.get('alert_type')=='collision_risk':
                messages.append(alert)
                got.set()
        except ValueError:pass

    client.on_connect=on_connect
    client.on_subscribe=on_subscribe
    client.on_message=on_message
    try:
        client.connect(host,port,keepalive=30)
        client.loop_start()
        if not ready.wait(timeout):raise TimeoutError('MQTT subscription not ready')
        leader=position(rsu,segment,'veh-9002',10,s_m+10,0)
        follower=position(rsu,segment,'veh-9001',25,s_m,0)
        for item in (leader,follower):
            topic=f'v2x/{rsu}/{segment}/{item["source_id"]}/position'
            info=client.publish(topic,json.dumps(item),qos=1)
            info.wait_for_publish(timeout=5)
            if not info.is_published():raise TimeoutError('MQTT publish was not acknowledged')
        if not got.wait(timeout):raise TimeoutError(f'No local collision alert received for {rsu}')
        print(f'[PASS] {rsu}: MQTT alert_id={messages[0]["alert_id"]}')
    finally:
        client.disconnect()
        client.loop_stop()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--rsu1-host',default='127.0.0.1');p.add_argument('--rsu1-port',default=1883,type=int)
    p.add_argument('--rsu2-host',default='127.0.0.1');p.add_argument('--rsu2-port',default=1884,type=int)
    p.add_argument('--user',default='sim')
    p.add_argument('--password',default=os.environ.get('MQTT_VEHICLE_PASSWORD',''))
    p.add_argument('--timeout',type=float,default=10)
    a=p.parse_args()
    check_rsu(a.rsu1_host,a.rsu1_port,'rsu-1','S01',105,a.user,a.password,a.timeout)
    check_rsu(a.rsu2_host,a.rsu2_port,'rsu-2','S07',705,a.user,a.password,a.timeout)


if __name__=='__main__':main()
