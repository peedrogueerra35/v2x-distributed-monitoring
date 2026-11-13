"""Persona B: deterministic unit tests, no real AWS/MQTT brokers required."""
import copy
import json
from pathlib import Path

import jsonschema
import pytest

from services.rsu.core import EdgeStore, compact, iso
from simulator.transport import mqtt_event_topic

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def edge(tmp_path):
    store = EdgeStore(tmp_path / "rsu.sqlite3", "rsu-1", ROOT / "schema/v2x-event.schema.json",
                      window_s=5, lateness_s=2)
    yield store
    store.close()


def event(vid, eid, when, speed=15.0, s=110, kind="position", rsu="rsu-1", segment="S01"):
    payload = ({"speed_mps": speed, "s_m": s, "accel_mps2": 0.0}
               if kind == "position" else {"decel_mps2": 5.0, "speed_before_mps": speed})
    return {"event_id": eid, "source_id": vid, "source_type": "vehicle", "seq": 0,
            "event_type": kind, "event_time": iso(when), "schema_version": "1.0",
            "location": {"segment_id": segment, "rsu_id": rsu, "lane": 0}, "payload": payload}


def publish(store, ev, now):
    return store.process(mqtt_event_topic(ev), ev, now=now)


def test_tumbling_reuses_edge_analytics_and_sends_one_summary(edge):
    base = 1_800_000_000    # multiple of five
    e1 = event("veh-0001", "a", base + 1, speed=10)
    e2 = event("veh-0002", "b", base + 2, speed=20, s=175)
    e3 = event("veh-0001", "c", base + 4, speed=30, s=120)
    assert all(publish(edge, e, base + 4.5) == "accepted" for e in (e1, e2, e3))
    assert edge.emit_due_windows(now=base + 6.9) == 0  # window 0:5 + 2 s allowed lateness
    assert edge.emit_due_windows(now=base + 7.1) == 1
    summary = json.loads(edge.next_outbox(now=base+8)["body"])
    assert summary["event_type"] == "density"
    assert summary["payload"]["vehicle_count"] == 2
    assert summary["payload"]["sample_count"] == 3
    assert summary["payload"]["mean_speed_mps"] == 20
    assert summary["payload"]["window_s"] == 5
    assert summary["trace"]["aggregated"] is True
    jsonschema.validate(summary, edge.schema)
    assert edge.emit_due_windows(now=base + 15) == 0


def test_duplicate_is_filtered_before_aggregation(edge):
    t = 1_800_000_000
    ev = event('veh-0001', 'same', t + 1)
    assert publish(edge, ev, t + 1) == 'accepted'
    assert publish(edge, ev, t + 1.1) == 'duplicate'
    assert edge.emit_due_windows(now=t+8) == 1
    body = json.loads(edge.next_outbox(now=t+8)['body'])
    assert body['payload']['sample_count'] == 1
    assert edge.metrics()['mqtt_duplicate'] == 1


def test_wrong_topic_and_wrong_zone_are_invalid(edge):
    t = 1_800_000_000
    ev = event('veh-0001','x',t+1)
    assert edge.process('v2x/rsu-1/alerts/S01',ev,now=t+1) == 'invalid'
    assert edge.process('v2x/rsu-2/S01/veh-0001/position',ev,now=t+1) == 'invalid'
    ev['location']['rsu_id'] = 'rsu-2'
    assert edge.process('v2x/rsu-1/S01/veh-0001/position',ev,now=t+1) == 'invalid'
    assert edge.metrics()['mqtt_invalid'] == 3


def test_critical_is_immediate_priority_and_retained_after_restart(tmp_path):
    p=tmp_path/'persistent.sqlite3'
    schema=ROOT/'schema/v2x-event.schema.json'
    t=1_800_000_000
    s=EdgeStore(p,'rsu-1',schema)
    publish(s,event('veh-0001','p',t+1),t+1)
    s.emit_due_windows(now=t+8)
    brake=event('veh-0001','critical',t+8,kind='brake')
    assert publish(s,brake,t+8) == 'accepted'
    assert s.next_outbox(now=t+9)['event_id'] == 'critical'
    s.close()
    s=EdgeStore(p,'rsu-1',schema)
    first=s.next_outbox(now=t+9)
    assert first['event_id'] == 'critical'
    s.failed(first,now=t+9,backoff_base=2)
    second=s.next_outbox(now=t+9)
    assert json.loads(second['body'])['event_type'] == 'density'
    s.sent(second,now=t+10)
    assert s.next_outbox(now=t+11)['event_id'] == 'critical'
    s.sent(s.next_outbox(now=t+11),now=t+11)
    assert s.metrics()['outbox_pending'] == 0
    s.close()


def test_local_ttc_produces_edge_alert_and_cloud_collision_risk(edge):
    t=1_800_000_000
    leader=event('veh-0002','lead',t+1,speed=10,s=115)
    follower=event('veh-0001','follow',t+1,speed=25,s=105)
    assert publish(edge,leader,t+1) == 'accepted'
    assert publish(edge,follower,t+1.1) == 'accepted'
    alert=edge.next_mqtt_alert()
    assert alert is not None
    a=json.loads(alert['body'])
    assert a['origin'] == 'rsu-1'
    assert a['alert_type'] == 'collision_risk'
    jsonschema.validate(a, json.loads((ROOT/'schema/v2x-alert.schema.json').read_text()))
    assert a['zona'] == 'rsu-1' and a['valid_until'] and a['cause_event_ids']
    assert alert['topic'] == 'v2x/rsu-1/alerts/S01'
    risk=json.loads(edge.next_outbox(now=t+2)['body'])
    assert risk['event_type'] == 'collision_risk'
    assert risk['payload']['ttc_s'] < 2
    assert risk['payload']['pair'] == ['veh-0001','veh-0002']
    assert risk['payload']['gap_m'] > 0
    assert risk['source_type'] == 'rsu'
    assert risk['seq'] == 0
    jsonschema.validate(risk, edge.schema)
    assert edge.metrics()['local_alerts_generated'] == 1
    assert publish(edge,copy.deepcopy(follower) | {'event_id':'follow2'},t+1.2) == 'accepted'
    assert edge.metrics()['local_alerts_generated'] == 1  # cooldown


def test_edge_is_durable_if_window_not_yet_closed(tmp_path):
    t=1_800_000_000
    path=tmp_path/'db.sqlite3'; schema=ROOT/'schema/v2x-event.schema.json'
    s=EdgeStore(path,'rsu-1',schema)
    assert publish(s,event('veh-0001','p',t+1),t+1) == 'accepted'
    s.close()
    s=EdgeStore(path,'rsu-1',schema)
    assert s.emit_due_windows(now=t+8) == 1
    assert json.loads(s.next_outbox(now=t+8)['body'])['payload']['vehicle_count'] == 1
    s.close()


def test_late_positions_not_resurrect_window(edge):
    t=1_800_000_000
    ev=event('veh-0001','late',t+1)
    assert publish(edge,ev,t+15) == 'late'
    assert edge.metrics()['windows_pending'] == 0


def test_sns_dedup_and_broadcast_requires_signed_body_zone(edge):
    t=1_800_000_000
    cloud={'alert_id':'cloud-1', 'origin':'cloud', 'zona':'broadcast',
           'location':{'rsu_id':'rsu-2','segment_id':'S07'},
           'created_time':iso(t),'valid_until':iso(t+300),
           'alert_type':'emergency_priority','severity':2,'schema_version':'1.0',
           'cause_event_ids':['evt-123'], 'message':'Emergency'}
    assert edge.receive_sns_alert(cloud,now=t) == 'accepted'
    assert edge.receive_sns_alert(cloud,now=t) == 'duplicate'
    alert=edge.next_mqtt_alert()
    assert alert['topic'] == 'v2x/rsu-1/alerts/all'
    assert edge.metrics()['sns_duplicates'] == 1
    assert edge.receive_sns_alert(dict(cloud,alert_id='new',zona='rsu-2'),now=t) == 'wrong_zone'


def test_mqtt_simulator_does_not_generate_density():
    from simulator import scenario
    from simulator.engine import Simulator
    from simulator.transport import ListTransport
    cfg=scenario.load(str(ROOT/'simulator/scenarios/baseline.yaml'))
    cfg['duration_s'] = 7
    out=ListTransport()
    Simulator(cfg,out,realtime=False,generate_density=False,start_wall=1_800_000_000).run()
    assert not any(e['event_type']=='density' for e in out.events)
    assert all('s_m' in e['payload'] for e in out.events if e['event_type']=='position')


def test_generated_rsu_sequence_is_consecutive_and_persistent(tmp_path):
    t=1_800_000_000
    schema=ROOT/'schema/v2x-event.schema.json'
    path=tmp_path/'seq.sqlite'
    s=EdgeStore(path,'rsu-1',schema)
    for idx in range(3):
        e=event('veh-0001',f'p{idx}',t+idx+1,speed=10,s=110)
        assert publish(s,e,t+idx+1) == 'accepted'
    s.emit_due_windows(now=t+8)
    first=json.loads(s.next_outbox(now=t+9)['body'])
    assert first['seq']==0 and first['event_type']=='density'
    s.close()
    s=EdgeStore(path,'rsu-1',schema)
    for idx in range(3):
        e=event('veh-0001',f'q{idx}',t+idx+11,speed=10,s=110)
        assert publish(s,e,t+idx+11) == 'accepted'
    s.emit_due_windows(now=t+19)
    evs=[json.loads(row['body']) for row in s.db.execute("SELECT body FROM outbox ORDER BY queued_at")]
    assert sorted(e['seq'] for e in evs)==[0,1]
    s.close()


def test_cloud_alert_must_match_main_alert_schema(edge):
    t=1_800_000_000
    incomplete={'alert_id':'bad','zona':'broadcast'}
    assert edge.receive_sns_alert(incomplete,now=t)=='invalid'
    assert edge.metrics()['mqtt_alerts_pending']==0
