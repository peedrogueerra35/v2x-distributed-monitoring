"""Executable contracts against Person A's unchanged JSON Schemas and deploy files."""
import json
from pathlib import Path

import jsonschema
import yaml

from services.rsu.core import EdgeStore, iso
from simulator.transport import mqtt_event_topic

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_EVENT = json.loads((ROOT / 'schema/v2x-event.schema.json').read_text())
SCHEMA_ALERT = json.loads((ROOT / 'schema/v2x-alert.schema.json').read_text())


def pos(rsu, vid, e_id, t, segment, s, speed):
    return {'event_id': e_id, 'source_id': vid, 'source_type': 'vehicle', 'seq': 0,
            'event_type': 'position', 'event_time': iso(t), 'schema_version': '1.1',
            'location': {'segment_id': segment, 'rsu_id': rsu, 'lane': 0},
            'payload': {'s_m': s, 'speed_mps': speed}}


def test_local_collision_and_density_match_main_in_both_zones(tmp_path):
    t=1_800_000_000
    for rsu, segment, base in [('rsu-1', 'S01', 100), ('rsu-2', 'S07', 700)]:
        store=EdgeStore(tmp_path/(rsu+'.db'), rsu, ROOT/'schema/v2x-event.schema.json')
        try:
            leader=pos(rsu,'veh-0002',f'{rsu}-leader',t+1,segment,base+15,10)
            follower=pos(rsu,'veh-0001',f'{rsu}-follower',t+1,segment,base+5,25)
            assert store.process(mqtt_event_topic(leader),leader,now=t+1)=='accepted'
            assert store.process(mqtt_event_topic(follower),follower,now=t+1.1)=='accepted'
            alert=json.loads(store.next_mqtt_alert()['body'])
            jsonschema.validate(alert,SCHEMA_ALERT)
            assert (alert['origin'],alert['zona'],alert['alert_type'])==(rsu,rsu,'collision_risk')
            store.emit_due_windows(now=t+8)
            rows=store.db.execute('SELECT body FROM outbox ORDER BY priority, queued_at').fetchall()
            events=[json.loads(row['body']) for row in rows]
            assert {e['event_type'] for e in events}=={'collision_risk','density'}
            for ev in events:
                jsonschema.validate(ev,SCHEMA_EVENT)
                assert ev['source_id']==rsu and ev['source_type']=='rsu'
                assert ev['schema_version']=='1.1'
                assert not ('s_m' in ev or 'speed_mps' in ev)
            assert sorted(e['seq'] for e in events)==[0,1]
            risk=next(e for e in events if e['event_type']=='collision_risk')
            assert len(risk['payload']['pair'])==2
            assert risk['payload']['gap_m'] > 0
            den=next(e for e in events if e['event_type']=='density')
            assert den['trace']['aggregated'] is True
            assert den['payload']['vehicle_count']==2
            assert den['payload']['window_s']==5
        finally:
            store.close()


def test_aws_compose_matches_live_app_and_deploy_contract():
    prod=yaml.safe_load((ROOT/'infra/deploy/compose.rsu.yml').read_text())
    local=yaml.safe_load((ROOT/'services/rsu/compose.yml').read_text())
    assert set(prod['services'])=={'mosquitto','rsu'}
    assert set(local['services'])=={'mosquitto','rsu'}
    assert '${RSU_IMAGE' in prod['services']['rsu']['image']
    assert '${RSU_IMAGE' in local['services']['rsu']['image']
    env=prod['services']['rsu']['environment']
    assert {'RSU_ID','AWS_DEFAULT_REGION','SQS_QUEUE_NAME','SNS_TOPIC_ARN',
            'MQTT_APP_PASSWORD','METRICS_TOKEN','MQTT_BROKER_HOST'} <= set(env)
    assert '/data' in prod['services']['rsu']['volumes'][0]
    conf=(ROOT/'services/rsu/mosquitto/mosquitto.conf').read_text()
    acl=(ROOT/'services/rsu/mosquitto/acl').read_text()
    assert 'listener 1883' in conf
    assert 'password_file /mosquitto/config/passwd' in conf
    assert 'persistence true' in conf
    assert 'user sim' in acl and 'user rsu' in acl
