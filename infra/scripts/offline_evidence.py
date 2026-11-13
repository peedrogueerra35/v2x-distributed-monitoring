#!/usr/bin/env python3
"""Reproducible offline A-main + B-edge pipeline, no Docker/AWS required.

Use from repo root: python infra/scripts/offline_evidence.py
Evidence counts are events selected into local SQLite outboxes, not delivered to AWS.
"""
import json
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))

from services.rsu.core import EdgeStore
from simulator import scenario
from simulator.engine import Simulator
from simulator.transport import ListTransport, mqtt_event_topic



def run(duration=100.0, seed=42):
    cfg=scenario.load(str(ROOT/'simulator/scenarios/accident.yaml'))
    cfg['duration_s']=float(duration)
    output=ListTransport()
    start=1_800_000_000.0
    stats=Simulator(cfg,output,seed=seed,realtime=False,start_wall=start,
                    generate_density=False).run()
    with tempfile.TemporaryDirectory() as tmp:
        stores={r:EdgeStore(Path(tmp)/(r+'.sqlite'),r,ROOT/'schema/v2x-event.schema.json')
                for r in ('rsu-1','rsu-2')}
        try:
            for event in output.events:
                at=datetime.fromisoformat(event['event_time'].replace('Z','+00:00')).timestamp()
                stores[event['location']['rsu_id']].process(mqtt_event_topic(event),event,now=at+0.01)
            result={}
            for rid,store in stores.items():
                store.emit_due_windows(now=start+duration+10)
                metrics=store.metrics()
                selected=metrics['outbox_pending']
                received=metrics.get('mqtt_received',0)
                result[rid]={'received':received,'positions_accepted':metrics.get('position_accepted',0),
                             'critical_enqueued':metrics.get('critical_enqueued',0),
                             'windows_emitted':metrics.get('windows_emitted',0),
                             'local_alerts_generated':metrics.get('local_alerts_generated',0),
                             'selected_to_sqs_outbox':selected,
                             'reduction_pct':round(100*(1-selected/received),2) if received else None}
            rec=sum(x['received'] for x in result.values())
            selected=sum(x['selected_to_sqs_outbox'] for x in result.values())
            return {'classification':'OFFLINE / no MQTT broker / no AWS / no SQS delivery',
                    'scenario':'accident','duration_s':duration,'seed':seed,
                    'simulator':{'generated':stats['generated'],'transported_in_memory':len(output.events)},
                    'rsus':result,'total_received':rec,'total_selected_to_sqs_outbox':selected,
                    'message_reduction_pct':round(100*(1-selected/rec),2) if rec else None}
        finally:
            for store in stores.values():store.close()


if __name__=='__main__':
    print(json.dumps(run(),indent=2,ensure_ascii=False))
