"""Full offline engine->edge simulation, without Docker or AWS."""
from pathlib import Path
from datetime import datetime

from services.rsu.core import EdgeStore
from simulator import scenario
from simulator.engine import Simulator
from simulator.transport import ListTransport, mqtt_event_topic

ROOT=Path(__file__).resolve().parents[2]


def test_offline_two_rsus_do_real_aggregation_and_prioritize(tmp_path):
    cfg=scenario.load(str(ROOT/'simulator/scenarios/accident.yaml'))
    cfg['duration_s']=100
    out=ListTransport()
    start=1_800_000_000
    Simulator(cfg,out,realtime=False,start_wall=start,generate_density=False).run()
    stores={rid: EdgeStore(tmp_path/f'{rid}.sqlite',rid,ROOT/'schema/v2x-event.schema.json')
            for rid in ('rsu-1','rsu-2')}
    try:
        for ev in out.events:
            ts=datetime.fromisoformat(ev['event_time'].replace('Z','+00:00')).timestamp()
            stores[ev['location']['rsu_id']].process(mqtt_event_topic(ev),ev,now=ts+0.01)
        for store in stores.values():
            store.emit_due_windows(now=start+110)
            stats=store.metrics()
            assert stats.get('position_accepted',0)>0
            assert stats.get('windows_emitted',0)>0
            assert stats['outbox_pending']>0
            assert stats['outbox_pending']<stats.get('position_accepted',0)+stats.get('critical_enqueued',0)+stats.get('local_alerts_generated',0)
    finally:
        for store in stores.values():store.close()
