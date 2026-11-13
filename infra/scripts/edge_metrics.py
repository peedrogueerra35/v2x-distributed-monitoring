#!/usr/bin/env python3
"""Persona B: summarize a simulator run and live/offline RSU counters.

No AWS credentials necessary. Per-vehicle alerts contain the observed latency.
"""
import argparse
import json
import os
import statistics
import urllib.request
from collections import Counter
from pathlib import Path


def rows(path):
    path=Path(path)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def percentile(seq, q):
    seq=sorted(seq)
    return round(seq[min(len(seq)-1, int((len(seq)-1)*q))], 2) if seq else None


def fetch_metrics(url, token=""):
    req=urllib.request.Request(url.rstrip('/') + '/metrics',
                               headers={'X-Metrics-Token':token} if token else {})
    with urllib.request.urlopen(req,timeout=5) as r:
        return json.loads(r.read())


def summarize(run_dir, rsu_snapshots=None):
    run_dir=Path(run_dir)
    summary=json.loads((run_dir/'summary.json').read_text())
    config=json.loads((run_dir/'config.json').read_text())
    faults=rows(run_dir/'faults.jsonl')
    received=rows(run_dir/'vehicle_alerts.jsonl')
    result={
        'run_id':config['run_id'], 'transport':config['transport'],
        'simulator':summary, 'fault_injection':dict(Counter(x['fault'] for x in faults)),
        'vehicle_alerts':{'received':len(received), 'by_type':dict(Counter(e.get('alert_type') for e in received)),
                          'by_origin':dict(Counter(e.get('origin') for e in received)),
                          'latency_ms_p50':percentile([e['latency_ms'] for e in received if e.get('latency_ms') is not None],.5),
                          'latency_ms_p95':percentile([e['latency_ms'] for e in received if e.get('latency_ms') is not None],.95)},
        'rsus':rsu_snapshots or {}
    }
    for rid, data in result['rsus'].items():
        inbound=data.get('position_accepted',0)+data.get('critical_enqueued',0)
        outbound=data.get('sqs_sent',0)+data.get('outbox_pending',0)
        data['reduction_messages_pct_approx']=(round(100*(1-outbound/inbound),2) if inbound else None)
        data['note']='outbound = SQS sent + SQLite pending; cumulative since RSU DB creation; use experiment deltas for per-run attribution'
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument('run_dir',help='runs/<run_id>')
    p.add_argument('--rsu1',help='http://localhost:8090')
    p.add_argument('--rsu2',help='http://localhost:8091')
    p.add_argument('--token',default=os.getenv('METRICS_TOKEN',''))
    p.add_argument('--snapshots',help='optional JSON dictionary of offline RSU metric snapshots')
    p.add_argument('--out',help='where to write final JSON (also prints to stdout)')
    args=p.parse_args()
    snapshots=json.loads(Path(args.snapshots).read_text()) if args.snapshots else {}
    for key, url in (('rsu-1',args.rsu1),('rsu-2',args.rsu2)):
        if url:
            snapshots[key]=fetch_metrics(url,args.token)
    result=summarize(args.run_dir,snapshots)
    value=json.dumps(result,indent=2,ensure_ascii=False)
    if args.out:Path(args.out).write_text(value+'\n',encoding='utf-8')
    print(value)


if __name__=='__main__':main()
