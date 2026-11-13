#!/usr/bin/env python3
"""Five controlled RSU->SQS outages, with metrics and deterministic recovery.

The outage is an application-level AWS/SQS fault injection: it pauses the
RSU's SQS sender using a durable file, while MQTT keeps receiving events.
It is NOT a physical/network disconnect and must be described accurately.

For real AWS, use --mode ssm --instance-id <RSU_EC2_ID>; for local Docker,
use --mode local. Never runs automatically without an explicit CLI command.
"""
import argparse
import csv
import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def metrics(url, token):
    req=urllib.request.Request(url.rstrip('/')+'/metrics',headers={'X-Metrics-Token':token} if token else {})
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read())


def run_command(command, *, dry_run=False):
    print('$', ' '.join(command), flush=True)
    if not dry_run:
        subprocess.run(command, check=True, timeout=60)


def toggle_pause(mode, action, args):
    if action not in ('pause','resume'):
        raise ValueError('invalid action')
    op='touch' if action=='pause' else 'rm -f'
    # app has its own SQLite volume /data. The file is visible to RSU SQS worker.
    if mode=='local':
        run_command(['docker','exec', args.container, '/bin/sh','-c',f'{op} /data/cloud.paused'],dry_run=args.dry_run)
    else:
        import boto3
        client=boto3.client('ssm',region_name=args.region)
        # One RSU per EC2 instance. Select its Compose app via label.
        script=('CID=$(docker ps -q --filter label=com.docker.compose.project=v2x --filter label=com.docker.compose.service=rsu | head -n 1); '
                'test -n "$CID" && docker exec "$CID" '+op+' /data/cloud.paused')
        if args.dry_run:
            print('SSM dry-run', args.instance_id, script)
            return
        response=client.send_command(InstanceIds=[args.instance_id],DocumentName='AWS-RunShellScript',
                                     Parameters={'commands':[script]})
        cid=response['Command']['CommandId']
        waiter=client.get_waiter('command_executed')
        waiter.wait(CommandId=cid,InstanceId=args.instance_id,WaiterConfig={'Delay':2,'MaxAttempts':25})
        result=client.get_command_invocation(CommandId=cid,InstanceId=args.instance_id)
        if result['Status']!='Success':
            raise RuntimeError(f'SSM command failed: {result}')


def record(snapshots, phase, url, token):
    sample=metrics(url,token)
    snapshots.append({'phase':phase,'epoch':time.time(),'metrics':sample})
    print(phase,'buffer=',sample['outbox_pending'],'sqs_sent=',sample.get('sqs_sent',0),flush=True)
    return sample


def audit(url, token, since):
    if not token:
        return []
    req=urllib.request.Request(url.rstrip('/')+f'/audit?since={since}',
                               headers={'X-Metrics-Token':token})
    with urllib.request.urlopen(req,timeout=8) as resp:
        return json.loads(resp.read())['events']


def order_inversions(audit_rows):
    # Count adjacent inversions in enqueue time ordered by successful SQS ACKs.
    # Priority messages may intentionally overtake density, and SQS is standard (not FIFO).
    sent=sorted((e for e in audit_rows if e['sent_at'] is not None),key=lambda e:e['sent_at'])
    return sum(a['queued_at']>b['queued_at'] for a,b in zip(sent,sent[1:]))


def main():
    p=argparse.ArgumentParser(description='E1 RSU SQS outage emulation and recovery')
    p.add_argument('--mode',choices=['local','ssm'],default='local')
    p.add_argument('--rsu-url',default='http://127.0.0.1:8090')
    p.add_argument('--token',default=os.getenv('METRICS_TOKEN',''))
    p.add_argument('--container',default='v2x-rsu1-rsu-1')
    p.add_argument('--instance-id',help='EC2 instance ID of chosen RSU; mandatory for ssm')
    p.add_argument('--region',default='us-east-1')
    p.add_argument('--repetitions',type=int,default=5)
    p.add_argument('--warmup-s',type=float,default=5)
    p.add_argument('--outage-s',type=float,default=20)
    p.add_argument('--recovery-timeout-s',type=float,default=90)
    p.add_argument('--sample-s',type=float,default=1)
    p.add_argument('--out',default='experiments/results/e1')
    p.add_argument('--dry-run',action='store_true',help='print commands, do not modify Docker/EC2')
    a=p.parse_args()
    if a.mode=='ssm' and not a.instance_id:p.error('--instance-id required for ssm')
    if min(a.repetitions,a.sample_s,a.outage_s)<=0:p.error('positive repetitions, sample and outage required')
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    results=[]
    for rep in range(1,a.repetitions+1):
        print(f'\nE1 repetition {rep}/{a.repetitions}. Start/keep simulator traffic running externally.',flush=True)
        snaps=[]
        if a.dry_run:
            toggle_pause(a.mode,'pause',a)
            toggle_pause(a.mode,'resume',a)
            break
        before=record(snaps,'before',a.rsu_url,a.token)
        if before['outbox_pending']:
            print('WARNING: outbox not empty before trial; result includes old backlog')
        time.sleep(a.warmup_s)
        start=time.time()
        try:
            toggle_pause(a.mode,'pause',a)
            while time.time()-start<a.outage_s:
                record(snaps,'paused',a.rsu_url,a.token)
                time.sleep(a.sample_s)
        finally:
            toggle_pause(a.mode,'resume',a)
        restored=time.time()
        until=restored+a.recovery_timeout_s
        while True:
            final=record(snaps,'recovering',a.rsu_url,a.token)
            if final['outbox_pending']==0:break
            if time.time()>until:break
            time.sleep(a.sample_s)
        recovery=time.time()-restored if final['outbox_pending']==0 else None
        after=record(snaps,'after',a.rsu_url,a.token)
        row={'repetition':rep,'outage_s':round(restored-start,2),
             'buffer_max':max(s['metrics']['outbox_pending'] for s in snaps),
             'buffer_at_restore':next((s['metrics']['outbox_pending'] for s in reversed(snaps)
                                       if s['phase']=='paused'),None),
             'recovery_s':round(recovery,2) if recovery is not None else None,
             'backlog_final':after['outbox_pending'],
             'unique_received_delta':(after.get('position_accepted',0)+after.get('critical_enqueued',0)
                 -before.get('position_accepted',0)-before.get('critical_enqueued',0)),
             'sqs_sent_delta':after.get('sqs_sent',0)-before.get('sqs_sent',0),
             'sqs_failures_delta':after.get('sqs_failures',0)-before.get('sqs_failures',0),
             'duplicates_delta':after.get('mqtt_duplicate',0)-before.get('mqtt_duplicate',0),
             'dropped_late_delta':after.get('position_too_late',0)-before.get('position_too_late',0)}
        audit_rows=audit(a.rsu_url,a.token,snaps[0]['epoch'])
        row['audit_events_queued']=len(audit_rows)
        row['audit_sqs_acknowledged']=sum(e['sent_at'] is not None for e in audit_rows)
        row['audit_pending']=sum(e['sent_at'] is None for e in audit_rows)
        row['audit_enqueue_to_sqs_order_inversions']=order_inversions(audit_rows)
        results.append(row)
        (out/f'e1_trial_{rep}.json').write_text(json.dumps({'result':row,'samples':snaps, 'audit':audit_rows},indent=2)+'\n')
        print('RESULT',json.dumps(row),flush=True)
    if results:
        with (out/'summary.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=list(results[0]));writer.writeheader();writer.writerows(results)
        (out/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
    print('Saved to',out)


if __name__=='__main__':main()
