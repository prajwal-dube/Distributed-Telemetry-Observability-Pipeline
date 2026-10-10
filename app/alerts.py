import json,os,time,uuid
import httpx
from psycopg.types.json import Jsonb
from .store import connect

def state_transition(pending,now,breach,for_seconds):
    if not breach:return None,False
    if pending is None:pending=now
    return pending,now-pending>=for_seconds

def evaluate_rules(snapshots,now):
    with connect() as c:
        rules=c.execute('SELECT * FROM alert_rules WHERE enabled FOR UPDATE').fetchall()
        for rule in rules:
            # Avoid repeated same-tick/backdated evaluations advancing episodes.
            if rule['last_evaluated'] is not None and now<=rule['last_evaluated']:continue
            snap=snapshots.get(rule['service'],{});metric=snap.get('p99_upper_bound_ms');fresh=now-snap.get('window_end',0)<=20
            breach=fresh and snap.get('count',0)>=rule['min_samples'] and metric is not None and metric>rule['threshold_ms']
            # Missing evaluator ticks break continuity rather than counting unobserved time.
            previous=rule['pending_since']
            if rule['last_evaluated'] is not None and now-rule['last_evaluated']>20:previous=None
            pending,fire=state_transition(previous,now,breach,rule['for_seconds'])
            c.execute('UPDATE alert_rules SET pending_since=%s,last_evaluated=%s WHERE id=%s',(pending,now,rule['id']))
            if not breach:c.execute('UPDATE alerts SET resolved_at=now() WHERE rule_id=%s AND resolved_at IS NULL',(rule['id'],))
            if fire:
                alert_id=uuid.uuid4();row=c.execute('INSERT INTO alerts(id,rule_id,service,metric_ms) VALUES(%s,%s,%s,%s) ON CONFLICT(rule_id) WHERE resolved_at IS NULL DO NOTHING RETURNING id',(alert_id,rule['id'],rule['service'],metric)).fetchone()
                if row:
                    payload={'alert_id':str(alert_id),'rule_id':rule['id'],'service':rule['service'],'p99_upper_bound_ms':metric};c.execute('INSERT INTO webhook_outbox(alert_id,payload) VALUES(%s,%s)',(alert_id,Jsonb(payload)))

def send_once():
    token=uuid.uuid4()
    with connect() as c:
        row=c.execute("SELECT * FROM webhook_outbox WHERE attempts<5 AND (status='PENDING' OR (status='SENDING' AND lease_until<now())) ORDER BY alert_id FOR UPDATE SKIP LOCKED LIMIT 1").fetchone()
        if not row:return False
        c.execute("UPDATE webhook_outbox SET status='SENDING',attempts=attempts+1,lease_token=%s,lease_until=now()+interval '30 seconds' WHERE alert_id=%s",(token,row['alert_id']))
    try:
        with httpx.Client(timeout=3,trust_env=False) as client:
            response=client.post(os.getenv('WEBHOOK_URL','http://localhost:8090/events'),json=row['payload'],headers={'Idempotency-Key':str(row['alert_id']),'Authorization':'Bearer '+os.getenv('WEBHOOK_SECRET','demo-webhook-secret')});response.raise_for_status()
        status='DELIVERED'
    except Exception:status='PENDING'
    with connect() as c:c.execute('UPDATE webhook_outbox SET status=%s,delivered_at=CASE WHEN %s=\'DELIVERED\' THEN now() ELSE NULL END WHERE alert_id=%s AND lease_token=%s',(status,status,row['alert_id'],token))
    return status=='DELIVERED'
