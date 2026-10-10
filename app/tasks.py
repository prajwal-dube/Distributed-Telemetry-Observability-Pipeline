import json,logging,os,time
from celery import Celery
from .store import R,PREFIX,connect
from .aggregation import aggregate,window
from psycopg.types.json import Jsonb
celery=Celery('telemetry',broker=os.getenv('CELERY_BROKER_URL','redis://localhost:6379/1'))
celery.conf.update(task_acks_late=True,task_reject_on_worker_lost=True,worker_prefetch_multiplier=1,broker_transport_options={'visibility_timeout':int(os.getenv('BROKER_VISIBILITY_TIMEOUT','60'))},task_serializer='json',accept_content=['json'],task_ignore_result=True,broker_connection_retry_on_startup=True,beat_schedule={'evaluate':{'task':'telemetry.evaluate','schedule':10.0},'send':{'task':'telemetry.send','schedule':2.0}},timezone='UTC')
def selected(trace_id):return int(trace_id[:8],16)%10000<int(float(os.getenv('TRACE_SAMPLE_RATE','0.01'))*10000)
@celery.task(name='telemetry.process',autoretry_for=(Exception,),retry_backoff=True,retry_kwargs={'max_retries':5})
def process(batch_id):
    if R.hget(PREFIX+'batch:'+batch_id,'status')=='DONE':return
    text=R.get(PREFIX+'payload:'+batch_id)
    if not text:raise ValueError('payload missing/expired')
    payload=json.loads(text);sample=[s for s in payload['spans'] if selected(s['trace_id'])]
    if sample:
        with connect() as c:
            with c.cursor() as cur:cur.executemany('INSERT INTO spans VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(trace_id,span_id) DO NOTHING',[(s['trace_id'],s['span_id'],s.get('parent_id'),s['service'],s['operation'],s['start_ms'],s['duration_ms'],s['status']) for s in sample])
    if os.getenv('CRASH_BEFORE_AGGREGATE')=='1':os._exit(42)
    aggregate(batch_id,payload)
@celery.task(name='telemetry.evaluate',autoretry_for=(Exception,),retry_backoff=True,retry_kwargs={'max_retries':5})
def evaluate():
    from .alerts import evaluate_rules
    with connect() as c:services=[r['name'] for r in c.execute('SELECT name FROM services').fetchall()]
    now=time.time();snapshots={s:window(s,now) for s in services}
    for s,v in snapshots.items():R.setex(PREFIX+'snapshot:'+s,30,json.dumps(v))
    evaluate_rules(snapshots,now)
@celery.task(name='telemetry.send',autoretry_for=(Exception,),retry_backoff=True,retry_kwargs={'max_retries':5})
def send():
    from .alerts import send_once
    for _ in range(20):
        if not send_once():break
