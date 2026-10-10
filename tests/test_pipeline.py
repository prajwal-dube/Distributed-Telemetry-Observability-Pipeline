import json,math,os,time,uuid
import pytest
from pydantic import ValidationError
from fastapi.testclient import TestClient
from app.models import Batch
from app.aggregation import percentile,admit,aggregate,window
from app.alerts import state_transition,evaluate_rules
from app.store import R,PREFIX,connect

def payload(n=3):
    trace=uuid.uuid4().hex;return {'batch_id':uuid.uuid4().hex,'spans':[{'trace_id':trace,'span_id':f'{i:016x}','service':'checkout','operation':'test','start_ms':int(time.time()*1000),'duration_ms':110,'status':500 if i==0 else 200} for i in range(n)]}
def test_histogram_percentile():
    assert percentile([0]*14) is None
    counts=[0]*14;counts[8]=100;assert percentile(counts)==200
    counts[0]=1000;counts[8]=1;assert percentile(counts)==.5
def test_alert_for_five_minutes_and_reset():
    pending=None
    for now in range(0,300,10):pending,fire=state_transition(pending,now,True,300);assert not fire
    pending,fire=state_transition(pending,300,True,300);assert fire
    assert state_transition(pending,310,False,300)==(None,False)
def test_bad_payload():
    p=payload();p['spans'][0]['duration_ms']=float('nan')
    with pytest.raises(ValidationError):Batch.model_validate(p)
    p=payload();p['spans'][0]['trace_id']='bad'
    with pytest.raises(ValidationError):Batch.model_validate(p)
    p=payload();p['spans'][0]['duration_ms']=-1
    with pytest.raises(ValidationError):Batch.model_validate(p)

integration=pytest.mark.skipif(os.getenv('INTEGRATION')!='1',reason='requires real Redis/PostgreSQL')
@integration
def test_batch_and_trace_replay(monkeypatch):
    monkeypatch.setenv('TRACE_SAMPLE_RATE','1');from app.tasks import process
    b=Batch.model_validate(payload());assert admit(b)=='NEW';assert admit(b)=='QUEUED'
    process.run(b.batch_id);process.run(b.batch_id)
    assert R.hget(PREFIX+'batch:'+b.batch_id,'status')=='DONE';assert admit(b)=='DONE'
    with connect() as c:assert c.execute('SELECT count(*) n FROM spans WHERE trace_id=%s',(b.spans[0].trace_id,)).fetchone()['n']==3
    b.spans[0].duration_ms=999;assert admit(b)=='CONFLICT'
@integration
def test_completed_sliding_window_boundaries():
    p=payload(10);b=Batch.model_validate(p);admit(b);data=json.loads(R.get(PREFIX+'payload:'+b.batch_id));now=math.floor(time.time()/10)*10;data['ingested_at']=now-10
    before=window('checkout',now)['count'];aggregate(b.batch_id,data);after=window('checkout',now);assert after['count']==before+10;assert after['p99_upper_bound_ms']==200
    assert window('checkout',now+70)['count']==0
@integration
def test_alert_dedup_recovery_and_missing_ticks():
    with connect() as c:
        c.execute('UPDATE alert_rules SET enabled=false');rule=c.execute("INSERT INTO alert_rules(service,threshold_ms,for_seconds) VALUES('checkout',100,20) RETURNING id").fetchone()['id']
    now=time.time();snapshot={'checkout':{'p99_upper_bound_ms':200,'count':20,'window_end':now}}
    for i in [0,10,20,30]:snapshot['checkout']['window_end']=now+i;evaluate_rules(snapshot,now+i)
    with connect() as c:
        assert c.execute('SELECT count(*) n FROM alerts WHERE rule_id=%s',(rule,)).fetchone()['n']==1
        assert c.execute('SELECT count(*) n FROM webhook_outbox w JOIN alerts a ON a.id=w.alert_id WHERE a.rule_id=%s',(rule,)).fetchone()['n']==1
    # Missing data resolves, and a scheduling gap restarts the pending period.
    evaluate_rules({},now+40);snapshot['checkout']['window_end']=now+50;evaluate_rules(snapshot,now+50);snapshot['checkout']['window_end']=now+100;evaluate_rules(snapshot,now+100)
    with connect() as c:
        row=c.execute('SELECT pending_since FROM alert_rules WHERE id=%s',(rule,)).fetchone();assert row['pending_since']==now+100;c.execute('UPDATE alert_rules SET enabled=false WHERE id=%s',(rule,))
@integration
def test_api_auth_validation():
    from app.api import app
    client=TestClient(app);headers={'Authorization':'Bearer demo-token'};p=payload()
    assert client.post('/ingest',json=p).status_code==422
    assert client.post('/ingest',json=p,headers={'Authorization':'Bearer bad'}).status_code==401
    assert client.post('/ingest',json=p,headers=headers).status_code==202
    p['spans'][0]['service']='unknown';p['batch_id']=uuid.uuid4().hex;assert client.post('/ingest',json=p,headers=headers).status_code==422
    assert client.get('/metrics').status_code==200
@integration
def test_local_webhook_receiver_replay():
    from app.webhook import app
    client=TestClient(app);alert_id=str(uuid.uuid4());event={'alert_id':alert_id,'service':'checkout'};headers={'Authorization':'Bearer demo-webhook-secret','Idempotency-Key':alert_id}
    assert client.post('/events',json=event,headers=headers).status_code==200
    assert client.post('/events',json=event,headers=headers).status_code==200
    with connect() as c:assert c.execute('SELECT count(*) n FROM webhook_receipts WHERE alert_id=%s',(alert_id,)).fetchone()['n']==1
