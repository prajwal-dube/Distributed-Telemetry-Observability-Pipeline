import json,re,time
from pathlib import Path
from fastapi import Depends,FastAPI,Header,HTTPException
from fastapi.responses import Response,HTMLResponse
from prometheus_client import CollectorRegistry,generate_latest
from prometheus_client.core import HistogramMetricFamily,CounterMetricFamily,GaugeMetricFamily
from .models import Batch
from .store import R,PREFIX,connect,key_hash,service
from .aggregation import admit,window,BUCKETS
app=FastAPI(title='Distributed Telemetry Pipeline',version='1.0.0')
def auth(authorization:str=Header(...)):
    if not authorization.startswith('Bearer '):raise HTTPException(401,'bearer token required')
    token=authorization[7:]
    with connect() as c:row=c.execute('SELECT id FROM users WHERE api_key_hash=%s AND active',(key_hash(token),)).fetchone()
    if not row:raise HTTPException(401,'invalid API token')
    return row['id']
@app.get('/health/live')
def live():return {'status':'up'}
@app.get('/health/ready')
def ready():
    R.ping()
    with connect() as c:c.execute('SELECT 1 FROM users LIMIT 1')
    return {'status':'ready'}
@app.post('/ingest',status_code=202)
def ingest(batch:Batch,user=Depends(auth)):
    for name in {s.service for s in batch.spans}:
        profile=service(name)
        if not profile or profile['owner_user_id']!=user:raise HTTPException(422,'unknown or unauthorized service')
    result=admit(batch)
    if result=='CONFLICT':raise HTTPException(409,'batch ID reused with different content')
    return {'batch_id':batch.batch_id,'status':'QUEUED' if result=='NEW' else result,'spans':len(batch.spans)}
@app.get('/batches/{batch_id}')
def batch(batch_id:str,user=Depends(auth)):
    if not re.fullmatch('[0-9a-f]{32}',batch_id):raise HTTPException(422,'invalid batch ID')
    result=R.hgetall(PREFIX+'batch:'+batch_id)
    if not result:raise HTTPException(404,'batch not found')
    # Single-tenant demo; batches are not isolated by user. See operations notes.
    return result
@app.get('/services/{name}/window')
def snapshot(name:str,user=Depends(auth)):
    profile=service(name)
    if not profile or profile['owner_user_id']!=user:raise HTTPException(404,'service not found')
    return window(name)
@app.get('/traces/{trace_id}')
def trace(trace_id:str,user=Depends(auth)):
    if not re.fullmatch('[0-9a-f]{32}',trace_id):raise HTTPException(422,'invalid trace ID')
    with connect() as c:rows=c.execute('SELECT s.* FROM spans s JOIN services v ON v.name=s.service WHERE trace_id=%s AND v.owner_user_id=%s ORDER BY start_ms,span_id',(trace_id,user)).fetchall()
    if not rows:raise HTTPException(404,'trace not sampled or not found')
    return {'trace_id':trace_id,'spans':rows}
@app.get('/alerts')
def alerts(user=Depends(auth)):
    with connect() as c:return c.execute('SELECT a.* FROM alerts a JOIN services s ON s.name=a.service WHERE s.owner_user_id=%s ORDER BY fired_at DESC LIMIT 100',(user,)).fetchall()
@app.get('/ui',response_class=HTMLResponse)
def ui():return Path(__file__).with_name('ui.html').read_text()

class Metrics:
    def collect(self):
        with connect() as c:names=[v['name'] for v in c.execute('SELECT name FROM services ORDER BY name').fetchall()]
        hist=HistogramMetricFamily('telemetry_request_duration_seconds','Cumulative processed duration histogram',labels=['service']);statuses=CounterMetricFamily('telemetry_requests','Processed spans by HTTP status class',labels=['service','status_class']);p99=GaugeMetricFamily('telemetry_window_p99_upper_bound_seconds','Upper-bound p99 for last six complete 10-second buckets',labels=['service']);mean=GaugeMetricFamily('telemetry_window_mean_seconds','Mean duration in last complete minute',labels=['service']);samples=GaugeMetricFamily('telemetry_window_samples','Window sample count',labels=['service'])
        for name in names:
            cumulative=R.hgetall(PREFIX+'cum:'+name);buckets=[];count=0
            for i,bound in enumerate(BUCKETS):count+=int(cumulative.get(f'bin:{i+1}',0));buckets.append((str(bound/1000),count))
            buckets.append(('+Inf',count));hist.add_metric([name],buckets,sum_value=float(cumulative.get('sum_ms',0))/1000)
            for cls in ['1xx','2xx','3xx','4xx','5xx']:statuses.add_metric([name,cls],int(cumulative.get('status:'+cls,0)))
            snap=window(name);samples.add_metric([name],snap['count'])
            if snap['count']:p99.add_metric([name],snap['p99_upper_bound_ms']/1000);mean.add_metric([name],snap['mean_ms']/1000)
        yield hist;yield statuses;yield p99;yield mean;yield samples
        runtime=json.loads(R.get(PREFIX+'agent_runtime') or '{}')
        for field,metric in [('heap_bytes','telemetry_agent_heap_bytes'),('peak_rss_bytes','telemetry_agent_peak_rss_bytes'),('cpu_cores','telemetry_agent_cpu_cores')]:
            g=GaugeMetricFamily(metric,'Latest agent process snapshot');g.add_metric([],runtime.get(field,0));yield g
        for field in ['spans','batches']:
            counter=CounterMetricFamily('telemetry_processed_'+field,'Deduplicated aggregation count');counter.add_metric([],int(R.hget(PREFIX+'totals',field) or 0));yield counter
registry=CollectorRegistry();registry.register(Metrics())
@app.get('/metrics')
def metrics():return Response(generate_latest(registry),media_type='text/plain; version=0.0.4')
