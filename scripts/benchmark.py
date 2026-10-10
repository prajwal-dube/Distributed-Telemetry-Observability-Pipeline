import argparse,asyncio,json,statistics,time,uuid
from pathlib import Path
import httpx

def pct(xs,q):return round(sorted(xs)[min(len(xs)-1,int((len(xs)-1)*q))],3)
def batch(n):
    return {'batch_id':uuid.uuid4().hex,'spans':[{'trace_id':uuid.uuid4().hex,'span_id':uuid.uuid4().hex[:16],'service':['checkout','inventory','payments'][i%3],'operation':'benchmark','start_ms':int(time.time()*1000),'duration_ms':(i%100)+1,'status':500 if i%100==0 else 200} for i in range(n)]}
async def run(url,token,output,batches=100,batch_size=500,concurrency=16):
    out=Path(output);out.mkdir(parents=True,exist_ok=True);summaries=[]
    async with httpx.AsyncClient(base_url=url,timeout=30,trust_env=False,headers={'Authorization':'Bearer '+token}) as c:
        for repeat in range(1,4):
            inputs=[batch(batch_size) for _ in range(batches)];q=iter(inputs);latencies=[];codes={};accepted=[]
            async def client():
                for p in q:
                    t=time.perf_counter();r=await c.post('/ingest',json=p);latencies.append((time.perf_counter()-t)*1000);codes[str(r.status_code)]=codes.get(str(r.status_code),0)+1
                    if r.status_code==202:accepted.append(p['batch_id'])
            start=time.perf_counter();await asyncio.gather(*(client() for _ in range(concurrency)));http_seconds=time.perf_counter()-start
            # Poll completion for all accepted batches, not just admission speed.
            pending=set(accepted)
            while pending:
                for bid in list(pending):
                    r=await c.get('/batches/'+bid);r.raise_for_status()
                    if r.json().get('status')=='DONE':pending.remove(bid)
                if time.perf_counter()-start>180:raise RuntimeError('drain timeout')
                if pending:await asyncio.sleep(.1)
            total=time.perf_counter()-start
            report={'repeat':repeat,'batches_attempted':batches,'spans_per_batch':batch_size,'concurrency':concurrency,'response_counts':codes,'accepted_batches':len(accepted),'completed_spans':len(accepted)*batch_size,'http_duration_seconds':round(http_seconds,4),'load_plus_drain_seconds':round(total,4),'accepted_batches_per_second':round(len(accepted)/http_seconds,2),'admitted_spans_per_second':round(len(accepted)*batch_size/http_seconds,2),'completed_spans_per_second':round(len(accepted)*batch_size/total,2),'batch_admission_latency_ms':{'p50':pct(latencies,.5),'p95':pct(latencies,.95),'p99':pct(latencies,.99)},'batch_ids':accepted}
            (out/f'run-{repeat}.json').write_text(json.dumps(report,indent=2)+'\n');summaries.append(report);print(json.dumps({k:v for k,v in report.items() if k!='batch_ids'}),flush=True)
    summary={'runs':summaries,'total_completed_spans':sum(r['completed_spans'] for r in summaries),'median_completed_spans_per_second':statistics.median(r['completed_spans_per_second'] for r in summaries),'trace_sample_rate_note':'worker environment determines trace sampling; record it with results'};(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');return summary
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--url',default='http://localhost:8082');p.add_argument('--token',default='demo-token');p.add_argument('--output',default='results/reproduction');p.add_argument('--batches',type=int,default=100);a=p.parse_args();asyncio.run(run(a.url,a.token,a.output,a.batches))
