"""Compare warm Redis config cache with SQL on the same reused connection."""
import json,time
from app.store import connect,service

def run(n=3000):
    report={}
    with connect() as conn:
        for cached in [False,True]:
            service('checkout',cached=True,conn=conn);times=[]
            for _ in range(n):
                start=time.perf_counter_ns();row=service('checkout',cached=cached,conn=conn);times.append((time.perf_counter_ns()-start)/1e6);assert row['name']=='checkout'
            times.sort();report['warm_cache' if cached else 'uncached_indexed_table']={'n':n,'p50_ms':times[int((n-1)*.5)],'p95_ms':times[int((n-1)*.95)],'p99_ms':times[int((n-1)*.99)]}
    a=report['uncached_indexed_table']['p50_ms'];b=report['warm_cache']['p50_ms'];report['p50_reduction_pct']=100*(a-b)/a;return report
if __name__=='__main__':print(json.dumps(run(),indent=2))
