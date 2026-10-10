"""Actual Celery process exit / Redis redelivery on an idle Compose stack."""
import json,os,subprocess,time,uuid
from pathlib import Path
import httpx

def docker(*args,env=None):return subprocess.run(['docker','compose',*args],env=env,check=True,capture_output=True,text=True).stdout

def main():
    env=os.environ.copy();env['BROKER_VISIBILITY_TIMEOUT']='5';docker('stop','worker','beat')
    p={'batch_id':uuid.uuid4().hex,'spans':[{'trace_id':'00000001000000000000000000000000','span_id':uuid.uuid4().hex[:16],'service':'checkout','operation':'recovery','start_ms':int(time.time()*1000),'duration_ms':500,'status':500}]}
    try:
        with httpx.Client(base_url='http://localhost:8082',headers={'Authorization':'Bearer demo-token'},trust_env=False,timeout=20) as c:
            c.post('/ingest',json=p).raise_for_status();crash=subprocess.Popen(['docker','compose','run','--rm','--no-deps','-e','CRASH_BEFORE_AGGREGATE=1','-e','BROKER_VISIBILITY_TIMEOUT=5','worker','python','-m','celery','-A','app.tasks','worker','--pool','solo','--loglevel','warning'],env=env)
            assert crash.wait(timeout=30)==42
            docker('up','-d','--force-recreate','--scale','worker=2','worker',env=env);start=time.monotonic()
            while time.monotonic()-start<240:
                state=c.get('/batches/'+p['batch_id']).json()
                if state.get('status')=='DONE':break
                time.sleep(.5)
            assert state.get('status')=='DONE';report={'batch_id':p['batch_id'],'crash_exit_code':42,'observed_recovery_seconds':time.monotonic()-start,'status':state['status']};Path('results/recovery-reproduction.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
    finally:docker('up','-d','--force-recreate','--scale','worker=2','worker','beat')
if __name__=='__main__':main()
