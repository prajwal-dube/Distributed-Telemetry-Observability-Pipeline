import hashlib,json,math,os,time
from .store import R,PREFIX
BUCKETS=[.5,1,2,5,10,25,50,100,200,500,1000,2000,5000,600000]
ADMIT=R.register_script('''
local old=redis.call('HGET',KEYS[1],'hash')
if old then if old~=ARGV[2] then return 'CONFLICT' end; return redis.call('HGET',KEYS[1],'status') end
redis.call('XADD',KEYS[3],'*','batch_id',ARGV[1])
redis.call('SET',KEYS[2],ARGV[3],'EX',86400)
redis.call('HSET',KEYS[1],'hash',ARGV[2],'status','QUEUED','spans',ARGV[4])
redis.call('EXPIRE',KEYS[1],86400)
return 'NEW'
''')
AGGREGATE=R.register_script('''
if redis.call('HGET',KEYS[1],'status')=='DONE' then return 0 end
if redis.call('EXISTS',KEYS[2])==0 then return redis.error_reply('batch payload expired') end
local batch=cjson.decode(ARGV[1]);local bounds=cjson.decode(ARGV[2]);local prefix=ARGV[3]
local slot=math.floor(batch.ingested_at/10)*10
-- Validate reserved accumulator key types before writes; Redis Lua is not rollback-capable.
for _,s in ipairs(batch.spans) do
 for _,key in ipairs({prefix..'cum:'..s.service,prefix..'window:'..s.service..':'..slot}) do
  local t=redis.call('TYPE',key).ok;if t~='none' and t~='hash' then return redis.error_reply('invalid accumulator type') end
 end
end
for _,s in ipairs(batch.spans) do
 local bin=#bounds;for i,b in ipairs(bounds) do if s.duration_ms<=b then bin=i;break end end
 for _,key in ipairs({prefix..'cum:'..s.service,prefix..'window:'..s.service..':'..slot}) do
  redis.call('HINCRBY',key,'count',1);redis.call('HINCRBYFLOAT',key,'sum_ms',s.duration_ms);redis.call('HINCRBY',key,'bin:'..bin,1)
  local cls=math.floor(s.status/100)..'xx';redis.call('HINCRBY',key,'status:'..cls,1)
 end
 redis.call('EXPIRE',prefix..'window:'..s.service..':'..slot,90)
end
redis.call('SET',prefix..'agent_runtime',cjson.encode(batch.runtime),'EX',120)
redis.call('HSET',KEYS[1],'status','DONE');redis.call('HINCRBY',prefix..'totals','batches',1);redis.call('HINCRBY',prefix..'totals','spans',#batch.spans)
return 1
''')
def admit(batch):
    payload=batch.model_dump(mode='json');canonical=json.dumps(payload,sort_keys=True,separators=(',',':'));digest=hashlib.sha256(canonical.encode()).hexdigest();
    # One Redis server timestamp per batch; avoid two TIME calls crossing a second.
    sec,micro=R.time();payload['ingested_at']=sec+micro/1e6
    return ADMIT(keys=[PREFIX+'batch:'+batch.batch_id,PREFIX+'payload:'+batch.batch_id,PREFIX+'outbox'],args=[batch.batch_id,digest,json.dumps(payload),len(batch.spans)])
def aggregate(batch_id,payload):return AGGREGATE(keys=[PREFIX+'batch:'+batch_id,PREFIX+'payload:'+batch_id],args=[json.dumps(payload),json.dumps(BUCKETS),PREFIX])
def percentile(counts,q=.99):
    total=sum(counts)
    if not total:return None
    rank=math.ceil(total*q);seen=0
    for bound,n in zip(BUCKETS,counts):
        seen+=n
        if seen>=rank:return bound
    return BUCKETS[-1]
def window(service,now=None):
    now=time.time() if now is None else now;end=math.floor(now/10)*10;keys=[PREFIX+f'window:{service}:{slot}' for slot in range(end-60,end,10)];pipe=R.pipeline()
    for key in keys:pipe.hgetall(key)
    values=pipe.execute();count=0;total=0;bins=[0]*len(BUCKETS)
    for v in values:
        count+=int(v.get('count',0));total+=float(v.get('sum_ms',0))
        for i in range(len(bins)):bins[i]+=int(v.get(f'bin:{i+1}',0))
    return {'service':service,'window_start':end-60,'window_end':end,'count':count,'mean_ms':total/count if count else None,'p99_upper_bound_ms':percentile(bins),'histogram_bins':bins}
