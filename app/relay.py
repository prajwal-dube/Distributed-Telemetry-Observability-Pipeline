import os,time
from redis.exceptions import ResponseError
from .store import R,PREFIX
from .tasks import process
STREAM=PREFIX+'outbox';GROUP='celery-publishers'
def once(consumer,cursor='0-0'):
    claimed=R.xautoclaim(STREAM,GROUP,consumer,2000,cursor,count=20);next_cursor=claimed[0];messages=claimed[1]
    new=R.xreadgroup(GROUP,consumer,{STREAM:'>'},count=20,block=500)
    for _,entries in new:messages+=entries
    for msg_id,fields in messages:
        process.apply_async(args=[fields['batch_id']]);R.xack(STREAM,GROUP,msg_id)
    return next_cursor
if __name__=='__main__':
    try:R.xgroup_create(STREAM,GROUP,'0',mkstream=True)
    except ResponseError as e:
        if 'BUSYGROUP' not in str(e):raise
    consumer=os.getenv('HOSTNAME','relay')+'-'+str(os.getpid());cursor='0-0'
    while True:
        try:cursor=once(consumer,cursor)
        except Exception as e:print('relay retry:',repr(e),flush=True);time.sleep(1)
