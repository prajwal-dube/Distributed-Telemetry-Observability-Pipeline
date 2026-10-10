import hashlib,json,os
from pathlib import Path
import psycopg,redis
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
R=redis.Redis.from_url(os.getenv('REDIS_URL','redis://localhost:6379/0'),decode_responses=True)
PREFIX='telemetry:'
def connect():return psycopg.connect(os.getenv('DATABASE_URL','postgresql://telemetry:telemetry@localhost:5432/telemetry'),row_factory=dict_row,connect_timeout=5)
def key_hash(token):return hashlib.sha256(token.encode()).hexdigest()
def migrate():
    with connect() as c:
        c.execute(Path(__file__).with_name('schema.sql').read_text())
        user=c.execute('INSERT INTO users(username,api_key_hash) VALUES(%s,%s) ON CONFLICT(username) DO UPDATE SET api_key_hash=excluded.api_key_hash RETURNING id',('demo',key_hash(os.getenv('API_TOKEN','demo-token')))).fetchone()['id']
        for service in ['checkout','inventory','payments']:c.execute('INSERT INTO services VALUES(%s,%s,%s) ON CONFLICT(name) DO NOTHING',(service,'commerce',user))
def service(name,cached=True,conn=None):
    key=PREFIX+'config:'+name
    if cached:
        value=R.get(key)
        if value:return json.loads(value)
    if conn:row=conn.execute('SELECT name,team,owner_user_id FROM services WHERE name=%s',(name,)).fetchone()
    else:
        with connect() as c:row=c.execute('SELECT name,team,owner_user_id FROM services WHERE name=%s',(name,)).fetchone()
    if row and cached:R.setex(key,30,json.dumps(row))
    return row
