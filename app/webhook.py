"""Local notification sink only. Demonstrates receiver-side durable deduplication."""
import os,uuid
from fastapi import FastAPI,Header,HTTPException
from psycopg.types.json import Jsonb
from .store import connect
app=FastAPI(title='Local webhook sink')
@app.post('/events')
def event(payload:dict,idempotency_key:uuid.UUID=Header(...),authorization:str=Header(...)):
    if authorization!='Bearer '+os.getenv('WEBHOOK_SECRET','demo-webhook-secret'):raise HTTPException(401,'invalid webhook secret')
    if payload.get('alert_id')!=str(idempotency_key):raise HTTPException(422,'event/key mismatch')
    with connect() as c:c.execute('INSERT INTO webhook_receipts(alert_id,payload) VALUES(%s,%s) ON CONFLICT(alert_id) DO NOTHING',(idempotency_key,Jsonb(payload)))
    return {'received':str(idempotency_key)}
