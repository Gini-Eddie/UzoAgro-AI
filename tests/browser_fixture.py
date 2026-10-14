"""Disposable UI-test accounts. Never used by trace_server or the production backend."""
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL'] = ''
os.environ['OPENAI_API_KEY'] = ''
from test_trace import NeonConnection
import auth_api
import logistics_api
from logistics_fixture import SCHEMA
from datetime import date, datetime, timezone
from trace_server import app
import uvicorn

with tempfile.TemporaryDirectory() as tmp:
    path = str(Path(tmp) / 'accounts.db')
    os.environ['TRACE_SQLITE_PATH'] = str(Path(tmp) / 'records.db')
    conn = sqlite3.connect(path)
    conn.executescript('''CREATE TABLE users(phone TEXT PRIMARY KEY,name TEXT,role TEXT,email TEXT,primary_city TEXT,nin TEXT,pin TEXT,pin_hash TEXT,pin_salt TEXT);
        CREATE TABLE user_sessions(token_hash TEXT PRIMARY KEY,phone TEXT,expires_at TEXT);
        CREATE TABLE login_attempts(phone TEXT PRIMARY KEY,failed_attempts INTEGER,locked_until TEXT);
        CREATE TABLE drivers(driver_id TEXT PRIMARY KEY,name TEXT,phone TEXT,current_city TEXT,home_base_city TEXT,available_capacity REAL,allowed_crops TEXT,available_date TEXT);''')
    conn.executescript(SCHEMA)
    for phone, name, role in [('00000000001', 'Ada Â· Demo', 'farmer'), ('00000000002', 'Chidi Â· Demo', 'driver')]:
        conn.execute('INSERT INTO users(phone,name,role,primary_city,pin) VALUES (?,?,?,?,?)', (phone,name,role,'Onitsha','1234'))
    stamp=datetime.now(timezone.utc).isoformat()
    conn.execute('INSERT INTO logistics_routes VALUES (?,?,?,?,?,?,?,?,?)', ('RTE-DEMO','00000000002','Onitsha','Lagos',date.today().isoformat(),15,'Cassava|Maize|Yam','available',stamp))
    # Fictional drivers cover useful matches plus capacity, crop, and direction exclusions.
    samples=[('00000000003','Ngozi · Demo','Onitsha','Lagos',8,'Cassava|Maize'),('00000000004','Musa · Demo','Abuja','Kano',12,'Rice|Beans'),('00000000005','Bisi · Demo','Lagos','Ibadan',6,'Tomatoes|Plantain'),('00000000006','Emeka · Demo','Onitsha','Lagos',0.1,'Cassava'),('00000000007','Ifeoma · Demo','Lagos','Onitsha',10,'Cassava|Yam')]
    for phone,name,origin,destination,capacity,crops in samples:
        conn.execute('INSERT INTO users(phone,name,role,primary_city,pin) VALUES (?,?,?,?,?)',(phone,name,'driver',origin,'1234'))
        conn.execute('INSERT INTO logistics_routes VALUES (?,?,?,?,?,?,?,?,?)',('RTE-'+phone,phone,origin,destination,date.today().isoformat(),capacity,crops,'available',stamp))
    conn.execute('INSERT INTO logistics_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',('DEMO-PENDING','RTE-DEMO','00000000001','00000000002','Onitsha','Lagos','Cassava',.25,date.today().isoformat(),None,'requested',stamp,stamp))
    for back, count in zip(range(5,-1,-1), [2,4,3,5,6,4]):
        absolute=date.today().year*12+date.today().month-1-back
        year,month=divmod(absolute,12)
        stamp=f'{year:04d}-{month+1:02d}-10T08:00:00+00:00'
        for index in range(count):
            status=('requested' if index==0 else 'picked_up' if index==1 else 'delivered') if back==0 else 'delivered'
            route_id=f'DEMO-RTE-{back}-{index}'
            route_status='available' if status=='requested' else 'booked' if status=='picked_up' else 'completed'
            conn.execute('INSERT INTO logistics_routes VALUES (?,?,?,?,?,?,?,?,?)', (route_id,'00000000002','Onitsha','Lagos',stamp[:10],5,'Cassava|Maize',route_status,stamp))
            conn.execute('INSERT INTO logistics_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', (f'DEMO-JOB-{back}-{index}',route_id,'00000000001','00000000002','Onitsha','Lagos','Cassava',.25,stamp[:10],None,status,stamp,stamp))
    conn.commit(); conn.close()
    @app.middleware('http')
    async def demo_label(request, call_next):
        response=await call_next(request)
        if request.url.path in ('/dashboard.html','/login.html'):
            from fastapi.responses import HTMLResponse
            html=(Path(__file__).resolve().parents[1]/'frontend'/request.url.path.lstrip('/')).read_text(encoding='utf-8')
            html=re.sub(r'(<body[^>]*>)', r'<div style="position:sticky;top:0;z-index:1000;background:#f8e4b5;padding:12px;text-align:center;font:13px Manrope,sans-serif;color:#59441c">DEMO WORKSPACE · Fictional accounts and routes · Data resets on restart</div>', html, count=1)
            return HTMLResponse(html)
        return response
    auth_api.get_db_connection = lambda: NeonConnection(path)
    logistics_api.get_db_connection = lambda: NeonConnection(path)
    uvicorn.run(app, host='127.0.0.1', port=int(os.environ.get('DEMO_PORT','8002')))
