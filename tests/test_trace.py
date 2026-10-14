import hashlib
import sqlite3
import auth_api
from psycopg2.errors import UniqueViolation
from datetime import datetime
import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

from fastapi.testclient import TestClient
from trace_server import app
import trace_api


class NeonCursor:
    def __init__(self, conn): self.conn = conn; self.cursor = conn.cursor(); self.special = None
    def __enter__(self): return self
    def __exit__(self, *args): self.cursor.close()
    def execute(self, sql, values=()):
        if 'information_schema.columns' in sql:
            self.special = [{'column_name': k} for k in ['driver_id','name','phone','current_city','home_base_city','available_capacity','allowed_crops','available_date']]; return
        self.special = None
        values = tuple(v.isoformat() if isinstance(v, datetime) else v for v in values)
        try: self.cursor.execute(sql.replace('%s','?').replace(' FOR UPDATE',''), values)
        except sqlite3.IntegrityError as err: raise UniqueViolation(str(err))
    def decode(self, row):
        if row is None: return None
        value = dict(row)
        if value.get('locked_until'): value['locked_until'] = datetime.fromisoformat(value['locked_until'])
        return value
    def fetchone(self): return self.decode(self.cursor.fetchone())
    def fetchall(self): return self.special if self.special is not None else [self.decode(r) for r in self.cursor.fetchall()]

class NeonConnection:
    def __init__(self, path): self.conn=sqlite3.connect(path); self.conn.row_factory=sqlite3.Row
    def cursor(self): return NeonCursor(self.conn)
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def close(self): self.conn.close()

class TraceJourney(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"DATABASE_URL": "", "TRACE_SQLITE_PATH": self.tmp.name + "/trace.db", "OPENAI_API_KEY": "", "TRACE_CONTRACT_ADDRESS": ""})
        self.env.start()
        self.neon_path = self.tmp.name + '/accounts.db'
        conn = sqlite3.connect(self.neon_path)
        conn.executescript("""CREATE TABLE users(phone TEXT PRIMARY KEY,name TEXT,role TEXT,email TEXT,primary_city TEXT,nin TEXT,pin TEXT,pin_hash TEXT,pin_salt TEXT);
            CREATE TABLE user_sessions(token_hash TEXT PRIMARY KEY,phone TEXT,expires_at TEXT);
            CREATE TABLE login_attempts(phone TEXT PRIMARY KEY,failed_attempts INTEGER,locked_until TEXT);
            CREATE TABLE drivers(driver_id TEXT PRIMARY KEY,name TEXT,phone TEXT,current_city TEXT,home_base_city TEXT,available_capacity REAL,allowed_crops TEXT,available_date TEXT);""")
        # An existing legacy farmer account, not a separately created Trace account.
        conn.execute("INSERT INTO users(phone,name,role,primary_city,pin) VALUES (?,?,?,?,?)", ('08012345678','Ada Okafor','farmer','Ogbaru','1234'))
        conn.commit(); conn.close()
        self.db_patch = patch('auth_api.get_db_connection', lambda: NeonConnection(self.neon_path))
        self.db_patch.start()
        self.client = TestClient(app)
        self.account = {"name": "Ada Okafor", "phone": "08012345678", "pin": "1234", "primary_city": "Ogbaru", "role": "farmer", "nin": "11111111111"}
        result = self.client.post('/api/login', json=self.account)
        self.assertEqual(result.status_code, 200, result.text)
        self.headers = {"Authorization": "Bearer " + result.json()["token"]}

    def tearDown(self):
        self.db_patch.stop()
        self.env.stop()
        self.tmp.cleanup()

    def batch(self):
        d = self.client.post('/api/trace/diagnose/sample', headers=self.headers).json()
        r = self.client.post('/api/trace/batches', headers=self.headers, json={"crop": "Cassava", "quantity": 250, "community": "Ogbaru", "harvest_date": date.today().isoformat(), "diagnosis_id": d['id']})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def test_journey_private_data_and_tamper_detection(self):
        b = self.batch()
        self.assertNotIn('owner', b)
        self.assertNotIn(self.account['phone'], str(b))
        self.assertTrue(b['metadata']['diagnosis']['sample'])
        self.assertEqual(len(b['hash']), 64)
        base = '/api/trace/batches/' + b['id']
        for stage, location in [('Picked up', 'Ogbaru'), ('Received at hub', 'Onitsha Hub'), ('Delivered to buyer', 'Onitsha Market')]:
            r = self.client.post(base + '/checkpoints', headers=self.headers, json={"stage": stage, "location": location})
            self.assertEqual(r.status_code, 200, r.text)
        verification = self.client.get(base + '/verify').json()
        self.assertTrue(verification['integrity'])
        self.assertFalse(verification['blockchain_verified'])
        self.assertEqual(len(verification['batch']['checkpoints']), 3)
        stored = trace_api.get('batch:' + b['id'])
        stored['metadata']['quantity'] = 251
        trace_api.put('batch:' + b['id'], stored)
        verification = self.client.get(base + '/verify').json()
        self.assertFalse(verification['integrity'])
        self.assertEqual(verification['status'], 'Fingerprint mismatch')

    def test_auth_access_and_validation(self):
        b = self.batch()
        self.assertEqual(self.client.post('/api/trace/batches', json={}).status_code, 401)
        self.assertEqual(self.client.post('/api/signup', json=self.account).status_code, 409)
        conn = sqlite3.connect(self.neon_path)
        record = conn.execute('SELECT pin_hash FROM users WHERE phone=?', (self.account['phone'],)).fetchone()
        conn.close()
        self.assertTrue(record[0])
        self.assertNotEqual(record[0], self.account['pin'])
        other = self.client.post('/api/signup', json=self.account | {"phone": "08087654321"}).json()
        h = {"Authorization": "Bearer " + other['token']}
        self.assertEqual(self.client.post('/api/trace/batches/' + b['id'] + '/checkpoints', headers=h, json={"stage": "Picked up", "location": "Ogbaru"}).status_code, 403)
        self.assertEqual(self.client.get('/api/trace/batches', headers=h).json(), [])
        self.assertEqual(self.client.post('/api/trace/batches/' + b['id'] + '/checkpoints', headers=self.headers, json={"stage": "Received at hub", "location": "Onitsha"}).status_code, 409)

    def test_missing_credentials_never_verify(self):
        b = self.batch()
        response = self.client.post('/api/trace/batches/' + b['id'] + '/anchor', headers=self.headers, json={"transaction": "0x" + "a" * 64})
        self.assertEqual(response.status_code, 503)
        self.assertFalse(self.client.get('/api/trace/batches/' + b['id'] + '/verify').json()['blockchain_verified'])
        self.assertEqual(self.client.post('/api/trace/diagnose', headers=self.headers, files={"file": ('crop.png', b'fake', 'image/png')}).status_code, 503)

    def test_login_throttle_and_logout(self):
        for _ in range(5):
            self.assertEqual(self.client.post('/api/login', json={"phone": self.account['phone'], "pin": "0000"}).status_code, 401)
        self.assertEqual(self.client.post('/api/login', json={"phone": self.account['phone'], "pin": "1234"}).status_code, 429)
        self.client.post('/api/logout', headers=self.headers)
        self.assertEqual(self.client.get('/api/trace/batches', headers=self.headers).status_code, 401)

    def test_shared_identity_roles_and_old_trace_session_rejected(self):
        profile = self.client.get('/api/me', headers=self.headers)
        self.assertEqual(profile.json()['user']['role'], 'farmer')
        self.assertEqual(profile.json()['user']['phone'], self.account['phone'])
        conn = sqlite3.connect(self.neon_path)
        conn.execute('UPDATE users SET role=? WHERE phone=?', ('driver', self.account['phone']))
        conn.commit(); conn.close()
        self.assertEqual(self.client.get('/api/me', headers=self.headers).json()['user']['role'], 'driver')
        self.assertEqual(self.client.get('/api/trace/batches', headers=self.headers).status_code, 403)
        conn = sqlite3.connect(self.neon_path)
        conn.execute('UPDATE users SET role=? WHERE phone=?', ('farmer', self.account['phone']))
        conn.commit(); conn.close()
        driver = self.client.post('/api/signup', json=self.account | {'role': 'driver', 'phone': '08099999999'}).json()
        headers = {'Authorization': 'Bearer '+driver['token']}
        self.assertEqual(self.client.get('/api/me', headers=headers).json()['user']['role'], 'driver')
        self.assertEqual(self.client.get('/api/trace/batches', headers=headers).status_code, 403)
        self.assertEqual(self.client.post('/api/trace/diagnose/sample', headers=headers).status_code, 403)
        self.assertEqual(self.client.post('/api/trace/batches', headers=headers, json={'crop':'Cassava','quantity':1,'community':'Ogbaru','harvest_date':date.today().isoformat()}).status_code, 403)
        trace_api.put('session:' + hashlib.sha256(b'old-trace-token').hexdigest(), {'user': self.account, 'expires':'2999-01-01'})
        self.assertEqual(self.client.get('/api/me', headers={'Authorization':'Bearer old-trace-token'}).status_code, 401)
        self.client.post('/api/logout', headers=self.headers)
        self.assertEqual(self.client.get('/api/me', headers=self.headers).status_code, 401)
        self.assertEqual(self.client.get('/api/trace/batches', headers=self.headers).status_code, 401)

    def test_qr_and_frontend(self):
        self.assertEqual(self.client.get('/trace.html').status_code, 200)
        qr = self.client.get('/api/trace/qr', params={"url": 'http://localhost:8000/trace.html#verify/UZA-TEST'})
        self.assertEqual(qr.status_code, 200)
        self.assertIn('image/svg+xml', qr.headers['content-type'])
        self.assertIn('<svg', qr.text)

    def test_missing_neon_never_falls_back_to_trace_accounts(self):
        from database import get_db_connection
        with patch('auth_api.get_db_connection', get_db_connection):
            response = self.client.post('/api/login', json={'phone':self.account['phone'], 'pin':'1234'})
            self.assertEqual(response.status_code, 503)
            self.assertIn('Neon', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
