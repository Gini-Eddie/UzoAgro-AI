"""One Neon-backed account and session for logistics and Trace."""
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from psycopg2.errors import UniqueViolation
from database import get_db_connection

router = APIRouter(prefix='/api', tags=['UzoAgro accounts'])
security = HTTPBearer(auto_error=False)


class Login(BaseModel):
    phone: str = Field(pattern=r'^\+?[0-9]{10,15}$')
    pin: str = Field(pattern=r'^[0-9]{4}$')


class Signup(Login):
    role: Literal['farmer', 'driver']
    name: str = Field(min_length=2, max_length=80)
    primary_city: str = Field(min_length=2, max_length=120)
    nin: str = Field(pattern=r'^[0-9]{11}$')
    email: str | None = Field(default=None, max_length=254)
    capacity: float = Field(default=15, gt=0, le=100)
    reference: str | None = None


def public(account):
    return {k: account.get(k) for k in ('name', 'role', 'phone', 'email', 'primary_city')}


def hash_pin(pin, salt):
    return hashlib.pbkdf2_hmac('sha256', pin.encode(), salt.encode(), 600000).hex()


def issue_session(cur, account):
    token = secrets.token_urlsafe(32)
    cur.execute('INSERT INTO user_sessions (token_hash, phone, expires_at) VALUES (%s, %s, %s)',
                (hashlib.sha256(token.encode()).hexdigest(), account['phone'], datetime.now(timezone.utc) + timedelta(days=1)))
    return {'status': 'success', 'token': token, 'user': public(account)}


def current_user(auth: HTTPAuthorizationCredentials = Depends(security)):
    if not auth:
        raise HTTPException(401, 'Sign in to your UzoAgro account.')
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('''SELECT u.name, u.role, u.phone, u.email, u.primary_city
                FROM user_sessions s JOIN users u ON u.phone=s.phone
                WHERE s.token_hash=%s AND s.expires_at>%s''',
                (hashlib.sha256(auth.credentials.encode()).hexdigest(), datetime.now(timezone.utc)))
            account = cur.fetchone()
        if not account:
            raise HTTPException(401, 'Your session expired. Please sign in again.')
        if account['role'] not in ('farmer', 'driver'):
            raise HTTPException(403, 'This account has an unsupported role.')
        return public(account)
    finally:
        conn.close()


def farmer(account=Depends(current_user)):
    if account['role'] != 'farmer':
        raise HTTPException(403, 'Crop assessments and batch management are for farmer accounts.')
    return account


@router.post('/login')
def login(data: Login):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT failed_attempts, locked_until FROM login_attempts WHERE phone=%s', (data.phone,))
            attempt = cur.fetchone()
            if attempt and attempt['locked_until'] and attempt['locked_until'] > datetime.now(timezone.utc):
                raise HTTPException(429, 'Too many attempts. Try again in 15 minutes.')
            cur.execute('SELECT name, role, phone, email, primary_city, pin, pin_hash, pin_salt FROM users WHERE phone=%s', (data.phone,))
            account = cur.fetchone()
            valid = account and (hmac.compare_digest(hash_pin(data.pin, account['pin_salt']), account['pin_hash'])
                                 if account['pin_hash'] and account['pin_salt'] else hmac.compare_digest(data.pin, str(account['pin'] or '')))
            if not valid:
                count = (attempt['failed_attempts'] if attempt and not attempt['locked_until'] else 0) + 1
                until = datetime.now(timezone.utc) + timedelta(minutes=15) if count >= 5 else None
                cur.execute('''INSERT INTO login_attempts(phone,failed_attempts,locked_until) VALUES (%s,%s,%s)
                    ON CONFLICT(phone) DO UPDATE SET failed_attempts=excluded.failed_attempts, locked_until=excluded.locked_until''', (data.phone, count, until))
                conn.commit()
                raise HTTPException(401, 'Invalid phone number or PIN.')
            if account['role'] not in ('farmer', 'driver'):
                raise HTTPException(403, 'This account has an unsupported role.')
            # Existing four-digit PINs remain valid; add a hash without changing the PIN.
            if not account['pin_hash']:
                salt = secrets.token_hex(16)
                cur.execute('UPDATE users SET pin_hash=%s, pin_salt=%s WHERE phone=%s', (hash_pin(data.pin, salt), salt, data.phone))
            cur.execute('DELETE FROM login_attempts WHERE phone=%s', (data.phone,))
            result = issue_session(cur, account)
        conn.commit()
        return result
    finally:
        conn.close()


@router.post('/signup')
def signup(data: Signup):
    conn = get_db_connection()
    try:
        salt = secrets.token_hex(16)
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO users(role,name,phone,nin,primary_city,email,pin_hash,pin_salt)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                (data.role, data.name, data.phone, data.nin, data.primary_city, data.email, hash_pin(data.pin, salt), salt))
            if data.role == 'driver':
                # Respect the actual columns in the existing drivers table.
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='drivers'")
                columns = {r['column_name'] for r in cur.fetchall()}
                values = {'driver_id': 'DRV-' + secrets.token_hex(6).upper(), 'name': data.name, 'driver_name': data.name,
                          'phone': data.phone, 'current_city': data.primary_city, 'home_base_city': data.primary_city,
                          'home_base': data.primary_city, 'available_capacity': data.capacity, 'capacity': data.capacity,
                          'allowed_crops': 'All', 'available_date': datetime.now(timezone.utc).date().isoformat()}
                selected = {k: v for k, v in values.items() if k in columns}
                cur.execute('INSERT INTO drivers (' + ','.join(selected) + ') VALUES (' + ','.join(['%s'] * len(selected)) + ')', tuple(selected.values()))
            result = issue_session(cur, data.model_dump())
        conn.commit()
        return result
    except UniqueViolation:
        conn.rollback()
        raise HTTPException(409, 'Phone number already registered. Sign in instead.')
    finally:
        conn.close()


@router.get('/me')
def me(account=Depends(current_user)):
    return {'user': account}


@router.post('/logout')
def logout(auth: HTTPAuthorizationCredentials = Depends(security)):
    if auth:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM user_sessions WHERE token_hash=%s', (hashlib.sha256(auth.credentials.encode()).hexdigest(),))
            conn.commit()
        finally:
            conn.close()
    return {'status': 'signed out'}
