import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
from pathlib import Path
from fastapi import HTTPException
from dotenv import load_dotenv

# Load the hidden variables from the .env file
load_dotenv(Path(__file__).resolve().with_name('.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-db")

# Securely pull the URL from the hidden file
DB_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Establish and return a connection to the Neon cloud database."""
    # RealDictCursor makes Postgres act exactly like our old SQLite setup!
    url = os.getenv('DATABASE_URL')
    if not url:
        raise HTTPException(503, 'Neon database is not configured. Set DATABASE_URL on the backend.')
    try:
        return psycopg2.connect(url, cursor_factory=RealDictCursor, connect_timeout=10)
    except psycopg2.Error:
        raise HTTPException(503, 'The account database is temporarily unavailable. Please try again.')

def init_db():
    """Create tables in the cloud if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Create Drivers Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id TEXT PRIMARY KEY, name TEXT, phone TEXT, current_city TEXT,
            current_lat REAL, current_lon REAL, home_base_city TEXT, home_base_lat REAL,
            home_base_lon REAL, available_date TEXT, available_capacity REAL, allowed_crops TEXT
        )
    ''')

    # 2. Create Requests Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            request_id TEXT PRIMARY KEY, sender_name TEXT, phone TEXT, pickup_city TEXT,
            pickup_lat REAL, pickup_lon REAL, dropoff_city TEXT, dropoff_lat REAL,
            dropoff_lon REAL, requested_date TEXT, required_capacity REAL, crop_type TEXT
        )
    ''')

    # 3. Create Users Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            phone TEXT PRIMARY KEY,
            role TEXT,
            name TEXT,
            nin TEXT,
            password TEXT,
            primary_city TEXT
        )
    ''')

    # Additive migration: retain the existing users, roles and four-digit PINs.
    cursor.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT')
    cursor.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS pin TEXT')
    cursor.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS pin_hash TEXT')
    cursor.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS pin_salt TEXT')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_sessions (
        token_hash TEXT PRIMARY KEY, phone TEXT NOT NULL REFERENCES users(phone),
        expires_at TIMESTAMPTZ NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS login_attempts (
        phone TEXT PRIMARY KEY, failed_attempts INTEGER NOT NULL DEFAULT 0,
        locked_until TIMESTAMPTZ)''')
    cursor.execute('CREATE TABLE IF NOT EXISTS trace_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
    cursor.execute('''CREATE TABLE IF NOT EXISTS logistics_routes (
        id TEXT PRIMARY KEY, driver_phone TEXT NOT NULL REFERENCES users(phone),
        origin TEXT NOT NULL, destination TEXT NOT NULL, available_date TEXT NOT NULL,
        capacity DOUBLE PRECISION NOT NULL CHECK(capacity>0), crops TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'available', created_at TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS logistics_jobs (
        id TEXT PRIMARY KEY, route_id TEXT NOT NULL REFERENCES logistics_routes(id),
        farmer_phone TEXT NOT NULL REFERENCES users(phone), driver_phone TEXT NOT NULL REFERENCES users(phone),
        pickup_city TEXT NOT NULL, dropoff_city TEXT NOT NULL, crop TEXT NOT NULL,
        tonnes DOUBLE PRECISION NOT NULL CHECK(tonnes>0), requested_date TEXT NOT NULL,
        batch_id TEXT, status TEXT NOT NULL DEFAULT 'requested', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)''')
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS logistics_active_request ON logistics_jobs(farmer_phone,route_id) WHERE status IN ('requested','accepted','picked_up')")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Neon Cloud Database initialization complete. Tables are live!")

if __name__ == "__main__":
    init_db()
