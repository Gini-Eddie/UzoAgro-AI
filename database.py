import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
from dotenv import load_dotenv

# Load the hidden variables from the .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-db")

# Securely pull the URL from the hidden file
DB_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Establish and return a connection to the Neon cloud database."""
    # RealDictCursor makes Postgres act exactly like our old SQLite setup!
    conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
    return conn

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

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Neon Cloud Database initialization complete. Tables are live!")

if __name__ == "__main__":
    init_db()