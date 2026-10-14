"""Read-only connection check. Never print the connection URL or driver error text."""
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv
import psycopg2

root = Path(__file__).resolve().parents[1]
load_dotenv(root / '.env')
url = (os.getenv('DATABASE_URL') or '').strip()
if not url:
    print('DATABASE_URL is missing. Add your existing Neon connection string to the project .env file.')
    sys.exit(1)
try:
    parsed = urlsplit(url)
    if parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname or not parsed.username or not parsed.path.strip('/'):
        raise ValueError()
except ValueError:
    print('DATABASE_URL is not a valid PostgreSQL connection URL. Copy the complete connection string from Neon.')
    sys.exit(1)

try:
    conn = psycopg2.connect(url, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name IN ('users','user_sessions','login_attempts','trace_records')")
            tables = {row[0] for row in cur.fetchall()}
        print('Neon connection successful. No account records were read or modified.')
        if 'users' not in tables:
            print('The users table is missing. Confirm that this is the existing UzoAgro branch and database before starting the backend.')
            sys.exit(2)
        missing = {'user_sessions', 'login_attempts', 'trace_records'} - tables
        if missing:
            print('Existing users table found. Restarting the updated backend will add the shared-session and Trace tables.')
        else:
            print('Existing users and shared-session tables found. Restart the local backend to load the connection.')
    finally:
        conn.close()
except psycopg2.Error as error:
    if error.pgcode == '28P01':
        print('Neon rejected the database credentials. Check the connection string for the selected role.')
    else:
        print('Neon could not be reached or rejected the connection. Check the complete URL, SSL settings, and network access.')
    sys.exit(1)
