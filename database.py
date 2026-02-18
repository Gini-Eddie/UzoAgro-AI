"""
UzoAgro AI - SQLite Database Manager

Handles database connection, table creation, and migrating the initial
synthetic CSV datasets into the persistent SQLite database.
"""

import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-db")

DB_PATH = "uzoagro.db"


def get_db_connection():
    """Establish and return a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn


def init_db():
    """Create tables if they don't exist and load initial CSV data."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create Drivers Table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS drivers
                   (
                       driver_id
                       TEXT
                       PRIMARY
                       KEY,
                       name
                       TEXT,
                       phone
                       TEXT,
                       current_city
                       TEXT,
                       current_lat
                       REAL,
                       current_lon
                       REAL,
                       home_base_city
                       TEXT,
                       home_base_lat
                       REAL,
                       home_base_lon
                       REAL,
                       available_date
                       TEXT,
                       available_capacity
                       REAL,
                       allowed_crops
                       TEXT
                   )
                   ''')

    # Create Requests Table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS requests
                   (
                       request_id
                       TEXT
                       PRIMARY
                       KEY,
                       sender_name
                       TEXT,
                       phone
                       TEXT,
                       pickup_city
                       TEXT,
                       pickup_lat
                       REAL,
                       pickup_lon
                       REAL,
                       dropoff_city
                       TEXT,
                       dropoff_lat
                       REAL,
                       dropoff_lon
                       REAL,
                       requested_date
                       TEXT,
                       required_capacity
                       REAL,
                       crop_type
                       TEXT
                   )
                   ''')

    # Create Users Table for Authentication
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS users
                   (
                       phone
                       TEXT
                       PRIMARY
                       KEY,
                       role
                       TEXT,
                       name
                       TEXT,
                       nin
                       TEXT,
                       password
                       TEXT,
                       primary_city
                       TEXT
                   )
                   ''')

    conn.commit()

    # Migrate CSV data if tables are empty
    cursor.execute("SELECT COUNT(*) FROM drivers")
    if cursor.fetchone()[0] == 0:
        logger.info("Migrating drivers.csv to SQLite database...")
        if os.path.exists("data/drivers.csv"):
            df_drivers = pd.read_csv("data/drivers.csv")
            df_drivers.to_sql("drivers", conn, if_exists="append", index=False)

    cursor.execute("SELECT COUNT(*) FROM requests")
    if cursor.fetchone()[0] == 0:
        logger.info("Migrating requests.csv to SQLite database...")
        if os.path.exists("data/requests.csv"):
            df_req = pd.read_csv("data/requests.csv")
            df_req.to_sql("requests", conn, if_exists="append", index=False)

    conn.close()
    logger.info("Database initialization complete.")


if __name__ == "__main__":
    # Run this script directly to initialize the database
    init_db()