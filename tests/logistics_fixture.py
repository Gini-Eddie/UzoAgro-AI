"""Disposable logistics schema for tests; never imported by the production API."""
SCHEMA = '''CREATE TABLE logistics_routes (
    id TEXT PRIMARY KEY,driver_phone TEXT,origin TEXT,destination TEXT,available_date TEXT,
    capacity REAL,crops TEXT,status TEXT,created_at TEXT);
CREATE TABLE logistics_jobs (
    id TEXT PRIMARY KEY,route_id TEXT,farmer_phone TEXT,driver_phone TEXT,pickup_city TEXT,
    dropoff_city TEXT,crop TEXT,tonnes REAL,requested_date TEXT,batch_id TEXT,status TEXT,
    created_at TEXT,updated_at TEXT);
CREATE UNIQUE INDEX logistics_active_request ON logistics_jobs(farmer_phone,route_id)
    WHERE status IN ('requested','accepted','picked_up');'''
