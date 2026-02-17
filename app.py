from typing import Any, Dict, List, Optional
import logging
import sqlite3
import pandas as pd

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

# Import our new database manager and the matching engine
from database import get_db_connection, init_db
from matching_engine import run_matching_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-api")

app = FastAPI(title="UzoAgro AI Matching API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Expanded 15-City Geographic Dictionary (North, South, East, West, Middle-Belt)
CITIES = {
    # West / Mid-West
    "Lagos": (6.5244, 3.3792), "Ibadan": (7.3775, 3.9470), "Ilorin": (8.4799, 4.5418), "Akure": (7.2571, 5.2058),
    # North / Middle-Belt
    "Abuja": (9.0765, 7.3986), "Kano": (12.0022, 8.5920), "Kaduna": (10.5105, 7.4165), "Jos": (9.8965, 8.8583), "Sokoto": (13.0059, 5.2476),
    # East
    "Enugu": (6.4402, 7.4943), "Awka": (6.2100, 7.0700), "Onitsha": (6.1500, 6.7800), "Owerri": (5.4833, 7.0333),
    # South-South
    "Port Harcourt": (4.8156, 7.0498), "Uyo": (5.0380, 7.9098)
}

# --- Teammate's AI Botanical Diagnostic Placeholder ---
class DiagnosisRequest(BaseModel):
    crop_type: str
    symptoms: str

class FarmerRequest(BaseModel):
    farmer_name: str
    pickup_city: str
    dropoff_city: str
    crop_type: str
    required_capacity: float
    requested_date: str

@app.post("/diagnose", response_class=JSONResponse)
def diagnose_crop(req: DiagnosisRequest):
    """
    Endpoint for the AI Botanical Diagnostics feature.
    Your teammate will inject their NLP logic here.
    """
    # Placeholder logic for the UI to test against
    return JSONResponse(content={
        "status": "success",
        "crop_analyzed": req.crop_type,
        "symptoms_received": req.symptoms,
        "ai_diagnosis": "Analysis pending...",
        "herbal_remedy": "Natural botanical solution will populate here."
    })


@app.on_event("startup")
def startup_load_data():
    """Ensure database is ready when the server starts."""
    try:
        init_db()
    except Exception as e:
        logger.exception("Failed to initialize database on startup")
        raise


@app.get("/", response_class=JSONResponse)
def root():
    return JSONResponse(content={"status": "UzoAgro AI API & Database running"})


@app.post("/match/custom", response_class=JSONResponse)
def match_custom_request(req: FarmerRequest):
    """Saves the request to the DB and returns the top 3 AI matches."""
    if req.pickup_city not in CITIES or req.dropoff_city not in CITIES:
        raise HTTPException(status_code=400, detail="Invalid city selected")

    p_lat, p_lon = CITIES[req.pickup_city]
    d_lat, d_lon = CITIES[req.dropoff_city]

    # Generate a new unique request ID
    req_id = f"REQ-LIVE-{pd.Timestamp.now().strftime('%H%M%S')}"

    # 1. Save the new request permanently to the SQLite database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
                   INSERT INTO requests (request_id, sender_name, phone, pickup_city, pickup_lat, pickup_lon,
                                         dropoff_city, dropoff_lat, dropoff_lon, requested_date, required_capacity,
                                         crop_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ''', (
                       req_id, req.farmer_name, "PENDING", req.pickup_city, p_lat, p_lon,
                       req.dropoff_city, d_lat, d_lon, req.requested_date, req.required_capacity, req.crop_type
                   ))
    conn.commit()

    # 2. Pull all available drivers dynamically from the database
    drivers_df = pd.read_sql("SELECT * FROM drivers", conn, parse_dates=["available_date"])
    conn.close()

    # 3. Create a DataFrame for this specific request to feed the AI Engine
    single_req_df = pd.DataFrame([{
        "request_id": req_id,
        "pickup_lat": p_lat,
        "pickup_lon": p_lon,
        "dropoff_lat": d_lat,
        "dropoff_lon": d_lon,
        "requested_date": pd.to_datetime(req.requested_date),
        "required_capacity": req.required_capacity,
        "crop_type": req.crop_type
    }])

    # 4. Run the matching intelligence
    try:
        matches_df = run_matching_engine(drivers_df, single_req_df, top_k=3)
        payload = jsonable_encoder(matches_df.to_dict(orient="records"))
        return JSONResponse(content=payload)
    except Exception as e:
        logger.exception("Error computing live matches")
        raise HTTPException(status_code=500, detail=str(e))