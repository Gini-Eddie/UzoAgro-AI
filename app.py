from typing import Any, Dict, List, Optional
import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

import pandas as pd

# Import the matching engine loader and matcher
from matching_engine import load_data, run_matching_engine


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-api")

app = FastAPI(title="UzoAgro AI Matching API", version="0.1.0")

# Allow all origins for now (frontend will be added later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Globals to hold datasets in memory
drivers_df: Optional[pd.DataFrame] = None
requests_df: Optional[pd.DataFrame] = None


@app.on_event("startup")
def startup_load_data() -> None:
    """Load drivers and requests into memory when the app starts."""
    global drivers_df, requests_df
    try:
        drivers_df, requests_df = load_data("data/drivers.csv", "data/requests.csv")
        logger.info(f"Loaded {len(drivers_df)} drivers and {len(requests_df)} requests into memory")
    except Exception as e:
        logger.exception("Failed to load datasets on startup")
        # Re-raise to prevent the app from starting in a broken state
        raise


@app.get("/", response_class=JSONResponse)
def root() -> JSONResponse:
    return JSONResponse(content={"status": "UzoAgro AI API running"})


@app.get("/requests", response_class=JSONResponse)
def list_requests() -> JSONResponse:
    """Return all requests currently loaded in memory."""
    if requests_df is None:
        raise HTTPException(status_code=500, detail="Requests dataset is not loaded")
    try:
        # Use jsonable_encoder to convert numpy/pandas types to JSON serializable
        payload = jsonable_encoder(requests_df.to_dict(orient="records"))
        return JSONResponse(content=payload)
    except Exception as e:
        logger.exception("Error serializing requests dataframe")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/drivers", response_class=JSONResponse)
def list_drivers() -> JSONResponse:
    """Return all drivers currently loaded in memory."""
    if drivers_df is None:
        raise HTTPException(status_code=500, detail="Drivers dataset is not loaded")
    try:
        payload = jsonable_encoder(drivers_df.to_dict(orient="records"))
        return JSONResponse(content=payload)
    except Exception as e:
        logger.exception("Error serializing drivers dataframe")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/match/{request_id}", response_class=JSONResponse)
def match_request(request_id: str) -> JSONResponse:
    """Return top 3 driver matches for the given request_id.

    This runs the matching logic in-memory for the single request and does NOT
    save results to disk.
    """
    if drivers_df is None or requests_df is None:
        raise HTTPException(status_code=500, detail="Datasets are not loaded")

    # Find the request row
    matched_rows = requests_df[requests_df["request_id"].astype(str) == str(request_id)]
    if matched_rows.empty:
        raise HTTPException(status_code=404, detail=f"Request {request_id} not found")

    # Use a DataFrame with only that request
    single_req_df = matched_rows.copy()

    try:
        matches_df = run_matching_engine(drivers_df, single_req_df, top_k=3)
    except Exception as e:
        logger.exception("Error computing matches")
        raise HTTPException(status_code=500, detail=str(e))

    # Serialize and return
    try:
        payload = jsonable_encoder(matches_df.to_dict(orient="records"))
        return JSONResponse(content=payload)
    except Exception as e:
        logger.exception("Error serializing matches")
        raise HTTPException(status_code=500, detail=str(e))


from pydantic import BaseModel
from datetime import datetime

# Geographic Dictionary for the API to translate cities back to coordinates
CITIES = {
    "Lagos": (6.5244, 3.3792), "Enugu": (6.4402, 7.4943), "Awka": (6.2100, 7.0700),
    "Onitsha": (6.1500, 6.7800), "Abuja": (9.0765, 7.3986), "Port Harcourt": (4.8156, 7.0498),
    "Ibadan": (7.3775, 3.9470), "Kano": (12.0022, 8.5920), "Kaduna": (10.5105, 7.4165),
    "Ilorin": (8.4799, 4.5418)
}


class FarmerRequest(BaseModel):
    pickup_city: str
    dropoff_city: str
    crop_type: str
    required_capacity: float
    requested_date: str


@app.post("/match/custom", response_class=JSONResponse)
def match_custom_request(req: FarmerRequest) -> JSONResponse:
    """Handles dynamic requests from the frontend Farmer Form."""
    if drivers_df is None:
        raise HTTPException(status_code=500, detail="Drivers dataset not loaded")

    if req.pickup_city not in CITIES or req.dropoff_city not in CITIES:
        raise HTTPException(status_code=400, detail="Invalid city selected")

    p_lat, p_lon = CITIES[req.pickup_city]
    d_lat, d_lon = CITIES[req.dropoff_city]

    # Create a temporary DataFrame row for the engine
    single_req_df = pd.DataFrame([{
        "request_id": "TEST-DEMO",
        "pickup_city": req.pickup_city,
        "pickup_lat": p_lat,
        "pickup_lon": p_lon,
        "dropoff_city": req.dropoff_city,
        "dropoff_lat": d_lat,
        "dropoff_lon": d_lon,
        "requested_date": pd.to_datetime(req.requested_date),
        "required_capacity": req.required_capacity,
        "crop_type": req.crop_type
    }])

    try:
        matches_df = run_matching_engine(drivers_df, single_req_df, top_k=3)
        payload = jsonable_encoder(matches_df.to_dict(orient="records"))
        return JSONResponse(content=payload)
    except Exception as e:
        logger.exception("Error computing custom matches")
        raise HTTPException(status_code=500, detail=str(e))

# The app is run with: uvicorn app:app --reload
