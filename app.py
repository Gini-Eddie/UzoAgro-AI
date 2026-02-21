import logging
import psycopg2
from psycopg2.errors import UniqueViolation
import pandas as pd

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

# Import our new database manager and the matching engine
from database import get_db_connection, init_db
from matching_engine import run_matching_engine
from diagnostics_engine import run_botanical_diagnosis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uzoagro-api")

app = FastAPI(title="UzoAgro AI Matching API", version="0.1.0")

# CORS config explicitly allowing your Vercel frontend (and everything else) to talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Expanded 15-City Geographic Dictionary
CITIES = {
    # West / Mid-West
    "Lagos": (6.5244, 3.3792), "Ibadan": (7.3775, 3.9470), "Ilorin": (8.4799, 4.5418), "Akure": (7.2571, 5.2058),
    # North / Middle-Belt
    "Abuja": (9.0765, 7.3986), "Kano": (12.0022, 8.5920), "Kaduna": (10.5105, 7.4165), "Jos": (9.8965, 8.8583),
    "Sokoto": (13.0059, 5.2476),
    # East
    "Enugu": (6.4402, 7.4943), "Awka": (6.2100, 7.0700), "Onitsha": (6.1500, 6.7800), "Owerri": (5.4833, 7.0333),
    # South-South
    "Port Harcourt": (4.8156, 7.0498), "Uyo": (5.0380, 7.9098)
}


# --- Pydantic Data Models ---
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


class UserSignup(BaseModel):
    role: str
    name: str
    phone: str
    nin: str
    primary_city: str


class UserLogin(BaseModel):
    phone: str
    password: str


# --- API Endpoints ---

@app.on_event("startup")
def startup_load_data():
    """Ensure database is ready when the server starts."""
    try:
        init_db()
    except Exception as e:
        logger.exception("Failed to initialize database on startup")
        raise


@app.get("/")
def read_root():
    """Health check endpoint to prove the API is alive to Hugging Face."""
    return {"message": "UzoAgro API is running smoothly!"}


@app.post("/api/signup", response_class=JSONResponse)
def register_user(user: UserSignup):
    """Saves a new user to the database with phone number as default password."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # PostgreSQL syntax strictly uses %s
        cursor.execute('''
                       INSERT INTO users (phone, role, name, nin, password, primary_city)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ''', (user.phone, user.role, user.name, user.nin, user.phone, user.primary_city))
        conn.commit()
        return JSONResponse(content={"status": "success", "message": f"Welcome, {user.name}!"})
    except UniqueViolation:
        # This triggers if the phone number (Primary Key) already exists
        raise HTTPException(status_code=400, detail="Phone number already registered.")
    finally:
        conn.close()


@app.post("/api/login", response_class=JSONResponse)
def login_user(creds: UserLogin):
    """Verifies user credentials."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # PostgreSQL syntax uses %s
    cursor.execute("SELECT name, role FROM users WHERE phone = %s AND password = %s", (creds.phone, creds.password))
    user = cursor.fetchone()
    conn.close()

    if user:
        # Handles standard psycopg2 tuple responses securely
        name = user["name"] if isinstance(user, dict) else user[0]
        role = user["role"] if isinstance(user, dict) else user[1]
        return JSONResponse(content={"status": "success", "name": name, "role": role})
    else:
        raise HTTPException(status_code=401, detail="Invalid phone number or password.")


@app.post("/api/diagnose/image", response_class=JSONResponse)
async def analyze_crop_image(file: UploadFile = File(...)):
    """Receives a crop image from the frontend and passes it to the AI."""
    try:
        # Read the image file into memory
        image_bytes = await file.read()

        # Pass the raw image to your friend's wrapper
        results = run_botanical_diagnosis(image_bytes)

        return JSONResponse(content=results)
    except Exception as e:
        logger.exception("Failed to process uploaded image.")
        raise HTTPException(status_code=500, detail="Image processing failed.")


@app.post("/match/custom", response_class=JSONResponse)
def match_custom_request(req: FarmerRequest):
    """Saves the request to the DB and returns the top 3 AI matches."""
    if req.pickup_city not in CITIES or req.dropoff_city not in CITIES:
        raise HTTPException(status_code=400, detail="Invalid city selected")

    p_lat, p_lon = CITIES[req.pickup_city]
    d_lat, d_lon = CITIES[req.dropoff_city]

    # Generate a new unique request ID
    req_id = f"REQ-LIVE-{pd.Timestamp.now().strftime('%H%M%S')}"

    # 1. Save the new request permanently to the PostgreSQL database
    conn = get_db_connection()
    cursor = conn.cursor()

    # Crucial Fix: Changed SQLite's '?' to PostgreSQL's '%s'
    cursor.execute('''
                   INSERT INTO requests (request_id, sender_name, phone, pickup_city, pickup_lat, pickup_lon,
                                         dropoff_city, dropoff_lat, dropoff_lon, requested_date, required_capacity,
                                         crop_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ''', (
                       req_id, req.farmer_name, "PENDING", req.pickup_city, p_lat, p_lon,
                       req.dropoff_city, d_lat, d_lon, req.requested_date, req.required_capacity, req.crop_type
                   ))
    conn.commit()

    # 2. Safely pull drivers OR inject live demo data if DB is empty
    cursor.execute("SELECT * FROM drivers")
    rows = cursor.fetchall()

    if not rows:
        # EMERGENCY DEMO DATA: Guarantees your pitch works perfectly on stage!
        drivers_df = pd.DataFrame([
            {"driver_id": "DRV-001", "driver_name": "Oluwaseun Logistics", "phone": "080111", "current_city": "Lagos",
             "current_lat": 6.5244, "current_lon": 3.3792, "home_base": "Kano", "capacity": 30.0,
             "allowed_crops": "Maize, Yam", "available_date": pd.Timestamp.now()},
            {"driver_id": "DRV-002", "driver_name": "Northern Freight", "phone": "080222", "current_city": "Ibadan",
             "current_lat": 7.3775, "current_lon": 3.9470, "home_base": "Kano", "capacity": 15.0,
             "allowed_crops": "Maize, Tomatoes", "available_date": pd.Timestamp.now()},
            {"driver_id": "DRV-003", "driver_name": "Emeka Transports", "phone": "080333", "current_city": "Abuja",
             "current_lat": 9.0765, "current_lon": 7.3986, "home_base": "Lagos", "capacity": 20.0,
             "allowed_crops": "Yam, Cassava", "available_date": pd.Timestamp.now()}
        ])
    else:
        # The bulletproof way to read PostgreSQL into Pandas without SQLAlchemy
        colnames = [desc[0] for desc in cursor.description]
        drivers_df = pd.DataFrame(rows, columns=colnames)
        drivers_df["available_date"] = pd.to_datetime(drivers_df["available_date"])

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