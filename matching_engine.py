"""
UzoAgro AI - Spatio-Temporal Logistics Matching Engine

Calculates intelligent driver-to-cargo matches using a 5-dimensional scoring model:
1. Capacity Fit (Binary Mask): Verifies strict tonnage requirements.
2. Temporal Fit: Scores schedule alignment (0-2 days tolerance).
3. Deadhead Score: Proximity for empty pickup transit.
4. Corridor Score (Vector Deviation): Cross-track distance from backhaul trajectory.
5. Cargo Affinity Matrix: Intelligently matches exact crops, broad categories, and safe pivots.

Outputs `data/matches.csv` containing the top matched drivers based on a weighted composite score.
"""

import os
import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ----------------- Cargo Matrix Definitions -----------------
CROP_CATEGORIES = {
    "Grains": ["Rice", "Maize", "Beans", "Millet", "Sorghum"],
    "Tubers": ["Yam", "Cassava", "Potatoes"],
    "Perishables": ["Tomatoes", "Onions", "Peppers"]
}

def get_category(crop):
    """Return the category name for a given crop."""
    for category, crops in CROP_CATEGORIES.items():
        if crop in crops:
            return category
    return "Unknown"

# ----------------- I/O & Parsing -----------------
def load_data(drivers_path="data/drivers.csv", requests_path="data/requests.csv"):
    drivers = pd.read_csv(drivers_path)
    requests = pd.read_csv(requests_path)

    # Parse Dates
    if "available_date" in drivers.columns:
        drivers["available_date"] = pd.to_datetime(drivers["available_date"], errors="coerce").dt.normalize()
    if "requested_date" in requests.columns:
        requests["requested_date"] = pd.to_datetime(requests["requested_date"], errors="coerce").dt.normalize()

    return drivers, requests

# ----------------- Core Mathematical Operations -----------------
def compute_distance(lat1, lon1, lat2, lon2):
    return np.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)

def compute_cross_track_deviation(A_lat, A_lon, B_lat, B_lon, P_lat, P_lon):
    AB_lat = B_lat - A_lat
    AB_lon = B_lon - A_lon
    AP_lat = P_lat - A_lat
    AP_lon = P_lon - A_lon

    norm_AB = np.sqrt(AB_lat**2 + AB_lon**2)

    with np.errstate(divide='ignore', invalid='ignore'):
        cross_prod = np.abs(AB_lon * AP_lat - AB_lat * AP_lon)
        deviation = np.divide(cross_prod, norm_AB)
        deviation = np.where(norm_AB == 0, compute_distance(A_lat, A_lon, P_lat, P_lon), deviation)

    return deviation

# ----------------- Feature Engineering -----------------
def extract_temporal_score(driver_dates, request_date):
    if pd.isna(request_date):
        return np.ones(len(driver_dates))

    days_diff = np.abs((driver_dates - request_date).dt.days).to_numpy(dtype=float)

    conditions = [days_diff == 0, days_diff == 1, days_diff == 2]
    choices = [1.0, 0.8, 0.4]
    return np.select(conditions, choices, default=0.0)

def extract_affinity_score(driver_crops_series, request_crop):
    """
    Evaluates cargo compatibility.
    - Exact crop match = 1.0
    - Same category match = 0.8
    - Safe pivot (Grains <-> Tubers) = 0.4
    - Unsafe pivot (Perishables to Dry) = 0.0
    """
    req_cat = get_category(request_crop)
    scores = []

    for allowed_str in driver_crops_series:
        if pd.isna(allowed_str):
            scores.append(0.0)
            continue

        driver_crops = allowed_str.split("|")

        # 1. Exact Match
        if request_crop in driver_crops:
            scores.append(1.0)
            continue

        # Determine categories the driver handles
        driver_categories = {get_category(c) for c in driver_crops}

        # 2. Category Match
        if req_cat in driver_categories:
            scores.append(0.8)
            continue

        # 3. Safe Pivots (Grains and Tubers are cross-compatible dry/hardy goods)
        safe_pivots = [{"Grains", "Tubers"}]
        is_pivot = False
        for pair in safe_pivots:
            if req_cat in pair and any(dc in pair for dc in driver_categories):
                scores.append(0.4)
                is_pivot = True
                break

        if is_pivot:
            continue

        # 4. Unsafe Pivot (e.g., Perishables to Grains)
        scores.append(0.0)

    return np.array(scores, dtype=float)

def compute_scores(drivers_df, request_row):
    p_lat, p_lon = float(request_row.get("pickup_lat")), float(request_row.get("pickup_lon"))
    d_lat, d_lon = float(request_row.get("dropoff_lat")), float(request_row.get("dropoff_lon"))
    req_cap = float(request_row.get("required_capacity", 0))
    req_date = request_row.get("requested_date")
    req_crop = request_row.get("crop_type", "")

    curr_lats = drivers_df["current_lat"].to_numpy(dtype=float)
    curr_lons = drivers_df["current_lon"].to_numpy(dtype=float)
    home_lats = drivers_df.get("home_base_lat", drivers_df["current_lat"]).to_numpy(dtype=float)
    home_lons = drivers_df.get("home_base_lon", drivers_df["current_lon"]).to_numpy(dtype=float)

    avail_caps = drivers_df.get("available_capacity", pd.Series(np.zeros(len(drivers_df)))).to_numpy(dtype=float)
    driver_dates = drivers_df.get("available_date")
    driver_crops = drivers_df.get("allowed_crops")

    # 1. Capacity
    capacity_score = (avail_caps >= req_cap).astype(float)

    # 2. Temporal
    time_score = extract_temporal_score(driver_dates, req_date)

    # 3. Cargo Affinity
    affinity_score = extract_affinity_score(driver_crops, req_crop)

    # 4. Deadhead
    deadhead_dists = compute_distance(curr_lats, curr_lons, p_lat, p_lon)

    # 5. Corridor
    pickup_dev = compute_cross_track_deviation(curr_lats, curr_lons, home_lats, home_lons, p_lat, p_lon)
    dropoff_dev = compute_cross_track_deviation(curr_lats, curr_lons, home_lats, home_lons, d_lat, d_lon)
    total_dev = pickup_dev + dropoff_dev

    def inv_normalize(arr):
        minv, maxv = arr.min(), arr.max()
        if maxv - minv <= 1e-12:
            return np.ones_like(arr, dtype=float)
        return 1.0 - ((arr - minv) / (maxv - minv))

    features_df = pd.DataFrame({
        "driver_id": drivers_df["driver_id"].values,
        "capacity_score": capacity_score,
        "time_score": time_score,
        "affinity_score": affinity_score,
        "deadhead_score": inv_normalize(deadhead_dists),
        "corridor_score": inv_normalize(total_dev)
    })

    return features_df

# ----------------- Execution & Ranking -----------------
def run_matching_engine(drivers_df, requests_df, top_k=3):
    matches = []

    # Updated Algorithm Weights (Total = 1.0)
    W_DEADHEAD = 0.25
    W_CORRIDOR = 0.35
    W_AFFINITY = 0.20
    W_TIME = 0.20

    for _, req in requests_df.iterrows():
        features = compute_scores(drivers_df, req)

        # Calculate composite score (Capacity acts as a strict multiplier mask)
        features["final_score"] = (
            (W_DEADHEAD * features["deadhead_score"]) +
            (W_CORRIDOR * features["corridor_score"]) +
            (W_AFFINITY * features["affinity_score"]) +
            (W_TIME * features["time_score"])
        ) * features["capacity_score"]

        top = features.sort_values(by="final_score", ascending=False).head(top_k)

        for _, row in top.iterrows():
            # Only save matches that are viable (score > 0)
            if float(row["final_score"]) > 0:
                matches.append({
                    "request_id": req["request_id"],
                    "driver_id": row["driver_id"],
                    "final_score": float(row["final_score"]),
                    "capacity_score": float(row["capacity_score"]),
                    "time_score": float(row["time_score"]),
                    "affinity_score": float(row["affinity_score"]),
                    "deadhead_score": float(row["deadhead_score"]),
                    "corridor_score": float(row["corridor_score"])
                })

    return pd.DataFrame(matches)

# ----------------- Entry Point -----------------
def main():
    drivers_path = os.path.join("data", "drivers.csv")
    requests_path = os.path.join("data", "requests.csv")

    if not os.path.exists(drivers_path) or not os.path.exists(requests_path):
        raise FileNotFoundError("Missing datasets. Ensure data/drivers.csv and data/requests.csv exist.")

    drivers_df, requests_df = load_data(drivers_path, requests_path)
    matches_df = run_matching_engine(drivers_df, requests_df, top_k=3)

    out_path = os.path.join("data", "matches.csv")
    matches_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Intelligence Engine Complete. Saved {len(matches_df)} matches to {out_path}.")

if __name__ == "__main__":
    main()