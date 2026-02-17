"""
UzoAgro AI - Synthetic Logistics Dataset Generator

Generates realistic, domain-specific logistics data.
Produces `data/drivers.csv` and `data/requests.csv`.

Key updates:
- Implements Cargo Affinity: Drivers now have 'allowed_crops' (pipe-separated).
- Requests maintain a single 'crop_type'.
- Organizes crops into distinct agricultural logistics categories.
"""

import os
import random
from datetime import datetime, timedelta
import csv

try:
    import pandas as pd
except ImportError:
    pd = None

# --- Geographic Coordinate Dictionary ---
cities = {
    "Lagos": (6.5244, 3.3792),
    "Enugu": (6.4402, 7.4943),
    "Awka": (6.2100, 7.0700),
    "Onitsha": (6.1500, 6.7800),
    "Abuja": (9.0765, 7.3986),
    "Port Harcourt": (4.8156, 7.0498),
    "Ibadan": (7.3775, 3.9470),
    "Kano": (12.0022, 8.5920),
    "Kaduna": (10.5105, 7.4165),
    "Ilorin": (8.4799, 4.5418)
}

# --- Realism Pools & Cargo Categories ---
FIRST_NAMES = ["Chukwuemeka", "Aisha", "Emeka", "Ngozi", "Ahmed", "Ifeanyi", "Fatima", "Samuel", "Tunde", "Amaka", "Ibrahim", "Uche", "Bala"]
LAST_NAMES = ["Okonkwo", "Ibrahim", "Eze", "Abiola", "Ogunleye", "Chukwu", "Nwosu", "Balogun", "Adeniyi", "Uba", "Okafor", "Bello", "Adeyemi"]

# The Cargo Matrix
GRAINS = ["Rice", "Maize", "Beans", "Millet", "Sorghum"]
TUBERS = ["Yam", "Cassava", "Potatoes"]
PERISHABLES = ["Tomatoes", "Onions", "Peppers"]
ALL_CROPS = GRAINS + TUBERS + PERISHABLES

PHONE_PREFIXES = ["080", "081", "090", "070"]
STANDARD_TONNAGES = [5, 10, 15, 20, 30, 45]

random.seed(42)

# ---------------- Utility functions -----------------
def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def make_unique_phone(existing_set):
    for _ in range(1000):
        phone = f"{random.choice(PHONE_PREFIXES)}{random.randint(10000000, 99999999):08d}"
        if phone not in existing_set:
            existing_set.add(phone)
            return phone
    return f"080{random.randint(10000000, 99999999)}"

def random_date_within_days(days):
    now = datetime.now()
    delta_days = random.randint(0, days)
    dt = now + timedelta(days=delta_days)
    return dt.strftime("%Y-%m-%d")

def generate_driver_crops():
    """Simulates a driver selecting 1 to 3 crops they are willing to carry."""
    # Usually, drivers stick to one category, but sometimes mix safe ones
    base_category = random.choice([GRAINS, TUBERS, PERISHABLES])
    num_choices = random.randint(1, 3)

    # Select from base category
    selected = random.sample(base_category, min(num_choices, len(base_category)))
    return "|".join(selected)


# ---------------- Generators -----------------
def generate_drivers(n=50):
    drivers = []
    cities_list = list(cities.keys())
    phones = set()

    for i in range(1, n + 1):
        driver_id = f"DRV{i:04d}"
        current_city = random.choice(cities_list)
        current_lat, current_lon = cities[current_city]

        home_base_city = random.choice([c for c in cities_list if c != current_city])
        home_base_lat, home_base_lon = cities[home_base_city]

        drivers.append({
            "driver_id": driver_id,
            "name": random_name(),
            "phone": make_unique_phone(phones),
            "current_city": current_city,
            "current_lat": current_lat,
            "current_lon": current_lon,
            "home_base_city": home_base_city,
            "home_base_lat": home_base_lat,
            "home_base_lon": home_base_lon,
            "available_date": random_date_within_days(4),
            "available_capacity": random.choice(STANDARD_TONNAGES),
            "allowed_crops": generate_driver_crops()
        })
    return drivers

def generate_requests(n=100):
    requests = []
    cities_list = list(cities.keys())
    phones = set()

    for i in range(1, n + 1):
        request_id = f"REQ{i:04d}"
        pickup_city = random.choice(cities_list)
        pickup_lat, pickup_lon = cities[pickup_city]

        dropoff_city = random.choice([c for c in cities_list if c != pickup_city])
        dropoff_lat, dropoff_lon = cities[dropoff_city]

        requests.append({
            "request_id": request_id,
            "sender_name": random_name(),
            "phone": make_unique_phone(phones),
            "pickup_city": pickup_city,
            "pickup_lat": pickup_lat,
            "pickup_lon": pickup_lon,
            "dropoff_city": dropoff_city,
            "dropoff_lat": dropoff_lat,
            "dropoff_lon": dropoff_lon,
            "requested_date": random_date_within_days(4),
            "required_capacity": random.choice(STANDARD_TONNAGES),
            "crop_type": random.choice(ALL_CROPS)
        })
    return requests

# ---------------- I/O -----------------
def save_to_csv(rows, path, columns):
    if pd is not None:
        df = pd.DataFrame(rows)[columns]
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        with open(path, "w", newline='', encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k, "") for k in columns})

# ---------------- Main -----------------
def main():
    out_dir = "data"
    os.makedirs(out_dir, exist_ok=True)

    drivers = generate_drivers(50)
    requests = generate_requests(100)

    drivers_columns = [
        "driver_id", "name", "phone", "current_city", "current_lat", "current_lon",
        "home_base_city", "home_base_lat", "home_base_lon", "available_date",
        "available_capacity", "allowed_crops"
    ]

    requests_columns = [
        "request_id", "sender_name", "phone", "pickup_city", "pickup_lat", "pickup_lon",
        "dropoff_city", "dropoff_lat", "dropoff_lon", "requested_date",
        "required_capacity", "crop_type"
    ]

    save_to_csv(drivers, os.path.join(out_dir, "drivers.csv"), drivers_columns)
    save_to_csv(requests, os.path.join(out_dir, "requests.csv"), requests_columns)

    print("Successfully generated UzoAgro Logistics Datasets with Cargo Affinity.")

if __name__ == "__main__":
    main()