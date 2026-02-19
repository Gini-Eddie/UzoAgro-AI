import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Securely load your Neon Cloud Database URL
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")


def migrate_csvs():
    print("Connecting to Neon Cloud...")
    # SQLAlchemy requires the URL to start with postgresql://
    engine = create_engine(DB_URL)

    # 1. Upload the Mock Drivers
    if os.path.exists("data/drivers.csv"):
        print("Uploading drivers.csv...")
        df_drivers = pd.read_csv("data/drivers.csv")
        # Append data without overwriting the table structure
        df_drivers.to_sql("drivers", engine, if_exists="append", index=False)
        print("Drivers uploaded successfully!")

    # 2. Upload the Mock Requests
    if os.path.exists("data/requests.csv"):
        print("Uploading requests.csv...")
        df_req = pd.read_csv("data/requests.csv")
        df_req.to_sql("requests", engine, if_exists="append", index=False)
        print("Requests uploaded successfully!")


if __name__ == "__main__":
    migrate_csvs()