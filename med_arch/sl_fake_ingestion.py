import os
import pandas as pd # type: ignore
from sqlalchemy import create_engine, text # type: ignore
from datetime import datetime
from dotenv import load_dotenv # type: ignore

# Load environment variables
load_dotenv()

# Create DB connection
DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)

# Get ingestion timestamps from Bronze and Silver
try:
    with engine.connect() as conn:
        bronze_ts_df = pd.read_sql("SELECT DISTINCT bronze_ingestion_timestamp FROM br_fake.fake_data", conn)
        silver_ts_df = pd.read_sql("SELECT DISTINCT silver_ingestion_timestamp FROM sl_fake.fake_data_cleaned", conn)

    bronze_ts = set(bronze_ts_df["bronze_ingestion_timestamp"])
    silver_ts = set(silver_ts_df["silver_ingestion_timestamp"])
    bronze_only = bronze_ts - silver_ts

except Exception as e:
    print(f"Failed to fetch bronze ingestion timestamps: {e}")
    exit(1)

if not bronze_only:
    print("Silver layer is up to date. No new Bronze data to process.")
    exit(0)

print(f"New Bronze batches to process: {len(bronze_only)}")

# Loop through new ingestion_timestamp values
for ts in bronze_only:
    print(f"\nProcessing bronze_ingestion_timestamp: {ts}")

    try:
        query = text("SELECT * FROM br_fake.fake_data WHERE bronze_ingestion_timestamp = :ts")
        bronze_batch = pd.read_sql(query, engine, params={"ts": ts})

        if bronze_batch.empty:
            print(f"No data found for {ts}, skipping.")
            continue

        # Deduplicate based on business columns ONLY
        business_keys = [
            "booking_date",
            "travel_date",
            "origin_destination"
        ]

        before = len(bronze_batch)
        clean_batch = bronze_batch.drop_duplicates(subset=business_keys, keep='last').copy()
        after = len(clean_batch)

        # Add silver ingestion timestamp
        clean_batch["silver_ingestion_timestamp"] = datetime.now()

        print(f"Deduplicated {before - after} rows. Keeping {after} unique rows.")

        # Write to Silver table
        clean_batch.to_sql(
            name="fake_data_cleaned",
            con=engine,
            schema="sl_fake",
            if_exists="append",
            index=False,
            method='multi'
        )

        print(f"Batch with bronze_ingestion_timestamp {ts} written to sl_fake.fake_data_cleaned.")
    except Exception as e:
        print(f"Error processing bronze_ingestion_timestamp {ts}: {e}")

print("\nSilver ingestion complete.")
