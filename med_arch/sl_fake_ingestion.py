import os
import pandas as pd # type: ignore
from sqlalchemy import create_engine, text, inspect # type: ignore
from datetime import datetime
from dotenv import load_dotenv # type: ignore

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

bronze_ts = set()
try:
    if inspector.has_table('fake_data', schema='br_fake'):
        with engine.connect() as conn:
            bronze_ts_df = pd.read_sql("SELECT DISTINCT bronze_ingestion_timestamp FROM br_fake.fake_data", conn)
            bronze_ts = set(bronze_ts_df["bronze_ingestion_timestamp"])
except Exception as e:
    print(f"Error reading Bronze timestamps: {e}")
    exit(1)

if not bronze_ts:
    print("No Bronze batches found to process.")
    exit(0)

silver_ts = set()
silver_table_exists = inspector.has_table('fake_data_cleaned', schema='sl_fake')

if silver_table_exists:
    try:
        with engine.connect() as conn:
            silver_ts_df = pd.read_sql("SELECT DISTINCT bronze_ingestion_timestamp FROM sl_fake.fake_data_cleaned", conn)
            silver_ts = set(silver_ts_df["bronze_ingestion_timestamp"])
    except Exception as e:
        print(f"Error reading Silver timestamps: {e}")
        exit(1)

bronze_only = bronze_ts - silver_ts

if not bronze_only:
    print("Silver table is up to date. No new Bronze batches to process.")
    exit(0)

first_batch = True if not silver_table_exists else False

for ts in sorted(bronze_only):
    print(f"Processing Bronze ingestion_timestamp: {ts}")
    try:
        query = text("SELECT * FROM br_fake.fake_data WHERE bronze_ingestion_timestamp = :ts")
        bronze_batch = pd.read_sql(query, engine, params={"ts": ts})

        if bronze_batch.empty:
            print(f"Skipped batch {ts}: No records")
            continue

        business_keys = [
            "booking_date",
            "travel_date",
            "origin_destination"
        ]
        before = len(bronze_batch)
        clean_batch = bronze_batch.drop_duplicates(subset=business_keys, keep='last').copy()
        after = len(clean_batch)

        silver_ingestion_timestamp = datetime.now()
        clean_batch["silver_ingestion_timestamp"] = silver_ingestion_timestamp

        print(f"Deduplicated {before - after} rows. Keeping {after} rows.")

        if_exists_mode = 'replace' if first_batch else 'append'
        clean_batch.to_sql(
            name="fake_data_cleaned",
            con=engine,
            schema="sl_fake",
            if_exists=if_exists_mode,
            index=False,
            method='multi'
        )
        first_batch = False

        print(f"Loaded batch {ts} into sl_fake.fake_data_cleaned.")
    except Exception as e:
        print(f"Error processing batch {ts}: {e}")

print("Silver ingestion complete.")
