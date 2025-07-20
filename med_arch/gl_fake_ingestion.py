import os
import pandas as pd # type: ignore
from sqlalchemy import create_engine, text, inspect # type: ignore
from datetime import datetime
from dotenv import load_dotenv # type: ignore

# Load environment variables
load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

# Step 1: Identify new Silver batches for Gold aggregation
silver_ts = set()
try:
    if inspector.has_table('fake_data_cleaned', schema='sl_fake'):
        with engine.connect() as conn:
            silver_ts_df = pd.read_sql(
                "SELECT DISTINCT silver_ingestion_timestamp FROM sl_fake.fake_data_cleaned", conn
            )
            silver_ts = set(silver_ts_df["silver_ingestion_timestamp"])
except Exception as e:
    print(f"Error fetching Silver timestamps: {e}")
    exit(1)

if not silver_ts:
    print("No Silver data available for Gold aggregation.")
    exit(0)

gold_ts = set()
gold_table_exists = inspector.has_table('booking_summary', schema='gl_fake')
if gold_table_exists:
    try:
        with engine.connect() as conn:
            gold_ts_df = pd.read_sql(
                "SELECT DISTINCT silver_ingestion_timestamp FROM gl_fake.booking_summary", conn
            )
            gold_ts = set(gold_ts_df["silver_ingestion_timestamp"])
    except Exception as e:
        print(f"Error fetching Gold table silver timestamps: {e}")
        exit(1)

silver_only = silver_ts - gold_ts
if not silver_only:
    print("Gold table is up to date. No new Silver batches to process.")
    exit(0)

first_batch = False if gold_table_exists else True

for ts in sorted(silver_only):
    print(f"Processing Silver batch: {ts}")
    try:
        # Load batch from Silver
        query = text("SELECT * FROM sl_fake.fake_data_cleaned WHERE silver_ingestion_timestamp = :ts")
        silver_batch = pd.read_sql(query, engine, params={"ts": ts})

        if silver_batch.empty:
            print(f"Skipped batch {ts}: No records.")
            continue

        # Gold aggregation (example: sum by route and travel date)
        agg = (
            silver_batch.groupby(['origin_destination', 'travel_date'])
            .agg(
                total_passengers=pd.NamedAgg(column="passenger_count", aggfunc="sum"),
                total_sales=pd.NamedAgg(column="sale_amount", aggfunc="sum"),
                unique_bookings=pd.NamedAgg(column="batch_id", aggfunc="count")
            )
            .reset_index()
        )

        gold_ingestion_timestamp = datetime.now()
        agg["silver_ingestion_timestamp"] = ts
        agg["gold_ingestion_timestamp"] = gold_ingestion_timestamp

        print(f"Aggregated to {len(agg)} rows for batch {ts}.")

        if_exists_mode = 'replace' if first_batch else 'append'
        agg.to_sql(
            name="booking_summary",
            con=engine,
            schema="gl_fake",
            if_exists=if_exists_mode,
            index=False,
            method='multi'
        )
        first_batch = False

        print(f"Loaded aggregation for {ts} into gl_fake.booking_summary.")
    except Exception as e:
        print(f"Error in Gold aggregation for batch {ts}: {e}")

print("Gold aggregation complete.")
