import os
import pandas as pd # type: ignore
import uuid
from datetime import datetime
from sqlalchemy import create_engine, text # type: ignore
from dotenv import load_dotenv # type: ignore

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    print(f"Failed to load environment variables: {e}")
    exit(1)

# Setup DB connection
try:
    DATABASE_URL = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(DATABASE_URL)
except Exception as e:
    print(f"Failed to connect to database: {e}")
    exit(1)

# Setup paths
csv_dir = os.path.join(os.path.dirname(__file__), 'daily_ingest')
today_str = datetime.now().strftime('%Y-%m-%d')

try:
    csv_files = [
        f for f in os.listdir(csv_dir)
        if f.startswith(f'booking_data_{today_str}_') and f.endswith('.csv')
    ]
except Exception as e:
    print(f"Error accessing files in {csv_dir}: {e}")
    exit(1)

if not csv_files:
    print(f"No files to process today: {today_str}")
    exit(0)

new_files_ingested = 0  # Track new ingestions

for file in csv_files:
    file_path = os.path.join(csv_dir, file)
    print(f"\nFound file: {file}")

    # 🛑 Check if already ingested before loading
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM br_fake.fake_data WHERE source_file_name = :fname LIMIT 1"),
                {'fname': file}
            ).fetchone()
    except Exception as e:
        print(f"Error querying ingestion history: {e}")
        continue

    if result:
        print(f"Skipping {file} (already ingested)")
        continue

    # ✅ Process new file
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"Skipped {file}: empty CSV.")
            continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

    # Add ingestion metadata
    ingestion_timestamp = datetime.now()
    batch_id = str(uuid.uuid4())
    df["bronze_ingestion_timestamp"] = ingestion_timestamp
    df["batch_id"] = batch_id
    df["source_file_name"] = file
    df["row_number_within_batch"] = range(1, len(df) + 1)

    try:
        df.to_sql(
            name='fake_data',
            con=engine,
            schema='br_fake',
            if_exists='append',
            index=False,
            method='multi'
        )
        new_files_ingested += 1
        print(f"Ingested {len(df)} rows from {file} | Batch ID: {batch_id}")
    except Exception as e:
        print(f"Failed to ingest {file}: {e}")
        continue

# ✅ Final message
if new_files_ingested == 0:
    print("\nAll files are up to date. No new data ingested.")
else:
    print(f"\nIngestion complete! {new_files_ingested} file(s) loaded into br_fake.fake_data")

