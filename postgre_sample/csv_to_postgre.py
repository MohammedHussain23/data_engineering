import pandas as pd # type: ignore
import os
from sqlalchemy import create_engine # type: ignore
from dotenv import load_dotenv # type: ignore

# Load environment variables from .env file
load_dotenv()

engine = create_engine(
    f'postgresql+psycopg2://'
    f'{os.getenv('DB_USER')}:'
    f'{os.getenv('DB_PASSWORD')}@'
    f'{os.getenv('DB_HOST')}:'
    f'{os.getenv('DB_PORT')}/'
    f'{os.getenv('DB_NAME')}'
)

def get_csv_path(csv_file):
    """Get the absolute path of the CSV file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, csv_file)

csv_path = get_csv_path('sample_data.csv')
df = pd.read_csv(csv_path)
# print(df.head())

# Step 2: Write full raw data (with duplicates) to table
df.to_sql(
    'sample_people_raw',        # Saves the raw data with duplicates
    engine,
    schema='test_schema',       # Specify the schema
    if_exists='replace', # Replaces the table if it already exists
    index=False
)

print("Raw data (with duplicates) loaded into table: sample_people_raw")

# Step 3: Remove duplicates
df_cleaned = df.drop_duplicates()

# Step 4: Overwrite table with cleaned data
df_cleaned.to_sql(
    'sample_people_cleaned',    # Creates a new cleaned table
    engine,
    schema='test_schema',       # Specify the schema
    if_exists='replace', # Overwrites table with de-duplicated data
    index=False
)
print("Cleaned data (duplicates removed) written to table: sample_people_cleaned")
