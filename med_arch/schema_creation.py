import os
from sqlalchemy import create_engine, text # type: ignore
from dotenv import load_dotenv # type: ignore

# Load environment variables from .env
load_dotenv()

# Setup the database connection
DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")

# Schemas you want to create
schemas = ['br_fake', 'sl_fake', 'gl_fake']

with engine.connect() as conn:
    for schema in schemas:
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            conn.commit()
            print(f"✅ Created schema (or already exists): {schema}")
        except Exception as e:
            print(f"❌ Failed to create schema {schema}: {e}")
