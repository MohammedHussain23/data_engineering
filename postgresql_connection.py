import psycopg2 # type: ignore
import os 
from dotenv import load_dotenv # type: ignore

# Load environment variables from .env file
load_dotenv()

# Replace these values with your actual db details
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

cur = conn.cursor()

# Example: Fetch the first five customers
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
""")
rows = cur.fetchall()

for row in rows:
    print(row[0])

cur.close()
conn.close()
