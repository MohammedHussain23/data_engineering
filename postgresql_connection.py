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
cur.execute("SELECT customer_id, first_name, last_name FROM customer LIMIT 5;")
rows = cur.fetchall()

for row in rows:
    print(row)

cur.close()
conn.close()
