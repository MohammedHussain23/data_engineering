import os
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore
from sqlalchemy import create_engine # type: ignore

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
query = """
SELECT f.title, COUNT(*) AS rental_count
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.title
ORDER BY rental_count DESC
LIMIT 10;
"""
df = pd.read_sql(query, engine)
print(df)
