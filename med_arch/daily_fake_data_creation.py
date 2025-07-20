import pandas as pd # type: ignore
import random
from faker import Faker # type: ignore
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)
Faker.seed(42)

def generate_rows(n, base_date):
    rows = []
    for _ in range(n):
        booking_offset = random.randint(0, 10)
        travel_offset = booking_offset + random.randint(1, 5)
        booking_date = (base_date - timedelta(days=booking_offset)).date()
        travel_date = (base_date + timedelta(days=travel_offset)).date()
        origin = random.choice(['DEL', 'BOM', 'MAA', 'HYD', 'BLR', 'COK', 'CCU', 'JAI', 'PNQ', 'LKO'])
        dest = random.choice([x for x in ['DEL', 'BOM', 'MAA', 'HYD', 'BLR', 'COK', 'CCU', 'JAI', 'PNQ', 'LKO'] if x != origin])
        origin_dest = f"{origin}-{dest}"
        passengers = random.randint(100, 500)
        amount = round(random.uniform(100000, 500000), 2)
        rows.append([
            booking_date,
            travel_date,
            origin_dest,
            passengers,
            amount
        ])
    return rows

# Use the current date when the script is run
current_date = datetime.now()

# Add some duplicate rows randomly
data1 = generate_rows(190, current_date)
data2 = random.choices(data1, k=50)  # 50 duplicates

df = pd.DataFrame(
    data1 + data2,
    columns=[
        "booking_date",
        "travel_date",
        "origin_destination",
        "passenger_count",
        "sale_amount"
    ]
)

output_dir = os.path.join(os.path.dirname(__file__), "daily_ingest")
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, f'booking_data_{current_date.date()}.csv')
df.to_csv(output_path, index=False)


print(f"Generated dynamic sample CSV at: {output_path}")
