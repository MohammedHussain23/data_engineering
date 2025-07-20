import pandas as pd  # type: ignore
import random
from faker import Faker  # type: ignore
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)
Faker.seed(42)


def generate_rows(n, base_date):
    rows = []
    for _ in range(n):
        booking_offset = random.randint(0, 364)
        travel_offset = random.randint(0, 364)
        booking_date = (base_date - timedelta(days=booking_offset)).date()
        travel_date = (base_date + timedelta(days=travel_offset) - timedelta(days=booking_offset)).date()
        locations = ['DEL', 'BOM', 'MAA', 'HYD', 'BLR', 'COK', 'CCU', 'JAI', 'PNQ', 'LKO','JFK','AMD','SFO','LAX', 'ORD', 'ATL', 'DFW', 'SEA', 'MIA', 'BOS', 'PHX', 'IAH', 'CLT', 'DEN', 'LAS', 'EWR', 'MSP', 'DTW', 'HNL', 'OAK', 'SAN', 'MCO', 'BWI', 'SLC', 'STL', 'TPA', 'HOU', 'PIT', 'RDU', 'AUS', 'CLE', 'CMH', 'IND', 'MIL', 'JAX', 'MEM', 'BNA', 'OKC', 'OMA', 'ALB', 'BUF', 'RNO', 'SJC', 'SLC', 'TUS', 'PVD', 'RIC', 'SAV', 'TUL', 'WAS', 'PHL', 'BWI', 'BOS', 'MKE', 'MSY', 'PIT', 'SDF', 'SNA', 'SMF', 'SYR', 'TYS', 'XNA', 'YUL', 'YYZ', 'YVR', 'LHR', 'CDG', 'FRA', 'AMS', 'MAD', 'BCN', 'ZRH', 'IST', 'DXB', 'HKG', 'SIN', 'NRT', 'PEK', 'PVG', 'ICN', 'BKK', 'KUL']
        origin = random.choice(locations)
        dest = random.choice([x for x in locations if x != origin])
        origin_dest = f"{origin}-{dest}"
        passengers = random.randint(100, 500)
        amount = round(random.uniform(100000, 5000000), 2)
        rows.append([
            booking_date,
            travel_date,
            origin_dest,
            passengers,
            amount
        ])
    return rows


# Use the current datetime for base
current_date = datetime.now()
date_str = current_date.date().isoformat()  # For filename like 2025-07-20

# Generate and sample data
number_of_rows = random.randint(100, 20000)
number_of_duplicates = random.randint(10, 500)
data1 = generate_rows(number_of_rows, current_date)
data2 = random.choices(data1, k=number_of_duplicates)  # 50 duplicates

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

# Set up output directory
output_dir = os.path.join(os.path.dirname(__file__), "daily_ingest")
os.makedirs(output_dir, exist_ok=True)

# Generate next available filename using counter
base_filename = f"booking_data_{date_str}"
counter = 1
while True:
    candidate_filename = f"{base_filename}_{counter}.csv"
    output_path = os.path.join(output_dir, candidate_filename)
    if not os.path.exists(output_path):
        break
    counter += 1

# Save CSV
df.to_csv(output_path, index=False)
print(f"Generated CSV file: {output_path}")
