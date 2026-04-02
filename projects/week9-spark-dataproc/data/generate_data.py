# generate_data.py
# Generates a larger e-commerce orders dataset for realistic ETL testing

import random
import csv
from datetime import date, timedelta

random.seed(42)

cities = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home"]
statuses = ["completed", "completed", "completed", "cancelled", "returned"]
customers = [f"C{str(i).zfill(3)}" for i in range(1, 51)]

rows = []
start_date = date(2024, 1, 1)

for order_id in range(1001, 1501):
    rows.append({
        "order_id": order_id,
        "customer_id": random.choice(customers),
        "city": random.choice(cities),
        "product_category": random.choice(categories),
        "quantity": random.randint(1, 20),
        "unit_price": round(random.uniform(100, 50000), 2),
        "order_date": str(start_date + timedelta(days=random.randint(0, 180))),
        "status": random.choice(statuses),
    })

with open("orders_large.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} orders to orders_large.csv")