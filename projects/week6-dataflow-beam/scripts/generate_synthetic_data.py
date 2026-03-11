import csv
import random
import os

random.seed(42)
os.makedirs("data", exist_ok=True)

states = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
comps = ["Above the national average", "Same as the national average", "Below the national average", "Not Available"]

rows = []
for i in range(4500):
    rows.append({
        "Facility ID": str(100000 + i),
        "Facility Name": f"Hospital {i}",
        "City": f"City {i % 100}",
        "State": random.choice(states),
        "ZIP Code": str(10000 + i),
        "County Name": f"County {i % 50}",
        "Hospital Type": "Acute Care Hospitals",
        "Hospital Ownership": "Voluntary non-profit - Private",
        "Emergency Services": random.choice(["Yes", "No"]),
        "Hospital overall rating": random.choice(["1", "2", "3", "4", "5"]),
        "Readmission national comparison": random.choice(comps),
        "Patient experience national comparison": random.choice(comps),
        "Mortality national comparison": random.choice(comps),
    })

output_path = "data/hospitals_raw.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Done: {len(rows)} rows written to {output_path}")