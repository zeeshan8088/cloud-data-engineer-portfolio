"""
Generate realistic hospital data — same schema as real CMS dataset
4,500 records across all US states with realistic distributions
"""
import csv, random, os
random.seed(42)

os.makedirs("data", exist_ok=True)

states = ["NY","CA","TX","FL","IL","PA","OH","GA","NC","MI",
          "WA","AZ","MA","TN","IN","MO","MD","WI","CO","MN",
          "AL","SC","LA","KY","OR","OK","CT","UT","NV","AR"]

hospital_types = [
    "Acute Care Hospitals",
    "Critical Access Hospitals", 
    "Psychiatric",
    "Childrens",
    "Department of Defense"
]

ownership = [
    "Voluntary non-profit - Private",
    "Government - Local",
    "Proprietary",
    "Government - State",
    "Government - Federal"
]

name_parts = ["General","Memorial","Community","Regional","University","Saint","Baptist","Methodist","Presbyterian","Mercy"]
suffixes    = ["Hospital","Medical Center","Health System","Healthcare","Clinic"]

comparisons = [
    "Above the national average",
    "Same as the national average",
    "Below the national average",
    "Not Available"
]

# Weighted so most hospitals are average (realistic)
comparison_weights = [0.15, 0.55, 0.20, 0.10]

rows = []
for i in range(4500):
    # Inject some bad records intentionally (for dead letter testing)
    is_bad = (i % 150 == 0)

    rows.append({
        "Facility ID":                          "" if is_bad else f"{100000+i}",
        "Facility Name":                        f"{random.choice(name_parts)} {random.choice(suffixes)} {i}",
        "Address":                              f"{random.randint(100,9999)} Main St",
        "City":                                 f"City_{random.randint(1,500)}",
        "State":                                "XX" if is_bad else random.choice(states),
        "ZIP Code":                             f"{random.randint(10000,99999)}",
        "County Name":                          f"County_{random.randint(1,200)}",
        "Phone Number":                         f"{random.randint(2000000000,9999999999)}",
        "Hospital Type":                        random.choice(hospital_types),
        "Hospital Ownership":                   random.choice(ownership),
        "Emergency Services":                   random.choice(["Yes","No"]),
        "Hospital overall rating":              random.choice(["1","2","3","4","5","Not Available"]),
        "Readmission national comparison":      random.choices(comparisons, comparison_weights)[0],
        "Patient experience national comparison": random.choices(comparisons, comparison_weights)[0],
        "Mortality national comparison":        random.choices(comparisons, comparison_weights)[0],
    })

output_path = "data/hospitals_raw.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

# Quick stats
bad_count = sum(1 for r in rows if not r["Facility ID"])
high_readmission = sum(1 for r in rows if r["Readmission national comparison"] == "Above the national average")

print(f"✅ Generated {len(rows)} hospital records → {output_path}")
print(f"   Intentional bad records (for dead letter): {bad_count}")
print(f"   High readmission hospitals: {high_readmission}")
print(f"   States covered: {len(set(r['State'] for r in rows))}")