"""
Simulates a hospital system publishing admission events to Pub/Sub.
Publishes one event per second — realistic hospital admission rate.

Run this in one terminal while the pipeline runs in another.
"""

import json
import random
import time
from datetime import datetime

# We use the google-cloud-pubsub library
from google.cloud import pubsub_v1

PROJECT_ID = "intricate-ward-459513-e1"
TOPIC_ID   = "hospital-admissions"

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

HOSPITALS = [
    {"id": "H001", "name": "General Hospital NY",    "state": "NY", "type": "Acute Care"},
    {"id": "H002", "name": "Memorial Hospital CA",   "state": "CA", "type": "Acute Care"},
    {"id": "H003", "name": "Community Hospital TX",  "state": "TX", "type": "Critical Access"},
    {"id": "H004", "name": "Regional Hospital FL",   "state": "FL", "type": "Acute Care"},
    {"id": "H005", "name": "University Hospital IL", "state": "IL", "type": "Acute Care"},
]

SEVERITY_LEVELS  = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
SEVERITY_WEIGHTS = [0.40,  0.35,     0.20,   0.05]

CONDITIONS = [
    "Chest Pain", "Fracture", "Respiratory Distress",
    "Stroke Symptoms", "Abdominal Pain", "Trauma",
    "Cardiac Arrest", "Sepsis", "Appendicitis"
]

print(f"Publishing admission events to {topic_path}")
print("Press Ctrl+C to stop\n")

count = 0
while True:
    hospital   = random.choice(HOSPITALS)
    severity   = random.choices(SEVERITY_LEVELS, SEVERITY_WEIGHTS)[0]
    condition  = random.choice(CONDITIONS)
    patient_id = f"P{random.randint(10000, 99999)}"

    event = {
        "event_type":    "admission",
        "patient_id":    patient_id,
        "hospital_id":   hospital["id"],
        "hospital_name": hospital["name"],
        "state":         hospital["state"],
        "hospital_type": hospital["type"],
        "condition":     condition,
        "severity":      severity,
        "is_emergency":  severity in ("HIGH", "CRITICAL"),
        "timestamp":     datetime.utcnow().isoformat(),
        "event_id":      f"EVT{random.randint(100000, 999999)}",
    }

    message_bytes = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, message_bytes)
    future.result()

    count += 1
    print(f"[{count:4d}] {hospital['name'][:25]:<25} | {severity:<8} | {condition}")

    # Publish every 1 second
    # Occasionally burst (simulates shift change at hospitals)
    if count % 20 == 0:
        print(f"      *** Shift change burst — publishing 5 rapid events ***")
        time.sleep(0.1)
    else:
        time.sleep(1)