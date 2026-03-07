import requests
import json
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)

# API endpoint
from airflow.models import Variable

url = Variable.get("bitcoin_api_url")
response = requests.get(url)
data = response.json()

# Extract price
price = data["bitcoin"]["usd"]

record = {
    "timestamp": datetime.utcnow().isoformat(),
    "price": price
}

# Data lake path
file_path = "/opt/airflow/data_lake/api_data/api_data.json"

# ensure directory exists
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# write file
with open(file_path, "w") as f:
    json.dump(record, f)

logging.info("API data extracted and stored in Data Lake successfully")