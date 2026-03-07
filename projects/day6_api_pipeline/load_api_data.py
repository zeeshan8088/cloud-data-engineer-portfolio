from google.cloud import bigquery
import json
import logging
logging.basicConfig(level=logging.INFO)

client = bigquery.Client()

table_id = "intricate-ward-459513-e1.data_engineering_pipeline.bitcoin_api_data"

# Read from Data Lake
with open("/opt/airflow/data_lake/api_data/api_data.json") as f:
    data = json.load(f)

rows = [{
    "bitcoin_price": data["price"]
}]

# temporary newline JSON file
json_path = "/tmp/bitcoin_load.json"

with open(json_path, "w") as f:
    for row in rows:
        f.write(json.dumps(row) + "\n")

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)

with open(json_path, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_id,
        job_config=job_config
    )

job.result()

logging.info("Data loaded into BigQuery successfully")