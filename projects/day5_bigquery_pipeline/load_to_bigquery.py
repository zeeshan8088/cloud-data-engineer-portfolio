from google.cloud import bigquery
import json

client = bigquery.Client()

table_id = "intricate-ward-459513-e1.data_engineering_pipeline.sample_data"

# Read generated data
with open("/opt/airflow/projects/day4_pipeline/output.json") as f:
    data = json.load(f)

rows = [{"value": int(x)} for x in data]

json_path = "/tmp/data.json"

# Write newline JSON
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

print("Loaded data into BigQuery successfully")