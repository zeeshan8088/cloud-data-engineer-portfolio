from google.cloud import bigquery

client = bigquery.Client()

query = """
CREATE OR REPLACE TABLE `intricate-ward-459513-e1.data_engineering_pipeline.bitcoin_price_analytics` AS
SELECT
AVG(bitcoin_price) AS avg_price
FROM `intricate-ward-459513-e1.data_engineering_pipeline.bitcoin_api_data`
"""

query_job = client.query(query)

query_job.result()

print("Transformation completed")