from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "zeeshan",
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "day6_api_pipeline",
    default_args=default_args,
    description="Bitcoin API ingestion pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
)


# Task 1 — Extract API data
extract_api_data = BashOperator(
    task_id="extract_api_data",
    bash_command="python /opt/airflow/projects/day6_api_pipeline/extract_api_data.py",
    sla=timedelta(minutes=5),
    dag=dag
)


# Task 2 — Load API data to BigQuery
load_api_to_bigquery = BashOperator(
    task_id="load_api_to_bigquery",
    bash_command="python /opt/airflow/projects/day6_api_pipeline/load_api_data.py",
    sla=timedelta(minutes=5),
    dag=dag
)


# Task 3 — Run SQL transformation
run_sql_transformation = BashOperator(
    task_id="run_sql_transformation",
    bash_command="python /opt/airflow/projects/day7_analytics_pipeline/run_transform.py",
    sla=timedelta(minutes=5),
    dag=dag
)


# Pipeline order
extract_api_data >> load_api_to_bigquery >> run_sql_transformation