from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="day7_analytics_pipeline",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    extract_api = BashOperator(
        task_id="extract_api_data",
        bash_command="python /opt/airflow/projects/day6_api_pipeline/extract_api_data.py"
    )

    load_bigquery = BashOperator(
        task_id="load_api_to_bigquery",
        bash_command="python /opt/airflow/projects/day6_api_pipeline/load_api_data.py"
    )

    transform_data = BashOperator(
        task_id="run_sql_transformation",
        bash_command="python /opt/airflow/projects/day7_analytics_pipeline/run_transform.py"
    )

    extract_api >> load_bigquery >> transform_data