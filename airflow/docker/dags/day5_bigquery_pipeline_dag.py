from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="day5_bigquery_pipeline",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/projects/day4_pipeline/data_pipeline.py"
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command="python /opt/airflow/projects/day5_bigquery_pipeline/load_to_bigquery.py"
    )

    generate_data >> load_bigquery