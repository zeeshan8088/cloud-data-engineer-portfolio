from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="day4_data_pipeline",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    run_pipeline = BashOperator(
        task_id="run_data_pipeline",
        bash_command="python /opt/airflow/projects/day4_pipeline/data_pipeline.py"
    )