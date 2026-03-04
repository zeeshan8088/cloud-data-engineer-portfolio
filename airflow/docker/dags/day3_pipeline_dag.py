from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def start():
    print("Pipeline started")


def ingest():
    print("Ingesting raw data")


def process():
    print("Processing data")


def finish():
    print("Pipeline finished successfully")


with DAG(
    dag_id="day3_pipeline_dag",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start,
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest,
    )

    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process,
    )

    finish_task = PythonOperator(
        task_id="finish_pipeline",
        python_callable=finish,
    )

    start_task >> ingest_task >> process_task >> finish_task