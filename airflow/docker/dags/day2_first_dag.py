from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_airflow():
    print("🔥 Day 2: Airflow is running successfully!")


with DAG(
    dag_id="day2_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_airflow,
    )