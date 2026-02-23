from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def ping():
    return "airflow_ok"

with DAG(
    dag_id="mp_healthcheck",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["marketpulse", "health"],
) as dag:
    t1 = PythonOperator(task_id="ping", python_callable=ping)
