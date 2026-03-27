from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="ffengine_dummy_heartbeat",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ffengine", "test"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
