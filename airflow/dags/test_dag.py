"""
DAG de ejemplo para verificar la arquitectura distribuida.
Ejecuta tasks simples y registra en qué worker corrió cada una.
"""
from datetime import datetime, timedelta
import socket

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def report_worker(**context):
    hostname = socket.gethostname()
    task_id = context["task_instance"].task_id
    print(f"Task '{task_id}' ejecutada en worker: {hostname}")
    return hostname

with DAG(
    dag_id="example_distributed_dag",
    description="Verifica que los workers reciben y ejecutan tasks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "distributed"],
) as dag:

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=report_worker,
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=report_worker,
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=report_worker,
    )

    task_a >> [task_b, task_c]