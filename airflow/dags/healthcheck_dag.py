"""
Healthcheck DAG for verifying Airflow scheduler and executor integrity.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "cloudscale",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def log_health_status():
    print("CloudScale Airflow scheduler and runner are healthy.")
    return True


with DAG(
    dag_id="healthcheck_dag",
    default_args=default_args,
    description="Verification DAG for CloudScale local development stack",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["infra", "healthcheck"],
) as dag:
    t1 = BashOperator(
        task_id="echo_start",
        bash_command='echo "Airflow pipeline healthcheck initiated at $(date)"',
    )

    t2 = PythonOperator(
        task_id="verify_runner",
        python_callable=log_health_status,
    )

    t1 >> t2
