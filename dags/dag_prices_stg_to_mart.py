
from datetime import datetime, timedelta

from airflow.sdk import (
    DAG,
    DeadlineAlert,
    DeadlineReference,
    AsyncCallback,
)
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.hooks.base import BaseHook

from utils.default_args import default_args
from utils.callbacks import task_failure_alert, log_deadline_miss
from etl import prices_stg_to_mart as etl_task


def run_etl_task(conn_id: str, view_name: str, concurrently: bool):
    try:
        conn = BaseHook.get_connection(conn_id)
        db_url = conn.get_uri()
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        etl_task.run_mart_etl(db_url, view_name,  False)
    except Exception as e:
        raise AirflowFailException(f"Ошибка получения строки подключения: {e}")

with DAG(
    dag_id="prices_etl_stg_to_mart",
    default_args=default_args,
    description="Обновление представления route_card",
    max_active_runs=1,
    tags=["extract", "prices", "bronze", "postgres"],
    on_failure_callback=task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(log_deadline_miss),
    ),
    params={
        "db_conn_name": "my_postgres_db_conn",
        "view_name": "mart.route_card",
        "concurrently": False
    },
) as dag:
    start_task = EmptyOperator(task_id="start")

    update_task = PythonOperator(
        task_id="update_view",
        python_callable=run_etl_task,
        op_kwargs={
            "conn_id": "{{ params.db_conn_name }}", 
            "view_name": "{{ params.view_name }}", 
            "concurrently": "{{ params.concurrently }}", 
        }
    )

    end_task = EmptyOperator(task_id="end")

start_task >> update_task >> end_task