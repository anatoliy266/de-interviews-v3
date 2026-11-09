
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
from etl import prices_raw_to_stg as etl_task


def run_etl_task(conn_id: str, start_date, end_date, provider: str, csv_path: str | None):
    try:
        conn = BaseHook.get_connection(conn_id)
        db_url = conn.get_uri()
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        etl_task.run_core_etl(db_url, start_date, end_date, provider, csv_path)
    except Exception as e:
        raise AirflowFailException(f"Ошибка получения строки подключения: {e}")

with DAG(
    dag_id="prices_etl_raw_to_stg",
    default_args=default_args,
    description="Загрузка, трансформация и очистка данных транспортных сделок за период",
    max_active_runs=1,
    tags=["extract", "prices", "bronze", "postgres"],
    on_failure_callback=task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(log_deadline_miss),
    ),
    params={
        "date_from_iso": "1970-01-01",
        "date_to_iso": "2099-01-01",
        "data_provider": "sql",
        "db_conn_name": "my_postgres_db_conn",
        "csv_path": None
        
    },
) as dag:
    start_task = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_prices_to_stg",
        python_callable=run_etl_task,
        op_kwargs={
            "provider": "{{ params.data_provider }}",
            "conn_id": "{{ params.db_conn_name }}", 
            "start_date": "{{ params.date_from_iso }}",
            "end_date": "{{ params.date_to_iso }}",
            "csv_path": "{{ params.csv_path }}"
        }
    )

    end_task = EmptyOperator(task_id="end")

start_task >> extract_task >> end_task