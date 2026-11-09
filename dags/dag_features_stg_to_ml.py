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
from ml import features_stg_to_ml as etl_task


def run_etl_task(conn_id: str, start_date: str, end_date: str):
    """Обертка для запуска ETL с безопасным извлечением типизированных параметров."""
    try:
        conn = BaseHook.get_connection(conn_id)
        db_url = conn.get_uri()
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        etl_task.run_price_features_etl(
            db_url=db_url, 
            start_date=start_date, 
            end_date=end_date
        )
        
    except Exception as e:
        raise AirflowFailException(f"Критическая ошибка при выполнении ML Feature ETL: {e}")


with DAG(
    dag_id="ml_price_features_generation",
    default_args=default_args,
    description="Расчет и генерация календарных признаков для моделей ML",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    tags=["ml", "features", "prices", "postgres"],
    on_failure_callback=task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(log_deadline_miss),
    ),
    params={
        "db_conn_name": "my_postgres_db_conn",
        "start_date": "2026-01-01",
        "end_date": "2026-01-07",
    },
) as dag:

    start_task = EmptyOperator(task_id="start")

    extract_features_task = PythonOperator(
        task_id="calculate_ml_calendar_features",
        python_callable=run_etl_task,
        op_kwargs={
            "conn_id": "{{ params.db_conn_name }}",
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
        }
    )

    end_task = EmptyOperator(task_id="end")

start_task >> extract_features_task >> end_task