from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def reconcile_available_days():
    from batch.reconcile import run_available
    run_available()


with DAG(
    dag_id="daily_profitability",
    description="Poll daily expense arrivals and restate ready simulated days",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["fleet", "lambda"],
) as dag:
    PythonOperator(task_id="validate_reconcile_publish", python_callable=reconcile_available_days)
