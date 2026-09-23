"""Daily profitability DAG.

One DAG run per minute. The DAG is triggered by the minute-cadence
sensor; max_active_runs=1 serialises concurrent triggers.
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

def _run_pipeline(**ctx):
    """Run the entire daily profitability pipeline for all eligible dates."""
    from batch.reconcile import run_available
    run_available()

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_profitability",
    description="Daily profitability pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=_DEFAULT_ARGS,
    tags=["fleet", "lambda", "batch"],
) as dag:
    run_pipeline = PythonOperator(
        task_id="run_profitability_pipeline",
        python_callable=_run_pipeline,
    )
