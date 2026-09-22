"""Daily profitability DAG — multi-stage pipeline per PROJECT_PLAN.md §5.1.

Stage graph:
    wait_for_expense_file
        -> validate_expense_file
            -> aggregate_trips
                -> join_costs_and_compute_profit
                    -> publish_serving
                        -> render_report
                            -> emit_metrics

One DAG run per simulated day.  The DAG is triggered by the minute-cadence
sensor; max_active_runs=1 serialises concurrent triggers.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


# ---------------------------------------------------------------------------
# Stage callables
# ---------------------------------------------------------------------------

def _wait_for_expense_file(**ctx):
    """Return the first unprocessed report_date that has both a committed
    Parquet archive AND an expense CSV, or skip the run if none is ready."""
    from batch.reconcile import next_available_date
    report_date = next_available_date()
    if report_date is None:
        return "skip_run"
    ctx["ti"].xcom_push(key="report_date", value=report_date)
    return "validate_expense_file"


def _validate_expense_file(**ctx):
    """DQ gate: validate the expense CSV for the report_date.
    Quarantines bad rows.  Fails the task if rejection rate > DQ_FAILURE_THRESHOLD."""
    from batch.reconcile import validate_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    validate_day(report_date)


def _aggregate_trips(**ctx):
    """Read the day's committed Parquet archive and verify archive integrity."""
    from batch.reconcile import aggregate_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    aggregate_day(report_date)


def _join_costs_and_compute_profit(**ctx):
    """Join aggregated trips with validated expenses; compute profit/margin."""
    from batch.reconcile import join_and_compute_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    join_and_compute_day(report_date)


def _publish_serving(**ctx):
    """Idempotent upsert of daily_vehicle_profit + daily_zone_summary rows."""
    from batch.reconcile import publish_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    publish_day(report_date)


def _render_report(**ctx):
    """Write the JSON + CSV + HTML daily report files to /data/reports/."""
    from batch.reconcile import render_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    render_day(report_date)


def _emit_metrics(**ctx):
    """Push pipeline counters to the /metrics Prometheus endpoint."""
    from batch.reconcile import emit_metrics_day
    report_date = ctx["ti"].xcom_pull(key="report_date")
    emit_metrics_day(report_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_profitability",
    description="Multi-stage daily profitability pipeline per PROJECT_PLAN.md §5.1",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=_DEFAULT_ARGS,
    tags=["fleet", "lambda", "batch"],
) as dag:

    wait = BranchPythonOperator(
        task_id="wait_for_expense_file",
        python_callable=_wait_for_expense_file,
    )

    skip = EmptyOperator(task_id="skip_run")

    validate = PythonOperator(
        task_id="validate_expense_file",
        python_callable=_validate_expense_file,
    )

    aggregate = PythonOperator(
        task_id="aggregate_trips",
        python_callable=_aggregate_trips,
    )

    join = PythonOperator(
        task_id="join_costs_and_compute_profit",
        python_callable=_join_costs_and_compute_profit,
    )

    publish = PythonOperator(
        task_id="publish_serving",
        python_callable=_publish_serving,
    )

    render = PythonOperator(
        task_id="render_report",
        python_callable=_render_report,
    )

    metrics = PythonOperator(
        task_id="emit_metrics",
        python_callable=_emit_metrics,
    )

    # Stage dependencies
    wait >> [skip, validate]
    validate >> aggregate >> join >> publish >> render >> metrics
