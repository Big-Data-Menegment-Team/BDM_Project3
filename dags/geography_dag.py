"""
Customer Geography DAG - custom-scenario gold tables.

Independent of the main `lakehouse` DAG. Reads silver+bronze CDC and writes:
  - lakehouse.cdc.gold_customer_geography  (rebuilt every run, INSERT OVERWRITE)
  - lakehouse.cdc.gold_geography_trend     (append-only, one row per (run_id, country))

Tasks:
    connector_health  - HTTP sensor on Debezium connector (same gate as lakehouse DAG)
    gold_geography    - run jobs/gold_geography.py with the Airflow run_id

Schedule: `5 * * * *` - 5 minutes past every hour. The main `lakehouse` DAG runs
on `@hourly` (top of the hour) and finishes in ~75s, so by :05 silver_cdc has
committed and the geography snapshot sees the freshly-merged state. If silver
hasn't been refreshed (e.g. main DAG paused or failing), the snapshot reflects
the last committed state and the trend row records "stable" for that interval.

Idempotency: the spark job is keyed on run_id - re-running for the same run_id
overwrites the geography table with the same content and skips appending a
duplicate trend row.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor


SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
)


def spark_task_cmd(script, *extra_args):
    extra = " ".join(extra_args)
    return (
        f"docker exec -w /home/jovyan/project/jobs jupyter "
        f"spark-submit --packages {SPARK_PACKAGES} {script} {extra}"
    ).strip()


def on_failure(context):
    ti = context.get("task_instance")
    print(f"FAILED - dag={ti.dag_id} task={ti.task_id} run={ti.run_id}")


default_args = {
    "owner": "bdm",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=15),
    "on_failure_callback": on_failure,
}


with DAG(
    dag_id="customer_geography",
    description="Per-country gold tables driven by CDC silver - custom scenario (issue #1)",
    schedule_interval="5 * * * *",  # 5 min past the hour, after the main DAG's MERGE
    start_date=datetime(2026, 4, 26),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bdm", "project3", "cdc", "custom-scenario"],
) as dag:

    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_connect",
        endpoint="/connectors/pg-cdc-connector/status",
        response_check=lambda r: r.json().get("connector", {}).get("state") == "RUNNING",
        poke_interval=20,
        timeout=300,
        mode="poke",
    )

    # Single-quote the run_id since Airflow renders something like
    # `scheduled__2026-04-26T19:00:00+00:00` which contains shell-special chars.
    gold_geography = BashOperator(
        task_id="gold_geography",
        bash_command=spark_task_cmd(
            "gold_geography.py", "--run-id", "'{{ run_id }}'",
        ),
    )

    connector_health >> gold_geography
