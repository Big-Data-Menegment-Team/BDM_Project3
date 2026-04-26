"""
Tasks:
    connector_health - HTTP sensor on Debezium connector status
    bronze_cdc - load new CDC events into bronze
    bronze_taxi - load new taxi events into bronze
    silver_cdc - MERGE bronze -> silver for CDC tables
    silver_taxi - clean + enrich taxi events into silver
    gold_taxi - aggregate silver taxi data into gold
    validate - compare silver CDC row counts against PostgreSQL

Schedule: @hourly. Justification:
- Customer + driver tables turn over slowly. 1-hour freshness covers reporting needs.
- @hourly leaves headroom for retries (3 retries with exponential backoff up to 10min)
- Iceberg compaction stays manageable (one MERGE per hour vs many per minute).

All tasks are idempotent. re-running the same logical date yields the same Iceberg state
(bronze tracks MAX(kafka_offset), silver MERGE is deterministic, gold uses INSERT OVERWRITE).

Spark jobs run inside the existing `jupyter` container via `docker exec` (airflow
container has docker.io installed and the docker socket mounted from the host).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor


# ── Spark-submit boilerplate (same packages as PYSPARK_SUBMIT_ARGS in compose.yml) ─
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
)


def spark_task_cmd(script):
    """Return a bash command that runs a jobs/<script> via spark-submit inside jupyter."""
    return (
        f"docker exec -w /home/jovyan/project/jobs jupyter "
        f"spark-submit --packages {SPARK_PACKAGES} {script}"
    )


def on_failure(context):
    ti = context.get("task_instance")
    print(f"FAILED - dag={ti.dag_id} task={ti.task_id} run={ti.run_id}")


default_args = {
    "owner": "bdm",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
    "on_failure_callback": on_failure,
}


with DAG(
    dag_id="lakehouse",
    description="CDC + taxi medallion pipeline orchestrated end-to-end",
    schedule_interval="@hourly",
    start_date=datetime(2026, 4, 26),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bdm", "project3", "cdc", "taxi"],
) as dag:

    # 1. Health gate - Debezium connector must be RUNNING before anything downstream
    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_connect",
        endpoint="/connectors/pg-cdc-connector/status",
        response_check=lambda r: r.json().get("connector", {}).get("state") == "RUNNING",
        poke_interval=20,
        timeout=300,
        mode="poke",
    )

    # 2. Bronze (Kafka -> Iceberg, append-only). both paths run in parallel after health gate
    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=spark_task_cmd("bronze_cdc.py"),
    )
    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=spark_task_cmd("bronze_taxi.py"),
    )

    # 3. Silver, depends only on its own bronze
    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=spark_task_cmd("silver_cdc.py"),
    )
    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=spark_task_cmd("silver_taxi.py"),
    )

    # 4. Gold, taxi only (CDC has no gold layer)
    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=spark_task_cmd("gold_taxi.py"),
    )

    # 5. Validation, silver CDC counts vs Postgres
    validate = BashOperator(
        task_id="validate",
        bash_command=spark_task_cmd("validate.py"),
    )

    #Dependency
    connector_health >> [bronze_cdc, bronze_taxi]
    bronze_cdc  >> silver_cdc
    bronze_taxi >> silver_taxi >> gold_taxi
    [silver_cdc, gold_taxi] >> validate
