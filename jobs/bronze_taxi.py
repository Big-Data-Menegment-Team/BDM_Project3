"""
Bronze taxi layer - append-only Iceberg table holding raw taxi-trips JSON events.

Same medallion pipeline as Project 2, but triggered by Airflow as a batch job
(not Structured Streaming). Idempotent: start offset = MAX(kafka_offset) + 1.

Schema mirrors BDM_project_2/pipeline.py:create_tables - same column shape.

Usage:
    docker exec -w /home/jovyan/project/jobs jupyter spark-submit \\
      --packages <see PYSPARK_SUBMIT_ARGS in compose.yml> \\
      bronze_taxi.py
"""

import json
import os
import sys

import pyspark.sql.functions as F

from spark_session import get_spark


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "taxi-trips"
DB = "lakehouse.taxi"
BRONZE_TABLE = f"{DB}.bronze"


def ensure_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            vendor_id_key   STRING,
            json_payload    STRING,
            topic           STRING,
            partition       INT,
            offset          LONG,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)


def get_starting_offsets(spark):
    """Idempotent incremental read: start one past the current MAX(offset)."""
    row = spark.sql(f"SELECT MAX(offset) AS m FROM {BRONZE_TABLE}").collect()[0]
    if row["m"] is None:
        return "earliest", 0
    next_offset = int(row["m"]) + 1
    return (json.dumps({TOPIC: {"0": next_offset}}), next_offset)


def run(spark):
    ensure_table(spark)
    starting, start_offset = get_starting_offsets(spark)
    print(f"[taxi] reading from offset {start_offset}  (topic={TOPIC})")

    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting)
        .load()
        .filter(F.col("value").isNotNull())
    )

    bronze = kafka_df.selectExpr(
        "CAST(key AS STRING) AS vendor_id_key",
        "CAST(value AS STRING) AS json_payload",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
    )

    new_count = bronze.count()
    print(f"[taxi] new events to ingest: {new_count}")
    if new_count > 0:
        bronze.writeTo(BRONZE_TABLE).append()

    total = spark.table(BRONZE_TABLE).count()
    print(f"[taxi] bronze total rows: {total}")
    return new_count, total


def main():
    spark = get_spark("bronze_taxi")
    run(spark)


if __name__ == "__main__":
    main()
