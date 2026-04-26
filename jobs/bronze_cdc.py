"""
Bronze CDC layer — append-only Iceberg table per source table.

  - Reads Kafka CDC topics with Spark batch
  - Parses the Debezium envelope from $.payload.*
  - Appends every event to a bronze Iceberg table
  - Includes Kafka metadata alongside CDC fields
  - Tombstones (null-value records) are filtered out

Idempotent: each run only ingests Kafka offsets greater than the
current MAX(kafka_offset) already in the bronze table. So rerunning with no new
events appends 0 rows. Bronze is the source of truth.

Usage:
    docker exec jupyter python /home/jovyan/project/jobs/bronze_cdc.py
    # Or one table at a time:
    docker exec jupyter python /home/jovyan/project/jobs/bronze_cdc.py customers
"""

import os
import sys

import pyspark.sql.functions as F

from spark_session import get_spark


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DB = "lakehouse.cdc"


# ── per-table config ──────────────────────────────────────────────────────────
# Each entry: (kafka_topic, bronze_table, list of source-row column specs).
# Specs are tuples: (column_name, json_path_under_after, spark_cast_or_None)
TABLES = {
    "customers": {
        "topic": "dbserver1.public.customers",
        "bronze": f"{DB}.bronze_customers",
        "fields": [
            ("id",      "id",      "int"),
            ("name",    "name",    None),   # STRING
            ("email",   "email",   None),
            ("country", "country", None),
        ],
    },
    "drivers": {
        "topic": "dbserver1.public.drivers",
        "bronze": f"{DB}.bronze_drivers",
        "fields": [
            ("id",             "id",             "int"),
            ("name",           "name",           None),
            ("license_number", "license_number", None),
            ("rating",         "rating",         "double"),
            ("city",           "city",           None),
            ("active",         "active",         "boolean"),
        ],
    },
}


def _bronze_columns_ddl(fields):
    """Build the typed before_*/after_* column list for the CREATE TABLE."""
    cols = []
    for col, _, cast in fields:
        sql_type = (cast.upper() if cast else "STRING")
        cols.append(f"before_{col} {sql_type}")
        cols.append(f"after_{col}  {sql_type}")
    return ",\n            ".join(cols)


def ensure_bronze_table(spark, bronze_table, fields):
    """Create the bronze Iceberg table if it doesn't exist. Append-only."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
            kafka_offset    BIGINT,
            kafka_partition INT,
            kafka_timestamp TIMESTAMP,
            op              STRING,
            {_bronze_columns_ddl(fields)},
            source_lsn      BIGINT,
            ts_ms           BIGINT
        ) USING iceberg
    """)


def parse_envelope(kafka_df, fields):
    """
    Parse Debezium envelope from raw Kafka rows.

    Identical shape to week05 cell c3: extract from $.payload.{op,before.*,after.*,
    source.lsn,ts_ms}. Tombstones (null value) filtered out before parsing.
    """
    raw = kafka_df.select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_value"),
    ).filter(F.col("raw_value").isNotNull())

    selects = [
        F.col("kafka_offset"),
        F.col("kafka_partition"),
        F.col("kafka_timestamp"),
        F.get_json_object("raw_value", "$.payload.op").alias("op"),
    ]
    for col, json_key, cast in fields:
        path_before = f"$.payload.before.{json_key}"
        path_after = f"$.payload.after.{json_key}"
        before_expr = F.get_json_object("raw_value", path_before)
        after_expr = F.get_json_object("raw_value", path_after)
        if cast:
            before_expr = before_expr.cast(cast)
            after_expr = after_expr.cast(cast)
        selects.append(before_expr.alias(f"before_{col}"))
        selects.append(after_expr.alias(f"after_{col}"))
    selects.append(F.get_json_object("raw_value", "$.payload.source.lsn").cast("long").alias("source_lsn"))
    selects.append(F.get_json_object("raw_value", "$.payload.ts_ms").cast("long").alias("ts_ms"))

    return raw.select(*selects)


def get_starting_offsets(spark, bronze_table, topic):
    """
    Return the per-partition `startingOffsets` JSON for the Kafka source.

    Idempotency: start one past the current MAX(kafka_offset) we've already stored.
    If bronze is empty, start from earliest.
    """
    # Bronze table just got created so it might be empty - handle that.
    row = spark.sql(f"SELECT MAX(kafka_offset) AS m, MAX(kafka_partition) AS p FROM {bronze_table}").collect()[0]
    if row["m"] is None:
        return "earliest", 0
    next_offset = int(row["m"]) + 1
    # Single-partition CDC topic (Debezium default). If you ever scale up partitions,
    # this should query MAX per partition.
    return ({topic: {"0": next_offset}}, next_offset)


def run_one(spark, table_key):
    cfg = TABLES[table_key]
    topic, bronze_table, fields = cfg["topic"], cfg["bronze"], cfg["fields"]

    ensure_bronze_table(spark, bronze_table, fields)

    starting, start_offset = get_starting_offsets(spark, bronze_table, topic)
    starting_str = starting if isinstance(starting, str) else __import__("json").dumps(starting)

    print(f"[{table_key}] reading from offset {start_offset}  (topic={topic})")

    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting_str)
        .load()
    )

    # `endingOffsets` defaults to latest - exactly what we want for a batch run.
    parsed = parse_envelope(kafka_df, fields)
    new_count = parsed.count()
    print(f"[{table_key}] new events to ingest: {new_count}")

    if new_count > 0:
        parsed.writeTo(bronze_table).append()

    total = spark.table(bronze_table).count()
    print(f"[{table_key}] bronze total rows: {total}")
    return new_count, total


def main(argv):
    spark = get_spark("bronze_cdc")
    keys = argv[1:] if len(argv) > 1 else list(TABLES.keys())
    for k in keys:
        if k not in TABLES:
            sys.exit(f"Unknown table '{k}'. Choices: {list(TABLES.keys())}")
        print("=" * 60)
        run_one(spark, k)


if __name__ == "__main__":
    main(sys.argv)
