"""
Silver CDC layer - MERGE bronze events into a current-state mirror per source table.

  - Reads bronze, dedups to one event per primary key (latest by ts_ms, kafka_offset).
  - MERGE INTO silver:
      op = 'd'                  -> DELETE the row
      op IN ('c', 'u', 'r')     -> UPDATE if exists, INSERT if not
  - After MERGE, silver mirrors the current state of the PostgreSQL source.

Idempotent by construction:
  1. Bronze events are unique per (kafka_topic, kafka_offset).
  2. Dedup keeps exactly one row per PK -> input to MERGE is keyed by PK.
  3. MERGE on a PK predicate is deterministic -> replaying the same dedup view is a no-op.
  4. Even with overlapping windows, ts_ms DESC wins, target state stays the same.

Usage:
    docker exec jupyter python /home/jovyan/project/jobs/silver_cdc.py
    docker exec jupyter python /home/jovyan/project/jobs/silver_cdc.py customers
"""

import sys

from spark_session import get_spark


DB = "lakehouse.cdc"


# Per-table config: mirrors bronze_cdc.TABLES but with the business-column projection
# the silver MERGE needs.  `pk` is the primary key column.  `columns` includes the PK.
TABLES = {
    "customers": {
        "bronze": f"{DB}.bronze_customers",
        "silver": f"{DB}.silver_customers",
        "pk": "id",
        "columns": ["id", "name", "email", "country"],
        "ddl": f"""
            CREATE TABLE IF NOT EXISTS {DB}.silver_customers (
                id         INT,
                name       STRING,
                email      STRING,
                country    STRING,
                updated_ts BIGINT
            ) USING iceberg
        """,
    },
    "drivers": {
        "bronze": f"{DB}.bronze_drivers",
        "silver": f"{DB}.silver_drivers",
        "pk": "id",
        "columns": ["id", "name", "license_number", "rating", "city", "active"],
        "ddl": f"""
            CREATE TABLE IF NOT EXISTS {DB}.silver_drivers (
                id             INT,
                name           STRING,
                license_number STRING,
                rating         DOUBLE,
                city           STRING,
                active         BOOLEAN,
                updated_ts     BIGINT
            ) USING iceberg
        """,
    },
}


def ensure_silver_table(spark, table_key):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(TABLES[table_key]["ddl"])


def run_one(spark, table_key):
    cfg = TABLES[table_key]
    bronze, silver, pk, cols = cfg["bronze"], cfg["silver"], cfg["pk"], cfg["columns"]

    ensure_silver_table(spark, table_key)

    # ── Build the dedup view: latest event per PK from bronze ─────────────
    # PK comes from after_<pk> or before_<pk> (delete events have NULL after_*).
    # Other columns come from after_* - they're NULL for op='d', which is fine
    # because the MERGE 'd' branch is a DELETE that doesn't read them.
    other_cols = [c for c in cols if c != pk]
    after_select = ",\n            ".join(f"after_{c} AS {c}" for c in other_cols)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW cdc_latest AS
        SELECT * FROM (
            SELECT
                op,
                COALESCE(after_{pk}, before_{pk}) AS {pk},
                {after_select},
                ts_ms,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(after_{pk}, before_{pk})
                    ORDER BY ts_ms DESC, kafka_offset DESC
                ) AS rn
            FROM {bronze}
            WHERE op IS NOT NULL
        )
        WHERE rn = 1
    """)

    # ── MERGE INTO ─────────────────────────────────────────────────────────
    update_set = ",\n            ".join(f"{c} = source.{c}" for c in other_cols)
    insert_cols = ", ".join(cols + ["updated_ts"])
    insert_vals = ", ".join(f"source.{c}" for c in cols) + ", source.ts_ms"

    before_count = spark.table(silver).count()
    spark.sql(f"""
        MERGE INTO {silver} AS target
        USING cdc_latest AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED AND source.op = 'd' THEN DELETE
        WHEN MATCHED AND source.op IN ('u', 'c', 'r') THEN UPDATE SET
            {update_set},
            updated_ts = source.ts_ms
        WHEN NOT MATCHED AND source.op IN ('c', 'r', 'u') THEN INSERT
            ({insert_cols})
            VALUES ({insert_vals})
    """)
    after_count = spark.table(silver).count()
    print(f"[{table_key}] silver: {before_count} -> {after_count} rows")
    return before_count, after_count


def main(argv):
    spark = get_spark("silver_cdc")
    keys = argv[1:] if len(argv) > 1 else list(TABLES.keys())
    for k in keys:
        if k not in TABLES:
            sys.exit(f"Unknown table '{k}'. Choices: {list(TABLES.keys())}")
        print("=" * 60)
        run_one(spark, k)


if __name__ == "__main__":
    main(sys.argv)
