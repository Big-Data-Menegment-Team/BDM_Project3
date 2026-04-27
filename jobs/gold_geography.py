"""
Custom-scenario gold layer - per-country customer geography snapshot + trend.

Two output tables under lakehouse.cdc:
  gold_customer_geography  - one row per country, full overwrite each run
  gold_geography_trend     - one row per (run_id, country), append-only

Sources:
  lakehouse.cdc.silver_customers - current-state mirror (active customers)
  lakehouse.cdc.bronze_customers - raw CDC events (24h add/delete deltas)

Idempotency:
  - geography uses INSERT OVERWRITE - same input, same rows.
  - trend appends one row per country per run_id; skipped if run_id is already present,
    so DAG re-runs / `airflow tasks test` don't duplicate snapshots.

Usage:
    spark-submit gold_geography.py --run-id <id>      # from the DAG via {{ run_id }}
    spark-submit gold_geography.py                    # ad-hoc; run-id defaults to manual_<unix>
"""

import argparse
import sys
import time
from datetime import datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from spark_session import get_spark


DB = "lakehouse.cdc"
SILVER = f"{DB}.silver_customers"
BRONZE = f"{DB}.bronze_customers"
GEOG   = f"{DB}.gold_customer_geography"
TREND  = f"{DB}.gold_geography_trend"

WINDOW_HOURS = 24
DROP_THRESHOLD_PCT = -20.0


def ensure_tables(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GEOG} (
            run_id            STRING,
            snapshot_ts       TIMESTAMP,
            country           STRING,
            active_customers  BIGINT,
            added_24h         BIGINT,
            deleted_24h       BIGINT,
            net_change_24h    BIGINT,
            pct_of_total      DOUBLE
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TREND} (
            run_id            STRING,
            snapshot_ts       TIMESTAMP,
            country           STRING,
            active_customers  BIGINT,
            prev_active       BIGINT,
            delta             BIGINT,
            pct_change        DOUBLE,
            dropped_20pct     BOOLEAN
        ) USING iceberg
    """)


def compute_geography(spark, run_id, snapshot_ts):
    """Build the per-country snapshot DataFrame matching the GEOG schema."""
    cutoff_ms = int(time.time() * 1000) - WINDOW_HOURS * 3600 * 1000

    active = (
        spark.table(SILVER)
        .groupBy("country")
        .agg(F.count("*").cast("long").alias("active_customers"))
    )

    # For deletes the country lives in before_country (after_country is NULL).
    deltas = (
        spark.table(BRONZE)
        .where(F.col("ts_ms") >= F.lit(cutoff_ms))
        .where(F.col("op").isin("c", "d"))
        .withColumn("country", F.coalesce(F.col("after_country"), F.col("before_country")))
        .filter(F.col("country").isNotNull())
        .groupBy("country")
        .agg(
            F.sum(F.when(F.col("op") == "c", 1).otherwise(0)).cast("long").alias("added_24h"),
            F.sum(F.when(F.col("op") == "d", 1).otherwise(0)).cast("long").alias("deleted_24h"),
        )
    )

    joined = (
        active.join(deltas, on="country", how="full_outer")
        .fillna(0, subset=["active_customers", "added_24h", "deleted_24h"])
    )
    total_active = (joined.agg(F.sum("active_customers").alias("t")).collect()[0]["t"]) or 0

    return (
        joined
        .withColumn("net_change_24h", (F.col("added_24h") - F.col("deleted_24h")).cast("long"))
        .withColumn(
            "pct_of_total",
            F.when(F.lit(total_active) > 0,
                   F.col("active_customers") * 100.0 / F.lit(total_active))
             .otherwise(F.lit(0.0))
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("snapshot_ts", F.lit(snapshot_ts).cast("timestamp"))
        .select(
            "run_id", "snapshot_ts", "country",
            "active_customers", "added_24h", "deleted_24h",
            "net_change_24h", "pct_of_total",
        )
    )


def append_trend(spark, run_id, snapshot_ts, geography_df):
    """Append one trend row per country for this run. Skipped if run_id already in trend."""
    existing = spark.table(TREND).filter(F.col("run_id") == run_id).count()
    if existing > 0:
        print(f"[geography] trend already has run_id={run_id} ({existing} rows), skipping append")
        return 0

    # Most recent (country, active_customers) before this run, one row per country.
    prev = (
        spark.table(TREND)
        .where(F.col("run_id") != run_id)
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("country")
                      .orderBy(F.col("snapshot_ts").desc(), F.col("run_id").desc())
            )
        )
        .where(F.col("rn") == 1)
        .select("country", F.col("active_customers").alias("prev_active"))
    )

    current = geography_df.select("country", "active_customers")

    # Union of current + prior countries: catches countries that disappeared from silver
    # this run (active=0), so we still flag big drops including all-the-way-to-zero.
    countries = current.select("country").union(prev.select("country")).distinct()

    out = (
        countries
        .join(current, on="country", how="left")
        .fillna(0, subset=["active_customers"])
        .join(prev, on="country", how="left")
        .withColumn(
            "delta",
            (F.col("active_customers") - F.coalesce(F.col("prev_active"), F.lit(0))).cast("long")
        )
        .withColumn(
            "pct_change",
            F.when(
                (F.col("prev_active").isNotNull()) & (F.col("prev_active") > 0),
                (F.col("active_customers") - F.col("prev_active")) * 100.0 / F.col("prev_active")
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "dropped_20pct",
            F.when(F.col("pct_change") <= F.lit(DROP_THRESHOLD_PCT), F.lit(True))
             .otherwise(F.lit(False))
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("snapshot_ts", F.lit(snapshot_ts).cast("timestamp"))
        .select(
            "run_id", "snapshot_ts", "country",
            "active_customers", "prev_active", "delta", "pct_change", "dropped_20pct",
        )
    )

    n = out.count()
    out.writeTo(TREND).append()
    print(f"[geography] trend appended {n} rows for run_id={run_id}")
    return n


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=None,
                   help="Identifier for this run; defaults to manual_<unix_ts>")
    return p.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    run_id = args.run_id or f"manual_{int(time.time())}"
    snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    spark = get_spark("gold_geography")
    ensure_tables(spark)

    geography = compute_geography(spark, run_id, snapshot_ts).cache()
    geo_n = geography.count()

    geography.createOrReplaceTempView("_geo_new")
    spark.sql(f"INSERT OVERWRITE {GEOG} SELECT * FROM _geo_new")
    print(f"[geography] gold_customer_geography rebuilt: {geo_n} rows for run_id={run_id}")

    appended = append_trend(spark, run_id, snapshot_ts, geography)
    total_trend = spark.table(TREND).count()
    print(f"[geography] trend total rows: {total_trend}")

    geography.unpersist()
    return geo_n, appended


if __name__ == "__main__":
    main(sys.argv[1:])
