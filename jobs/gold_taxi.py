"""
Gold taxi layer - hourly aggregation per zone-pair.

Schema mirrors Project 2's gold table (BDM_project_2/pipeline.py:create_tables).

Idempotency improvement over Project 2: full INSERT OVERWRITE rebuilds gold from
silver on every run - re-running with the same silver state produces identical gold.
Project 2 used Structured Streaming `outputMode('complete')` which has state-recovery
problems on restart; this batch + overwrite pattern is simpler and replayable.

Aggregation grain: (hour, pickup_zone, pickup_borough, dropoff_zone, dropoff_borough).
"""

import sys
import pyspark.sql.functions as F
from spark_session import get_spark


DB = "lakehouse.taxi"
SILVER_TABLE = f"{DB}.silver"
GOLD_TABLE   = f"{DB}.gold"


def ensure_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            hour                TIMESTAMP,
            hour_end            TIMESTAMP,
            pickup_zone         STRING,
            pickup_borough      STRING,
            dropoff_zone        STRING,
            dropoff_borough     STRING,
            trip_count          BIGINT,
            total_revenue       DOUBLE,
            avg_fare            DOUBLE,
            avg_distance_miles  DOUBLE,
            avg_duration_min    DOUBLE,
            total_passengers    BIGINT,
            revenue_per_trip    DOUBLE
        )
        USING iceberg
        PARTITIONED BY (pickup_zone)
    """)


def run(spark):
    ensure_table(spark)

    silver = spark.table(SILVER_TABLE)
    silver_n = silver.count()
    print(f"[taxi-gold] silver rows: {silver_n}")
    if silver_n == 0:
        print("[taxi-gold] silver empty, gold left unchanged")
        return 0, spark.table(GOLD_TABLE).count()

    gold = (
        silver
        .groupBy(
            F.window("pickup_datetime", "1 hour").alias("window"),
            "pickup_zone",
            "pickup_borough",
            "dropoff_zone",
            "dropoff_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.sum("passenger_count").cast("long").alias("total_passengers"),
        )
        .select(
            F.col("window.start").alias("hour"),
            F.col("window.end").alias("hour_end"),
            "pickup_zone",
            "pickup_borough",
            "dropoff_zone",
            "dropoff_borough",
            "trip_count",
            "total_revenue",
            "avg_fare",
            "avg_distance_miles",
            "avg_duration_min",
            "total_passengers",
            (F.col("total_revenue") / F.col("trip_count")).alias("revenue_per_trip"),
        )
    )

    gold.writeTo(GOLD_TABLE).overwritePartitions()
    total = spark.table(GOLD_TABLE).count()
    print(f"[taxi-gold] gold aggregated rows: {total}")
    return total, total


def main():
    spark = get_spark("gold_taxi")
    run(spark)


if __name__ == "__main__":
    main()
