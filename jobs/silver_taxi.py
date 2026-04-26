"""
Silver taxi layer - parse + clean + zone-enrich bronze taxi events.

Same as Project 2's silver, but as an Airflow-triggered batch job.
Schema and cleaning rules lifted from BDM_project_2/pipeline.py:implement_silver.

Idempotent: silver tracks MAX(bronze_kafka_offset) and only processes new bronze rows.
Partitioned by pickup_date.
"""

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

from spark_session import get_spark


DB = "lakehouse.taxi"
BRONZE_TABLE = f"{DB}.bronze"
SILVER_TABLE = f"{DB}.silver"
ZONES_PATH = "/home/jovyan/project/data/taxi_zone_lookup.parquet"


# Schema of the JSON payload in bronze, from Project 2's implement_silver
JSON_SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])


def ensure_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            bronze_kafka_offset    LONG,
            VendorID               INT,
            tpep_pickup_datetime   STRING,
            tpep_dropoff_datetime  STRING,
            passenger_count        INT,
            trip_distance          DOUBLE,
            RatecodeID             INT,
            store_and_fwd_flag     STRING,
            PULocationID           INT,
            DOLocationID           INT,
            payment_type           INT,
            fare_amount            DOUBLE,
            extra                  DOUBLE,
            mta_tax                DOUBLE,
            tip_amount             DOUBLE,
            tolls_amount           DOUBLE,
            improvement_surcharge  DOUBLE,
            total_amount           DOUBLE,
            congestion_surcharge   DOUBLE,
            Airport_fee            DOUBLE,
            cbd_congestion_fee     DOUBLE,
            kafka_timestamp        TIMESTAMP,
            pickup_datetime        TIMESTAMP,
            dropoff_datetime       TIMESTAMP,
            pickup_date            DATE,
            trip_duration_min      DOUBLE,
            pickup_borough         STRING,
            pickup_zone            STRING,
            dropoff_borough        STRING,
            dropoff_zone           STRING
        )
        USING iceberg
        PARTITIONED BY (pickup_date)
    """)


def get_start_offset(spark):
    """Idempotent: only process bronze rows past the highest one we've already silvered."""
    row = spark.sql(f"SELECT MAX(bronze_kafka_offset) AS m FROM {SILVER_TABLE}").collect()[0]
    return -1 if row["m"] is None else int(row["m"])


def run(spark):
    ensure_table(spark)
    start_offset = get_start_offset(spark)
    print(f"[taxi-silver] reading bronze rows with kafka offset > {start_offset}")

    bronze_new = (
        spark.table(BRONZE_TABLE)
        .where(F.col("offset") > start_offset)
    )
    new_in_bronze = bronze_new.count()
    print(f"[taxi-silver] new bronze rows: {new_in_bronze}")
    if new_in_bronze == 0:
        total = spark.table(SILVER_TABLE).count()
        print(f"[taxi-silver] silver total rows: {total}")
        return 0, total

    # Parse JSON, cast types, derive timestamps + duration
    parsed = (
        bronze_new
        .withColumn("data", F.from_json(F.col("json_payload"), JSON_SCHEMA))
        .select(
            F.col("offset").alias("bronze_kafka_offset"),
            F.col("kafka_timestamp"),
            "data.*",
        )
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
        )
    )

    # Cleaning filters (from Project 2)
    cleaned = parsed.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        (F.col("trip_distance") > 0) &
        (F.coalesce(F.col("passenger_count"), F.lit(1)) > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        F.col("PULocationID").isNotNull() &
        F.col("DOLocationID").isNotNull()
    )

    # Zone enrichment via broadcast join
    zones = spark.read.parquet(ZONES_PATH).select(
        F.col("LocationID").cast("int"),
        F.col("Zone").alias("zone_name"),
        F.col("Borough").alias("borough"),
    )
    pickup_zones  = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("zone_name").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("zone_name").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    enriched = (
        cleaned
        .join(F.broadcast(pickup_zones),  on="PULocationID", how="left")
        .join(F.broadcast(dropoff_zones), on="DOLocationID", how="left")
    )

    # Project columns in the silver table order
    final = enriched.select(
        "bronze_kafka_offset",
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
        "kafka_timestamp",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "trip_duration_min",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
    )

    inserted = final.count()
    final.writeTo(SILVER_TABLE).append()

    total = spark.table(SILVER_TABLE).count()
    rejected = new_in_bronze - inserted
    print(f"[taxi-silver] inserted={inserted}, rejected={rejected}, silver total = {total}")
    return inserted, total


def main():
    spark = get_spark("silver_taxi")
    run(spark)


if __name__ == "__main__":
    main()
