"""
Shared SparkSession factory using the Iceberg REST catalog backed by MinIO.

Matches project2 pipeline.py so taxi and CDC jobs share one
catalog `lakehouse` -> use as `lakehouse.<database>.<table>`.
"""

import os
from pyspark.sql import SparkSession


def get_spark(app_name="bdm_project3"):
    minio_user = os.environ["AWS_ACCESS_KEY_ID"]
    minio_pass = os.environ["AWS_SECRET_ACCESS_KEY"]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")

        # Iceberg + REST catalog
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", minio_user)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", minio_pass)
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")

        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
