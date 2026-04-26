"""
Validate silver CDC mirrors PostgreSQL.

Compares row counts of silver_customers and silver_drivers against the live
Postgres source. Allows a small tolerance because the simulator may still be
mutating Postgres in the gap between the silver MERGE and this check.
"""

import os
import sys

from spark_session import get_spark


def _ensure(pkg, import_name=None):
    import importlib.util, subprocess
    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("psycopg2-binary", "psycopg2")
import psycopg2


PG = dict(
    host=os.environ.get("PG_HOST", "postgres"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "sourcedb"),
    user=os.environ.get("PG_USER", "cdc_user"),
    password=os.environ.get("PG_PASSWORD", "cdc_pass"),
)
TOLERANCE = int(os.environ.get("VALIDATE_TOLERANCE", "5"))

# Map silver table name -> Postgres source table name
TABLES = {
    "silver_customers": "customers",
    "silver_drivers": "drivers",
}


def pg_count(table):
    conn = psycopg2.connect(**PG); conn.autocommit = True
    cur = conn.cursor(); cur.execute(f"SELECT COUNT(*) FROM {table};")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    return n


def run(spark):
    failures = []
    for silver_name, pg_name in TABLES.items():
        silver_n = spark.table(f"lakehouse.cdc.{silver_name}").count()
        pg_n     = pg_count(pg_name)
        delta    = abs(silver_n - pg_n)
        ok       = delta <= TOLERANCE
        status   = "OK" if ok else "FAIL"
        print(f"[validate] {silver_name}={silver_n}  pg={pg_n}  delta={delta}  tolerance={TOLERANCE}  {status}")
        if not ok:
            failures.append(f"{silver_name}: silver={silver_n} pg={pg_n} delta={delta}")
    if failures:
        raise ValueError("Silver does not mirror Postgres within tolerance: " + "; ".join(failures))
    print("[validate] all silver tables mirror Postgres within tolerance")


def main():
    spark = get_spark("validate")
    run(spark)


if __name__ == "__main__":
    main()
