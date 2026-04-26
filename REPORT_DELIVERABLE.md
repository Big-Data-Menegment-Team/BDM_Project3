# Project 3 - Report

CDC + Orchestrated Lakehouse Pipeline with Debezium, Airflow, and Iceberg.

Same thing done in jupyer notebook in [`walkthrough.ipynb`](walkthrough.ipynb).

---

## 1. CDC correctness

### 1.1 Silver mirrors PostgreSQL - row counts + spot-check

The `silver_cdc` MERGE produces a typed, deduplicated mirror of each source table.

```python
for table, pg_table in [('customers', 'customers'), ('drivers', 'drivers')]:
    silver_n = spark.table(f'lakehouse.cdc.silver_{table}').count()
    pg_n     = pg(f'SELECT COUNT(*) FROM {pg_table};', fetch=True)[0][0]
    print(f'{table:<10} silver={silver_n:<3} postgres={pg_n:<3} match={silver_n == pg_n}')

print('\nSilver customers (5 rows):')
spark.sql('SELECT id, name, email, country FROM lakehouse.cdc.silver_customers ORDER BY id LIMIT 5').show(truncate=False)

print('Postgres customers (same 5 rows for cross-check):')
print(pg('SELECT id, name, email, country FROM customers ORDER BY id LIMIT 5;', fetch=True))
```

```
customers  silver=9   postgres=9   match=True
drivers    silver=9   postgres=9   match=True
```

Five-row spot-check. silver vs Postgres are identical column-for-column, including the `test@example.com` value applied via an `UPDATE id=1` and the `bronze-test@example.com` from `UPDATE id=2`:

```
silver_customers (5 rows):
+---+--------------+-----------------------+---------+
|id |name          |email                  |country  |
+---+--------------+-----------------------+---------+
|1  |Alice Mets    |test@example.com       |Estonia  |
|2  |Bob Virtanen  |bronze-test@example.com|Finland  |
|4  |David Jonaitis|david@example.com      |Lithuania|
|5  |Eva Svensson  |eva@example.com        |Sweden   |
|6  |Frank Muller  |frank@example.com      |Germany  |
+---+--------------+-----------------------+---------+

postgres customers (same 5 rows):
[(1, 'Alice Mets', 'test@example.com', 'Estonia'),
 (2, 'Bob Virtanen', 'bronze-test@example.com', 'Finland'),
 (4, 'David Jonaitis', 'david@example.com', 'Lithuania'),
 (5, 'Eva Svensson', 'eva@example.com', 'Sweden'),
 (6, 'Frank Muller', 'frank@example.com', 'Germany')]
```

(ids 3 and 10 are absent - both got deleted; the bronze table further down shows the `op='d'` events for them.)

### 1.2 DELETEs reflected as absent rows

```python
target_id = pg('SELECT id FROM customers ORDER BY id DESC LIMIT 1;', fetch=True)[0][0]
print(f'will delete customer id={target_id}')

pg(f"DELETE FROM customers WHERE id={target_id};")
time.sleep(2)
bronze_cdc.run_one(spark, 'customers')
silver_cdc.run_one(spark, 'customers')

still_in_silver = spark.sql(f'SELECT COUNT(*) AS n FROM lakehouse.cdc.silver_customers WHERE id={target_id}').collect()[0]['n']
still_in_pg     = pg(f'SELECT COUNT(*) FROM customers WHERE id={target_id};', fetch=True)[0][0]
print(f'after DELETE: silver={still_in_silver} postgres={still_in_pg}  (both should be 0)')
```

```
will delete customer id=12
[customers] reading from offset 18  (topic=dbserver1.public.customers)
[customers] new events to ingest: 1
[customers] bronze total rows: 17
[customers] silver: 10 -> 9 rows
after DELETE: silver=0 postgres=0  (both should be 0)
```

The MERGE branch responsible: `WHEN MATCHED AND source.op = 'd' THEN DELETE` - see [`jobs/silver_cdc.py`](jobs/silver_cdc.py).

### 1.3 Idempotency - re-runs with no new bronze events leave silver unchanged

Bronze is idempotent because the Kafka start offset is `MAX(kafka_offset) + 1` per topic - no new events means nothing to append:

```
[customers] reading from offset 12  new events to ingest: 0  bronze total rows: 12
[drivers]   reading from offset 12  new events to ingest: 0  bronze total rows: 12
```

Silver is idempotent because MERGE on a PK predicate is deterministic - replaying the same dedup view doesn't change target state:

```
[customers] silver: 9 -> 9 rows   unchanged=True
[drivers]   silver: 9 -> 9 rows   unchanged=True
```

**Why it's idempotent (proof sketch):**
1. Bronze events are unique per `(topic, kafka_offset)`.
2. Dedup keeps exactly one row per primary key (latest by `ts_ms`, then `kafka_offset`).
3. MERGE on a PK predicate is deterministic - same dedup view, same target state.
4. Even with overlapping incremental windows, `ts_ms DESC` resolves the winner deterministically.

### 1.4 All four Debezium op codes appear in bronze

```python
spark.sql("""
    SELECT op, after_id, after_name, after_email, source_lsn, ts_ms
    FROM lakehouse.cdc.bronze_customers
    ORDER BY kafka_offset
""").show(20, truncate=False)
ops = sorted({row['op'] for row in spark.sql('SELECT DISTINCT op FROM lakehouse.cdc.bronze_customers').collect()})
print(f'distinct op codes: {ops}')
```

```
+---+--------+--------------+-----------------------+----------+-------------+
|op |after_id|after_name    |after_email            |source_lsn|ts_ms        |
+---+--------+--------------+-----------------------+----------+-------------+
|r  |1       |Alice Mets    |alice@example.com      |26832872  |1777234484983|
|r  |2       |Bob Virtanen  |bob@example.com        |26832872  |1777234484986|
|r  |3       |Carol Ozols   |carol@example.com      |26832872  |1777234484989|
|r  |4       |David Jonaitis|david@example.com      |26832872  |1777234484989|
|r  |5       |Eva Svensson  |eva@example.com        |26832872  |1777234484990|
|r  |6       |Frank Muller  |frank@example.com      |26832872  |1777234484990|
|r  |7       |Grace Kim     |grace@example.com      |26832872  |1777234484990|
|r  |8       |Hiro Tanaka   |hiro@example.com       |26832872  |1777234484992|
|r  |9       |Ingrid Larsen |ingrid@example.com     |26832872  |1777234484992|
|r  |10      |Javier Garcia |javier@example.com     |26832872  |1777234484992|
|u  |1       |Alice Mets    |test@example.com       |26832912  |1777234486759|
|d  |NULL    |NULL          |NULL                   |26833256  |1777234489836|
|u  |2       |Bob Virtanen  |bronze-test@example.com|26834000  |1777234572691|
|c  |11      |Test Insert   |test-c@example.com     |26834184  |1777234577300|
|d  |NULL    |NULL          |NULL                   |26834408  |1777234577301|
+---+--------+--------------+-----------------------+----------+-------------+

distinct op codes: ['c', 'd', 'r', 'u']
```

Reading the table:
- 10× `op='r'` - initial snapshot reads from when the connector started.
- `op='u'` × 2 - `UPDATE id=1` (forced live) and `UPDATE id=2` (bronze-test).
- `op='c'` × 1 - `INSERT id=11 'Test Insert'` (the deliberate INSERT-then-DELETE pair).
- `op='d'` × 2 - `DELETE id=10` (simulator burst) and `DELETE id=3` (the deliberate INSERT-then-DELETE pair). After-fields are `NULL`; the deleted PK lives in `before_*` columns.

For DELETEs (`op='d'`), `after_*` is NULL and the primary key lives in `before_*` - the dedup `COALESCE(after_id, before_id)` picks it up.

### 1.5 Implementation choices and why

| Choice | Why |
|---|---|
| `snapshot.mode = initial` | Emits `op='r'` events for the rows present at connector-start time, so silver gets populated on the first run without needing a backfill step. |
| `decimal.handling.mode = double` | Without this, Postgres `NUMERIC` columns arrive as base64-encoded bytesand Spark's `cast(decimal(3,2))` crashes. Setting it to `double` makes them arrive as plain JSON numbers - we found this on `drivers.rating`. |
| One bronze per source table (typed cols) | Each source table gets its own bronze, with `before_*`/`after_*` parsed into typed columns. Schema is readable via `DESCRIBE TABLE`, and the dedup query on the silver side is straightforward. |
| Bronze incremental = `MAX(kafka_offset) + 1` | Bronze itself remembers what's already been ingested - no separate state file. Survives DAG restarts and means re-running with no new Kafka events appends 0 rows. |
| Iceberg REST catalog | The `iceberg-rest` service is already in `compose.yml` and Project 2's Spark config uses the REST catalog too - keeps everything consistent. |
| MERGE pattern | `op='d'→DELETE`, `op IN ('c','u','r')→UPSERT` maps cleanly onto Iceberg's `MERGE INTO` - same dedup view → same target state, so re-running is a no-op. |

### 1.6 Tombstone arithmetic

A DELETE in Postgres produces **two** Kafka events: the `d`-envelope and a tombstone (null-value record for log compaction). Bronze filters tombstones via `value.isNotNull()`.

```python
before_kafka  = topic_offset('dbserver1.public.customers')
before_bronze = spark.table('lakehouse.cdc.bronze_customers').count()
pg("INSERT INTO customers (name, email, country) VALUES ('Test Insert', 'test-c@example.com', 'France');")
pg("DELETE FROM customers WHERE id=3;")
time.sleep(2)
after_kafka = topic_offset('dbserver1.public.customers')
new, after_bronze = bronze_cdc.run_one(spark, 'customers')
print(f'kafka: {before_kafka} -> {after_kafka}  (+{after_kafka - before_kafka})')
print(f'bronze: {before_bronze} -> {after_bronze}  (+{after_bronze - before_bronze})')
print(f'tombstones filtered: {(after_kafka - before_kafka) - (after_bronze - before_bronze)}')
```

```
[customers] reading from offset 14  (topic=dbserver1.public.customers)
[customers] new events to ingest: 2
[customers] bronze total rows: 15
kafka: 14 -> 17  (+3)
bronze: 13 -> 15  (+2)
tombstones filtered: 1
```

---

## 2. Lakehouse design

### 2.1 Schemas - Bronze CDC

```sql
DESCRIBE TABLE lakehouse.cdc.bronze_customers;
```

```
+---------------+---------+
|col_name       |data_type|
+---------------+---------+
|kafka_offset   |bigint   |
|kafka_partition|int      |
|kafka_timestamp|timestamp|
|op             |string   |
|before_id      |int      |
|after_id       |int      |
|before_name    |string   |
|after_name     |string   |
|before_email   |string   |
|after_email    |string   |
|before_country |string   |
|after_country  |string   |
|source_lsn     |bigint   |
|ts_ms          |bigint   |
+---------------+---------+
```

`bronze_drivers` mirrors the shape with the additional source columns:
`before_/after_license_number STRING`, `before_/after_rating DOUBLE`, `before_/after_city STRING`, `before_/after_active BOOLEAN`. The `DOUBLE` type for `rating` is the visible result of the `decimal.handling.mode=double` connector setting.

Both tables are append-only - every CDC event is preserved. Kafka metadata (`kafka_offset`, `kafka_partition`, `kafka_timestamp`) supports replay and audit.

### 2.2 Schemas - Silver CDC

```sql
DESCRIBE TABLE lakehouse.cdc.silver_customers;
```

```
+----------+---------+
|col_name  |data_type|
+----------+---------+
|id        |int      |
|name      |string   |
|email     |string   |
|country   |string   |
|updated_ts|bigint   |
+----------+---------+
```

`silver_drivers` likewise: `id INT, name STRING, license_number STRING, rating DOUBLE, city STRING, active BOOLEAN, updated_ts BIGINT`.

Silver collapses bronze's many events per row into one - the latest. `updated_ts` is the `ts_ms` of the bronze event that drove the row, useful for downstream incremental consumers and as a tie-breaker.

### 2.3 Schemas - Taxi (Bronze, Silver, Gold)

`lakehouse.taxi.bronze` - raw JSON + Kafka metadata, append-only:

```
+---------------+---------+
|col_name       |data_type|
+---------------+---------+
|vendor_id_key  |string   |
|json_payload   |string   |
|topic          |string   |
|partition      |int      |
|offset         |bigint   |
|kafka_timestamp|timestamp|
+---------------+---------+
```

`lakehouse.taxi.silver` (partitioned by `pickup_date`) - typed, cleaned, zone-enriched. Every JSON field from bronze becomes a typed column, plus tracking, parsed timestamps, derived metrics, and zone lookups:

```
+----------------------+---------+
|col_name              |data_type|
+----------------------+---------+
|bronze_kafka_offset   |bigint   |   ← idempotency tracker: next silver reads bronze WHERE offset > MAX
|VendorID              |int      |   ┐
|tpep_pickup_datetime  |string   |   │
|tpep_dropoff_datetime |string   |   │
|passenger_count       |int      |   │
|trip_distance         |double   |   │
|RatecodeID            |int      |   │
|store_and_fwd_flag    |string   |   │
|PULocationID          |int      |   │
|DOLocationID          |int      |   │  Typed source columns parsed from
|payment_type          |int      |   │  bronze.json_payload via from_json(JSON_SCHEMA)
|fare_amount           |double   |   │
|extra                 |double   |   │
|mta_tax               |double   |   │
|tip_amount            |double   |   │
|tolls_amount          |double   |   │
|improvement_surcharge |double   |   │
|total_amount          |double   |   │
|congestion_surcharge  |double   |   │
|Airport_fee           |double   |   │
|cbd_congestion_fee    |double   |   ┘
|kafka_timestamp       |timestamp|   ← from bronze metadata
|pickup_datetime       |timestamp|   ← derived: tpep_pickup_datetime cast to timestamp
|dropoff_datetime      |timestamp|   ← derived: tpep_dropoff_datetime cast to timestamp
|pickup_date           |date     |   ← derived; partition column
|trip_duration_min     |double   |   ← derived: (dropoff_datetime − pickup_datetime) / 60
|pickup_borough        |string   |   ┐
|pickup_zone           |string   |   │ Joined from data/taxi_zone_lookup.parquet
|dropoff_borough       |string   |   │ via broadcast joins on PU/DO LocationID
|dropoff_zone          |string   |   ┘
+----------------------+---------+
```

`lakehouse.taxi.gold` (partitioned by `pickup_zone`) - hourly aggregation, rebuilt via `INSERT OVERWRITE` each run:

```
hour, hour_end, pickup_zone, pickup_borough, dropoff_zone, dropoff_borough,
trip_count, total_revenue, avg_fare, avg_distance_miles, avg_duration_min,
total_passengers, revenue_per_trip
```

Why each layer differs from the previous:
- Bronze keeps the raw JSON for replay/audit, no parsing risk.
- Silver applies type-cast + cleaning + enrichment (broadcast-join with `taxi_zone_lookup`). Cleaning rules: `dropoff > pickup`, `trip_distance > 0`, `fare_amount > 0`, `total_amount > 0`, location ids non-null. Rows that violate any of these are dropped.
- Gold pre-aggregates by `(hour, pickup_zone, dropoff_zone, …)` so dashboards don't re-scan silver. Rebuilt via `INSERT OVERWRITE` so the table is always a deterministic function of silver.

### 2.4 Iceberg snapshot history (Silver CDC)

```sql
SELECT snapshot_id, committed_at, operation
FROM lakehouse.cdc.silver_customers.snapshots
ORDER BY committed_at;
```

```
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
| 640455864885529593|2026-04-26 20:16:23.459|append   |  ← first MERGE (created table + inserted)
| 967098209013265569|2026-04-26 20:16:25.470|overwrite|  ← idempotent re-run
|2541636599065452148|2026-04-26 20:16:31.210|overwrite|  ← MERGE after live INSERT (Silver Test, id=12)
|6406087165402811239|2026-04-26 20:16:35.914|overwrite|  ← MERGE after DELETE id=12
+-------------------+-----------------------+---------+
```

### 2.5 Time travel - query Silver before a specific MERGE

```sql
SELECT id, name, email FROM lakehouse.cdc.silver_customers
VERSION AS OF 640455864885529593
ORDER BY id LIMIT 5;
```

returns the silver state at the first snapshot. To **roll back a bad MERGE in production**, run:

```sql
CALL lakehouse.system.rollback_to_snapshot('cdc.silver_customers', <good_snapshot_id>);
```

This restores silver to the named snapshot's state in one ACID step.

---

## 3. Orchestration design

DAG file: [`dags/lakehouse_dag.py`](dags/lakehouse_dag.py). Seven tasks: `connector_health`, `bronze_cdc`, `bronze_taxi`, `silver_cdc`, `silver_taxi`, `gold_taxi`, `validate`.

### 3.1 Task dependency chain

```
connector_health
   │
   ├──► bronze_cdc ──► silver_cdc ───────────┐
   │                                          ▼
   └──► bronze_taxi ──► silver_taxi ──► gold_taxi
                                              │
                                              ▼
                                          validate
```

- `connector_health` is an `HttpSensor` polling `/connectors/pg-cdc-connector/status` until `state == "RUNNING"`. **All downstream tasks are gated on it** - if the sensor times out the rest of the DAG never runs (default `trigger_rule='all_success'`).
- Bronze CDC and bronze taxi are independent - could run in parallel under a real executor (we use `SequentialExecutor` for simplicity, so they serialise).
- Silver CDC depends only on bronze CDC, silver taxi depends only on bronze taxi. Gold taxi depends on silver taxi. Validate depends on both silver branches converging.

### 3.2 Scheduling strategy + freshness SLA

`schedule_interval="@hourly"`, `catchup=False`, `max_active_runs=1`.

- **Why `@hourly`**: customer/driver mutation rate is low. 1-hour freshness is plenty for the operational dashboards downstream of silver and gold. Iceberg compaction stays manageable with one MERGE per hour vs many per minute.
- **Freshness SLA**: a row written to Postgres at minute *m* of hour *H* is visible in silver by the *next* run start (≤ 60 min). Inside that, our actual end-to-end latency for one run is ~75 s (see the per-task durations table below).
- **`catchup=False`**: we don't backfill missed slots automatically. If a logical date needs to be re-processed, the operator triggers it manually with the date as the run id - and because every layer is idempotent, re-running is safe (same output).
- **`max_active_runs=1`**: prevents two runs from racing on the same bronze offset state.

### 3.3 Retries, SLA, failure handling

`default_args` from [`dags/lakehouse_dag.py`](dags/lakehouse_dag.py):

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
    "on_failure_callback": on_failure,  # logs FAILED dag/task/run for paging
}
```

- 3 retries with exponential backoff (1, 2, 4, 8, 10, 10 min) gives ~25 min of recovery headroom inside the 30-min SLA.
- `on_failure_callback` logs `FAILED - dag=… task=… run=…` for any failed task - easy to grep in Airflow logs and the hook for an alerting integration.

### 3.4 Three consecutive successful runs

```
+--------------------------------------+----------+-----------------------+
|run_id                                |state     |start_date             |
+--------------------------------------+----------+-----------------------+
|notebook__fault_1777234799            |success   |2026-04-26T20:19:59    |  ← fault-injection run (see fault-injection block below)
|notebook__1777234606                  |success   |2026-04-26T20:18:26    |
|scheduled__2026-04-26T19:00:00+00:00  |success   |2026-04-26T20:16:47    |  ← auto-fired by @hourly schedule on un-pause
+--------------------------------------+----------+-----------------------+
```

Per-task durations (the manual `notebook__1777234606` run, ~70 s end-to-end on `SequentialExecutor`):

| task | duration | result |
|---|---|---|
| `connector_health`  | 0.08 s   | ✓ (connector already RUNNING - instant pass) |
| `bronze_taxi`       | 11.7 s   | ✓ |
| `bronze_cdc`        | 13.8 s   | ✓ |
| `silver_taxi`       | 10.7 s   | ✓ |
| `silver_cdc`        | 12.9 s   | ✓ |
| `gold_taxi`         | 12.7 s   | ✓ |
| `validate`          | 8.8 s    | ✓ silver counts match Postgres within tolerance |

### 3.5 Failed task + recovery example

Test: delete the Debezium connector mid-run, then restart it.

```bash
curl -X DELETE http://localhost:8083/connectors/pg-cdc-connector
# Trigger DAG run
# … sensor pokes every 20s, gets HTTP 404 each time, stays in `running` ...
# All downstream tasks blocked at `None` (not even queued).
docker exec jupyter python /home/jovyan/project/jobs/register_connector.py
# Sensor's next poke succeeds → DAG cascades downstream
```

Snapshot 30 s after triggering, while the connector was deleted:

```
  bronze_cdc           None
  bronze_taxi          None
  connector_health     running
  gold_taxi            None
  silver_cdc           None
  silver_taxi          None
  validate             None
```

After re-registering the connector, the sensor's next 20-second poke succeeds and the DAG cascades. Final state = `success`, with `connector_health` taking **40 s** (2 unsuccessful 20-s pokes before the 3rd succeeded post-recovery). Other task durations are identical to a normal run.

This is exactly the desired behaviour: a downstream system outage **delays** the pipeline (sensor blocks) but never produces inconsistent output. As soon as the dependency recovers, the pipeline picks up automatically.

### 3.6 Backfill / idempotent re-runs

Because every layer is idempotent (bronze tracks `MAX(kafka_offset) + 1`, silver MERGE is deterministic, gold uses `INSERT OVERWRITE`), re-running the DAG for the same logical date produces the same Iceberg state. To backfill a missed range, an operator triggers `airflow dags backfill -s <start> -e <end> lakehouse` - each run is independent and safe.

---

## 4. Streaming pipeline (taxi)

`bronze_taxi → silver_taxi → gold_taxi`. Same medallion shape as Project 2, ported from Structured Streaming to Airflow-triggered batch.

### 4.1 Bronze taxi

[`jobs/bronze_taxi.py`](jobs/bronze_taxi.py). Spark batch read from `taxi-trips` Kafka topic. Same offset-tracking pattern as bronze CDC (`MAX(offset) + 1` start), append-only into Iceberg. Kafka metadata preserved.

```
[taxi] reading from offset 0  (topic=taxi-trips)
[taxi] new events to ingest: 50
[taxi] bronze total rows: 50
```

Idempotent re-run: `+0` new events.

Sample bronze rows:

```
+-------------+------+-----------------------+--------------------------------------------------------------------------------+
|vendor_id_key|offset|kafka_timestamp        |json_head                                                                       |
+-------------+------+-----------------------+--------------------------------------------------------------------------------+
|1            |0     |2026-04-26 20:16:39.046|{"VendorID": 1, "tpep_pickup_datetime": "2025-01-01T00:18:38", "tpep_dropoff_dat|
|1            |1     |2026-04-26 20:16:39.068|{"VendorID": 1, "tpep_pickup_datetime": "2025-01-01T00:32:40", "tpep_dropoff_dat|
|1            |2     |2026-04-26 20:16:39.089|{"VendorID": 1, "tpep_pickup_datetime": "2025-01-01T00:44:04", "tpep_dropoff_dat|
+-------------+------+-----------------------+--------------------------------------------------------------------------------+
```

### 4.2 Silver taxi

[`jobs/silver_taxi.py`](jobs/silver_taxi.py). Reads bronze rows where `offset > MAX(silver.bronze_kafka_offset)`, parses `json_payload` against an explicit `StructType` schema, applies the cleaning filters from Project 2's `implement_silver`, broadcast-joins `taxi_zone_lookup.parquet` for `pickup_zone`/`pickup_borough`/`dropoff_zone`/`dropoff_borough`, writes append into `lakehouse.taxi.silver` partitioned by `pickup_date`.

```
[taxi-silver] reading bronze rows with kafka offset > -1
[taxi-silver] new bronze rows: 50
[taxi-silver] inserted=45, rejected=5, silver total = 45
```

The 5 rejected rows hit cleaning rules (`trip_distance > 0`, `fare_amount > 0`, `total_amount > 0`, location ids non-null, `dropoff > pickup`). Idempotent re-run: 0 new rows.

Sample silver rows (parsed + zone-enriched):

```
+--------+-------------------+-------------+-----------+---------------------+--------------+
|VendorID|pickup_datetime    |trip_distance|fare_amount|pickup_zone          |dropoff_zone  |
+--------+-------------------+-------------+-----------+---------------------+--------------+
|2       |2025-01-01 00:00:02|1.71         |11.4       |Upper East Side South|Yorkville East|
|2       |2025-01-01 00:00:37|0.86         |6.5        |Lenox Hill East      |Yorkville West|
|2       |2025-01-01 00:01:41|0.71         |7.2        |East Village         |Gramercy      |
+--------+-------------------+-------------+-----------+---------------------+--------------+
```

### 4.3 Gold taxi

[`jobs/gold_taxi.py`](jobs/gold_taxi.py). Aggregates silver into hourly buckets per `(pickup_zone, pickup_borough, dropoff_zone, dropoff_borough)`. Metrics: `trip_count, total_revenue, avg_fare, avg_distance_miles, avg_duration_min, total_passengers, revenue_per_trip`. Written via `writeTo(...).overwritePartitions()` - that's the deliberate improvement over Project 2 (explained at the end of the section).

```
[taxi-gold] silver rows: 45
[taxi-gold] gold aggregated rows: 44
```

Re-run produces the same 44 rows.

Sample gold rows (top by trip_count):

```
+-------------------+-----------------------+-----------------------+----------+-------------+--------+
|hour               |pickup_zone            |dropoff_zone           |trip_count|total_revenue|avg_fare|
+-------------------+-----------------------+-----------------------+----------+-------------+--------+
|2025-01-01 00:00:00|Upper East Side South  |Lenox Hill East        |2         |37.68        |10.7    |
|2025-01-01 00:00:00|Greenwich Village South|East Village           |1         |19.6         |10.7    |
|2025-01-01 00:00:00|Lenox Hill West        |Greenwich Village North|1         |38.9         |26.1    |
|2025-01-01 00:00:00|Greenwich Village South|Midtown Center         |1         |50.76        |37.3    |
|2025-01-01 00:00:00|Yorkville West         |Gramercy               |1         |27.24        |17.7    |
+-------------------+-----------------------+-----------------------+----------+-------------+--------+
```

### 4.4 Improvement over Project 2

Project 2 used Structured Streaming `outputMode("complete")` for the gold layer. That mode rebuilds the entire aggregation in memory each micro-batch and writes it. It has two problems for orchestration:
1. State recovery on restart is fragile (checkpoint state can mismatch source state).
2. Not directly callable from a DAG task - it's a long-running stream.

Project 3's `gold_taxi.py` is a batch job that:
1. `INSERT OVERWRITE` on the partitioned target - Iceberg dynamically replaces only the affected `pickup_zone` partitions (no append duplication).
2. Re-runnable for the same logical date with identical output - perfect fit for Airflow's idempotency model.
3. Composes into the DAG as a single task that finishes in seconds.

That's the deliberate improvement over Project 2.

---

## 5. Custom scenario

TODO

---

## 6. Reproducing the evidence above

Full sequence from a clean repo to a fully-orchestrated, validated lakehouse:

```bash
# 1. Stack
docker compose up -d
until curl -sf -u admin:admin http://localhost:8094/api/v1/dags >/dev/null 2>&1; do sleep 3; done

# 2. Sources
docker exec jupyter python /home/jovyan/project/seed.py                               # Postgres
docker exec jupyter python /home/jovyan/project/jobs/register_connector.py            # Debezium
docker exec jupyter python /home/jovyan/project/produce.py --rate 50 --limit 50       # Taxi events

# 3. Register the Airflow Connection that HttpSensor uses
curl -s -u admin:admin -H 'Content-Type: application/json' -X POST \
  http://localhost:8094/api/v1/connections \
  -d '{"connection_id":"debezium_connect","conn_type":"http","host":"connect","port":8083,"schema":"http"}'

# 4. Unpause + trigger the DAG
curl -s -u admin:admin -X PATCH -H 'Content-Type: application/json' \
  http://localhost:8094/api/v1/dags/lakehouse -d '{"is_paused": false}'
curl -s -u admin:admin -X POST -H 'Content-Type: application/json' \
  http://localhost:8094/api/v1/dags/lakehouse/dagRuns -d '{"dag_run_id":"manual__test"}'
```

Or run the layer-by-layer scripts directly (no DAG):

```bash
SPARK_PKGS="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0"
for j in bronze_cdc bronze_taxi silver_cdc silver_taxi gold_taxi validate; do
  docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" "$j.py"
done
```

Or open [`walkthrough.ipynb`](walkthrough.ipynb) in Jupyter (`http://localhost:8888`, token from `.env`) and run cells top-to-bottom.