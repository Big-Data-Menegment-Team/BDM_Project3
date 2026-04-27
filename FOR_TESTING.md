# FOR_TESTING
## 0. Stack lifecycle

```bash
# Fresh start (wipes all data)
docker compose down -v
docker compose up -d

# Restart preserving services state
docker compose down
docker compose up -d
```

---

## 1. Service health - verify all 7 containers

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E 'kafka|postgres|connect|minio|iceberg|airflow|jupyter'
```

Expected: all rows `(healthy)` or `Up` (iceberg-rest has no healthcheck. airflow may say `health: starting` for ~60s on first boot).

### Endpoint tests

```bash
curl -sf http://localhost:8083/ | head -c 120              # Kafka Connect
curl -sf http://localhost:8181/v1/config | head -c 120     # Iceberg REST
curl -sf http://localhost:9000/minio/health/live -o /dev/null -w 'MinIO HTTP %{http_code}\n'
docker exec postgres pg_isready -U cdc_user -d sourcedb    # PostgreSQL
curl -sf http://localhost:8094/health | head -c 200        # Airflow
```

---

## 2. Postgres source - seed + inspect

```bash
# Seed customers (10 rows) + drivers (8 rows)
docker exec jupyter python /home/jovyan/project/seed.py

# query directly
docker exec postgres psql -U cdc_user -d sourcedb -c 'SELECT COUNT(*) FROM customers; SELECT COUNT(*) FROM drivers;'
docker exec postgres psql -U cdc_user -d sourcedb -c 'SELECT id,name,country FROM customers ORDER BY id;'
```

---

## 3. Debezium connector (Path A - CDC)

```bash
# Register / update the connector
docker exec jupyter python /home/jovyan/project/jobs/register_connector.py

# Status check
curl -s http://localhost:8083/connectors/pg-cdc-connector/status | python3 -m json.tool
```

Expected: `connector.state = RUNNING`, every task `state = RUNNING`.

### Verify CDC topics + event counts

```bash
# List CDC topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep dbserver1

# Event counts per topic (format: topic:partition:offset)
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.drivers
```

Expected after a fresh seed: `customers = 10`, `drivers = 8`.

### Inspect the first CDC event (the Debezium envelope)

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.customers \
  --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null \
  | python3 -c "import json,sys; e=json.loads(sys.stdin.read()); print(json.dumps(e['payload'], indent=2))"
```

Expected: `op: "r"`, `after.id: 1`, `after.name: "Alice Mets"`, `source.lsn: <int>`, `ts_ms: <int>`.

### Force one live change and watch it land in Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
docker exec postgres psql -U cdc_user -d sourcedb -c "UPDATE customers SET email='test@example.com' WHERE id=1;"
sleep 2
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
```

Expected: second offset = first offset + 1.

### Smoke test - simulator burst flows to Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
docker exec jupyter python /home/jovyan/project/simulate.py --rate 5 --limit 5
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
```

Expected: second offset > first. (A DELETE emits 2 events - the `d` envelope so 5 random ops will grow the offsets by 5-10 dpending on the operations)

---

## 4. Full from-scratch reproducibility check

all in one block that nukes state and walks the pipeline through the first "source is ready" milestone.

```bash
docker compose down -v
docker compose up -d

# wait for airflow
until curl -sf -u admin:admin http://localhost:8094/api/v1/dags >/dev/null 2>&1; do sleep 3; done
echo "Airflow up"

# seed + register
docker exec jupyter python /home/jovyan/project/seed.py | tail -3
docker exec jupyter python /home/jovyan/project/jobs/register_connector.py | tail -3

# verify CDC
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dbserver1.public.drivers
```

Expected final output:

```
dbserver1.public.customers:0:10
dbserver1.public.drivers:0:8
```

---

## 5. Custom scenario - Customer Geography (issue #1)

The custom scenario lives in [`jobs/gold_geography.py`](jobs/gold_geography.py) and is orchestrated by a separate DAG [`dags/geography_dag.py`](dags/geography_dag.py). It reads `lakehouse.cdc.silver_customers` + `bronze_customers` and writes two new gold tables under `lakehouse.cdc`.

### 5.1 Smoke test the spark job directly (no Airflow)

```bash
SPARK_PKGS="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0"

# First run - will create both tables
docker exec -w /home/jovyan/project/jobs jupyter \
  spark-submit --packages "$SPARK_PKGS" gold_geography.py --run-id smoke_1
```

Expected tail of the output:

```
[geography] gold_customer_geography rebuilt: <N> rows for run_id=smoke_1
[geography] trend appended <N> rows for run_id=smoke_1
[geography] trend total rows: <N>
```

### 5.2 Generate >=3 snapshots with movement

```bash
# Run 1
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" gold_geography.py --run-id snap_1

# Mutate Postgres so silver shifts on the next geography run
docker exec jupyter python /home/jovyan/project/simulate.py --rate 5 --limit 5
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" /home/jovyan/project/jobs/bronze_cdc.py
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" /home/jovyan/project/jobs/silver_cdc.py

# Run 2
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" gold_geography.py --run-id snap_2

# Repeat the simulate + bronze + silver block...

# Run 3
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" gold_geography.py --run-id snap_3
```

### 5.3 Verify the two tables in the Jupyter notebook (or psql/Spark SQL)

```python
spark.sql("""
    SELECT country, active_customers, added_24h, deleted_24h, net_change_24h,
           ROUND(pct_of_total, 1) AS pct
    FROM lakehouse.cdc.gold_customer_geography
    ORDER BY active_customers DESC
""").show(truncate=False)

# Most customers
spark.sql("""
    SELECT country, active_customers FROM lakehouse.cdc.gold_customer_geography
    ORDER BY active_customers DESC LIMIT 1
""").show()

# Lost the most in the last hour
spark.sql("""
    SELECT country, prev_active, active_customers, delta,
           ROUND(pct_change, 1) AS pct_change, dropped_20pct
    FROM lakehouse.cdc.gold_geography_trend
    WHERE snapshot_ts > current_timestamp() - INTERVAL 1 HOUR
    ORDER BY delta ASC LIMIT 1
""").show()

# All trend snapshots
spark.sql("""
    SELECT run_id, snapshot_ts, country, active_customers, prev_active, delta,
           ROUND(pct_change, 1) AS pct_change, dropped_20pct
    FROM lakehouse.cdc.gold_geography_trend
    ORDER BY snapshot_ts, country
""").show(truncate=False)
```

### 5.4 Idempotency check

```bash
# Re-run the same run_id - geography is overwritten with identical rows,
# trend skips the append (logs "skipping").
docker exec -w /home/jovyan/project/jobs jupyter spark-submit --packages "$SPARK_PKGS" gold_geography.py --run-id snap_3
```

Expected: `[geography] trend already has run_id=snap_3 (...), skipping append`. Trend row count is unchanged.

### 5.5 Run via the Airflow DAG

The geography DAG reuses the same `debezium_connect` Airflow connection that the main `lakehouse` DAG uses, so no extra setup is needed if the main DAG is already wired up. Trigger it the same way:

```bash
curl -s -u admin:admin -X PATCH -H 'Content-Type: application/json' \
  http://localhost:8094/api/v1/dags/customer_geography -d '{"is_paused": false}'
curl -s -u admin:admin -X POST -H 'Content-Type: application/json' \
  http://localhost:8094/api/v1/dags/customer_geography/dagRuns -d '{"dag_run_id":"manual__geo_1"}'
```

The DAG has two tasks: `connector_health` (HttpSensor) -> `gold_geography` (BashOperator that runs the spark job with `--run-id '{{ run_id }}'`).

---

## Open service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8094 | admin / admin |
| Jupyter | http://localhost:8888 | token `changeme` (from `.env`) |
| Spark UI (when a SparkSession is live) | http://localhost:4040 | - |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Kafka Connect REST | http://localhost:8083 | - |
| Iceberg REST | http://localhost:8181/v1/namespaces | - |
| Postgres | `localhost:5444`, db `sourcedb` | `cdc_user` / `cdc_pass` |