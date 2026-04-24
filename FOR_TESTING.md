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