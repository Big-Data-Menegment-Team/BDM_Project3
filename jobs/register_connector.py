"""
Register (or update) the Debezium PostgreSQL connector via the Kafka Connect REST API.

Idempotent: uses PUT /connectors/<name>/config so re-running updates the config in place.

Usage:
    docker exec jupyter python /home/jovyan/project/jobs/register_connector.py
"""

import json
import os
import sys
import time


def _ensure(pkg, import_name=None):
    import importlib.util, subprocess
    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("requests")

import requests


CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
CONNECTOR_NAME = "pg-cdc-connector"

CONNECTOR_CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": os.environ.get("PG_HOST", "postgres"),
    "database.port": os.environ.get("PG_PORT", "5432"),
    "database.user": os.environ.get("PG_USER", "cdc_user"),
    "database.password": os.environ.get("PG_PASSWORD", "cdc_pass"),
    "database.dbname": os.environ.get("PG_DB", "sourcedb"),
    "topic.prefix": "dbserver1",
    "table.include.list": "public.customers,public.drivers",
    "plugin.name": "pgoutput",
    "snapshot.mode": "initial",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
}


def wait_for_connect(retries=30, delay=3):
    """Poll Kafka Connect until the root endpoint responds"""
    for i in range(retries):
        try:
            r = requests.get(f"{CONNECT_URL}/")
            if r.status_code == 200:
                print(f"Kafka Connect is ready: {r.json()}")
                return
        except Exception:
            pass
        print(f"Waiting for Kafka Connect... ({i + 1})")
        time.sleep(delay)
    raise RuntimeError("Kafka Connect did not start in time!")


def register():
    wait_for_connect()

    # Idempotent upsert via PUT
    r = requests.put(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(CONNECTOR_CONFIG),
    )
    print(f"PUT /connectors/{CONNECTOR_NAME}/config -> HTTP {r.status_code}")
    if r.status_code not in (200, 201):
        print(r.text)
        sys.exit(1)

    # Poll status until RUNNING
    for i in range(20):
        time.sleep(2)
        s = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
        if s.status_code != 200:
            continue
        info = s.json()
        conn_state = info["connector"]["state"]
        task_states = [t["state"] for t in info.get("tasks", [])]
        print(f"  connector={conn_state}  tasks={task_states}")
        if conn_state == "FAILED" or "FAILED" in task_states:
            print(json.dumps(info, indent=2))
            sys.exit("Connector or task FAILED")
        if conn_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
            print("Connector is RUNNING")
            return
    sys.exit("Connector did not reach RUNNING within timeout")


if __name__ == "__main__":
    register()
