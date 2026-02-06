import logging
import time
import psutil
import sqlite3
from datetime import datetime
from random import randint
import os
import socket
import atexit

import boto3
import watchtower

from dotenv import load_dotenv

load_dotenv()

# -------------------------
# Logging Configuration
# -------------------------
LOG_GROUP = os.getenv("CW_LOG_GROUP", "fantastic-formula")
LOG_STREAM = os.getenv("CW_LOG_STREAM", socket.gethostname())

region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
session = boto3.Session(region_name=region)

logging.raiseExceptions = True

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers.clear()

cw = watchtower.CloudWatchLogHandler(
    boto3_client=session.client("logs", region_name=region),
    log_group=LOG_GROUP,
    stream_name=LOG_STREAM,
    create_log_group=True,
    create_log_stream=True,
    send_interval=1,
    max_batch_count=50,
)
cw.setLevel(logging.INFO)

fmt = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
cw.setFormatter(fmt)

stdout = logging.StreamHandler()
stdout.setFormatter(fmt)

root_logger.addHandler(cw)
root_logger.addHandler(stdout)

atexit.register(cw.close)

logger = root_logger


# -------------------------
# System Metrics
# -------------------------
def get_system_metrics():
    return {
        "cpu_percent": psutil.cpu_percent(interval=0.5),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "boot_time": datetime.fromtimestamp(psutil.boot_time()).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


# -------------------------
# Database Connection Example
# -------------------------
def get_db_connection():
    try:
        conn = sqlite3.connect("example.db")
        logger.info("Database connection established.")
        return conn
    except Exception:
        logger.exception("Database connection failed")
        return None


# -------------------------
# Simulated API Call
# -------------------------
def api_call_simulation(endpoint: str):
    logger.info(f"API START - Endpoint: {endpoint}")

    latency = randint(50, 500) / 1000
    time.sleep(latency)

    if randint(1, 10) > 8:
        logger.error(
            f"API FAILURE - Endpoint: {endpoint} - Latency: {latency*1000:.2f}ms"
        )
        return False, latency

    logger.info(f"API END - Endpoint: {endpoint} - Latency: {latency*1000:.2f}ms")
    return True, latency


# -------------------------
# Logging Metrics with API/DB info
# -------------------------
def log_metrics_with_context(endpoint: str, db_connections: int):
    metrics = get_system_metrics()
    logger.info(
        f"System Metrics | CPU: {metrics['cpu_percent']}%, "
        f"Memory: {metrics['memory_percent']}%, Disk: {metrics['disk_percent']}%, "
        f"DB Connections: {db_connections}"
    )


# -------------------------
# Main Monitoring Loop
# -------------------------
def monitor_app(interval=5):
    logger.info("=== Starting Application Monitoring ===")
    db_conn = get_db_connection()
    db_connections = 1 if db_conn else 0

    try:
        while True:
            endpoints = ["/login", "/get-data", "/update", "/delete"]
            for endpoint in endpoints:
                api_call_simulation(endpoint)

            log_metrics_with_context(endpoint="/all", db_connections=db_connections)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("=== Stopped Application Monitoring ===")
        print("Monitoring stopped by user.")
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")
        cw.close()


# -------------------------
# Run Monitor
# -------------------------
if __name__ == "__main__":
    monitor_app(interval=10)
