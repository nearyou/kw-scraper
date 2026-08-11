"""Wait for PostgreSQL and RabbitMQ before starting a container service."""

import os
import sys
import time

import psycopg2
from kombu import Connection


def check_database(database_url):
    """Open a real authenticated database connection and immediately close it."""
    psycopg_url = database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    connection = psycopg2.connect(psycopg_url, connect_timeout=5)
    connection.close()


def check_broker(broker_url):
    """Open a real authenticated broker connection and immediately close it."""
    connection = Connection(broker_url, connect_timeout=5)
    try:
        connection.ensure_connection(max_retries=0)
    finally:
        connection.release()


def wait_for_dependencies():
    checks = []
    database_url = os.getenv("DATABASE_URL")
    broker_url = os.getenv("CELERY_BROKER_URL")
    if database_url:
        checks.append(("storage (PostgreSQL)", lambda: check_database(database_url)))
    if broker_url:
        checks.append(("queue (RabbitMQ)", lambda: check_broker(broker_url)))

    max_attempts = max(1, int(os.getenv("DEPENDENCY_MAX_ATTEMPTS", "30")))
    max_delay = max(1, int(os.getenv("DEPENDENCY_RETRY_MAX_DELAY", "10")))

    for attempt in range(1, max_attempts + 1):
        failures = []
        for name, check in checks:
            try:
                check()
            except Exception as error:
                failures.append(f"{name}: {error}")

        if not failures:
            print("Startup dependencies are ready", flush=True)
            return True

        print(
            f"Dependencies unavailable (attempt {attempt}/{max_attempts}): "
            + "; ".join(failures),
            flush=True,
        )
        if attempt < max_attempts:
            # Short capped backoff avoids both startup storms and long shutdown waits.
            time.sleep(min(2 ** (attempt - 1), max_delay))

    return False


if __name__ == "__main__":
    sys.exit(0 if wait_for_dependencies() else 1)
