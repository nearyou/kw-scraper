"""Dependency probes and scraper status reporting for the health endpoint."""

import logging
import time
from datetime import datetime, timedelta, timezone

from kombu import Connection
from sqlalchemy import func, text


logger = logging.getLogger(__name__)


def _utc_datetime(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _public_error(error):
    """Return useful diagnostics without exposing connection credentials."""
    return {"error_type": type(error).__name__}


def probe_queue(broker_url, *, timeout=3, connection_factory=Connection):
    """Verify that an authenticated connection to the queue can be opened."""
    started_at = time.monotonic()
    connection = None
    try:
        connection = connection_factory(broker_url, connect_timeout=timeout)
        connection.ensure_connection(max_retries=0)
        return {
            "status": "accessible",
            "latency_ms": round((time.monotonic() - started_at) * 1000, 2),
        }
    except Exception as error:
        logger.warning(
            "health_queue_probe_failed",
            extra={"context": _public_error(error)},
        )
        return {"status": "unavailable", **_public_error(error)}
    finally:
        if connection is not None:
            try:
                connection.release()
            except Exception:
                pass


def probe_storage(session):
    """Run a minimal query and release the health-check transaction."""
    started_at = time.monotonic()
    try:
        session.execute(text("SELECT 1"))
        result = {
            "status": "working",
            "latency_ms": round((time.monotonic() - started_at) * 1000, 2),
        }
    except Exception as error:
        logger.warning(
            "health_storage_probe_failed",
            extra={"context": _public_error(error)},
        )
        result = {"status": "unavailable", **_public_error(error)}
    try:
        session.rollback()
    except Exception as error:
        logger.warning(
            "health_storage_rollback_failed",
            extra={"context": _public_error(error)},
        )
        result = {"status": "unavailable", **_public_error(error)}
    return result


def load_scrape_metrics(session, sources, *, now=None, window_seconds=3600):
    """Aggregate completed and failed scraper runs inside a rolling window.

    A source is ``(model, timestamp column, success statuses, failure statuses)``.
    This supports both the regular queue and the idle scraper without a new table.
    """
    now = _utc_datetime(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(seconds=window_seconds)
    # Existing models use timezone-naive UTC DateTime columns.
    database_cutoff = cutoff.replace(tzinfo=None)
    successful_runs = 0
    failed_runs = 0
    last_success = None

    for model, timestamp_column, success_statuses, failure_statuses in sources:
        recent_successes = (
            session.query(func.count(model.id))
            .filter(
                model.status.in_(success_statuses),
                timestamp_column >= database_cutoff,
            )
            .scalar()
            or 0
        )
        recent_failures = (
            session.query(func.count(model.id))
            .filter(
                model.status.in_(failure_statuses),
                timestamp_column >= database_cutoff,
            )
            .scalar()
            or 0
        )
        source_last_success = (
            session.query(func.max(timestamp_column))
            .filter(model.status.in_(success_statuses))
            .scalar()
        )
        successful_runs += recent_successes
        failed_runs += recent_failures
        source_last_success = _utc_datetime(source_last_success)
        if source_last_success and (
            last_success is None or source_last_success > last_success
        ):
            last_success = source_last_success

    attempts = successful_runs + failed_runs
    return {
        "successful_runs": successful_runs,
        "failed_runs": failed_runs,
        "observed_runs": attempts,
        "error_rate": round(failed_runs / attempts, 4) if attempts else 0.0,
        "last_successful_scrape": last_success,
        "window_seconds": window_seconds,
    }


def evaluate_scraper(
    metrics, *, now=None, max_age_seconds=86400, max_error_rate=0.25
):
    """Turn raw metrics into a public scraper check and list of problems."""
    now = _utc_datetime(now or datetime.now(timezone.utc))
    last_success = _utc_datetime(metrics.get("last_successful_scrape"))
    age_seconds = None
    problems = []

    if last_success is not None:
        age_seconds = max(0, round((now - last_success).total_seconds(), 2))
        if max_age_seconds > 0 and age_seconds > max_age_seconds:
            problems.append("last_success_is_stale")

    if metrics["observed_runs"] and metrics["error_rate"] > max_error_rate:
        problems.append("error_rate_too_high")

    if problems:
        status = "unhealthy"
    elif last_success is None and metrics["observed_runs"] == 0:
        status = "no_data"
    else:
        status = "healthy"

    return {
        "status": status,
        "last_successful_scrape_at": last_success.isoformat() if last_success else None,
        "seconds_since_last_success": age_seconds,
        "error_rate": metrics["error_rate"],
        "error_rate_threshold": max_error_rate,
        "successful_runs": metrics["successful_runs"],
        "failed_runs": metrics["failed_runs"],
        "observed_runs": metrics["observed_runs"],
        "observation_window_seconds": metrics["window_seconds"],
        "max_success_age_seconds": max_age_seconds,
        "problems": problems,
    }


def build_health_report(queue_check, storage_check, scraper_check, *, now=None):
    """Build the response body and HTTP status from all component checks."""
    unhealthy = (
        queue_check["status"] != "accessible"
        or storage_check["status"] != "working"
        or scraper_check["status"] == "unhealthy"
    )
    report = {
        "status": "unhealthy" if unhealthy else "healthy",
        "timestamp": _utc_datetime(now or datetime.now(timezone.utc)).isoformat(),
        "checks": {
            "queue": queue_check,
            "storage": storage_check,
            "scraper": scraper_check,
        },
    }
    return report, 503 if unhealthy else 200
