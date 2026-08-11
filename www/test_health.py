import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from www.health import (
    build_health_report,
    evaluate_scraper,
    load_scrape_metrics,
    probe_queue,
    probe_storage,
)


Base = declarative_base()


class Run(Base):
    __tablename__ = "health_test_runs"
    id = Column(Integer, primary_key=True)
    status = Column(String)
    finished_at = Column(DateTime)


class FakeQueueConnection:
    released = False

    def __init__(self, _url, connect_timeout):
        self.timeout = connect_timeout

    def ensure_connection(self, max_retries):
        if max_retries != 0:
            raise AssertionError("health probes must not retry")

    def release(self):
        self.released = True


class HealthCheckTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_dependency_probes_report_accessible_services(self):
        queue = probe_queue(
            "amqp://example", connection_factory=FakeQueueConnection
        )
        storage = probe_storage(self.session)

        self.assertEqual(queue["status"], "accessible")
        self.assertEqual(storage["status"], "working")

    def test_metrics_report_last_success_and_rolling_error_rate(self):
        self.session.add_all(
            [
                Run(status="completed", finished_at=self.now - timedelta(minutes=5)),
                Run(status="failed", finished_at=self.now - timedelta(minutes=10)),
                Run(status="failed", finished_at=self.now - timedelta(hours=2)),
            ]
        )
        self.session.commit()

        metrics = load_scrape_metrics(
            self.session,
            [(Run, Run.finished_at, ("completed",), ("failed",))],
            now=self.now,
            window_seconds=3600,
        )

        self.assertEqual(metrics["successful_runs"], 1)
        self.assertEqual(metrics["failed_runs"], 1)
        self.assertEqual(metrics["error_rate"], 0.5)
        self.assertEqual(
            metrics["last_successful_scrape"], self.now - timedelta(minutes=5)
        )

    def test_high_error_rate_returns_503(self):
        metrics = {
            "successful_runs": 1,
            "failed_runs": 2,
            "observed_runs": 3,
            "error_rate": 0.6667,
            "last_successful_scrape": self.now - timedelta(minutes=5),
            "window_seconds": 3600,
        }
        scraper = evaluate_scraper(metrics, now=self.now, max_error_rate=0.25)
        report, status_code = build_health_report(
            {"status": "accessible"},
            {"status": "working"},
            scraper,
            now=self.now,
        )

        self.assertEqual(status_code, 503)
        self.assertEqual(report["status"], "unhealthy")
        self.assertIn("error_rate_too_high", scraper["problems"])

    def test_unavailable_queue_returns_503(self):
        scraper = evaluate_scraper(
            {
                "successful_runs": 0,
                "failed_runs": 0,
                "observed_runs": 0,
                "error_rate": 0.0,
                "last_successful_scrape": None,
                "window_seconds": 3600,
            },
            now=self.now,
        )
        report, status_code = build_health_report(
            {"status": "unavailable", "error_type": "OSError"},
            {"status": "working"},
            scraper,
            now=self.now,
        )

        self.assertEqual(status_code, 503)
        self.assertEqual(report["checks"]["queue"]["status"], "unavailable")

    def test_no_scrape_history_is_reported_without_failing_dependencies(self):
        metrics = {
            "successful_runs": 0,
            "failed_runs": 0,
            "observed_runs": 0,
            "error_rate": 0.0,
            "last_successful_scrape": None,
            "window_seconds": 3600,
        }
        scraper = evaluate_scraper(metrics, now=self.now)
        _, status_code = build_health_report(
            {"status": "accessible"}, {"status": "working"}, scraper, now=self.now
        )

        self.assertEqual(scraper["status"], "no_data")
        self.assertEqual(status_code, 200)


if __name__ == "__main__":
    unittest.main()
