"""Tests for the container's graceful-shutdown and restart contract."""

import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


def compose_service_block(compose_text, service_name):
    """Return one top-level Compose service block without a YAML dependency."""
    match = re.search(
        rf"(?ms)^  {re.escape(service_name)}:\s*$\n(.*?)(?=^  [a-zA-Z0-9_-]+:\s*$|^volumes:\s*$)",
        compose_text,
    )
    if not match:
        raise AssertionError(f"Compose service {service_name!r} was not found")
    return match.group(1)


class ContainerLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        cls.entrypoint = (PROJECT_ROOT / "docker-entrypoint.sh").read_text(
            encoding="utf-8"
        )
        cls.compose = (PROJECT_ROOT / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

    def test_tini_forwards_sigterm_to_the_application(self):
        self.assertIn("tini", self.dockerfile)
        self.assertIn("STOPSIGNAL SIGTERM", self.dockerfile)
        self.assertIn(
            'ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker-entrypoint.sh"]',
            self.dockerfile,
        )

    def test_entrypoint_replaces_shell_with_service_process(self):
        executable_lines = [
            line.strip()
            for line in self.entrypoint.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        self.assertEqual(executable_lines[-1], 'exec "$@"')

    def test_services_restart_and_allow_time_for_clean_shutdown(self):
        expectations = {
            "rabbitmq": ("unless-stopped", "30s"),
            "celery": ("unless-stopped", "60s"),
            "web": ("unless-stopped", "45s"),
            "scraper": ('"on-failure:5"', "60s"),
        }

        for service, (restart_policy, grace_period) in expectations.items():
            with self.subTest(service=service):
                block = compose_service_block(self.compose, service)
                self.assertIn(f"restart: {restart_policy}", block)
                self.assertIn(f"stop_grace_period: {grace_period}", block)

    def test_web_shutdown_window_exceeds_gunicorn_graceful_timeout(self):
        web = compose_service_block(self.compose, "web")
        stop_seconds = int(re.search(r"stop_grace_period: (\d+)s", web).group(1))
        timeout_match = re.search(
            r"- --graceful-timeout\s*\n\s*- \"(\d+)\"", web
        )

        self.assertIsNotNone(timeout_match)
        self.assertGreater(stop_seconds, int(timeout_match.group(1)))


if __name__ == "__main__":
    unittest.main()
