"""Start the real Flask app with isolated dependencies and call /health."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ApplicationSmokeTests(unittest.TestCase):
    def test_application_starts_and_healthy_dependencies_return_200(self):
        command = (
            "from www.main import app, db; "
            "from www.models import TaskQueue; "
            "ctx=app.app_context(); ctx.push(); db.create_all(); "
            "db.session.add_all(["
            "TaskQueue(department_code='AA1A', start_from=1, end_at=1),"
            "TaskQueue(department_code='BB1B', start_from=1, end_at=1)]); "
            "db.session.commit(); first=TaskQueue.claim_next_pending_task(); "
            "second=TaskQueue.claim_next_pending_task(); "
            "assert first.id != second.id; "
            "response=app.test_client().get('/health'); "
            "assert response.status_code == 200, response.get_data(as_text=True); "
            "ctx.pop()"
        )
        with tempfile.TemporaryDirectory() as directory:
            environment = os.environ.copy()
            environment.update(
                {
                    "DATABASE_URL": "sqlite:///:memory:",
                    "CELERY_BROKER_URL": "memory://",
                    "LOG_FILE": str(Path(directory) / "smoke.log"),
                    "SERVICE_NAME": "smoke-test",
                    "TEST_MODE": "true",
                    "IDLE_SCRAPE_ENABLED": "false",
                    "SENTRY_DSN": "",
                }
            )
            result = subprocess.run(
                [sys.executable, "-B", "-c", command],
                cwd=PROJECT_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
