import os
import unittest
from unittest.mock import Mock, patch

from scripts import wait_for_dependencies as startup


class StartupDependencyTests(unittest.TestCase):
    def test_startup_retries_dependencies_with_capped_exponential_delays(self):
        database_check = Mock(
            side_effect=[OSError("offline"), OSError("offline"), None]
        )
        sleep = Mock()

        with (
            patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "postgresql://configured",
                    "CELERY_BROKER_URL": "",
                    "DEPENDENCY_MAX_ATTEMPTS": "3",
                    "DEPENDENCY_RETRY_MAX_DELAY": "10",
                },
            ),
            patch.object(startup, "check_database", database_check),
            patch.object(startup.time, "sleep", sleep),
            patch.object(startup, "logger", Mock()),
        ):
            self.assertTrue(startup.wait_for_dependencies())

        self.assertEqual(database_check.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2])

    def test_startup_fails_after_configured_attempt_limit(self):
        broker_check = Mock(side_effect=OSError("offline"))

        with (
            patch.dict(
                os.environ,
                {
                    "DATABASE_URL": "",
                    "CELERY_BROKER_URL": "amqp://configured",
                    "DEPENDENCY_MAX_ATTEMPTS": "2",
                    "DEPENDENCY_RETRY_MAX_DELAY": "1",
                },
            ),
            patch.object(startup, "check_broker", broker_check),
            patch.object(startup.time, "sleep"),
            patch.object(startup, "logger", Mock()),
        ):
            self.assertFalse(startup.wait_for_dependencies())

        self.assertEqual(broker_check.call_count, 2)


if __name__ == "__main__":
    unittest.main()
