"""Regression tests for safe Celery startup ordering."""

import unittest
from pathlib import Path


MAIN_SOURCE = (Path(__file__).with_name("main.py")).read_text(encoding="utf-8")


class StartupContractTests(unittest.TestCase):
    def test_queue_manager_is_scheduled_only_after_worker_is_ready(self):
        self.assertIn("@worker_ready.connect", MAIN_SOURCE)
        self.assertNotIn("@celery.on_after_configure.connect", MAIN_SOURCE)

    def test_failed_download_outcome_is_not_treated_as_truthy_success(self):
        self.assertIn(
            "succeeded = result is not DownloadOutcome.FAILED",
            MAIN_SOURCE,
        )
        self.assertNotIn("return bool(result)", MAIN_SOURCE)

    def test_queue_manager_claims_rows_atomically(self):
        models_source = (Path(__file__).with_name("models.py")).read_text(
            encoding="utf-8"
        )

        self.assertIn("def claim_next_pending_task", models_source)
        self.assertGreaterEqual(models_source.count("with_for_update(skip_locked=True)"), 2)
        self.assertIn("TaskQueue.claim_next_pending_task()", MAIN_SOURCE)


if __name__ == "__main__":
    unittest.main()
