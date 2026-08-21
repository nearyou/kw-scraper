import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from scraping_functions.errors import DataError
from scraping_functions.session_manager import BrowserSessionManager, choose_user_agent
from scraping_functions.progress import ContiguousProgressTracker
from scraping_functions.validation import (
    validate_book_identifier,
    validate_html_document,
    validate_scrape_result,
)


def html(label):
    return f"<html><body>{label}{'x' * 250}</body></html>"


class ValidationTests(unittest.TestCase):
    def test_accepts_complete_result(self):
        result = {"success": "1"}
        for section in ("main", "zeroth", "first", "second", "third", "fourth"):
            result[section] = html(section)
        self.assertIs(validate_scrape_result(result), result)

    def test_rejects_missing_or_blocked_data(self):
        with self.assertRaises(DataError):
            validate_html_document("<html>short</html>", section="main")
        with self.assertRaises(DataError):
            validate_html_document(html("Sorry, you have been blocked"), section="main")

    def test_validates_book_identifier(self):
        self.assertTrue(validate_book_identifier("BB1C", "00600001", "1"))
        with self.assertRaises(DataError):
            validate_book_identifier("bad", "1", "x")


class SessionManagerTests(unittest.TestCase):
    def setUp(self):
        self.now = [10.0]
        self.browser = Mock(process_id=123)
        self.browser.cookies.return_value = [
            {"name": "session", "value": "abc", "domain": ".gov.pl", "ignored": "x"}
        ]
        self.factory = Mock(return_value=self.browser)
        self.proxy = SimpleNamespace(
            id="proxy-1", cookies_valid=True, cookies=[{"name": "old", "value": "1"}]
        )
        self.manager = BrowserSessionManager(
            self.factory,
            ["agent-a", "agent-b"],
            max_requests=2,
            max_age_seconds=30,
            clock=lambda: self.now[0],
        )

    def test_reuses_session_and_restores_then_checkpoints_cookies(self):
        first = self.manager.acquire(self.proxy)
        second = self.manager.acquire(self.proxy)
        self.assertIs(first, second)
        self.factory.assert_called_once()
        self.browser.set.cookies.assert_called_once_with(
            [{"name": "old", "value": "1"}]
        )

        self.manager.checkpoint(self.proxy)
        self.browser.cookies.assert_called_once_with(all_domains=True, all_info=True)
        self.assertTrue(self.proxy.cookies_valid)
        self.assertEqual(self.proxy.cookies[0]["name"], "session")
        self.assertNotIn("ignored", self.proxy.cookies[0])

    def test_rotates_after_request_limit(self):
        self.manager.acquire(self.proxy)
        self.manager.checkpoint(self.proxy)
        self.manager.checkpoint(self.proxy)
        self.manager.acquire(self.proxy)
        self.assertEqual(self.factory.call_count, 2)
        self.browser.quit.assert_called_once()

    def test_invalidating_session_clears_cookies(self):
        self.manager.acquire(self.proxy)
        self.manager.invalidate(self.proxy)
        self.assertFalse(self.proxy.cookies_valid)
        self.assertEqual(self.proxy.cookies, [])

    def test_user_agent_changes_when_possible(self):
        self.assertEqual(
            choose_user_agent(["one", "two"], previous="one", chooser=lambda values: values[0]),
            "two",
        )


class ProgressTests(unittest.TestCase):
    def test_checkpoint_waits_for_out_of_order_gap(self):
        tracker = ContiguousProgressTracker(10)
        self.assertEqual(tracker.mark_completed(11), (9, 0))
        self.assertEqual(tracker.mark_completed(12), (9, 0))
        self.assertEqual(tracker.mark_completed(10), (12, 3))

    def test_failed_gap_is_not_skipped(self):
        tracker = ContiguousProgressTracker(20)
        tracker.mark_completed(20)
        tracker.mark_completed(22)
        self.assertEqual(tracker.last_checkpoint, 20)


if __name__ == "__main__":
    unittest.main()
