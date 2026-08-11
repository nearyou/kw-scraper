import json
import logging
import tempfile
import unittest
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from logging_config import (
    MAX_LOG_BYTES,
    JsonFormatter,
    build_handlers,
    correlation_context,
    get_correlation_id,
)


class JsonLoggingTests(unittest.TestCase):
    def test_formatter_includes_timestamp_context_and_correlation(self):
        record = logging.LogRecord("test", logging.INFO, __file__, 10, "hello", (), None)
        record.context = {"task_id": 42}
        with correlation_context("request-123"):
            payload = json.loads(JsonFormatter("tests").format(record))

        datetime.fromisoformat(payload["timestamp"])
        self.assertEqual(payload["correlation_id"], "request-123")
        self.assertEqual(payload["context"]["task_id"], 42)
        self.assertEqual(payload["service"], "tests")

    def test_context_is_reset_after_scope(self):
        original = get_correlation_id()
        with correlation_context("book-7"):
            self.assertEqual(get_correlation_id(), "book-7")
        self.assertEqual(get_correlation_id(), original)

    def test_file_handler_rotates_at_exactly_ten_megabytes(self):
        with tempfile.TemporaryDirectory() as directory:
            console, file_handler = build_handlers(
                Path(directory) / "app.log", service="tests"
            )
            try:
                self.assertIsInstance(file_handler, RotatingFileHandler)
                self.assertEqual(file_handler.maxBytes, MAX_LOG_BYTES)
                self.assertEqual(file_handler.maxBytes, 10 * 1024 * 1024)
                self.assertGreater(file_handler.backupCount, 0)
            finally:
                console.close()
                file_handler.close()


if __name__ == "__main__":
    unittest.main()
