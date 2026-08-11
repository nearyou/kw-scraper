"""Central JSON logging, rotation, and correlation context."""

import contextlib
import contextvars
import json
import logging
import os
import sys
import threading
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path


MAX_LOG_BYTES = 10 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 5
_correlation_id = contextvars.ContextVar("correlation_id", default="-")
_original_stdout = sys.__stdout__
_original_stderr = sys.__stderr__

STANDARD_RECORD_FIELDS = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "module", "msecs",
    "message", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
}


def get_correlation_id():
    return _correlation_id.get()


def set_correlation_id(value=None):
    value = str(value or uuid.uuid4())[:128]
    safe_value = "".join(character for character in value if character.isalnum() or character in "-_.")
    return _correlation_id.set(safe_value or str(uuid.uuid4()))


def reset_correlation_id(token):
    _correlation_id.reset(token)


@contextlib.contextmanager
def correlation_context(value=None):
    token = set_correlation_id(value)
    try:
        yield get_correlation_id()
    finally:
        reset_correlation_id(token)


class JsonFormatter(logging.Formatter):
    def __init__(self, service):
        super().__init__()
        self.service = service

    def format(self, record):
        context = {}
        explicit_context = getattr(record, "context", None)
        if isinstance(explicit_context, dict):
            context.update(explicit_context)
        for key, value in record.__dict__.items():
            if key not in STANDARD_RECORD_FIELDS and key not in {"context", "service"}:
                context.setdefault(key, value)

        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", self.service),
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
            "context": context,
            "process": {"id": record.process, "name": record.processName},
            "thread": {"id": record.thread, "name": record.threadName},
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":"))


def build_handlers(log_file, *, service, max_bytes=MAX_LOG_BYTES, backup_count=DEFAULT_BACKUP_COUNT):
    formatter = JsonFormatter(service)
    console = logging.StreamHandler(_original_stdout)
    console.setFormatter(formatter)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rotating_file = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
        delay=True,
    )
    rotating_file.setFormatter(formatter)
    return console, rotating_file


class _LogStream:
    """Turn legacy print output into one structured log event per line."""

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._buffers = threading.local()

    def write(self, value):
        if not value:
            return 0
        buffer = getattr(self._buffers, "value", "") + str(value)
        lines = buffer.split("\n")
        self._buffers.value = lines.pop()
        for line in lines:
            if line.strip():
                self.logger.log(
                    self.level,
                    line.rstrip(),
                    extra={"context": {"source": "legacy_stream"}},
                )
        return len(value)

    def flush(self):
        buffer = getattr(self._buffers, "value", "")
        if buffer.strip():
            self.logger.log(
                self.level,
                buffer.rstrip(),
                extra={"context": {"source": "legacy_stream"}},
            )
        self._buffers.value = ""

    def isatty(self):
        return False

    @property
    def encoding(self):
        return "utf-8"


def configure_logging(*, service=None, log_file=None, level=None, capture_streams=False):
    """Configure idempotent JSON console and 10 MB rotating file logging."""
    service = service or os.getenv("SERVICE_NAME", "kw-scraper")
    log_file = log_file or os.getenv("LOG_FILE", "logs/kw-scraper.log")
    level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    resolved_level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()

    if not getattr(root, "_kw_json_configured", False):
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        for handler in build_handlers(log_file, service=service):
            root.addHandler(handler)
        root._kw_json_configured = True
    root.setLevel(resolved_level)
    logging.captureWarnings(True)

    if capture_streams and not isinstance(sys.stdout, _LogStream):
        sys.stdout = _LogStream(logging.getLogger("stdout"), logging.INFO)
        sys.stderr = _LogStream(logging.getLogger("stderr"), logging.ERROR)
    return root
