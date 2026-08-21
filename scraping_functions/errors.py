"""Typed application errors and safe structured error logging."""

import logging


class ApplicationError(Exception):
    """Base error carrying an operation name and safe diagnostic context."""

    code = "application_error"
    retryable = False

    def __init__(self, message, *, operation=None, context=None):
        super().__init__(message)
        self.operation = operation
        self.context = context or {}

    def as_dict(self):
        return {
            "error_type": type(self).__name__,
            "error_code": self.code,
            "message": str(self),
            "operation": self.operation,
            "retryable": self.retryable,
            "context": self.context,
        }


class NetworkError(ApplicationError):
    code = "network_error"
    retryable = True


class QueueError(ApplicationError):
    code = "queue_error"
    retryable = True


class DataError(ApplicationError):
    code = "data_error"


class CaptchaError(ApplicationError):
    code = "captcha_error"
    retryable = True


class CircuitOpenError(NetworkError):
    code = "circuit_open"

    def __init__(self, message, *, retry_after, operation=None, context=None):
        context = dict(context or {})
        context["retry_after_seconds"] = round(max(0, retry_after), 2)
        super().__init__(message, operation=operation, context=context)
        self.retry_after = retry_after


def log_error(logger, error, *, level=logging.ERROR, exc_info=False):
    """Log consistent JSON diagnostics without leaking credentials or page data."""
    if isinstance(error, ApplicationError):
        payload = error.as_dict()
    else:
        payload = {
            "error_type": type(error).__name__,
            "error_code": "unexpected_error",
            "message": str(error),
            "retryable": False,
        }
    logger.log(
        level,
        "application_error",
        extra={"context": payload},
        exc_info=exc_info,
    )
