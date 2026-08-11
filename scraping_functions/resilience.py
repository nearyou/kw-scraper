"""Exponential retry and circuit-breaker primitives for external services."""

import logging
import random
import threading
import time

from scraping_functions.errors import (
    CaptchaError,
    CircuitOpenError,
    NetworkError,
    QueueError,
    log_error,
)


RETRYABLE_ERRORS = (NetworkError, QueueError, CaptchaError)


def backoff_delay(attempt, *, base=1.0, maximum=30.0, jitter=0.0):
    """Return a capped exponential delay for a one-based failed attempt."""
    delay = min(maximum, base * (2 ** max(0, attempt - 1)))
    if jitter:
        delay += random.uniform(0, delay * jitter)
    return delay


class CircuitBreaker:
    """Stop calls after repeated failures, then allow one recovery probe."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        name,
        *,
        failure_threshold=5,
        recovery_timeout=60,
        clock=time.monotonic,
    ):
        self.name = name
        self.failure_threshold = max(1, failure_threshold)
        self.recovery_timeout = max(0, recovery_timeout)
        self.clock = clock
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = None
        self._probe_in_progress = False
        self._lock = threading.Lock()

    @property
    def state(self):
        with self._lock:
            return self._state

    def before_call(self, operation=None):
        with self._lock:
            if self._state == self.CLOSED:
                return

            elapsed = self.clock() - self._opened_at
            if self._state == self.OPEN and elapsed >= self.recovery_timeout:
                self._state = self.HALF_OPEN
                self._probe_in_progress = True
                return

            if self._state == self.HALF_OPEN and not self._probe_in_progress:
                self._probe_in_progress = True
                return

            retry_after = max(0, self.recovery_timeout - elapsed)
            raise CircuitOpenError(
                f"Circuit '{self.name}' is open; external call was skipped",
                retry_after=retry_after,
                operation=operation,
                context={"circuit": self.name, "state": self._state},
            )

    def record_success(self):
        with self._lock:
            self._state = self.CLOSED
            self._failures = 0
            self._opened_at = None
            self._probe_in_progress = False

    def record_failure(self):
        with self._lock:
            self._failures += 1
            if self._state == self.HALF_OPEN or self._failures >= self.failure_threshold:
                self._state = self.OPEN
                self._opened_at = self.clock()
            self._probe_in_progress = False

    def trip(self):
        """Open immediately when an explicit maintenance response is received."""
        with self._lock:
            self._failures = self.failure_threshold
            self._state = self.OPEN
            self._opened_at = self.clock()
            self._probe_in_progress = False


def retry_external_call(
    call,
    *,
    operation,
    attempts=4,
    base_delay=1.0,
    max_delay=30.0,
    jitter=0.0,
    retry_on=RETRYABLE_ERRORS,
    circuit_breaker=None,
    record_success=True,
    sleep=time.sleep,
    logger=None,
):
    """Run an external call with exponential backoff and optional circuit breaker."""
    logger = logger or logging.getLogger(__name__)
    attempts = max(1, attempts)

    for attempt in range(1, attempts + 1):
        if circuit_breaker:
            circuit_breaker.before_call(operation)
        try:
            result = call()
            if circuit_breaker and record_success:
                circuit_breaker.record_success()
            return result
        except retry_on as error:
            if circuit_breaker:
                circuit_breaker.record_failure()
            log_error(logger, error)
            if attempt == attempts:
                raise
            delay = backoff_delay(
                attempt, base=base_delay, maximum=max_delay, jitter=jitter
            )
            logger.warning(
                "external_call_retry operation=%s attempt=%s/%s delay_seconds=%.2f",
                operation,
                attempt,
                attempts,
                delay,
            )
            sleep(delay)
