import unittest
from unittest.mock import Mock

from scraping_functions.errors import CircuitOpenError, NetworkError
from scraping_functions.resilience import CircuitBreaker, backoff_delay, retry_external_call


class CircuitBreakerTests(unittest.TestCase):
    def test_trips_exactly_at_the_failure_threshold(self):
        breaker = CircuitBreaker("government-site", failure_threshold=3)

        breaker.record_failure()
        breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.CLOSED)

        breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.OPEN)
        with self.assertRaises(CircuitOpenError):
            breaker.before_call("search")

    def test_opens_then_allows_one_probe_after_timeout(self):
        now = [10.0]
        breaker = CircuitBreaker(
            "government-site",
            failure_threshold=2,
            recovery_timeout=30,
            clock=lambda: now[0],
        )
        breaker.record_failure()
        breaker.record_failure()

        with self.assertRaises(CircuitOpenError):
            breaker.before_call("search")

        now[0] = 40.0
        breaker.before_call("search")
        self.assertEqual(breaker.state, CircuitBreaker.HALF_OPEN)
        with self.assertRaises(CircuitOpenError):
            breaker.before_call("second-probe")

        breaker.record_success()
        self.assertEqual(breaker.state, CircuitBreaker.CLOSED)

    def test_failed_probe_reopens_circuit(self):
        now = [0.0]
        breaker = CircuitBreaker(
            "site", failure_threshold=1, recovery_timeout=5, clock=lambda: now[0]
        )
        breaker.record_failure()
        now[0] = 5.0
        breaker.before_call()
        breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.OPEN)

    def test_partial_success_does_not_reset_composite_call_failures(self):
        breaker = CircuitBreaker("site", failure_threshold=2)
        breaker.record_failure()

        retry_external_call(
            Mock(return_value="menu-loaded"),
            operation="site.load_menu",
            attempts=1,
            circuit_breaker=breaker,
            record_success=False,
            logger=Mock(),
        )
        breaker.record_failure()

        self.assertEqual(breaker.state, CircuitBreaker.OPEN)

    def test_explicit_maintenance_can_trip_circuit_immediately(self):
        breaker = CircuitBreaker("site", failure_threshold=10)
        breaker.trip()

        with self.assertRaises(CircuitOpenError):
            breaker.before_call("search")


class RetryTests(unittest.TestCase):
    def test_uses_exponential_backoff_then_returns(self):
        call = Mock(side_effect=[NetworkError("one"), NetworkError("two"), "ok"])
        sleep = Mock()

        result = retry_external_call(
            call,
            operation="site.search",
            attempts=3,
            sleep=sleep,
            logger=Mock(),
        )

        self.assertEqual(result, "ok")
        self.assertEqual([item.args[0] for item in sleep.call_args_list], [1.0, 2.0])

    def test_backoff_is_capped(self):
        self.assertEqual(backoff_delay(1, base=2, maximum=5), 2)
        self.assertEqual(backoff_delay(2, base=2, maximum=5), 4)
        self.assertEqual(backoff_delay(3, base=2, maximum=5), 5)


if __name__ == "__main__":
    unittest.main()
