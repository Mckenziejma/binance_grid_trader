import unittest
from collections.abc import Callable

from gridtrader.exchange.binance_coinm.errors import (
    ExchangeAmbiguousResultError,
    ExchangeBannedError,
    ExchangePermanentRequestError,
    ExchangeRateLimitError,
    ExchangeTimeoutError,
    ExchangeUnavailableError,
    RequestClass,
)
from gridtrader.exchange.binance_coinm.retry_policy import RetryPolicy, RetrySettings


class SdkError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_message = message
        self.retry_after = retry_after


class DeterministicTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def scripted_operation(outcomes: list[object]) -> tuple[Callable[[], object], list[int]]:
    calls = [0]

    def operation() -> object:
        index = calls[0]
        calls[0] += 1
        outcome = outcomes[index]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return operation, calls


class RetryPolicyTests(unittest.TestCase):
    def test_transient_read_uses_exponential_full_jitter_then_succeeds(self) -> None:
        time = DeterministicTime()
        random_values = iter((0.25, 0.75))
        policy = RetryPolicy(
            RetrySettings(
                max_attempts=4,
                base_delay_seconds=0.5,
                max_delay_seconds=5,
                total_deadline_seconds=10,
            ),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: next(random_values),
        )
        operation, calls = scripted_operation(
            [ConnectionError("offline"), ConnectionError("offline"), "ok"]
        )

        self.assertEqual(policy.execute(operation), "ok")
        self.assertEqual(calls[0], 3)
        # Full jitter samples uniformly between zero and the exponential ceiling.
        self.assertEqual(time.sleeps, [0.125, 0.75])

    def test_attempt_count_and_exponential_delay_are_bounded(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(
                max_attempts=4,
                base_delay_seconds=1,
                max_delay_seconds=1.5,
                total_deadline_seconds=20,
            ),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 1,
        )
        operation, calls = scripted_operation(
            [ConnectionError("offline") for _ in range(4)]
        )

        with self.assertRaises(ExchangeUnavailableError):
            policy.execute(operation)
        self.assertEqual(calls[0], 4)
        self.assertEqual(time.sleeps, [1, 1.5, 1.5])

    def test_total_deadline_stops_before_an_over_budget_sleep(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(
                max_attempts=10,
                base_delay_seconds=2,
                max_delay_seconds=4,
                total_deadline_seconds=3,
            ),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 1,
        )
        operation, calls = scripted_operation(
            [ConnectionError("offline") for _ in range(10)]
        )

        with self.assertRaises(ExchangeUnavailableError):
            policy.execute(operation)
        self.assertEqual(calls[0], 2)
        self.assertEqual(time.sleeps, [2])

    def test_total_deadline_rejects_a_success_that_returns_too_late(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(
                max_attempts=2,
                base_delay_seconds=0,
                max_delay_seconds=0,
                total_deadline_seconds=8,
            ),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 0,
        )
        calls = [0]

        def slow_operation() -> str:
            calls[0] += 1
            if calls[0] == 1:
                time.advance(7)
                raise ConnectionError("offline")
            time.advance(2)
            return "too late"

        with self.assertRaisesRegex(ExchangeTimeoutError, "retry deadline"):
            policy.execute(slow_operation)

        self.assertEqual(calls[0], 2)
        self.assertEqual(time.sleeps, [0])
        self.assertEqual(time.now, 9)

    def test_429_honors_retry_after_for_read_requests(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(max_attempts=2, total_deadline_seconds=5),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 0,
        )
        operation, calls = scripted_operation(
            [SdkError("weight exceeded", status_code=429, retry_after=1.75), "ok"]
        )

        self.assertEqual(policy.execute(operation), "ok")
        self.assertEqual(calls[0], 2)
        self.assertEqual(time.sleeps, [1.75])

    def test_permanent_4xx_is_not_retried(self) -> None:
        policy = RetryPolicy(sleep=lambda _seconds: self.fail("must not sleep"))
        operation, calls = scripted_operation(
            [SdkError("bad parameter", status_code=400)]
        )

        with self.assertRaises(ExchangePermanentRequestError):
            policy.execute(operation)
        self.assertEqual(calls[0], 1)

    def test_write_request_is_never_retried_even_for_a_transient_failure(self) -> None:
        policy = RetryPolicy(sleep=lambda _seconds: self.fail("must not sleep"))
        operation, calls = scripted_operation([ConnectionError("connection lost")])

        with self.assertRaises(ExchangeAmbiguousResultError) as caught:
            policy.execute(operation, request_class=RequestClass.WRITE)
        self.assertEqual(calls[0], 1)
        self.assertIs(caught.exception.request_class, RequestClass.WRITE)

    def test_503_execution_unknown_is_not_retried(self) -> None:
        policy = RetryPolicy(sleep=lambda _seconds: self.fail("must not sleep"))
        operation, calls = scripted_operation(
            [SdkError("Unknown error, please check your request.", status_code=503)]
        )

        with self.assertRaises(ExchangeAmbiguousResultError) as caught:
            policy.execute(operation)
        self.assertEqual(calls[0], 1)
        self.assertFalse(caught.exception.retryable)

    def test_503_service_unavailable_is_bounded_and_can_recover(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(
                max_attempts=3,
                base_delay_seconds=0.5,
                max_delay_seconds=1,
                total_deadline_seconds=5,
            ),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 1,
        )
        operation, calls = scripted_operation(
            [
                SdkError("Service Unavailable", status_code=503),
                SdkError("Service Unavailable", status_code=503),
                "ok",
            ]
        )

        self.assertEqual(policy.execute(operation), "ok")
        self.assertEqual(calls[0], 3)
        self.assertEqual(time.sleeps, [0.5, 1])

    def test_503_service_unavailable_exhausts_at_attempt_bound(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(max_attempts=2, total_deadline_seconds=5),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 0,
        )
        operation, calls = scripted_operation(
            [SdkError("Service Unavailable", status_code=503) for _ in range(2)]
        )

        with self.assertRaises(ExchangeUnavailableError):
            policy.execute(operation)
        self.assertEqual(calls[0], 2)

    def test_http_418_opens_a_fail_fast_circuit(self) -> None:
        policy = RetryPolicy(sleep=lambda _seconds: self.fail("must not sleep"))
        first, first_calls = scripted_operation(
            [SdkError("IP auto-banned", status_code=418)]
        )
        with self.assertRaises(ExchangeBannedError):
            policy.execute(first)
        self.assertEqual(first_calls[0], 1)
        self.assertTrue(policy.circuit_open)

        second_calls = [0]

        def must_not_run() -> None:
            second_calls[0] += 1

        with self.assertRaises(ExchangeBannedError):
            policy.execute(must_not_run)
        self.assertEqual(second_calls[0], 0)

    def test_preclassified_rate_limit_remains_retryable_for_reads_only(self) -> None:
        time = DeterministicTime()
        policy = RetryPolicy(
            RetrySettings(max_attempts=2, total_deadline_seconds=2),
            sleep=time.sleep,
            monotonic=time.monotonic,
            random_value=lambda: 0,
        )
        operation, calls = scripted_operation(
            [
                ExchangeRateLimitError(
                    "rate limited",
                    http_status=429,
                    retry_after_seconds=0.5,
                ),
                "ok",
            ]
        )
        self.assertEqual(policy.execute(operation), "ok")
        self.assertEqual(calls[0], 2)
        self.assertEqual(time.sleeps, [0.5])


if __name__ == "__main__":
    unittest.main()
