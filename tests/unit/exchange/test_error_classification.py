import unittest

from gridtrader.exchange.binance_coinm.errors import (
    ExchangeAmbiguousResultError,
    ExchangeAuthError,
    ExchangeBannedError,
    ExchangeInsufficientMarginError,
    ExchangeInvalidResponseError,
    ExchangeNotFoundError,
    ExchangePermanentRequestError,
    ExchangePermissionError,
    ExchangeRateLimitError,
    ExchangeRulesChangedError,
    ExchangeTimeoutError,
    ExchangeUnavailableError,
    RequestClass,
    classify_sdk_exception,
)


class SdkError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: int | str | None = None,
        retry_after: float | str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = message
        self.retry_after = retry_after


def official_sdk_error(
    class_name: str,
    message: str,
    *,
    status_code: int,
    retry_after: float | None = None,
) -> Exception:
    """Mirror binance-common's public exception attribute layout.

    The current official SDK stores Binance's JSON ``code`` in ``status_code``
    for 4xx responses. The exception class name is therefore needed to recover
    the actual HTTP category without losing the provider code.
    """

    error_type = type(class_name, (Exception,), {})
    error = error_type(message)
    error.error_message = message  # type: ignore[attr-defined]
    error.status_code = status_code  # type: ignore[attr-defined]
    error.retry_after = retry_after  # type: ignore[attr-defined]
    return error


class ExchangeErrorClassificationTests(unittest.TestCase):
    def test_rate_limit_preserves_structured_context_and_retry_after(self) -> None:
        error = classify_sdk_exception(
            SdkError(
                "request weight exceeded",
                status_code=429,
                error_code=-1003,
                retry_after="2.5",
            ),
            request_class=RequestClass.READ_ONLY,
        )
        self.assertIsInstance(error, ExchangeRateLimitError)
        self.assertEqual(error.http_status, 429)
        self.assertEqual(error.code, -1003)
        self.assertIs(error.request_class, RequestClass.READ_ONLY)
        self.assertTrue(error.retryable)
        self.assertEqual(error.retry_after_seconds, 2.5)

    def test_http_418_is_a_non_retryable_ban(self) -> None:
        error = classify_sdk_exception(
            SdkError("IP banned", status_code=418, error_code=-1003)
        )
        self.assertIsInstance(error, ExchangeBannedError)
        self.assertFalse(error.retryable)
        self.assertEqual(error.http_status, 418)

    def test_503_execution_unknown_is_ambiguous_and_never_retryable(self) -> None:
        for message in (
            "Unknown error, please check your request or try again later.",
            "Internal error; execution status unknown.",
        ):
            with self.subTest(message=message):
                error = classify_sdk_exception(
                    SdkError(message, status_code=503),
                    request_class=RequestClass.WRITE,
                )
                self.assertIsInstance(error, ExchangeAmbiguousResultError)
                self.assertFalse(error.retryable)
                self.assertIs(error.request_class, RequestClass.WRITE)

    def test_known_503_service_unavailability_is_retryable(self) -> None:
        error = classify_sdk_exception(
            SdkError("Service Unavailable.", status_code=503)
        )
        self.assertIsInstance(error, ExchangeUnavailableError)
        self.assertTrue(error.retryable)
        self.assertEqual(error.http_status, 503)

    def test_generic_4xx_is_permanent_and_not_retryable(self) -> None:
        error = classify_sdk_exception(
            SdkError("invalid parameter", status_code=400, error_code=-1100)
        )
        self.assertIsInstance(error, ExchangePermanentRequestError)
        self.assertFalse(error.retryable)
        self.assertEqual(error.code, -1100)

    def test_specific_binance_failures_have_distinct_categories(self) -> None:
        cases = (
            (
                SdkError("invalid api key", status_code=401, error_code=-2015),
                ExchangeAuthError,
            ),
            (SdkError("forbidden", status_code=403), ExchangePermissionError),
            (
                SdkError("Margin is insufficient", status_code=400, error_code=-2019),
                ExchangeInsufficientMarginError,
            ),
            (
                SdkError("invalid quantity", status_code=400, error_code=-1013),
                ExchangeRulesChangedError,
            ),
            (
                SdkError("order does not exist", status_code=400, error_code=-2013),
                ExchangeNotFoundError,
            ),
        )
        for raw, expected_type in cases:
            with self.subTest(expected_type=expected_type.__name__):
                mapped = classify_sdk_exception(raw)
                self.assertIsInstance(mapped, expected_type)
                self.assertFalse(mapped.retryable)

    def test_official_sdk_exception_layout_preserves_http_and_binance_codes(self) -> None:
        cases = (
            (
                official_sdk_error(
                    "TooManyRequestsError",
                    "request weight exceeded",
                    status_code=-1003,
                    retry_after=2,
                ),
                ExchangeRateLimitError,
                429,
                -1003,
            ),
            (
                official_sdk_error(
                    "RateLimitBanError", "IP banned", status_code=-1003
                ),
                ExchangeBannedError,
                418,
                -1003,
            ),
            (
                official_sdk_error(
                    "BadRequestError", "Margin is insufficient", status_code=-2019
                ),
                ExchangeInsufficientMarginError,
                400,
                -2019,
            ),
            (
                official_sdk_error(
                    "UnauthorizedError", "invalid API key", status_code=-2015
                ),
                ExchangeAuthError,
                401,
                -2015,
            ),
            (
                official_sdk_error(
                    "NotFoundError", "order not found", status_code=-2013
                ),
                ExchangeNotFoundError,
                404,
                -2013,
            ),
        )
        for raw, expected_type, http_status, code in cases:
            with self.subTest(expected_type=expected_type.__name__):
                mapped = classify_sdk_exception(raw)
                self.assertIsInstance(mapped, expected_type)
                self.assertEqual(mapped.http_status, http_status)
                self.assertEqual(mapped.code, code)

    def test_transport_timeout_is_retryable_but_unclassified_errors_are_not(self) -> None:
        timeout = classify_sdk_exception(TimeoutError("read timed out"))
        self.assertIsInstance(timeout, ExchangeTimeoutError)
        self.assertTrue(timeout.retryable)

        invalid = classify_sdk_exception(ValueError("not an SDK response"))
        self.assertIsInstance(invalid, ExchangeInvalidResponseError)
        self.assertFalse(invalid.retryable)

    def test_official_network_error_timeout_message_preserves_timeout_taxonomy(self) -> None:
        raw = official_sdk_error(
            "NetworkError",
            "Network error: HTTPSConnectionPool read timed out",
            status_code=0,
        )
        read_error = classify_sdk_exception(
            raw,
            request_class=RequestClass.READ_ONLY,
        )
        self.assertIsInstance(read_error, ExchangeTimeoutError)
        self.assertTrue(read_error.retryable)

        write_error = classify_sdk_exception(
            raw,
            request_class=RequestClass.WRITE,
        )
        self.assertIsInstance(write_error, ExchangeAmbiguousResultError)
        self.assertFalse(write_error.retryable)

    def test_official_transient_negative_codes_are_not_misclassified_as_4xx(self) -> None:
        cases = (
            (-1003, ExchangeRateLimitError),
            (-1001, ExchangeUnavailableError),
            (-1007, ExchangeTimeoutError),
        )
        for code, expected_type in cases:
            with self.subTest(code=code):
                mapped = classify_sdk_exception(
                    official_sdk_error(
                        "BadRequestError",
                        "temporary Binance failure",
                        status_code=code,
                    )
                )
                self.assertIsInstance(mapped, expected_type)
                self.assertTrue(mapped.retryable)
                self.assertEqual(mapped.code, code)

    def test_transport_uncertainty_on_a_write_is_ambiguous_not_retryable(self) -> None:
        for raw in (
            TimeoutError("write timed out"),
            ConnectionError("connection reset"),
            official_sdk_error(
                "BadRequestError",
                "Timeout waiting for response from backend server",
                status_code=-1007,
            ),
        ):
            with self.subTest(raw=type(raw).__name__):
                mapped = classify_sdk_exception(
                    raw,
                    request_class=RequestClass.WRITE,
                )
                self.assertIsInstance(mapped, ExchangeAmbiguousResultError)
                self.assertFalse(mapped.retryable)

    def test_sensitive_values_are_redacted_from_the_public_message(self) -> None:
        error = classify_sdk_exception(
            SdkError(
                "signature=secret-signature api_key=secret-key listenKey=secret-listen-key",
                status_code=400,
            )
        )
        self.assertNotIn("secret-signature", str(error))
        self.assertNotIn("secret-key", str(error))
        self.assertNotIn("secret-listen-key", str(error))
        self.assertIn("signature=<redacted>", str(error))

    def test_json_and_colon_secret_forms_are_redacted(self) -> None:
        error = classify_sdk_exception(
            SdkError(
                '{"signature":"json-signature","apiSecret":"json-secret",'
                '"listenKey":"json-listen"} x-mbx-apikey: header-secret',
                status_code=400,
            )
        )
        public = str(error)
        for secret in (
            "json-signature",
            "json-secret",
            "json-listen",
            "header-secret",
        ):
            self.assertNotIn(secret, public)
        self.assertGreaterEqual(public.count("<redacted>"), 4)


if __name__ == "__main__":
    unittest.main()
