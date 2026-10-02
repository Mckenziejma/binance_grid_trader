"""Thin wrapper around Binance's official modular COIN-M SDK."""

from __future__ import annotations

from typing import Any, Callable

from .errors import ExchangeError, RequestClass, classify_sdk_exception
from .retry_policy import RetryPolicy


def unwrap_sdk_response(response: Any) -> Any:
    """Return SDK response data while accepting deterministic test doubles."""

    data = getattr(response, "data", None)
    if callable(data):
        return data()
    return response


class OfficialCoinMConnector:
    """Expose only Phase-2 read endpoints from the official SDK.

    The official client's built-in retries are disabled when this wrapper
    creates it.  Retry decisions then stay visible and operation-aware here.
    An already-created ``rest_api`` test double can be injected without the SDK
    installed, keeping all tests offline.
    """

    def __init__(self, rest_api: Any, *, retry_policy: RetryPolicy | None = None) -> None:
        self._rest_api = rest_api
        self._retry = retry_policy or RetryPolicy()

    @classmethod
    def from_credentials(
        cls,
        *,
        api_key: str,
        api_secret: str,
        timeout_ms: int = 5_000,
        base_path: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> "OfficialCoinMConnector":
        """Construct the official production SDK lazily; performs no request."""

        try:
            from binance_common.configuration import ConfigurationRestAPI
            from binance_common.constants import (
                DERIVATIVES_TRADING_COIN_FUTURES_REST_API_PROD_URL,
            )
            from binance_sdk_derivatives_trading_coin_futures.derivatives_trading_coin_futures import (  # noqa: E501
                DerivativesTradingCoinFutures,
            )
        except ImportError as exc:  # pragma: no cover - exercised only in live setup.
            raise RuntimeError(
                "install the optional binance-sdk-derivatives-trading-coin-futures dependency"
            ) from exc

        configuration = ConfigurationRestAPI(
            api_key=api_key,
            api_secret=api_secret,
            base_path=(
                base_path
                or DERIVATIVES_TRADING_COIN_FUTURES_REST_API_PROD_URL
            ),
            timeout=timeout_ms,
            retries=0,
            backoff=0,
        )
        client = DerivativesTradingCoinFutures(config_rest_api=configuration)
        return cls(client.rest_api, retry_policy=retry_policy)

    def _read(self, operation: Callable[[], Any]) -> Any:
        try:
            # Generated SDK response decoding can raise too (validation,
            # transport-stream and model errors).  Keep ``data()`` inside the
            # same retry/classification boundary as the HTTP operation.
            return self._retry.execute(
                lambda: unwrap_sdk_response(operation()),
                request_class=RequestClass.READ_ONLY,
            )
        except ExchangeError:
            raise
        except Exception as exc:  # defensive if a custom policy does not classify.
            raise classify_sdk_exception(exc, request_class=RequestClass.READ_ONLY) from exc

    def exchange_information(self) -> Any:
        return self._read(self._rest_api.exchange_information)

    def check_server_time(self) -> Any:
        return self._read(self._rest_api.check_server_time)

    def current_all_open_orders(self, *, symbol: str | None = None) -> Any:
        return self._read(
            lambda: self._rest_api.current_all_open_orders(symbol=symbol)
        )

    def position_information(self, *, pair: str | None = None) -> Any:
        return self._read(
            lambda: self._rest_api.position_information(pair=pair)
        )

    def account_information(self) -> Any:
        return self._read(self._rest_api.account_information)

    def futures_account_balance(self) -> Any:
        return self._read(self._rest_api.futures_account_balance)

    def get_current_position_mode(self) -> Any:
        return self._read(self._rest_api.get_current_position_mode)

    def account_trade_list(self, **params: Any) -> Any:
        return self._read(lambda: self._rest_api.account_trade_list(**params))

    def query_order(self, *, symbol: str, orig_client_order_id: str) -> Any:
        return self._read(
            lambda: self._rest_api.query_order(
                symbol=symbol,
                orig_client_order_id=orig_client_order_id,
            )
        )

    def query_order_by_order_id(self, *, symbol: str, order_id: int) -> Any:
        return self._read(
            lambda: self._rest_api.query_order(
                symbol=symbol,
                order_id=order_id,
            )
        )


__all__ = ["OfficialCoinMConnector", "unwrap_sdk_response"]
