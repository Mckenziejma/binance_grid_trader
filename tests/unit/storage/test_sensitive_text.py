from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gridtrader.storage import SensitiveDataError
from gridtrader.storage.ledger import ensure_safe_text

from .support import NOW, new_ledger, strategy_record


class SensitiveTextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ledger.sqlite3"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_unredacted_sensitive_values_are_rejected_in_common_formats(self) -> None:
        unsafe_values = (
            "request failed: signature=raw-signature",
            'response={"apiKey": "raw-api-key"}',
            "api_key: raw-api-key",
            "apiSecret='raw-api-secret'",
            'api_secret="raw-api-secret"',
            "listenKey=raw-listen-key",
            "listen_key: raw-listen-key",
            "X-MBX-APIKEY: raw-header-key",
            "secret_key=raw-secret-key",
            'apiSecret: "raw secret with spaces"',
        )

        for value in unsafe_values:
            with self.subTest(value=value):
                with self.assertRaises(SensitiveDataError):
                    ensure_safe_text(value, "error")

    def test_literal_redaction_marker_is_accepted_for_sensitive_values(self) -> None:
        safe_values = (
            "signature=<redacted>",
            'response={"apiKey": "<redacted>"}',
            "api_key: <redacted>",
            "apiSecret='<redacted>'",
            'api_secret="<redacted>"',
            "listenKey=<redacted>",
            "listen_key: <redacted>",
            "X-MBX-APIKEY: <redacted>",
        )

        for value in safe_values:
            with self.subTest(value=value):
                sanitized = ensure_safe_text(value, "error")
                self.assertIn("<redacted>", sanitized)
                self.assertNotRegex(
                    sanitized,
                    r"(?i)(signature|listen[_-]?key|api[_-]?(key|secret)|x-mbx)",
                )

    def test_private_websocket_listen_key_url_is_rejected_even_when_redacted(self) -> None:
        with self.assertRaises(SensitiveDataError):
            ensure_safe_text(
                "wss://dstream.binance.com/ws/listenKey=<redacted>",
                "error",
            )

    def test_strategy_last_error_rejects_secret_on_initial_insert(self) -> None:
        ledger = new_ledger(self.db_path)
        record = strategy_record()
        record["last_error"] = "api_key=raw-api-key"

        with ledger.unit_of_work() as uow:
            with self.assertRaises(SensitiveDataError):
                uow.strategies.add(record)

    def test_checkpoint_error_rejects_secret(self) -> None:
        ledger = new_ledger(self.db_path)

        with ledger.unit_of_work() as uow:
            with self.assertRaises(SensitiveDataError):
                uow.recovery_checkpoints.add({
                    "run_id": "run-sensitive-checkpoint",
                    "account_id": "coin-m-main",
                    "reason": "STARTUP",
                    "status": "STARTED",
                    "started_at_ms": NOW,
                    "error": "signature=raw-signature",
                })

    def test_event_error_rejects_secret(self) -> None:
        ledger = new_ledger(self.db_path)

        with ledger.unit_of_work() as uow:
            with self.assertRaises(SensitiveDataError):
                uow.events.append({
                    "source": "LOCAL",
                    "dedupe_key": "sensitive-event-error",
                    "event_type": "recovery_failed",
                    "received_at_ms": NOW,
                    "payload_json": {"status": "failed"},
                    "error": "X-MBX-APIKEY: raw-header-key",
                })

    def test_bot_run_error_rejects_secret(self) -> None:
        ledger = new_ledger(self.db_path)

        with ledger.unit_of_work() as uow:
            with self.assertRaises(SensitiveDataError):
                uow.bot_runs.add({
                    "run_id": "run-sensitive-error",
                    "instance_id": "instance-1",
                    "status": "FAILED",
                    "started_at_ms": NOW,
                    "heartbeat_at_ms": NOW,
                    "error": 'apiSecret="raw-api-secret"',
                })

    def test_redacted_error_can_be_persisted_by_all_error_repositories(self) -> None:
        ledger = new_ledger(self.db_path)
        redacted = "signature=<redacted>"
        sanitized = "<redacted>"
        strategy = strategy_record()
        strategy["last_error"] = redacted

        with ledger.unit_of_work() as uow:
            stored_strategy = uow.strategies.add(strategy)
            checkpoint = uow.recovery_checkpoints.add({
                "run_id": "run-redacted-checkpoint",
                "strategy_id": strategy["strategy_id"],
                "account_id": "coin-m-main",
                "symbol": "BTCUSD_PERP",
                "reason": "STARTUP",
                "status": "STARTED",
                "started_at_ms": NOW,
                "error": redacted,
            })
            event, inserted = uow.events.append({
                "source": "LOCAL",
                "dedupe_key": "redacted-event-error",
                "event_type": "recovery_failed",
                "received_at_ms": NOW,
                "payload_json": {"status": "failed"},
                "error": redacted,
            })
            bot_run = uow.bot_runs.add({
                "run_id": "run-redacted-error",
                "instance_id": "instance-1",
                "status": "FAILED",
                "started_at_ms": NOW,
                "heartbeat_at_ms": NOW,
                "error": redacted,
            })

        self.assertEqual(sanitized, stored_strategy["last_error"])
        self.assertEqual(sanitized, checkpoint["error"])
        self.assertTrue(inserted)
        self.assertEqual(sanitized, event["error"])
        self.assertEqual(sanitized, bot_run["error"])


if __name__ == "__main__":
    unittest.main()
