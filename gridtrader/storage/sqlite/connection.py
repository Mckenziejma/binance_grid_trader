"""SQLite connection setup and deterministic migration runner."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Iterator, List, Optional, Union

from ..ports import MigrationError


_JOURNAL_MODE_LOCK = Lock()
_INITIAL_LOCK_RETRY_SECONDS = 0.001
_MAX_LOCK_RETRY_SECONDS = 0.050


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    """Return whether an operational error is a retryable SQLite lock."""

    error_code = getattr(exc, "sqlite_errorcode", None)
    if error_code is not None:
        base_code = error_code & 0xFF
        return base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}

    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "database is locked",
            "database table is locked",
            "database schema is locked",
        )
    )


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str
    sql: str


class SQLiteDatabase:
    """Creates consistently configured SQLite connections and runs migrations."""

    def __init__(
        self,
        path: Union[str, Path],
        *,
        busy_timeout_ms: int = 5_000,
        migrations_path: Optional[Path] = None,
    ) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms cannot be negative")
        self.path = str(path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.migrations_path = migrations_path or Path(__file__).with_name("migrations")

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=max(self.busy_timeout_ms / 1000.0, 0.001),
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = {}".format(self.busy_timeout_ms))
            self._ensure_wal_mode(connection)
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    def _ensure_wal_mode(self, connection: sqlite3.Connection) -> None:
        """Enable persistent WAL mode without racing concurrent initializers."""

        deadline = time.monotonic() + (self.busy_timeout_ms / 1000.0)
        retry_delay = _INITIAL_LOCK_RETRY_SECONDS

        while True:
            try:
                with _JOURNAL_MODE_LOCK:
                    row = connection.execute("PRAGMA journal_mode").fetchone()
                    current_mode = "" if row is None else str(row[0]).lower()
                    if current_mode == "wal":
                        return

                    row = connection.execute("PRAGMA journal_mode = WAL").fetchone()
                    selected_mode = "" if row is None else str(row[0]).lower()
                    if selected_mode != "wal":
                        raise MigrationError(
                            "SQLite refused WAL journal mode; selected {!r}".format(
                                selected_mode
                            )
                        )
                    return
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_lock_error(exc):
                    raise

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(retry_delay, remaining))
                retry_delay = min(retry_delay * 2, _MAX_LOCK_RETRY_SECONDS)

    def migrate(self) -> None:
        connection = self.connect()
        try:
            self._ensure_migration_table(connection)
            applied = {
                int(row["version"]): row
                for row in connection.execute(
                    "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
                )
            }
            for migration in self._load_migrations():
                previous = applied.get(migration.version)
                if previous is not None:
                    if (
                        previous["name"] != migration.name
                        or previous["checksum"] != migration.checksum
                    ):
                        raise MigrationError(
                            "applied migration {} has changed".format(migration.version)
                        )
                    continue
                self._apply_migration(connection, migration)
        finally:
            connection.close()

    @staticmethod
    def _ensure_migration_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                checksum TEXT NOT NULL,
                applied_at_ms INTEGER NOT NULL
                    CHECK (typeof(applied_at_ms) = 'integer')
            ) STRICT
            """
        )

    def _load_migrations(self) -> List[Migration]:
        migrations: List[Migration] = []
        if not self.migrations_path.exists():
            raise MigrationError(
                "migration directory does not exist: {}".format(
                    self.migrations_path
                )
            )
        for path in sorted(self.migrations_path.glob("*.sql")):
            prefix, separator, _ = path.name.partition("_")
            if not separator or not prefix.isdigit():
                raise MigrationError("invalid migration filename: {}".format(path.name))
            sql = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            migrations.append(Migration(int(prefix), path.name, path, checksum, sql))
        versions = [migration.version for migration in migrations]
        if not migrations or len(versions) != len(set(versions)):
            raise MigrationError("migrations must contain unique numeric versions")
        return migrations

    @staticmethod
    def _sql_statements(script: str) -> Iterator[str]:
        buffer: List[str] = []
        for line in script.splitlines():
            buffer.append(line)
            candidate = "\n".join(buffer).strip()
            if candidate and sqlite3.complete_statement(candidate):
                yield candidate
                buffer = []
        remainder = "\n".join(buffer).strip()
        if remainder:
            raise MigrationError("migration ends with an incomplete SQL statement")

    def _apply_migration(self, connection: sqlite3.Connection, migration: Migration) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT name, checksum FROM schema_migrations WHERE version = ?",
                (migration.version,),
            ).fetchone()
            if previous is not None:
                if (
                    previous["name"] != migration.name
                    or previous["checksum"] != migration.checksum
                ):
                    raise MigrationError(
                        "applied migration {} has changed".format(migration.version)
                    )
                connection.commit()
                return
            for statement in self._sql_statements(migration.sql):
                connection.execute(statement)
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, checksum, applied_at_ms)
                VALUES (?, ?, ?, ?)
                """,
                (
                    migration.version,
                    migration.name,
                    migration.checksum,
                    time.time_ns() // 1_000_000,
                ),
            )
            connection.commit()
        except Exception as exc:
            if connection.in_transaction:
                connection.rollback()
            if isinstance(exc, MigrationError):
                raise
            raise MigrationError(
                "failed to apply migration {} ({})".format(migration.version, migration.name)
            ) from exc


__all__ = ["Migration", "SQLiteDatabase"]
