from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .support import new_ledger


class SchemaMigrationContractTests(unittest.TestCase):
    def test_two_initializers_converge_on_the_same_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "ledger.sqlite3"

            with ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(lambda _: new_ledger(db_path), range(2)))

            ledger = new_ledger(db_path)
            connection = ledger.database.connect()
            try:
                versions = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                self.assertEqual([1, 2], [row[0] for row in versions])
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
