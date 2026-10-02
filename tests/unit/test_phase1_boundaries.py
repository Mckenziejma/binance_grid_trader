"""Architecture-boundary tests for the isolated Phase 1 foundation."""

from __future__ import annotations

import ast
import tomllib
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
NEW_PACKAGE_ROOTS = (
    REPOSITORY_ROOT / "gridtrader" / "core",
    REPOSITORY_ROOT / "gridtrader" / "exchange",
    REPOSITORY_ROOT / "gridtrader" / "grid",
    REPOSITORY_ROOT / "gridtrader" / "orders",
    REPOSITORY_ROOT / "gridtrader" / "storage",
)
FORBIDDEN_IMPORT_PREFIXES = (
    "aiohttp",
    "binance",
    "gridtrader.api",
    "gridtrader.gateway",
    "gridtrader.trader",
    "PyQt5",
    "requests",
    "websocket",
)


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


class Phase1BoundaryTests(unittest.TestCase):
    def test_new_foundation_has_no_network_or_legacy_imports(self) -> None:
        violations: list[str] = []
        for package_root in NEW_PACKAGE_ROOTS:
            for path in package_root.rglob("*.py"):
                for module in imported_modules(path):
                    if module.startswith(FORBIDDEN_IMPORT_PREFIXES):
                        violations.append(f"{path.relative_to(REPOSITORY_ROOT)} -> {module}")
        self.assertEqual([], violations)

    def test_core_and_exchange_models_do_not_import_sqlite(self) -> None:
        violations: list[str] = []
        for package_name in ("core", "exchange", "grid", "orders"):
            package_root = REPOSITORY_ROOT / "gridtrader" / package_name
            for path in package_root.rglob("*.py"):
                if "sqlite3" in imported_modules(path):
                    violations.append(str(path.relative_to(REPOSITORY_ROOT)))
        self.assertEqual([], violations)

    def test_build_configuration_declares_required_development_tools(self) -> None:
        config = tomllib.loads(
            (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )
        dev_dependencies = config["project"]["optional-dependencies"]["dev"]
        for required_tool in ("pytest", "mypy", "ruff"):
            self.assertTrue(
                any(dependency.startswith(required_tool) for dependency in dev_dependencies),
                required_tool,
            )
        self.assertEqual(["tests"], config["tool"]["pytest"]["ini_options"]["testpaths"])
        self.assertEqual(
            ["migrations/*.sql"],
            config["tool"]["setuptools"]["package-data"][
                "gridtrader.storage.sqlite"
            ],
        )
        package_patterns = config["tool"]["setuptools"]["packages"]["find"]["include"]
        self.assertNotIn("gridtrader*", package_patterns)


if __name__ == "__main__":
    unittest.main()
