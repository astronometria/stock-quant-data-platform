"""
Release validation job.

This job executes SQL-first scientific checks against the mutable build DB
before publication.

Design goals:
- keep validation logic explicit and reviewable
- keep SQL as the source of truth for checks
- produce a machine-readable result for release workflows
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import logging

from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.config.settings import get_settings

LOGGER = logging.getLogger(__name__)


@dataclass
class ValidationCheckResult:
    """
    Structured in-memory representation of one validation check result.
    """

    check_name: str
    failed_rows: int

    @property
    def status(self) -> str:
        """Return pass/fail status derived from failed row count."""
        return "pass" if self.failed_rows == 0 else "fail"

    def to_dict(self) -> dict:
        """Convert the result to a JSON-serializable dictionary."""
        return {
            "name": self.check_name,
            "failed_rows": self.failed_rows,
            "status": self.status,
        }


@dataclass
class ValidationReport:
    """
    Aggregate validation report returned by the validation job.
    """

    checks_passed: bool
    checks: list[ValidationCheckResult]
    checks_file_path: Path | None = None

    def to_dict(self) -> dict:
        """Convert the report to a JSON-serializable dictionary."""
        return {
            "checks_passed": self.checks_passed,
            "checks_file_path": str(self.checks_file_path) if self.checks_file_path else None,
            "checks": [item.to_dict() for item in self.checks],
        }


def _read_sql_file(path: Path) -> str:
    """
    Read a SQL file as UTF-8 text.
    """
    return path.read_text(encoding="utf-8")


def run_validate_release(write_checks_to_build: bool = True) -> ValidationReport:
    """
    Execute scientific validation checks against the build DB.
    """
    settings = get_settings()
    project_root = settings.project_root
    foundation_checks_sql_path = project_root / "sql" / "checks" / "001_foundation_checks.sql"
    price_checks_sql_path = project_root / "sql" / "checks" / "002_price_checks.sql"
    checks_output_path = project_root / "data" / "build" / "checks_latest.json"

    LOGGER.info("Opening build database for validation")
    connection = connect_build_db()

    try:
        LOGGER.info("Executing validation checks file: %s", foundation_checks_sql_path)
        foundation_rows = connection.execute(
            _read_sql_file(foundation_checks_sql_path)
        ).fetchall()

        LOGGER.info("Executing validation checks file: %s", price_checks_sql_path)
        price_rows = connection.execute(
            _read_sql_file(price_checks_sql_path)
        ).fetchall()
    finally:
        connection.close()

    results: list[ValidationCheckResult] = []

    for row in foundation_rows + price_rows:
        results.append(
            ValidationCheckResult(
                check_name=str(row[0]),
                failed_rows=int(row[1]),
            )
        )

    checks_passed = all(item.failed_rows == 0 for item in results)

    report = ValidationReport(
        checks_passed=checks_passed,
        checks=results,
        checks_file_path=checks_output_path if write_checks_to_build else None,
    )

    if write_checks_to_build:
        checks_output_path.parent.mkdir(parents=True, exist_ok=True)
        checks_output_path.write_text(
            json.dumps(report.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        LOGGER.info("Validation report written to: %s", checks_output_path)

    if not checks_passed:
        failing = [item.check_name for item in results if item.failed_rows > 0]
        raise RuntimeError(
            "Blocking scientific validation failed. "
            f"Failing checks: {', '.join(failing)}"
        )

    LOGGER.info("All blocking scientific validation checks passed")
    return report
