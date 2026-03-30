"""
Validate build-database scientific invariants before publication.

Current scope:
- universe_membership_history interval validity
- universe_membership_history overlap detection

Design rule:
publication must fail if scientific invariants are violated.
"""

from __future__ import annotations

from pathlib import Path
import json
import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def fetch_scalar(conn, sql_text: str):
    """
    Execute a scalar query and return the first column of the first row.
    """
    return conn.execute(sql_text).fetchone()[0]


def fetch_rows(conn, sql_text: str) -> list[tuple]:
    """
    Execute a query and return all rows.
    """
    return conn.execute(sql_text).fetchall()


def build_checks_payload() -> dict:
    """
    Compute the validation payload for the current build database.
    """
    conn = connect_build_db()
    try:
        # --------------------------------------------------------------
        # Check 1:
        # effective_to must not be earlier than effective_from.
        # --------------------------------------------------------------
        invalid_interval_count = fetch_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM universe_membership_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            """,
        )

        invalid_interval_examples = fetch_rows(
            conn,
            """
            SELECT
                universe_membership_history_id,
                universe_id,
                instrument_id,
                membership_status,
                effective_from,
                effective_to
            FROM universe_membership_history
            WHERE effective_to IS NOT NULL
              AND effective_to < effective_from
            ORDER BY universe_id, instrument_id, effective_from
            LIMIT 20
            """,
        )

        # --------------------------------------------------------------
        # Check 2:
        # no overlapping intervals for same (universe_id, instrument_id).
        #
        # Overlap rule:
        #   a.effective_from < COALESCE(b.effective_to, far_future)
        #   AND b.effective_from < COALESCE(a.effective_to, far_future)
        #
        # We also ensure a.id < b.id to avoid duplicate pair reporting.
        # --------------------------------------------------------------
        overlap_rows = fetch_rows(
            conn,
            """
            SELECT
                a.universe_membership_history_id AS left_id,
                b.universe_membership_history_id AS right_id,
                a.universe_id,
                a.instrument_id,
                a.effective_from AS left_effective_from,
                a.effective_to   AS left_effective_to,
                b.effective_from AS right_effective_from,
                b.effective_to   AS right_effective_to
            FROM universe_membership_history AS a
            JOIN universe_membership_history AS b
              ON a.universe_id = b.universe_id
             AND a.instrument_id = b.instrument_id
             AND a.universe_membership_history_id < b.universe_membership_history_id
             AND a.effective_from < COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from < COALESCE(a.effective_to, DATE '9999-12-31')
            ORDER BY a.universe_id, a.instrument_id, a.effective_from, b.effective_from
            LIMIT 100
            """,
        )

        overlap_count = len(overlap_rows)

        checks = {
            "invalid_interval_count": invalid_interval_count,
            "invalid_interval_examples": [
                {
                    "universe_membership_history_id": row[0],
                    "universe_id": row[1],
                    "instrument_id": row[2],
                    "membership_status": row[3],
                    "effective_from": str(row[4]) if row[4] is not None else None,
                    "effective_to": str(row[5]) if row[5] is not None else None,
                }
                for row in invalid_interval_examples
            ],
            "overlap_count": overlap_count,
            "overlap_examples": [
                {
                    "left_id": row[0],
                    "right_id": row[1],
                    "universe_id": row[2],
                    "instrument_id": row[3],
                    "left_effective_from": str(row[4]) if row[4] is not None else None,
                    "left_effective_to": str(row[5]) if row[5] is not None else None,
                    "right_effective_from": str(row[6]) if row[6] is not None else None,
                    "right_effective_to": str(row[7]) if row[7] is not None else None,
                }
                for row in overlap_rows
            ],
        }

        checks_passed = (
            checks["invalid_interval_count"] == 0
            and checks["overlap_count"] == 0
        )

        return {
            "checks_passed": checks_passed,
            "checks": checks,
        }
    finally:
        conn.close()


def write_checks_file(path: Path, payload: dict) -> None:
    """
    Write validation payload to a JSON file.
    """
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run() -> None:
    """
    Run validation and print the result.

    Exit code stays 0 here because the CLI wrapper is intentionally thin.
    The publish job itself will enforce blocking behavior.
    """
    configure_logging()
    LOGGER.info("validate-release started")

    payload = build_checks_payload()

    print(payload)

    LOGGER.info(
        "validate-release finished | checks_passed=%s",
        payload["checks_passed"],
    )


if __name__ == "__main__":
    run()
