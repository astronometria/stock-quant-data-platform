"""
Database initialization job.

This job creates the minimum schemas, tables, and views needed for the
scientific foundation of the platform.

Design notes:
- idempotent for iterative development
- SQL-first
- job remains intentionally thin
"""

from __future__ import annotations

from pathlib import Path
import logging

from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)


def _read_sql_file(path: Path) -> str:
    """
    Read a SQL file as UTF-8 text.

    This helper stays tiny and explicit to keep SQL loading transparent.
    """
    return path.read_text(encoding="utf-8")


def run_init_db() -> None:
    """
    Initialize the build database with the core scientific foundation schema.

    Steps:
    1. open the mutable build DB
    2. execute DDL
    3. execute serving views
    4. seed a small default universe definition set
    """
    project_root = Path(__file__).resolve().parents[3]
    ddl_path = project_root / "sql" / "ddl" / "001_core_foundation.sql"
    views_path = project_root / "sql" / "views" / "001_serving_universes.sql"

    LOGGER.info("Opening build database")
    connection = connect_build_db()

    try:
        LOGGER.info("Executing DDL file: %s", ddl_path)
        connection.execute(_read_sql_file(ddl_path))

        LOGGER.info("Executing serving views file: %s", views_path)
        connection.execute(_read_sql_file(views_path))

        LOGGER.info("Seeding default universe definitions")
        connection.execute(
            """
            INSERT INTO core.universe_definition (
                universe_id,
                universe_name,
                description
            )
            SELECT * FROM (
                VALUES
                    (
                        'UNIV_US_LISTED_COMMON_STOCKS',
                        'US_LISTED_COMMON_STOCKS',
                        'Common stocks historically observed in the US listed universe.'
                    ),
                    (
                        'UNIV_US_LISTED_ETFS',
                        'US_LISTED_ETFS',
                        'ETFs historically observed in the US listed universe.'
                    ),
                    (
                        'UNIV_NASDAQ_LISTED',
                        'NASDAQ_LISTED',
                        'Securities historically observed as listed on NASDAQ.'
                    ),
                    (
                        'UNIV_NYSE_LISTED',
                        'NYSE_LISTED',
                        'Securities historically observed as listed on NYSE.'
                    )
            ) AS seed_data(universe_id, universe_name, description)
            WHERE NOT EXISTS (
                SELECT 1
                FROM core.universe_definition existing
                WHERE existing.universe_id = seed_data.universe_id
            )
            """
        )

        LOGGER.info("Database initialization completed successfully")
    finally:
        connection.close()
