"""
Database initialization job.

This job creates the minimum schemas, tables, views, and seed data needed
for the scientific foundation of the platform.

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
    """
    return path.read_text(encoding="utf-8")


def run_init_db() -> None:
    """
    Initialize the build database with the core scientific foundation schema.
    """
    project_root = Path(__file__).resolve().parents[3]

    ddl_foundation_path = project_root / "sql" / "ddl" / "001_core_foundation.sql"
    ddl_symbols_seed_path = project_root / "sql" / "ddl" / "002_symbols_seed.sql"
    ddl_prices_foundation_path = project_root / "sql" / "ddl" / "003_prices_foundation.sql"
    ddl_prices_seed_path = project_root / "sql" / "ddl" / "004_prices_seed.sql"
    ddl_raw_prices_foundation_path = project_root / "sql" / "ddl" / "005_raw_prices_foundation.sql"
    ddl_raw_stooq_prices_foundation_path = project_root / "sql" / "ddl" / "006_raw_stooq_prices_foundation.sql"

    views_universes_path = project_root / "sql" / "views" / "001_serving_universes.sql"
    views_symbols_path = project_root / "sql" / "views" / "002_api_symbols.sql"
    views_universe_snapshots_path = project_root / "sql" / "views" / "003_api_universe_snapshots.sql"
    views_listing_status_path = project_root / "sql" / "views" / "004_api_listing_status.sql"
    views_prices_path = project_root / "sql" / "views" / "005_api_prices.sql"

    LOGGER.info("Opening build database")
    connection = connect_build_db()

    try:
        LOGGER.info("Executing DDL file: %s", ddl_foundation_path)
        connection.execute(_read_sql_file(ddl_foundation_path))

        LOGGER.info("Executing DDL file: %s", ddl_prices_foundation_path)
        connection.execute(_read_sql_file(ddl_prices_foundation_path))

        LOGGER.info("Executing DDL file: %s", ddl_raw_prices_foundation_path)
        connection.execute(_read_sql_file(ddl_raw_prices_foundation_path))

        LOGGER.info("Executing DDL file: %s", ddl_raw_stooq_prices_foundation_path)
        connection.execute(_read_sql_file(ddl_raw_stooq_prices_foundation_path))

        LOGGER.info("Executing API views file: %s", views_universes_path)
        connection.execute(_read_sql_file(views_universes_path))

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

        LOGGER.info("Executing symbol seed file: %s", ddl_symbols_seed_path)
        connection.execute(_read_sql_file(ddl_symbols_seed_path))

        LOGGER.info("Executing price seed file: %s", ddl_prices_seed_path)
        connection.execute(_read_sql_file(ddl_prices_seed_path))

        LOGGER.info("Executing symbol API views file: %s", views_symbols_path)
        connection.execute(_read_sql_file(views_symbols_path))

        LOGGER.info("Executing universe snapshot API views file: %s", views_universe_snapshots_path)
        connection.execute(_read_sql_file(views_universe_snapshots_path))

        LOGGER.info("Executing listing status API views file: %s", views_listing_status_path)
        connection.execute(_read_sql_file(views_listing_status_path))

        LOGGER.info("Executing prices API views file: %s", views_prices_path)
        connection.execute(_read_sql_file(views_prices_path))

        LOGGER.info("Database initialization completed successfully")
    finally:
        connection.close()
