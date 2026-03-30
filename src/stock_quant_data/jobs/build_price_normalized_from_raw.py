"""
Build the unified normalized daily price staging table from raw source tables.

SQL-first design:
- rebuild normalized table from scratch
- generate normalized ids with ROW_NUMBER instead of relying on raw ids
- keep source_row_id for lineage, but not as the PK
"""

from __future__ import annotations

import logging

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

STOOQ_ID_OFFSET = 1_000_000_000_000
YAHOO_ID_OFFSET = 2_000_000_000_000


def run() -> None:
    configure_logging()
    LOGGER.info("build-price-normalized-from-raw started")

    run_init_price_raw_tables()

    conn = connect_build_db()
    try:
        conn.execute("DROP TABLE IF EXISTS price_source_daily_normalized")

        conn.execute(
            """
            CREATE TABLE price_source_daily_normalized (
                normalized_price_id BIGINT PRIMARY KEY,
                source_name VARCHAR NOT NULL,
                source_row_id BIGINT NOT NULL,
                raw_symbol VARCHAR NOT NULL,
                instrument_id BIGINT,
                price_date DATE NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                adj_close DOUBLE,
                volume BIGINT NOT NULL,
                symbol_resolution_status VARCHAR NOT NULL,
                normalization_notes VARCHAR,
                normalized_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            f"""
            INSERT INTO price_source_daily_normalized (
                normalized_price_id,
                source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes
            )
            WITH staged AS (
                SELECT
                    rs.raw_stooq_id AS source_row_id,
                    rs.raw_symbol,
                    srh.instrument_id,
                    rs.price_date,
                    rs.open,
                    rs.high,
                    rs.low,
                    rs.close,
                    rs.close AS adj_close,
                    CAST(ROUND(COALESCE(rs.raw_volume, 0)) AS BIGINT) AS volume,
                    CASE
                        WHEN srh.instrument_id IS NOT NULL THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    CASE
                        WHEN srh.instrument_id IS NOT NULL THEN 'resolved via symbol_reference_history'
                        ELSE 'no matching open-ended symbol mapping found'
                    END AS normalization_notes,
                    ROW_NUMBER() OVER (
                        ORDER BY rs.raw_symbol, rs.price_date, rs.source_file, rs.raw_time, rs.raw_ticker
                    ) AS rn
                FROM price_source_daily_raw_stooq AS rs
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol = rs.raw_symbol
                 AND srh.effective_to IS NULL
            )
            SELECT
                {STOOQ_ID_OFFSET} + rn AS normalized_price_id,
                'stooq' AS source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes
            FROM staged
            """
        )

        conn.execute(
            f"""
            INSERT INTO price_source_daily_normalized (
                normalized_price_id,
                source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes
            )
            WITH staged AS (
                SELECT
                    ry.raw_yahoo_id AS source_row_id,
                    ry.raw_symbol,
                    srh.instrument_id,
                    ry.price_date,
                    ry.open,
                    ry.high,
                    ry.low,
                    ry.close,
                    ry.adj_close,
                    ry.volume,
                    CASE
                        WHEN srh.instrument_id IS NOT NULL THEN 'RESOLVED'
                        ELSE 'UNRESOLVED'
                    END AS symbol_resolution_status,
                    CASE
                        WHEN srh.instrument_id IS NOT NULL THEN 'resolved via symbol_reference_history'
                        ELSE 'no matching open-ended symbol mapping found'
                    END AS normalization_notes,
                    ROW_NUMBER() OVER (
                        ORDER BY ry.raw_symbol, ry.price_date, ry.raw_yahoo_id
                    ) AS rn
                FROM price_source_daily_raw_yahoo AS ry
                LEFT JOIN symbol_reference_history AS srh
                  ON srh.symbol = ry.raw_symbol
                 AND srh.effective_to IS NULL
            )
            SELECT
                {YAHOO_ID_OFFSET} + rn AS normalized_price_id,
                'yahoo' AS source_name,
                source_row_id,
                raw_symbol,
                instrument_id,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                symbol_resolution_status,
                normalization_notes
            FROM staged
            """
        )

        normalized_count = conn.execute(
            "SELECT COUNT(*) FROM price_source_daily_normalized"
        ).fetchone()[0]

        unresolved_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM price_source_daily_normalized
            WHERE symbol_resolution_status <> 'RESOLVED'
            """
        ).fetchone()[0]

        by_source = conn.execute(
            """
            SELECT source_name, COUNT(*)
            FROM price_source_daily_normalized
            GROUP BY source_name
            ORDER BY source_name
            """
        ).fetchall()

        print(
            {
                "status": "ok",
                "job": "build-price-normalized-from-raw",
                "price_source_daily_normalized_count": normalized_count,
                "unresolved_symbol_count": unresolved_count,
                "rows_by_source": by_source,
            }
        )
    finally:
        conn.close()

    LOGGER.info("build-price-normalized-from-raw finished")


if __name__ == "__main__":
    run()
