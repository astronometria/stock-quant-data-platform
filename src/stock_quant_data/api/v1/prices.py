"""
Price endpoints.

These endpoints query only published serving data.
"""

from __future__ import annotations

from datetime import date
from fastapi import APIRouter, HTTPException, Query
import duckdb

from stock_quant_data.db.connections import connect_serving_db_read_only

router = APIRouter(tags=["prices"])


def _resolve_current_symbol_to_instrument(conn, symbol: str) -> tuple | None:
    """
    Resolve the current/open-ended symbol mapping to an instrument.

    Current rule for price endpoints:
    - use the currently active symbol mapping (effective_to IS NULL)
    """
    return conn.execute(
        """
        SELECT
            srh.instrument_id,
            i.primary_ticker,
            i.primary_exchange,
            i.security_type
        FROM symbol_reference_history AS srh
        JOIN instrument AS i
          ON i.instrument_id = srh.instrument_id
        WHERE srh.symbol = ?
          AND srh.effective_to IS NULL
        ORDER BY srh.is_primary DESC, srh.effective_from DESC, srh.symbol_reference_history_id DESC
        LIMIT 1
        """,
        [symbol.upper()],
    ).fetchone()


@router.get("/prices/{symbol}/history")
def get_price_history(
    symbol: str,
    start_date: date = Query(..., description="Start date inclusive, YYYY-MM-DD"),
    end_date: date = Query(..., description="End date inclusive, YYYY-MM-DD"),
) -> dict:
    """
    Return published historical prices for the resolved current symbol.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    conn = connect_serving_db_read_only()
    try:
        try:
            instrument_row = _resolve_current_symbol_to_instrument(conn, symbol)
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="Required price or symbol tables have not been published in the current release",
            )

        if instrument_row is None:
            raise HTTPException(status_code=404, detail="Current symbol mapping not found")

        rows = conn.execute(
            """
            SELECT
                ph.price_history_id,
                ph.instrument_id,
                ph.price_date,
                ph.open,
                ph.high,
                ph.low,
                ph.close,
                ph.adj_close,
                ph.volume,
                ph.source_name
            FROM price_history AS ph
            WHERE ph.instrument_id = ?
              AND ph.price_date >= CAST(? AS DATE)
              AND ph.price_date <= CAST(? AS DATE)
            ORDER BY ph.price_date
            """,
            [instrument_row[0], str(start_date), str(end_date)],
        ).fetchall()

        items = [
            {
                "price_history_id": row[0],
                "instrument_id": row[1],
                "price_date": str(row[2]) if row[2] is not None else None,
                "open": row[3],
                "high": row[4],
                "low": row[5],
                "close": row[6],
                "adj_close": row[7],
                "volume": row[8],
                "source_name": row[9],
            }
            for row in rows
        ]

        return {
            "symbol": symbol.upper(),
            "instrument_id": instrument_row[0],
            "primary_ticker": instrument_row[1],
            "primary_exchange": instrument_row[2],
            "security_type": instrument_row[3],
            "start_date": str(start_date),
            "end_date": str(end_date),
            "count": len(items),
            "items": items,
        }
    finally:
        conn.close()


@router.get("/prices/{symbol}/latest")
def get_latest_price(symbol: str) -> dict:
    """
    Return the latest published price for the resolved current symbol.
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            instrument_row = _resolve_current_symbol_to_instrument(conn, symbol)
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="Required price or symbol tables have not been published in the current release",
            )

        if instrument_row is None:
            raise HTTPException(status_code=404, detail="Current symbol mapping not found")

        row = conn.execute(
            """
            SELECT
                ph.price_history_id,
                ph.instrument_id,
                ph.price_date,
                ph.open,
                ph.high,
                ph.low,
                ph.close,
                ph.adj_close,
                ph.volume,
                ph.source_name
            FROM price_history AS ph
            WHERE ph.instrument_id = ?
            ORDER BY ph.price_date DESC, ph.price_history_id DESC
            LIMIT 1
            """,
            [instrument_row[0]],
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="No published prices found for symbol")

        return {
            "symbol": symbol.upper(),
            "instrument_id": instrument_row[0],
            "primary_ticker": instrument_row[1],
            "primary_exchange": instrument_row[2],
            "security_type": instrument_row[3],
            "price": {
                "price_history_id": row[0],
                "instrument_id": row[1],
                "price_date": str(row[2]) if row[2] is not None else None,
                "open": row[3],
                "high": row[4],
                "low": row[5],
                "close": row[6],
                "adj_close": row[7],
                "volume": row[8],
                "source_name": row[9],
            },
        }
    finally:
        conn.close()
