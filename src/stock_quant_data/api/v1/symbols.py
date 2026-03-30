"""
Symbol endpoints.

These endpoints query only published serving data.
"""

from __future__ import annotations

from datetime import date
from fastapi import APIRouter, HTTPException, Query
import duckdb

from stock_quant_data.db.connections import connect_serving_db_read_only

router = APIRouter(tags=["symbols"])


@router.get("/symbols/{symbol}")
def get_symbol(symbol: str) -> dict:
    """
    Return a summary for the given symbol from the published serving DB.

    Current behavior:
    - returns all matching historical rows for that symbol
    - sorted by effective_from
    - includes linked instrument metadata
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            rows = conn.execute(
                """
                SELECT
                    srh.symbol_reference_history_id,
                    srh.instrument_id,
                    srh.symbol,
                    srh.exchange,
                    srh.is_primary,
                    srh.effective_from,
                    srh.effective_to,
                    i.security_type,
                    i.company_id,
                    i.primary_ticker,
                    i.primary_exchange
                FROM symbol_reference_history AS srh
                JOIN instrument AS i
                  ON i.instrument_id = srh.instrument_id
                WHERE srh.symbol = ?
                ORDER BY srh.effective_from, srh.symbol_reference_history_id
                """,
                [symbol.upper()],
            ).fetchall()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="symbol_reference_history has not been published in the current release",
            )

        if not rows:
            raise HTTPException(status_code=404, detail="Symbol not found")

        items = [
            {
                "symbol_reference_history_id": row[0],
                "instrument_id": row[1],
                "symbol": row[2],
                "exchange": row[3],
                "is_primary": row[4],
                "effective_from": str(row[5]) if row[5] is not None else None,
                "effective_to": str(row[6]) if row[6] is not None else None,
                "security_type": row[7],
                "company_id": row[8],
                "primary_ticker": row[9],
                "primary_exchange": row[10],
            }
            for row in rows
        ]

        return {
            "symbol": symbol.upper(),
            "count": len(items),
            "items": items,
        }
    finally:
        conn.close()


@router.get("/symbols/{symbol}/history")
def get_symbol_history(symbol: str) -> dict:
    """
    Alias endpoint for symbol history.
    """
    return get_symbol(symbol=symbol)


@router.get("/symbols/{symbol}/resolve")
def resolve_symbol_as_of(
    symbol: str,
    as_of_date: date = Query(..., description="Resolve symbol at YYYY-MM-DD"),
) -> dict:
    """
    Resolve a symbol to an instrument as of a specific date.

    PIT rule:
        effective_from <= as_of_date
        AND (effective_to IS NULL OR effective_to > as_of_date)
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            rows = conn.execute(
                """
                SELECT
                    srh.symbol_reference_history_id,
                    srh.instrument_id,
                    srh.symbol,
                    srh.exchange,
                    srh.is_primary,
                    srh.effective_from,
                    srh.effective_to,
                    i.security_type,
                    i.company_id,
                    i.primary_ticker,
                    i.primary_exchange
                FROM symbol_reference_history AS srh
                JOIN instrument AS i
                  ON i.instrument_id = srh.instrument_id
                WHERE srh.symbol = ?
                  AND srh.effective_from <= CAST(? AS DATE)
                  AND (srh.effective_to IS NULL OR srh.effective_to > CAST(? AS DATE))
                ORDER BY srh.is_primary DESC, srh.effective_from DESC, srh.symbol_reference_history_id DESC
                """,
                [symbol.upper(), str(as_of_date), str(as_of_date)],
            ).fetchall()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="symbol_reference_history has not been published in the current release",
            )

        if not rows:
            raise HTTPException(status_code=404, detail="No symbol resolution found for this as_of_date")

        if len(rows) > 1:
            raise HTTPException(
                status_code=409,
                detail="Ambiguous symbol resolution for the requested as_of_date",
            )

        row = rows[0]
        return {
            "symbol": symbol.upper(),
            "as_of_date": str(as_of_date),
            "symbol_reference_history_id": row[0],
            "instrument_id": row[1],
            "exchange": row[3],
            "is_primary": row[4],
            "effective_from": str(row[5]) if row[5] is not None else None,
            "effective_to": str(row[6]) if row[6] is not None else None,
            "security_type": row[7],
            "company_id": row[8],
            "primary_ticker": row[9],
            "primary_exchange": row[10],
        }
    finally:
        conn.close()


@router.get("/symbols/{symbol}/listing-status-history")
def get_symbol_listing_status_history(symbol: str) -> dict:
    """
    Return listing status history rows for a given symbol.

    This endpoint is separate from symbol_reference_history:
    - symbol_reference_history answers identity mapping through time
    - listing_status_history answers listing lifecycle through time
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            rows = conn.execute(
                """
                SELECT
                    listing_status_history_id,
                    instrument_id,
                    symbol,
                    listing_status,
                    event_type,
                    effective_from,
                    effective_to,
                    source_name
                FROM listing_status_history
                WHERE symbol = ?
                ORDER BY effective_from, listing_status_history_id
                """,
                [symbol.upper()],
            ).fetchall()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="listing_status_history has not been published in the current release",
            )

        if not rows:
            raise HTTPException(status_code=404, detail="Listing status history not found for symbol")

        items = [
            {
                "listing_status_history_id": row[0],
                "instrument_id": row[1],
                "symbol": row[2],
                "listing_status": row[3],
                "event_type": row[4],
                "effective_from": str(row[5]) if row[5] is not None else None,
                "effective_to": str(row[6]) if row[6] is not None else None,
                "source_name": row[7],
            }
            for row in rows
        ]

        return {
            "symbol": symbol.upper(),
            "count": len(items),
            "items": items,
        }
    finally:
        conn.close()
