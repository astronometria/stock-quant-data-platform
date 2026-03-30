"""
Instrument endpoints.

These endpoints query only published serving data.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
import duckdb

from stock_quant_data.db.connections import connect_serving_db_read_only

router = APIRouter(tags=["instruments"])


@router.get("/instruments/{instrument_id}")
def get_instrument(instrument_id: int) -> dict:
    """
    Return one published instrument by stable instrument_id.
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    security_type,
                    company_id,
                    primary_ticker,
                    primary_exchange,
                    created_at
                FROM instrument
                WHERE instrument_id = ?
                """,
                [instrument_id],
            ).fetchone()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="instrument has not been published in the current release",
            )

        if row is None:
            raise HTTPException(status_code=404, detail="Instrument not found")

        return {
            "instrument_id": row[0],
            "security_type": row[1],
            "company_id": row[2],
            "primary_ticker": row[3],
            "primary_exchange": row[4],
            "created_at": str(row[5]) if row[5] is not None else None,
        }
    finally:
        conn.close()
