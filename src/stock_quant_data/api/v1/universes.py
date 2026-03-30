"""
Universe endpoints.

These endpoints are read-only and query only the published serving DB.
"""

from __future__ import annotations

from datetime import date
from fastapi import APIRouter, HTTPException, Query
import duckdb

from stock_quant_data.db.connections import connect_serving_db_read_only

router = APIRouter(tags=["universes"])


@router.get("/universes")
def list_universes() -> dict:
    """
    Return all published universe definitions.
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            rows = conn.execute(
                """
                SELECT
                    universe_id,
                    universe_name,
                    description,
                    created_at
                FROM universe_definition
                ORDER BY universe_name
                """
            ).fetchall()
        except duckdb.CatalogException:
            return {
                "count": 0,
                "items": [],
                "published_table_available": False,
            }

        items = [
            {
                "universe_id": row[0],
                "universe_name": row[1],
                "description": row[2],
                "created_at": str(row[3]) if row[3] is not None else None,
            }
            for row in rows
        ]

        return {
            "count": len(items),
            "items": items,
            "published_table_available": True,
        }
    finally:
        conn.close()


@router.get("/universes/{universe_name}")
def get_universe(universe_name: str) -> dict:
    """
    Return one published universe definition by logical name.
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            row = conn.execute(
                """
                SELECT
                    universe_id,
                    universe_name,
                    description,
                    created_at
                FROM universe_definition
                WHERE universe_name = ?
                """,
                [universe_name],
            ).fetchone()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="universe_definition has not been published in the current release",
            )

        if row is None:
            raise HTTPException(status_code=404, detail="Universe not found")

        return {
            "universe_id": row[0],
            "universe_name": row[1],
            "description": row[2],
            "created_at": str(row[3]) if row[3] is not None else None,
        }
    finally:
        conn.close()


@router.get("/universes/{universe_name}/members")
def get_universe_members_as_of(
    universe_name: str,
    as_of_date: date = Query(..., description="Universe snapshot date in YYYY-MM-DD format"),
) -> dict:
    """
    Return published members of a universe as of a specific date.

    PIT rule:
        effective_from <= as_of_date
        AND (effective_to IS NULL OR effective_to > as_of_date)
    """
    conn = connect_serving_db_read_only()
    try:
        try:
            universe_row = conn.execute(
                """
                SELECT universe_id, universe_name, description, created_at
                FROM universe_definition
                WHERE universe_name = ?
                """,
                [universe_name],
            ).fetchone()
        except duckdb.CatalogException:
            raise HTTPException(
                status_code=503,
                detail="Required universe tables have not been published in the current release",
            )

        if universe_row is None:
            raise HTTPException(status_code=404, detail="Universe not found")

        rows = conn.execute(
            """
            SELECT
                umh.universe_membership_history_id,
                umh.instrument_id,
                i.primary_ticker,
                i.primary_exchange,
                i.security_type,
                umh.membership_status,
                umh.effective_from,
                umh.effective_to,
                umh.source_name
            FROM universe_membership_history AS umh
            JOIN instrument AS i
              ON i.instrument_id = umh.instrument_id
            WHERE umh.universe_id = ?
              AND umh.effective_from <= CAST(? AS DATE)
              AND (umh.effective_to IS NULL OR umh.effective_to > CAST(? AS DATE))
            ORDER BY i.primary_ticker, umh.instrument_id
            """,
            [universe_row[0], str(as_of_date), str(as_of_date)],
        ).fetchall()

        items = [
            {
                "universe_membership_history_id": row[0],
                "instrument_id": row[1],
                "primary_ticker": row[2],
                "primary_exchange": row[3],
                "security_type": row[4],
                "membership_status": row[5],
                "effective_from": str(row[6]) if row[6] is not None else None,
                "effective_to": str(row[7]) if row[7] is not None else None,
                "source_name": row[8],
            }
            for row in rows
        ]

        return {
            "universe": {
                "universe_id": universe_row[0],
                "universe_name": universe_row[1],
                "description": universe_row[2],
                "created_at": str(universe_row[3]) if universe_row[3] is not None else None,
            },
            "as_of_date": str(as_of_date),
            "count": len(items),
            "items": items,
        }
    finally:
        conn.close()
