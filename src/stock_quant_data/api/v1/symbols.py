"""
Symbol endpoints.

These endpoints expose historical and as-of symbol mappings
from the published serving release.
"""

from fastapi import APIRouter, HTTPException, Query

from stock_quant_data.db.connections import connect_serving_db_read_only
from stock_quant_data.domains.symbols.repository import SymbolsRepository

router = APIRouter(tags=["symbols"])


@router.get("/symbols/{symbol}/history")
def get_symbol_history(symbol: str) -> dict:
    """
    Return the full historical mapping timeline for a symbol.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = SymbolsRepository(connection)
        items = repository.get_symbol_history(symbol)

        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"Symbol history not found for symbol '{symbol}'.",
            )

        return {
            "symbol": symbol.upper(),
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()


@router.get("/symbols/{symbol}/as-of")
def get_symbol_as_of(
    symbol: str,
    as_of_date: str = Query(..., description="Point-in-time date in YYYY-MM-DD format."),
) -> dict:
    """
    Return the active mapping for a symbol at a given historical date.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = SymbolsRepository(connection)
        item = repository.get_symbol_as_of(symbol, as_of_date)

        if item is None:
            raise HTTPException(
                status_code=404,
                detail=f"No active symbol mapping found for symbol '{symbol}' at as_of_date '{as_of_date}'.",
            )

        return {
            "symbol": symbol.upper(),
            "as_of_date": as_of_date,
            "item": item,
        }
    finally:
        connection.close()
