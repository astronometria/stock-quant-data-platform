"""
Listing endpoints.

These endpoints expose listing lifecycle history
from the published serving release.
"""

from fastapi import APIRouter, HTTPException

from stock_quant_data.db.connections import connect_serving_db_read_only
from stock_quant_data.domains.listings.repository import ListingsRepository

router = APIRouter(tags=["listings"])


@router.get("/symbols/{symbol}/listing-status/history")
def get_listing_status_history(symbol: str) -> dict:
    """
    Return the full listing lifecycle history for a symbol.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = ListingsRepository(connection)
        items = repository.get_listing_status_history(symbol)

        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"Listing status history not found for symbol '{symbol}'.",
            )

        return {
            "symbol": symbol.upper(),
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()
