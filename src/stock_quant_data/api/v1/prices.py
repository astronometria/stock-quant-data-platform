"""
Price endpoints.

These endpoints expose daily EOD price history
from the published serving release.
"""

from fastapi import APIRouter, HTTPException, Query

from stock_quant_data.db.connections import connect_serving_db_read_only
from stock_quant_data.domains.prices.repository import PricesRepository

router = APIRouter(tags=["prices"])


@router.get("/prices/{symbol}/eod")
def get_price_history(
    symbol: str,
    start_date: str = Query(..., description="Inclusive start date in YYYY-MM-DD format."),
    end_date: str = Query(..., description="Inclusive end date in YYYY-MM-DD format."),
) -> dict:
    """
    Return daily EOD price history for a symbol over a date range.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = PricesRepository(connection)
        items = repository.get_price_history(symbol, start_date, end_date)

        if not items:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No price history found for symbol '{symbol}' "
                    f"between '{start_date}' and '{end_date}'."
                ),
            )

        return {
            "symbol": symbol.upper(),
            "start_date": start_date,
            "end_date": end_date,
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()


@router.get("/prices/{symbol}/as-of")
def get_price_as_of(
    symbol: str,
    as_of_date: str = Query(..., description="Point-in-time date in YYYY-MM-DD format."),
) -> dict:
    """
    Return the latest available EOD price on or before the supplied date.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = PricesRepository(connection)
        item = repository.get_price_as_of(symbol, as_of_date)

        if item is None:
            raise HTTPException(
                status_code=404,
                detail=f"No price found for symbol '{symbol}' at as_of_date '{as_of_date}'.",
            )

        return {
            "symbol": symbol.upper(),
            "as_of_date": as_of_date,
            "item": item,
        }
    finally:
        connection.close()
