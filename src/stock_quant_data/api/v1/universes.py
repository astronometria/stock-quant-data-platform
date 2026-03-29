"""
Universe endpoints.

These endpoints are read-only and rely on the published serving database.
That is important because the API must never serve partially-built state.
"""

from fastapi import APIRouter, HTTPException, Query

from stock_quant_data.db.connections import connect_serving_db_read_only
from stock_quant_data.domains.universe.repository import UniverseRepository

router = APIRouter(tags=["universes"])


@router.get("/universes")
def list_universes() -> dict:
    """
    Return the list of published universes from the serving database.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = UniverseRepository(connection)
        items = repository.list_universes()
        return {
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()


@router.get("/universes/{universe_name}/members/as-of")
def get_universe_members_as_of(
    universe_name: str,
    as_of_date: str = Query(..., description="Point-in-time date in YYYY-MM-DD format."),
) -> dict:
    """
    Return PIT-active universe members for a given as_of date.
    """
    try:
        connection = connect_serving_db_read_only()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        repository = UniverseRepository(connection)
        items = repository.get_universe_members_as_of(universe_name, as_of_date)

        if not items:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No active universe members found for universe '{universe_name}' "
                    f"at as_of_date '{as_of_date}'."
                ),
            )

        return {
            "universe_name": universe_name.upper(),
            "as_of_date": as_of_date,
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()
