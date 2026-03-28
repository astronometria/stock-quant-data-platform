"""
Universe endpoints.

These endpoints are read-only and rely on the published serving database.
That is important because the API must never serve partially-built state.
"""

from fastapi import APIRouter
from stock_quant_data.db.connections import connect_serving_db_read_only
from stock_quant_data.domains.universe.repository import UniverseRepository

router = APIRouter(tags=["universes"])


@router.get("/universes")
def list_universes() -> dict:
    """
    Return the list of published universes from the serving database.

    Response shape is intentionally simple in v1:
    - total count
    - items list
    """
    connection = connect_serving_db_read_only()

    try:
        repository = UniverseRepository(connection)
        items = repository.list_universes()
        return {
            "count": len(items),
            "items": items,
        }
    finally:
        connection.close()
