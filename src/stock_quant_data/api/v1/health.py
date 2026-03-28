"""
Health and metadata endpoints.

These endpoints are intentionally simple but important:
- /health tells us the process is alive
- /ready tells us whether a serving release is actually available
- /release gives the active published release path
"""

from fastapi import APIRouter
from stock_quant_data.config.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    """
    Liveness endpoint.

    This confirms the API process is up.
    It does not guarantee that a release is published.
    """
    return {
        "status": "ok",
        "service": "stock-quant-data-platform",
    }


@router.get("/ready")
def ready() -> dict:
    """
    Readiness endpoint.

    The service is considered 'ready' only if the current release DB exists.
    """
    settings = get_settings()
    db_exists = settings.current_release_db_path.exists()

    return {
        "status": "ready" if db_exists else "not_ready",
        "current_release_db_path": str(settings.current_release_db_path),
        "current_release_db_exists": db_exists,
    }


@router.get("/release")
def release_info() -> dict:
    """
    Return basic information about the currently active release pointer.

    In later versions, this endpoint can also read and expose manifest.json.
    """
    settings = get_settings()

    return {
        "current_release_link": str(settings.current_release_link),
        "current_release_db_path": str(settings.current_release_db_path),
        "current_release_db_exists": settings.current_release_db_path.exists(),
    }
