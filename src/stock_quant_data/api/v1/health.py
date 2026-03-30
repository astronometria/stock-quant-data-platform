"""
Health and release metadata endpoints.
"""

from __future__ import annotations

from pathlib import Path
import json

from fastapi import APIRouter, HTTPException

from stock_quant_data.config.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    """
    Liveness endpoint.
    """
    return {
        "status": "ok",
        "service": "stock-quant-data-platform",
    }


@router.get("/ready")
def ready() -> dict:
    """
    Readiness endpoint.
    """
    settings = get_settings()
    db_exists = settings.current_release_db_path.exists()
    current_target = None

    if settings.current_release_link.exists() or settings.current_release_link.is_symlink():
        try:
            current_target = str(settings.current_release_link.resolve())
        except Exception:
            current_target = None

    return {
        "status": "ready" if db_exists else "not_ready",
        "project_root": str(Path.cwd()),
        "current_release_link": str(settings.current_release_link.resolve().parent / settings.current_release_link.name)
        if settings.current_release_link.exists() or settings.current_release_link.is_symlink()
        else str(settings.current_release_link),
        "current_release_target": current_target,
        "current_release_db_path": str(settings.current_release_db_path.resolve())
        if settings.current_release_db_path.exists()
        else str(settings.current_release_db_path),
        "current_release_db_exists": db_exists,
    }


@router.get("/release")
def release_info() -> dict:
    """
    Return basic information about the currently active release pointer.
    """
    settings = get_settings()
    current_target = None

    if settings.current_release_link.exists() or settings.current_release_link.is_symlink():
        try:
            current_target = str(settings.current_release_link.resolve())
        except Exception:
            current_target = None

    return {
        "project_root": str(Path.cwd()),
        "current_release_link": str(settings.current_release_link.resolve().parent / settings.current_release_link.name)
        if settings.current_release_link.exists() or settings.current_release_link.is_symlink()
        else str(settings.current_release_link),
        "current_release_target": current_target,
        "current_release_db_path": str(settings.current_release_db_path.resolve())
        if settings.current_release_db_path.exists()
        else str(settings.current_release_db_path),
        "current_release_db_exists": settings.current_release_db_path.exists(),
    }


@router.get("/release/checks")
def release_checks() -> dict:
    """
    Return the published checks payload for the current release.

    Primary source:
    - checks.json in the release directory

    Fallback:
    - serving_release_checks table inside serving.duckdb could be added later
    """
    settings = get_settings()

    if not settings.current_release_link.exists() and not settings.current_release_link.is_symlink():
        raise HTTPException(status_code=503, detail="No published release available")

    checks_path = settings.current_release_link / "checks.json"
    if not checks_path.exists():
        raise HTTPException(
            status_code=503,
            detail="checks.json not found for the current release",
        )

    return json.loads(checks_path.read_text(encoding="utf-8"))
