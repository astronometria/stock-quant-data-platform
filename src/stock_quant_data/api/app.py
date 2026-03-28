"""
FastAPI application factory.

This API is intentionally small in v1:
- read-only
- focused on serving published release metadata
- easy to extend with universe / symbols / prices endpoints later
"""

from fastapi import FastAPI

from stock_quant_data.api.v1.health import router as health_router
from stock_quant_data.api.v1.universes import router as universes_router
from stock_quant_data.api.v1.symbols import router as symbols_router


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    We keep the app factory explicit for future testability and flexibility.
    """
    app = FastAPI(
        title="Stock Quant Data Platform API",
        version="0.1.0",
        description=(
            "Read-only API exposing immutable PIT-aware market data releases."
        ),
    )

    app.include_router(health_router, prefix="/api/v1")
    app.include_router(universes_router, prefix="/api/v1")
    app.include_router(symbols_router, prefix="/api/v1")

    return app


app = create_app()
