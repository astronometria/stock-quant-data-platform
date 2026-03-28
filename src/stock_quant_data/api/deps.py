"""
FastAPI dependencies.

We centralize small reusable dependencies here so that endpoints stay thin.
"""

from stock_quant_data.config.settings import get_settings


def get_app_settings():
    """Expose cached application settings to FastAPI endpoints."""
    return get_settings()
