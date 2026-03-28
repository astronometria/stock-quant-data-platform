"""
Application settings.

Important design note:
- Build DB and serving release paths are intentionally separate.
- This helps prevent consumers from reading partially-built state.
- The API should only read from a published, immutable release.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Centralized runtime settings.

    We keep these settings small on purpose in v1.
    Future additions should remain explicit and well-documented.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    sq_build_db_path: str = "data/build/market_build.duckdb"
    sq_releases_root: str = "data/releases"
    sq_current_release_link: str = "data/current"
    sq_env: str = "dev"
    sq_api_host: str = "127.0.0.1"
    sq_api_port: int = 8000

    @property
    def build_db_path(self) -> Path:
        """Return the build database path as a Path object."""
        return Path(self.sq_build_db_path)

    @property
    def releases_root(self) -> Path:
        """Return the release root directory as a Path object."""
        return Path(self.sq_releases_root)

    @property
    def current_release_link(self) -> Path:
        """Return the 'current release' symlink path as a Path object."""
        return Path(self.sq_current_release_link)

    @property
    def current_release_db_path(self) -> Path:
        """
        Resolve the serving database path for the active release.

        Expected layout:
            data/current/serving.duckdb

        This path is what the API will try to open in read-only mode.
        """
        return self.current_release_link / "serving.duckdb"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings factory.

    Using a cache avoids reparsing environment variables for every request.
    """
    return Settings()
