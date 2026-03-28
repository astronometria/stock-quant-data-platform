"""
Release publication job.

This v1 job:
- verifies that the build DB exists
- creates a new release directory
- copies the full build DB into the release as serving.duckdb
- writes a manifest
- switches the current symlink atomically

This is intentionally simple and robust for the first serving release.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import shutil

from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db
from stock_quant_data.db.publish import (
    create_release_dir,
    switch_current_release_symlink,
    utc_release_id,
    write_manifest,
)

LOGGER = logging.getLogger(__name__)


def _collect_row_counts() -> dict[str, int]:
    """
    Collect minimal row counts from important core tables.

    This gives the release manifest quick visibility into what was published.
    """
    connection = connect_build_db()

    try:
        table_names = [
            "core.instrument",
            "core.symbol_reference_history",
            "core.listing_status_history",
            "core.universe_definition",
            "core.universe_membership_history",
        ]

        results: dict[str, int] = {}

        for table_name in table_names:
            count_value = connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            results[table_name] = int(count_value)

        return results
    finally:
        connection.close()


def run_publish_release() -> Path:
    """
    Publish a new immutable serving release.

    Returns
    -------
    Path
        The newly created release directory.
    """
    settings = get_settings()
    build_db_path = settings.build_db_path

    if not build_db_path.exists():
        raise FileNotFoundError(
            f"Build database does not exist: {build_db_path}"
        )

    release_id = utc_release_id()
    release_dir = create_release_dir(release_id=release_id)
    serving_db_path = release_dir / "serving.duckdb"

    LOGGER.info("Creating release directory: %s", release_dir)
    LOGGER.info("Copying build DB to serving DB: %s -> %s", build_db_path, serving_db_path)

    shutil.copy2(build_db_path, serving_db_path)

    row_counts = _collect_row_counts()

    manifest = {
        "release_id": release_id,
        "build_db_path": str(build_db_path),
        "serving_db_path": str(serving_db_path),
        "published_at": datetime.now(timezone.utc).isoformat(),
        "checks_passed": True,
        "row_counts": row_counts,
        "notes": "Initial immutable serving release copied from build database."
    }

    write_manifest(release_dir, manifest)

    checks_path = release_dir / "checks.json"
    checks_path.write_text(
        json.dumps(
            {
                "checks_passed": True,
                "checks": [
                    {
                        "name": "build_db_exists",
                        "status": "pass",
                    },
                    {
                        "name": "serving_db_copied",
                        "status": "pass",
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    switch_current_release_symlink(release_dir)

    LOGGER.info("Published release: %s", release_dir)
    return release_dir
