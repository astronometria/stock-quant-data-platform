"""
Universe repository.

This module intentionally keeps SQL access small and explicit.
The goal is to make it obvious which queries power the serving API.
"""

from __future__ import annotations

from typing import Any

import duckdb


class UniverseRepository:
    """
    Small repository focused on read access for universe-related queries.

    We keep the repository thin on purpose:
    - no hidden magic
    - explicit SQL
    - easy to test
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Store the active DuckDB connection.

        Parameters
        ----------
        connection:
            An already-open DuckDB connection.
        """
        self._connection = connection

    def list_universes(self) -> list[dict[str, Any]]:
        """
        Return the list of serving universes.

        The serving.universes view is used instead of raw core tables so that
        the API consumes the serving contract directly.
        """
        rows = self._connection.execute(
            """
            SELECT
                universe_id,
                universe_name,
                description,
                created_at,
                historical_instrument_count
            FROM serving.universes
            ORDER BY universe_name
            """
        ).fetchall()

        columns = [
            "universe_id",
            "universe_name",
            "description",
            "created_at",
            "historical_instrument_count",
        ]

        return [dict(zip(columns, row)) for row in rows]
