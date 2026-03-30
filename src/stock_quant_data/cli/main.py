"""
Thin CLI entrypoint.

Each command dispatches to a dedicated job module.
"""

from __future__ import annotations

import sys
from textwrap import dedent

from stock_quant_data.jobs.init_db import run as run_init_db
from stock_quant_data.jobs.publish_release import run as run_publish_release
from stock_quant_data.jobs.seed_instruments import run as run_seed_instruments
from stock_quant_data.jobs.seed_universes import run as run_seed_universes
from stock_quant_data.jobs.seed_universe_membership_history import (
    run as run_seed_universe_membership_history,
)
from stock_quant_data.jobs.validate_release import run as run_validate_release


def main() -> None:
    """
    Dispatch the requested CLI command.
    """
    argv = sys.argv[1:]

    if not argv:
        print(
            dedent(
                """
                stock-quant-data-platform CLI

                Available commands:
                  sq init-db
                  sq seed-instruments
                  sq seed-universes
                  sq seed-universe-membership-history
                  sq validate-release
                  sq publish-release
                """
            ).strip()
        )
        raise SystemExit(0)

    cmd = argv[0].strip().lower()

    if cmd == "init-db":
        run_init_db()
        raise SystemExit(0)

    if cmd == "seed-instruments":
        run_seed_instruments()
        raise SystemExit(0)

    if cmd == "seed-universes":
        run_seed_universes()
        raise SystemExit(0)

    if cmd == "seed-universe-membership-history":
        run_seed_universe_membership_history()
        raise SystemExit(0)

    if cmd == "validate-release":
        run_validate_release()
        raise SystemExit(0)

    if cmd == "publish-release":
        run_publish_release()
        raise SystemExit(0)

    print(f"Unknown command: {cmd}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
