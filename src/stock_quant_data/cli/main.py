"""
Minimal CLI entrypoint with basic command dispatch.

Important design rule:
- CLI stays thin
- jobs do the real work
- SQL remains the source of truth for schema and projections
"""

from __future__ import annotations

import logging
import sys
from textwrap import dedent

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_db import run_init_db
from stock_quant_data.jobs.publish_release import run_publish_release


def _print_help() -> None:
    """
    Print CLI help text.

    The list remains intentionally short to avoid a script explosion.
    """
    print(
        dedent(
            """
            stock-quant-data-platform CLI

            Commands:
              sq init-db
              sq publish-release
              sq help
            """
        ).strip()
    )


def main() -> None:
    """
    Minimal command dispatcher.

    Supported v1 commands:
    - init-db
    - publish-release
    """
    configure_logging(level=logging.INFO)

    if len(sys.argv) <= 1:
        _print_help()
        raise SystemExit(0)

    command = sys.argv[1].strip().lower()

    if command == "help":
        _print_help()
        raise SystemExit(0)

    if command == "init-db":
        run_init_db()
        print("DONE: init-db")
        raise SystemExit(0)

    if command == "publish-release":
        release_dir = run_publish_release()
        print(f"DONE: publish-release -> {release_dir}")
        raise SystemExit(0)

    print(f"ERROR: unknown command '{command}'")
    _print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
