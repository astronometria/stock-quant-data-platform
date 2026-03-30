"""
Minimal CLI entrypoint with basic command dispatch.

Important design rule:
- CLI stays thin
- jobs do the real work
- SQL remains the source of truth for schema and projections
"""

from __future__ import annotations

import json
import logging
import sys
from textwrap import dedent

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.jobs.init_db import run_init_db
from stock_quant_data.jobs.publish_release import run_publish_release
from stock_quant_data.jobs.validate_release import run_validate_release
from stock_quant_data.jobs.ingest_raw_prices_csv import run_ingest_raw_prices_csv
from stock_quant_data.jobs.ingest_raw_prices_stooq_dir import run_ingest_raw_prices_stooq_dir
from stock_quant_data.jobs.ingest_raw_prices_yfinance_dir import run_ingest_raw_prices_yfinance_dir
from stock_quant_data.jobs.build_core_prices import run_build_core_prices


def _print_help() -> None:
    print(
        dedent(
            """
            stock-quant-data-platform CLI

            Commands:
              sq init-db
              sq validate-release
              sq publish-release
              sq ingest-raw-prices-csv <csv_path>
              sq ingest-raw-prices-stooq-dir <root_dir>
              sq ingest-raw-prices-yfinance-dir <root_dir>
              sq build-core-prices
              sq help
            """
        ).strip()
    )


def main() -> None:
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

    if command == "validate-release":
        report = run_validate_release(write_checks_to_build=True)
        print(f"DONE: validate-release -> checks_passed={report.checks_passed}")
        raise SystemExit(0)

    if command == "publish-release":
        release_dir = run_publish_release()
        print(f"DONE: publish-release -> {release_dir}")
        raise SystemExit(0)

    if command == "ingest-raw-prices-csv":
        if len(sys.argv) < 3:
            print("ERROR: missing csv_path")
            print("Usage: sq ingest-raw-prices-csv <csv_path>")
            raise SystemExit(1)
        result = run_ingest_raw_prices_csv(sys.argv[2])
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(0)

    if command == "ingest-raw-prices-stooq-dir":
        if len(sys.argv) < 3:
            print("ERROR: missing root_dir")
            print("Usage: sq ingest-raw-prices-stooq-dir <root_dir>")
            raise SystemExit(1)
        result = run_ingest_raw_prices_stooq_dir(sys.argv[2])
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(0)

    if command == "ingest-raw-prices-yfinance-dir":
        if len(sys.argv) < 3:
            print("ERROR: missing root_dir")
            print("Usage: sq ingest-raw-prices-yfinance-dir <root_dir>")
            raise SystemExit(1)
        result = run_ingest_raw_prices_yfinance_dir(sys.argv[2])
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(0)

    if command == "build-core-prices":
        result = run_build_core_prices()
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(0)

    print(f"ERROR: unknown command '{command}'")
    _print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
