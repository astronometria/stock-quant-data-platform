"""
Main CLI entrypoint for stock-quant-data-platform.

This module keeps CLI wiring explicit and simple.
Each command maps to one job runner.
"""

from __future__ import annotations

import sys

from stock_quant_data.jobs.build_high_priority_unresolved_symbol_probe import (
    run as run_build_high_priority_unresolved_symbol_probe,
)
from stock_quant_data.jobs.build_price_history_from_raw import run as run_build_price_history_from_raw
from stock_quant_data.jobs.build_price_normalized_from_raw import run as run_build_price_normalized_from_raw
from stock_quant_data.jobs.build_stooq_symbol_normalization_map import run as run_build_stooq_symbol_normalization_map
from stock_quant_data.jobs.build_symbol_manual_override_map import run as run_build_symbol_manual_override_map
from stock_quant_data.jobs.build_symbol_reference_candidates_from_unresolved_stooq import (
    run as run_build_symbol_reference_candidates_from_unresolved_stooq,
)
from stock_quant_data.jobs.build_symbol_reference_from_nasdaq_latest import run as run_build_symbol_reference_from_nasdaq_latest
from stock_quant_data.jobs.build_symbol_reference_history_from_nasdaq_snapshots import (
    run as run_build_symbol_reference_history_from_nasdaq_snapshots,
)
from stock_quant_data.jobs.enrich_symbol_reference_from_manual_overrides import (
    run as run_enrich_symbol_reference_from_manual_overrides,
)
from stock_quant_data.jobs.init_price_raw_tables import run as run_init_price_raw_tables
from stock_quant_data.jobs.load_nasdaq_symbol_directory_raw_from_downloader import (
    run as run_load_nasdaq_symbol_directory_raw_from_downloader,
)
from stock_quant_data.jobs.load_price_source_daily_raw_stooq_from_disk import (
    run as run_load_price_source_daily_raw_stooq_from_disk,
)
from stock_quant_data.jobs.load_sec_submissions_identity_from_downloader import (
    run as run_load_sec_submissions_identity_from_downloader,
)
from stock_quant_data.jobs.load_sec_submissions_identity_targeted import (
    run as run_load_sec_submissions_identity_targeted,
)
from stock_quant_data.jobs.publish_release import run as run_publish_release
from stock_quant_data.jobs.validate_release import run as run_validate_release


def main() -> int:
    """
    Dispatch CLI commands to the correct job runner.

    The CLI is intentionally explicit instead of dynamic so command wiring
    stays readable and easy to audit.
    """
    if len(sys.argv) < 2:
        print("Usage: sq <command>")
        return 1

    command = sys.argv[1]

    if command == "init-price-raw-tables":
        run_init_price_raw_tables()
        return 0

    if command == "load-price-source-daily-raw-stooq-from-disk":
        run_load_price_source_daily_raw_stooq_from_disk()
        return 0

    if command == "build-stooq-symbol-normalization-map":
        run_build_stooq_symbol_normalization_map()
        return 0

    if command == "build-symbol-manual-override-map":
        run_build_symbol_manual_override_map()
        return 0

    if command == "enrich-symbol-reference-from-manual-overrides":
        run_enrich_symbol_reference_from_manual_overrides()
        return 0

    if command == "build-price-normalized-from-raw":
        run_build_price_normalized_from_raw()
        return 0

    if command == "build-price-history-from-raw":
        run_build_price_history_from_raw()
        return 0

    if command == "load-nasdaq-symbol-directory-raw-from-downloader":
        run_load_nasdaq_symbol_directory_raw_from_downloader()
        return 0

    if command == "build-symbol-reference-from-nasdaq-latest":
        run_build_symbol_reference_from_nasdaq_latest()
        return 0

    if command == "build-symbol-reference-history-from-nasdaq-snapshots":
        run_build_symbol_reference_history_from_nasdaq_snapshots()
        return 0

    if command == "load-sec-submissions-identity-from-downloader":
        run_load_sec_submissions_identity_from_downloader()
        return 0

    if command == "load-sec-submissions-identity-targeted":
        run_load_sec_submissions_identity_targeted()
        return 0

    if command == "build-symbol-reference-candidates-from-unresolved-stooq":
        run_build_symbol_reference_candidates_from_unresolved_stooq()
        return 0

    if command == "build-high-priority-unresolved-symbol-probe":
        run_build_high_priority_unresolved_symbol_probe()
        return 0

    if command == "validate-release":
        run_validate_release()
        return 0

    if command == "publish-release":
        run_publish_release()
        return 0

    print(f"Unknown command: {command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
