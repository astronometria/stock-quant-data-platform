"""
Minimal CLI entrypoint.

We keep the CLI intentionally thin.
At this stage it only prints guidance for available v1 commands.

Later, this module can dispatch to:
- init-db
- validate-release
- publish-release
- api
"""

from __future__ import annotations

import sys
from textwrap import dedent


def main() -> None:
    """
    Minimal command line entrypoint.

    We keep this tiny for the initial scaffold.
    """
    message = dedent(
        """
        stock-quant-data-platform CLI

        Initial scaffold ready.

        Planned commands:
          sq init-db
          sq validate-release
          sq publish-release
          sq api
        """
    ).strip()

    print(message)
    sys.exit(0)


if __name__ == "__main__":
    main()
