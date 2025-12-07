"""
Fetch all race result HTMLs for an entire year by iterating months.

Usage:
    python scripts/fetch_year_htmls.py 2024 [--overwrite] [--start-month 1] [--end-month 12]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_month_htmls import fetch_month  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_year(
    year: int,
    start_month: int = 1,
    end_month: int = 12,
    overwrite: bool = False,
    fail_log: Path | None = None,
) -> None:
    for month in range(start_month, end_month + 1):
        logger.info("Start fetch for %04d-%02d", year, month)
        try:
            fetch_month(year, month, overwrite=overwrite, fail_log=fail_log)
        except Exception:  # noqa: BLE001
            logger.exception("Failed fetch for %04d-%02d", year, month)
        logger.info("Finished fetch for %04d-%02d", year, month)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch all race HTMLs for a given year.")
    ap.add_argument("year", type=int, help="Year (e.g., 2024)")
    ap.add_argument("--start-month", type=int, default=1, help="Start month (default: 1)")
    ap.add_argument("--end-month", type=int, default=12, help="End month (default: 12)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing race_<race_id>.html")
    ap.add_argument("--fail-log", help="File path to append failed CNAMEs (accumulates across months)")
    args = ap.parse_args()
    fail_path = Path(args.fail_log) if args.fail_log else None
    fetch_year(
        args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        overwrite=args.overwrite,
        fail_log=fail_path,
    )


if __name__ == "__main__":
    main()
