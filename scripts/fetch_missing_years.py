"""
Fetch race result HTMLs for multiple years in one run.

Default targets: 2021, 2022, 2023, 2025  (2024 は取得済みを想定)

Usage:
    python scripts/fetch_missing_years.py
    python scripts/fetch_missing_years.py --overwrite
    python scripts/fetch_missing_years.py --years 2020,2021 --start-month 4 --end-month 12
    python scripts/fetch_missing_years.py --db data/keiba.db  # fetch後にETLも実行
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_year_htmls import fetch_year  # noqa: E402
from scripts.fetch_month_htmls import fetch_month  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_YEARS: List[int] = [2021, 2022, 2023, 2025]  # 2024 は取得済み前提


def parse_years(s: str | None) -> List[int]:
    if not s:
        return DEFAULT_YEARS
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _print_progress(done: int, total: int, label: str) -> None:
    pct = (done / total * 100) if total else 0
    sys.stderr.write(f"\r[{done}/{total}] {pct:5.1f}% - {label}   ")
    sys.stderr.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch race HTMLs for multiple years.")
    ap.add_argument("--years", help="Comma-separated years. Omit to use default: 2021,2022,2023,2025")
    ap.add_argument("--start-month", type=int, default=1, help="Start month (default: 1)")
    ap.add_argument("--end-month", type=int, default=12, help="End month (default: 12)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing race_<race_id>.html")
    ap.add_argument("--db", default="data/keiba.db", help="Run ETL after fetch against this DB path")
    ap.add_argument("--no-etl", action="store_true", help="Skip ETL after fetch")
    args = ap.parse_args()

    years = parse_years(args.years)
    months = list(range(args.start_month, args.end_month + 1))
    # total steps: per-month fetch + (optionally) ETL + prev link + feature views
    post_steps = 0 if args.no_etl else 3  # etl + prev + views
    total_steps = len(years) * len(months) + post_steps
    done = 0

    for y in years:
        fail_log = Path(f"fail_{y}.txt")
        for m in months:
            label = f"fetch {y}-{m:02d}"
            _print_progress(done, total_steps, label)
            try:
                fetch_month(y, m, overwrite=args.overwrite, fail_log=fail_log)
            except Exception:  # noqa: BLE001
                logger.exception("Fetch failed for %04d-%02d", y, m)
            done += 1
        logger.info("=== Fetch done: year=%s ===", y)

    # Run ETL after all fetches unless suppressed
    if not args.no_etl:
        steps = [
            ("ETL all HTMLs", [sys.executable, "scripts/etl_all_htmls.py", "--db", args.db]),
            ("prev_race_link", [sys.executable, "scripts/prev_race_link.py", "--db", args.db]),
            ("feature_views", [sys.executable, "scripts/create_feature_views.py", "--db", args.db]),
        ]
        for label, cmd in steps:
            _print_progress(done, total_steps, label)
            try:
                subprocess.check_call(cmd)
            except subprocess.CalledProcessError:
                logger.exception("%s failed", label)
                sys.exit(1)
            done += 1

    _print_progress(total_steps, total_steps, "all done")
    sys.stderr.write("\n")


if __name__ == "__main__":
    main()
