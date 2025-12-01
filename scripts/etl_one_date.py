"""
Date-driven ETL driver: given YYYYMMDD, crawl calendar -> race list -> fetch HTML -> ETL all races.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

# add project root to sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.etl_logging import logger  # noqa: E402
from scripts.crawl_jra_calendar import fetch_calendar_and_save  # noqa: E402
from scripts.crawl_jra_race_list import crawl_race_list  # noqa: E402
from scripts.fetch_jra_html import fetch_race_html  # noqa: E402
from scripts.etl_one_race_v4 import run_etl_for_one_race  # noqa: E402


def run_etl_for_date(date_yyyymmdd: str, db_path: str, overwrite_html: bool = False) -> None:
    year = int(date_yyyymmdd[:4])
    month = int(date_yyyymmdd[4:6])

    cnames_map = fetch_calendar_and_save(year, month)
    kaisaibi_cname = cnames_map.get(date_yyyymmdd)
    if not kaisaibi_cname:
        logger.info("No kaisaibi cname found for date=%s", date_yyyymmdd)
        return

    race_cnames: List[str] = crawl_race_list(kaisaibi_cname, date_yyyymmdd)
    if not race_cnames:
        logger.info("No race cnames found for date=%s", date_yyyymmdd)
        return

    total_count = len(race_cnames)
    skip_count = 0
    success_count = 0
    fail_count = 0

    for cname in race_cnames:
        try:
            html_path = fetch_race_html(cname, overwrite=overwrite_html)
            if html_path is None:
                skip_count += 1
                continue
            run_etl_for_one_race(html_path, db_path=db_path)
            success_count += 1
        except Exception:
            fail_count += 1
            logger.exception("Failed ETL for cname=%s date=%s", cname, date_yyyymmdd)
            continue

    logger.info(
        "ETL summary date=%s total=%d skip=%d success=%d fail=%d",
        date_yyyymmdd,
        total_count,
        skip_count,
        success_count,
        fail_count,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL for all races on a given date (YYYYMMDD).")
    parser.add_argument("date", help="YYYYMMDD")
    parser.add_argument("--db", default="data/keiba.db", help='SQLite DB path (default: "data/keiba.db")')
    parser.add_argument("--overwrite-html", action="store_true", help="Overwrite existing race HTML files")
    args = parser.parse_args()

    run_etl_for_date(args.date, db_path=args.db, overwrite_html=args.overwrite_html)


if __name__ == "__main__":
    main()
