"""
Date-driven ETL driver: given YYYYMMDD, traverse past-links to fetch HTML and ETL all races.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# add project root to sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl_common import logger  # noqa: E402
from scripts.jra_past_links import get_sde_cnames_for_date  # noqa: E402
from scripts.fetch_jra_html import fetch_race_html, cname_to_race_id  # noqa: E402
from scripts.etl_one_race_v4 import run_etl_for_one_race  # noqa: E402


def run_etl_for_date(date_yyyymmdd: str, db_path: str, overwrite_html: bool = False) -> None:
    sde_cnames = get_sde_cnames_for_date(date_yyyymmdd)
    if not sde_cnames:
        logger.warning("No races found for date=%s (probably no JRA meeting)", date_yyyymmdd)
        return

    total = len(sde_cnames)
    success = 0
    skip = 0
    fail = 0

    for idx, cname in enumerate(sde_cnames, start=1):
        try:
            race_id = cname_to_race_id(cname)
            html_path = fetch_race_html(race_id, cname=cname, overwrite=overwrite_html)
            if html_path is None:
                skip += 1
                continue
            run_etl_for_one_race(race_id, db_path=db_path)
            success += 1
        except Exception:
            fail += 1
            logger.exception("Failed ETL for cname=%s date=%s (%d/%d)", cname, date_yyyymmdd, idx, total)
            continue

    logger.info(
        "ETL summary date=%s total=%d success=%d skip=%d fail=%d",
        date_yyyymmdd,
        total,
        success,
        skip,
        fail,
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
