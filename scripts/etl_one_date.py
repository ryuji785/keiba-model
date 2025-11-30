"""
ETL for all races on a given date (YYYYMMDD).
Flow: calendar -> day links -> race list -> cnames -> per-race ETL.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

from etl_common import logger
from fetch_jra_calendar import fetch_calendar_html, extract_day_links
from fetch_jra_race_list import fetch_race_list, extract_cnames
from fetch_jra_race_html import fetch_race_html
from etl_one_race_v4 import run_etl_for_one_race


def run_etl_for_date(date_yyyymmdd: str, db_path: str, overwrite: bool = False) -> None:
    calendar_html = fetch_calendar_html(date_yyyymmdd)
    day_links = extract_day_links(calendar_html, date_yyyymmdd)
    if not day_links:
        logger.info("No race day links found for %s", date_yyyymmdd)
        return

    for day_link in day_links:
        race_list_html = fetch_race_list(day_link, date_yyyymmdd)
        cnames = extract_cnames(race_list_html)
        if not cnames:
            logger.info("No races found for link=%s", day_link)
            continue
        for cname in cnames:
            try:
                html_path = fetch_race_html(cname, date_yyyymmdd, overwrite=overwrite)
                run_etl_for_one_race(html_path, db_path=db_path)
            except Exception:
                logger.exception("Failed ETL for cname=%s date=%s", cname, date_yyyymmdd)
                continue


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL for all races on a given date (YYYYMMDD).")
    parser.add_argument("date", help="YYYYMMDD")
    parser.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing HTML")
    args = parser.parse_args()

    run_etl_for_date(args.date, db_path=args.db, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
