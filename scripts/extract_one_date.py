"""
Extract-only driver for a given date (YYYYMMDD).
Flow: calendar -> kaisaibi -> race list -> race result HTML fetch for all races.
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

from etl_common import logger  # noqa: E402
from scripts.crawl_jra_calendar import fetch_calendar_and_save  # noqa: E402
from scripts.crawl_jra_race_list import crawl_race_list  # noqa: E402
from scripts.fetch_jra_html import fetch_race_html  # noqa: E402


def extract_one_date(date_yyyymmdd: str, overwrite_html: bool = False) -> None:
    year = int(date_yyyymmdd[:4])
    month = int(date_yyyymmdd[4:6])

    msg = f"[extract_one_date] === START date={date_yyyymmdd} (year={year}, month={month}) ==="
    print(msg)
    logger.info(msg)

    # ---- カレンダー取得 ----
    print(f"[extract_one_date] fetching calendar for {year}-{month:02d} ...")
    cnames_map = fetch_calendar_and_save(year, month)

    # cnames_map の概要
    try:
        n_keys = len(cnames_map)
    except TypeError:
        n_keys = -1
    print(f"[extract_one_date] calendar cnames_map size={n_keys}")
    logger.info("calendar cnames_map size=%s", n_keys)

    # 指定日の開催日CNAMEを取得
    kaisaibi_cname = cnames_map.get(date_yyyymmdd)
    if not kaisaibi_cname:
        msg = (
            f"[extract_one_date] No kaisaibi CNAME found for date={date_yyyymmdd} "
            f"(たぶん JRA の開催日ではない日)"
        )
        print(msg)
        logger.warning(msg)
        print("[extract_one_date] === END (no races) ===")
        return

    print(f"[extract_one_date] kaisaibi CNAME for {date_yyyymmdd} = {kaisaibi_cname}")
    logger.info("Found kaisaibi CNAME: %s", kaisaibi_cname)

    # ---- レース一覧取得 ----
    print(f"[extract_one_date] fetching race list for date={date_yyyymmdd} ...")
    race_cnames: List[str] = crawl_race_list(kaisaibi_cname, date_yyyymmdd)

    if not race_cnames:
        msg = f"[extract_one_date] No race CNAMEs found for date={date_yyyymmdd}"
        print(msg)
        logger.warning(msg)
        print("[extract_one_date] === END (no race cnames) ===")
        return

    total = len(race_cnames)
    print(f"[extract_one_date] Found {total} race CNAMEs for {date_yyyymmdd}")
    logger.info("Found %d race CNAMEs for %s", total, date_yyyymmdd)

    skip = 0
    success = 0
    fail = 0

    # ---- 各レース HTML 取得 ----
    for idx, cname in enumerate(race_cnames, start=1):
        url = f"https://www.jra.go.jp/JRADB/accessS.html?CNAME={cname}"
        print(f"[extract_one_date] [{idx}/{total}] Fetching race URL: {url}")
        logger.info("Fetching race URL: %s", url)

        try:
            html_path = fetch_race_html(cname, overwrite=overwrite_html)

            if html_path is None:
                print(f"[extract_one_date]  -> SKIP (already exists)")
                logger.info("Skipped (already exists): %s", url)
                skip += 1
                continue

            print(f"[extract_one_date]  -> OK saved to {html_path}")
            logger.info("Saved HTML: %s", html_path)
            success += 1

        except Exception as e:
            fail += 1
            print(f"[extract_one_date]  -> FAIL ({e})")
            logger.exception(
                "Failed to fetch cname=%s date=%s URL=%s",
                cname,
                date_yyyymmdd,
                url,
            )
            continue

    # ---- 最終サマリー ----
    summary = (
        f"[extract_one_date] === SUMMARY date={date_yyyymmdd} "
        f"total={total} success={success} skip={skip} fail={fail} ==="
    )
    print(summary)
    logger.info(
        "Extract summary date=%s total=%d success=%d skip=%d fail=%d",
        date_yyyymmdd,
        total,
        success,
        skip,
        fail,
    )
    print("[extract_one_date] === END ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract all race HTMLs for a given date (YYYYMMDD)."
    )
    parser.add_argument("date", help="YYYYMMDD")
    parser.add_argument(
        "--overwrite-html",
        action="store_true",
        help="Overwrite existing race HTML files",
    )
    args = parser.parse_args()

    extract_one_date(args.date, overwrite_html=args.overwrite_html)


if __name__ == "__main__":
    main()
